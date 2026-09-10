import { push } from "./push";
import {
    isReenrollmentRequiredError,
    isUpgradeRequiredError,
    reenrollmentRequired,
    upgradeRequired,
} from "./transport-errors";
import { PerfTrace } from "./perf-trace";
import { HashWorkerFileDriftError } from "./desktop-hash-workers";
import { BulkObjectKind } from "./bulk-codec";
import { configureHashTuning, getHashTuning, hashTuningForRuntime } from "./hash-runtime";
import { reserveTransientWorkset, transientMemorySnapshot, type TransientWorkScope } from "./transient-memory";
import { mkdtemp, open, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
    drainTreeReachabilityRetirement,
    hasPendingTreeReachabilityRetirement,
    TreeReachabilityRetirementError,
} from "./tree-candidate-job";
import { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { CandidateOpenRootAdmissionDeniedError } from "./candidate-open-root-admission";
import { MobileRangeReadError, type MobileRangeReader } from "./mobile-ranged-source";
import { MemorySegmentedIO } from "./segmented-store-test-io";
import { MobilePreparedTransferPlan, PREPARED_STORE_OPERATION_BYTES,
    PREPARED_TRANSFER_LIMITS } from "./transfer-plan";
import { SyncMemoryArbiter } from "./sync-memory-arbiter";
import { PreparedMobileManifestError } from "./prepared-mobile-manifest";
import { ResourceVisibilityGate } from "./resource-governor";

(globalThis as any).window ??= globalThis;

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const keepHash = "a".repeat(64);
const goneHash = "b".repeat(64);

function fixture() {
    const entries = new Map<string, { hash: string; mtime: number; size: number }>([
        ["keep.md", { hash: keepHash, mtime: 1, size: 1 }],
        ["gone.md", { hash: goneHash, mtime: 2, size: 2 }],
    ]);
    let saved = 0;
    let allPathsCalls = 0;
    const syncBase = {
        allPaths: () => {
            allPathsCalls++;
            return [...entries.keys()];
        },
        entryCount: () => entries.size,
        getEntry: (path: string) => entries.get(path) ?? null,
        getTreeMtime: (path: string) => entries.get(path)?.mtime ?? null,
        removeEntry: (path: string) => { entries.delete(path); },
        setEntry: (path: string, hash: string, mtime: number, size: number) => {
            entries.set(path, { hash, mtime, size });
        },
        setLastSyncTimestamp: () => {},
        save: async () => { saved++; },
    } as any;

    let treeEntries = new Set(entries.keys());
    let candidateEntries: Set<string> | null = null;
    let deleteCalls = 0;
    let rebuildCalls = 0;
    let beginCalls = 0;
    let commitCalls = 0;
    let abortCalls = 0;
    const tree = {
        tree_version: () => 1,
        root_hash_hex: () => `root:${[...treeEntries].sort().join(",")}`,
        root_bytes: () => new Uint8Array([1]),
        total_files: () => treeEntries.size,
        begin_candidate: () => {
            beginCalls++;
            if (candidateEntries) throw new Error("candidate already active");
            candidateEntries = new Set(treeEntries);
        },
        has_candidate: () => candidateEntries !== null,
        candidate_root_hash_hex: () => candidateEntries
            ? `root:${[...candidateEntries].sort().join(",")}`
            : undefined,
        candidate_root_bytes: () => candidateEntries ? new Uint8Array([1]) : undefined,
        candidate_total_files: () => candidateEntries?.size ?? 0,
        candidate_delete_batch: (json: string) => {
            if (!candidateEntries) throw new Error("no candidate");
            for (const path of JSON.parse(json) as string[]) candidateEntries.delete(path);
        },
        candidate_update_batch: (json: string) => {
            if (!candidateEntries) throw new Error("no candidate");
            for (const row of JSON.parse(json) as Array<{ path: string }>) {
                candidateEntries.add(row.path);
            }
        },
        commit_candidate: () => {
            if (!candidateEntries) throw new Error("no candidate");
            commitCalls++;
            treeEntries = candidateEntries;
            candidateEntries = null;
            return { before: 2, reachable: treeEntries.size, removed: 1, after: treeEntries.size };
        },
        abort_candidate: () => {
            if (!candidateEntries) throw new Error("no candidate");
            abortCalls++;
            candidateEntries = null;
            return { before: 2, reachable: treeEntries.size, removed: 1, after: treeEntries.size };
        },
        // Legacy methods remain in the fixture so the assertions prove push
        // selected the transactional API rather than merely failing early.
        delete_batch: (json: string) => {
            deleteCalls++;
            for (const path of JSON.parse(json) as string[]) treeEntries.delete(path);
        },
        update_batch: () => {},
        build_from_entries: (json: string) => {
            rebuildCalls++;
            treeEntries = new Set((JSON.parse(json) as Array<{ path: string }>).map((row) => row.path));
        },
    } as any;
    const wasm = {
        wasm_should_chunk: () => false,
        wasm_tree_chunk_hashes: () => [],
        wasm_tree_committed_chunk_hashes: () => [],
        wasm_tree_candidate_chunk_hashes: () => [],
        wasm_tree_new_candidate_chunk_hashes: () => [],
        wasm_tree_get_chunk: () => null,
        wasm_tree_chunk_byte_length: () => 1,
    } as any;
    const io = {} as any;

    return {
        entries,
        syncBase,
        tree,
        wasm,
        io,
        treePaths: () => [...treeEntries].sort(),
        deleteCalls: () => deleteCalls,
        rebuildCalls: () => rebuildCalls,
        beginCalls: () => beginCalls,
        commitCalls: () => commitCalls,
        abortCalls: () => abortCalls,
        allPathsCalls: () => allPathsCalls,
        saves: () => saved,
    };
}

async function terminalPreflightDoesNotMutate(): Promise<void> {
    const f = fixture();
    const terminal = reenrollmentRequired("server upgraded — re-enroll device");
    const api = {
        ensureTransportReady: async () => { throw terminal; },
    } as any;

    let caught: unknown;
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "deleted", path: "gone.md" },
        ], "base");
    } catch (error) {
        caught = error;
    }

    check(isReenrollmentRequiredError(caught), "terminal preflight error lost its classification");
    check(
        isReenrollmentRequiredError(new Error("missing key — re-enroll the device")),
        "plain legacy re-enroll error was not classified",
    );
    check(
        !isReenrollmentRequiredError(new Error("temporary network failure")),
        "transient error was classified as terminal",
    );
    const upgrade = upgradeRequired(
        "upgrade required: update/reload the Obsetync plugin; enrollment remains valid",
    );
    check(isUpgradeRequiredError(upgrade), "HTTP 426 upgrade lost its classification");
    check(
        !isReenrollmentRequiredError(upgrade),
        "HTTP 426 upgrade was incorrectly classified as re-enrollment",
    );
    check(f.deleteCalls() === 0, "terminal preflight mutated the tree");
    check(f.beginCalls() === 0, "terminal preflight opened a candidate");
    check(f.entries.has("gone.md"), "terminal preflight mutated sync-base");
}

async function failedRootRestoresCandidate(): Promise<void> {
    const f = fixture();
    let baseWasIntactAtRequest = false;
    const api = {
        ensureTransportReady: async () => {},
        putRoot: async () => {
            baseWasIntactAtRequest = f.entries.has("gone.md");
            throw new Error("network down");
        },
    } as any;

    let failed = false;
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "deleted", path: "gone.md" },
        ], "base");
    } catch {
        failed = true;
    }

    check(failed, "failed root request unexpectedly succeeded");
    check(baseWasIntactAtRequest, "sync-base changed before root acceptance");
    check(f.entries.has("gone.md"), "failed root request changed sync-base");
    check(f.treePaths().join(",") === "gone.md,keep.md", "failed root request left tree mutated");
    check(f.abortCalls() === 1, "failed candidate was not aborted exactly once");
    check(f.commitCalls() === 0, "failed candidate was committed");
    check(f.rebuildCalls() === 0, "failed push rebuilt the complete tree");
    check(f.allPathsCalls() === 0, "failed incremental push enumerated the full sync-base");
    check(f.saves() === 0, "failed root request saved sync-base");
}

async function acceptedRootCommitsMetadata(): Promise<void> {
    const f = fixture();
    const trace = new PerfTrace({ monitorEventLoop: false });
    const operation = trace.begin("push");
    let baseWasIntactAtRequest = false;
    const api = {
        ensureTransportReady: async () => {},
        putRoot: async () => {
            baseWasIntactAtRequest = f.entries.has("gone.md");
            return { root_hash: "accepted", conflicts: [] };
        },
    } as any;

    await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
        { action: "deleted", path: "gone.md" },
    ], "base", undefined, operation);
    operation.finish();

    check(baseWasIntactAtRequest, "sync-base committed before root acceptance");
    check(!f.entries.has("gone.md"), "accepted root did not commit sync-base deletion");
    check(f.treePaths().join(",") === "keep.md", "accepted candidate tree was rolled back");
    check(f.beginCalls() === 1, "accepted push did not open exactly one candidate");
    check(f.commitCalls() === 1, "accepted push did not commit exactly one candidate");
    check(f.abortCalls() === 0, "accepted push aborted its candidate");
    check(f.rebuildCalls() === 0, "accepted root unexpectedly rebuilt the tree");
    check(f.allPathsCalls() === 0, "accepted incremental push enumerated the full sync-base");
    check(f.saves() === 1, "accepted root did not save sync-base exactly once");
    const record = trace.recent()[0];
    check(record.filesTotal === 1, "push trace lost total path count");
    check(record.bytesTotal === 0, "push trace counted deletion bytes");
    check(record.filesCompleted === 1, "push trace lost completed deletion");
    check(record.preparedFiles === 0, "deletion was mislabeled as volatile preparation");
    check(record.serverConfirmedFiles === 0, "deletion was mislabeled as an object confirmation");
    check(record.rootCommittedPaths === 1 && record.rootCommittedCuts === 1,
        "accepted deletion lost its authoritative root milestone");
    check(record.trackedDeletesCommitted === 1,
        "accepted tracked deletion lost its durable milestone");
    check(record.phases.tree_update !== undefined, "push trace missed tree update");
    check(record.phases.root_commit !== undefined, "push trace missed root commit");
}

/** Structural native port around the existing tree fixture. The orchestration,
 * scheduler, retained-owner registry and push transaction are production code;
 * this is not a packaged-WASM or real-network test. */
function deferredPushFixture() {
    const f = fixture();
    type Cursor = { token: number; kind: "begin" | "chunks"; retiring: boolean;
        traversed: boolean; completed: number; crossedFinalTurn: boolean };
    let cursor: Cursor | null = null, nextToken = 1, revision = 0;
    let failChunkCleanup = true, failAbort = false;
    const events: string[] = [], abortedRevisions: number[] = [];
    const cleanupFailure = new Error("fixture chunk retirement unavailable");
    const abortFailure = new Error("fixture candidate abort unavailable");
    const idle = () => { if (cursor) throw new Error("WASM tree has an active job"); };
    const owned = (token: number) => {
        check(cursor?.token === token, "deferred push used another native token");
        return cursor!;
    };
    const begin = (kind: Cursor["kind"]) => {
        idle();
        check(f.tree.has_candidate() === (kind === "chunks"), "native candidate admission fence was bypassed");
        cursor = { token: nextToken++, kind, retiring: false, traversed: false,
            completed: 0, crossedFinalTurn: false };
        events.push(`begin:${cursor.token}:${kind}`);
        return cursor.token;
    };
    const oldBegin = f.tree.begin_candidate, oldAbort = f.tree.abort_candidate;
    f.tree.begin_candidate = () => { idle(); oldBegin(); revision++; };
    f.tree.abort_candidate = () => {
        idle();
        if (failAbort) throw abortFailure;
        abortedRevisions.push(revision); events.push(`abort:${revision}`);
        const result = oldAbort(); revision++; return result;
    };
    for (const name of ["candidate_delete_batch", "candidate_update_batch", "commit_candidate"]) {
        const original = f.tree[name];
        f.tree[name] = (...args: unknown[]) => {
            idle(); const result = original(...args); revision++; return result;
        };
    }
    f.tree.candidate_revision = () => revision;
    f.tree.begin_candidate_job = () => begin("begin");
    f.tree.begin_candidate_chunks_job = () => begin("chunks");
    f.tree.step_reachability_job_deferred = (token: number, maxUnits: number) => {
        const owner = owned(token);
        check(maxUnits === 1 && !owner.retiring && !owner.traversed, "unexpected reachability step");
        owner.traversed = true;
        return { done: true, units: 1, completed: 1, remaining: 0,
            reachable: owner.kind === "begin" ? 1 : 0 };
    };
    f.tree.finish_candidate_job_deferred = (token: number) => {
        const owner = owned(token);
        check(owner.kind === "begin" && owner.traversed && !owner.retiring, "wrong candidate finish");
        oldBegin(); revision++; owner.retiring = true; return 1;
    };
    f.tree.finish_candidate_chunks_job_deferred = (token: number) => {
        const owner = owned(token);
        check(owner.kind === "chunks" && owner.traversed && !owner.retiring, "wrong chunk finish");
        owner.retiring = true; return { all: [], fresh: [] };
    };
    f.tree.cancel_reachability_job_deferred = (token: number) => { owned(token).retiring = true; };
    f.tree.step_reachability_retirement = (token: number, expectedCompleted: number, maxUnits: number) => {
        const owner = owned(token);
        check(owner.retiring && owner.completed === expectedCompleted && maxUnits === 256,
            "retirement did not resume its exact native work cut");
        if (owner.kind === "chunks" && owner.completed === 256 && failChunkCleanup) throw cleanupFailure;
        const units = Math.min(maxUnits, 257 - owner.completed);
        owner.completed += units;
        const done = owner.completed === 257;
        if (done) setImmediate(() => { owner.crossedFinalTurn = true; });
        return { done, units, completed: owner.completed };
    };
    f.tree.finish_reachability_retirement = (token: number, expectedCompleted: number) => {
        const owner = owned(token);
        check(owner.retiring && expectedCompleted === 257 && owner.completed === 257,
            "retirement finalized an incomplete native cursor");
        check(owner.crossedFinalTurn, "push retirement did not cross its final real host task");
        events.push(`finalize:${token}`); cursor = null;
    };
    for (const name of ["step_tree_job", "finish_candidate_job", "finish_candidate_chunks_job",
        "cancel_tree_job", "step_tree_retirement"]) {
        f.tree[name] = () => { throw new Error(`unexpected legacy native method: ${name}`); };
    }
    return { ...f, events, abortedRevisions, cleanupFailure, abortFailure,
        revision: () => revision,
        nativeCursor: () => cursor,
        allowCleanup: () => { failChunkCleanup = false; },
        failAbort: (value: boolean) => { failAbort = value; } };
}

function pagedChunkPushFixture(all: string[], fresh: string[]) {
    const f = deferredPushFixture(); f.allowCleanup();
    let traversal = 0, sortCompleted = 0, pageOffset = 0, pageFinishes = 0, chunkReads = 0;
    const originalStep = f.tree.step_reachability_job_deferred;
    f.tree.step_reachability_job_deferred = (token: number, maxUnits: number) => {
        const owner = f.nativeCursor();
        if (owner?.kind !== "chunks") return originalStep(token, maxUnits);
        check(maxUnits === 1 && !owner.retiring && traversal < all.length,
            "paged push used an invalid chunk traversal step");
        traversal++;
        return { done: traversal === all.length, units: 1, completed: traversal,
            remaining: all.length - traversal, reachable: traversal };
    };
    const plan = () => ({ schema: 1, scope: "candidate-chunk-plan-sort-workspace",
        hashCount: all.length, hashSizeBytes: 32, sourceHashesRequestedBytes: all.length * 32,
        scratchHashesRequestedBytes: all.length * 32, peakAdmissionBytes: all.length * 64,
        reachableSetUnmeasured: true, pageOutputUnmeasured: true,
        sortStrategy: "stable-lsd-radix-v1", pageMaxHashes: 256 });
    f.tree.candidate_chunks_sort_memory_plan_v1_job = (token: number) => {
        check(f.nativeCursor()?.token === token && traversal === all.length && sortCompleted === 0,
            "paged push requested sort admission outside ready traversal");
        return plan();
    };
    f.tree.resume_candidate_chunks_sort_memory_v1_job = (token: number, source: number, scratch: number) => {
        const expected = plan();
        check(f.nativeCursor()?.token === token && source === expected.sourceHashesRequestedBytes &&
            scratch === expected.scratchHashesRequestedBytes,
        "paged push resumed the wrong sort admission");
    };
    const total = all.length * 66;
    const phaseAt = (completed: number) => completed === total ? "ready"
        : completed < all.length ? "collect"
        : completed < all.length * 2 ? "initialize-scratch"
        : (completed - all.length * 2) % (all.length * 2) < all.length
            ? "count-byte" : "scatter-byte";
    f.tree.step_candidate_chunks_sort_v1_job = (token: number, maxUnits: number) => {
        check(f.nativeCursor()?.token === token && maxUnits === 4096,
            "paged push used the wrong sort quantum");
        const units = Math.min(maxUnits, total - sortCompleted);
        sortCompleted += units;
        return { schema: 1, scope: "candidate-chunk-plan-sort", done: sortCompleted === total,
            units, completed: sortCompleted, remaining: total - sortCompleted,
            allCount: all.length, phase: phaseAt(sortCompleted) };
    };
    f.tree.candidate_chunks_plan_info_v1_job = (token: number) => {
        check(f.nativeCursor()?.token === token && sortCompleted === total,
            "paged push requested page info before sort completion");
        return { schema: 1, scope: "candidate-chunk-plan-pages", allCount: all.length, pageMaxHashes: 256 };
    };
    f.tree.read_candidate_chunks_page_v1_job = (token: number, offset: number, maxHashes: number) => {
        check(f.nativeCursor()?.token === token && offset === pageOffset && maxHashes === 256,
            "paged push read a non-sequential native page");
        const nextOffset = Math.min(all.length, offset + maxHashes);
        const pageAll = all.slice(offset, nextOffset), selected = new Set(pageAll);
        const pageFresh = fresh.filter(hash => selected.has(hash));
        pageOffset = nextOffset;
        return { schema: 1, scope: "candidate-chunk-plan-page", offset, nextOffset,
            done: nextOffset === all.length, all: pageAll, fresh: pageFresh };
    };
    f.tree.finish_candidate_chunks_plan_v1_job = (token: number) => {
        const owner = f.nativeCursor();
        check(owner?.token === token && pageOffset === all.length && !owner.retiring,
            "paged push finished before native page EOF");
        owner!.retiring = true; pageFinishes++;
    };
    f.wasm.wasm_tree_get_chunk = (_tree: unknown, hash: string) => {
        check(f.nativeCursor() === null, "paged push exported a chunk before plan retirement");
        check(all.includes(hash), "paged push exported a hash outside the candidate plan");
        chunkReads++; return new Uint8Array([1]);
    };
    f.wasm.wasm_tree_chunk_byte_length = () => 1;
    return { ...f, pageFinishes: () => pageFinishes, chunkReads: () => chunkReads };
}

/** The native methods below are a structural port. push(), candidate-open
 * orchestration, scheduler, admission adapter and ledger are the real code. */
function admittedCandidateOpenPushFixture(capacityBytes: number) {
    const f = deferredPushFixture();
    f.allowCleanup();
    const admission = new RootTreeResidentAdmission({ capacityBytes });
    const seed = admission.reserve(f.tree, { peakBytes: 40, residentBytes: 40 });
    check(seed !== null, "candidate-open fixture could not seed its resident owner");
    admission.ready(seed!);
    admission.releaseRetired(admission.publish(seed!));
    const calls = { plan: 0, resume: 0, finish: 0, file: 0, content: 0, index: 0, root: 0 };
    let prepared = false;
    let firstRetirementResolve!: () => void;
    const firstRetirement = new Promise<void>(resolve => { firstRetirementResolve = resolve; });
    f.tree.tree_version = () => 2;
    f.tree.committed_revision = () => 0;
    f.tree.candidate_open_memory_plan_v1_job = (token: number) => {
        const cursor = f.nativeCursor();
        check(cursor?.token === token && cursor.kind === "begin" && cursor.traversed &&
            !cursor.retiring && !prepared, "push read a candidate-open plan outside ready input");
        calls.plan++;
        return { schema: 1, scope: "v2-candidate-open-root", residentChunkCount: 1,
            rootStringCount: 4, rootIdentityRequestedBytes: 12, rootEndpointRequestedBytes: 48,
            rootStringRequestedBytes: 60, baselineKeySnapshotRequestedBytes: 0,
            peakAdmissionBytes: 60, baselineStrategy: "insertion-generation-v1" };
    };
    f.tree.resume_candidate_open_memory_v1_job = (token: number, identity: number,
        endpoint: number, total: number) => {
        check(f.nativeCursor()?.token === token && !prepared && identity === 12 &&
            endpoint === 48 && total === 60, "push did not resume the exact candidate root witnesses");
        const memory = admission.snapshot();
        check(memory.privateBytes === 60 && memory.residentBytes === 40 &&
            memory.ledger.usedBytes === 100 && memory.ledger.activeLeases === 2,
        "native clone began before the complete disjoint root reservation");
        calls.resume++; prepared = true;
    };
    const finish = f.tree.finish_candidate_job_deferred;
    f.tree.finish_candidate_job_deferred = (token: number) => {
        check(prepared, "push finish bypassed admitted root preparation");
        const result = finish(token);
        prepared = false; calls.finish++;
        check(admission.snapshot().privateBytes === 60,
            "candidate root reservation disappeared before native finish returned");
        return result;
    };
    const cancel = f.tree.cancel_reachability_job_deferred;
    f.tree.cancel_reachability_job_deferred = (token: number) => {
        cancel(token); prepared = false;
    };
    const retire = f.tree.step_reachability_retirement;
    f.tree.step_reachability_retirement = (token: number, completed: number, budget: number) => {
        const result = retire(token, completed, budget);
        if (completed === 0) firstRetirementResolve();
        return result;
    };
    const unexpected = (field: "file" | "content" | "index" | "root") => {
        calls[field]++; throw new Error(`unexpected candidate-open fixture ${field} work`);
    };
    f.io.stat = async () => unexpected("file");
    f.io.readFile = async () => unexpected("file");
    const api = { ensureTransportReady: async () => {},
        checkContent: async () => unexpected("content"),
        checkChunks: async () => unexpected("index"),
        putObjects: async () => unexpected("index"),
        putRoot: async () => unexpected("root") } as any;
    const run = (signal?: AbortSignal) => push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
        { action: "created", path: "huge.bin", size: 512 * 1024 * 1024, mtime: 8 },
        { action: "deleted", path: "gone.md" },
    ], "base", undefined, undefined, undefined, undefined, signal, undefined, undefined,
    undefined, admission);
    return { ...f, admission, calls, firstRetirement, run };
}

async function candidateOpenAdmissionIsWiredBeforeNativeAndFileWork(): Promise<void> {
    const f = admittedCandidateOpenPushFixture(99);
    const originalEntries = JSON.stringify([...f.entries]);
    let caught: unknown;
    try { await f.run(); } catch (error) { caught = error; }
    check(caught instanceof CandidateOpenRootAdmissionDeniedError && caught.requestedBytes === 60 &&
        caught.capacityBytes === 99 && caught.usedBytes === 40, "push lost exact candidate-root denial");
    check(f.calls.plan === 1 && f.calls.resume === 0 && f.calls.finish === 0 && f.beginCalls() === 0,
        "denied candidate root nevertheless allocated or published");
    check(f.calls.file + f.calls.content + f.calls.index + f.calls.root === 0,
        "candidate-root denial crossed into file/index/root work");
    check(JSON.stringify([...f.entries]) === originalEntries && f.saves() === 0 &&
        f.treePaths().join(",") === "gone.md,keep.md" && f.commitCalls() === 0 && f.abortCalls() === 0,
        "candidate-root denial changed base or committed graph");
    const memory = f.admission.snapshot();
    check(memory.ledger.usedBytes === 40 && memory.privateAttempts === 0 && memory.retiringOwners === 0 &&
        !f.tree.has_candidate() && f.nativeCursor() === null && !hasPendingTreeReachabilityRetirement(f.tree),
        "candidate-root refusal leaked its traversal or reservation");
    f.admission.releaseResidentAfterFree(f.tree);
    check(f.admission.snapshot().ledger.usedBytes === 0, "denied fixture leaked its final resident lease");
}

async function candidateOpenAdmissionSurvivesRetirementAndTerminalAbort(): Promise<void> {
    for (const stopDuringRetirement of [false, true]) {
        const f = admittedCandidateOpenPushFixture(100);
        const controller = new AbortController(), stop = new Error("stop during candidate-open retirement");
        const originalEntries = JSON.stringify([...f.entries]);
        let settled = false;
        const operation = f.run(controller.signal).then(result => ({ result }), error => ({ error }))
            .finally(() => { settled = true; });
        await f.firstRetirement;
        const held = f.admission.snapshot();
        check(!settled && f.nativeCursor()?.retiring === true &&
            hasPendingTreeReachabilityRetirement(f.tree), "push escaped pending native retirement");
        check(f.tree.has_candidate() && f.calls.resume === 1 && f.calls.finish === 1 &&
            held.privateBytes === 0 && held.residentBytes === 100 && held.retiringBytes === 0 &&
            held.ledger.usedBytes === 100 && held.ledger.activeLeases === 1,
            "published candidate clone was not attached and charged across the real cleanup host turn");
        check(f.calls.file + f.calls.content + f.calls.index + f.calls.root === 0 && f.saves() === 0,
            "pending candidate retirement allowed file/root/base work");
        if (stopDuringRetirement) controller.abort(stop);
        const outcome = await operation;
        if (stopDuringRetirement) {
            check("error" in outcome && outcome.error === stop,
                "post-publication cancellation lost its original error");
        } else {
            check("result" in outcome && outcome.result.published === false &&
                outcome.result.deferred?.map(row => row.reason).join(",") === "source-too-large,dependent-delete",
                "admitted candidate did not hand off into the actual all-deferred push abort");
        }
        check(f.abortCalls() === 1 && f.abortedRevisions.join(",") === "1" && f.revision() === 2 &&
            f.commitCalls() === 0 && !f.tree.has_candidate(), "push did not abort exactly its opened candidate");
        check(f.nativeCursor() === null && !hasPendingTreeReachabilityRetirement(f.tree),
            "terminal candidate abort lost its native retirement ownership");
        check(JSON.stringify([...f.entries]) === originalEntries && f.saves() === 0 &&
            f.treePaths().join(",") === "gone.md,keep.md" &&
            f.calls.file + f.calls.content + f.calls.index + f.calls.root === 0,
            "unpublished candidate changed durable base/root or dispatched file work");
        const memory = f.admission.snapshot();
        check(memory.privateAttempts === 0 && memory.retiringOwners === 0 && memory.residentBytes === 40 &&
            memory.ledger.usedBytes === 40 && memory.ledger.activeLeases === 1,
            "terminal native abort leaked the candidate-root cohort or released the old resident");
        f.admission.releaseResidentAfterFree(f.tree);
        check(f.admission.snapshot().ledger.usedBytes === 0 && f.admission.snapshot().ledger.activeLeases === 0,
            "completed candidate-open fixture leaked ledger ownership");
    }
}

async function failedChunkRetirementRetainsExactAbortUntilPushRetry(): Promise<void> {
    for (const replaceAfterAbortFailure of [false, true]) {
        const f = deferredPushFixture();
        let roots = 0;
        const api = {
            ensureTransportReady: async () => {},
            putRoot: async () => {
                roots++; f.events.push("put-root");
                check(f.entries.has("gone.md"), "retry changed sync-base before root acceptance");
                return { root_hash: "accepted", conflicts: [] };
            },
        } as any;
        const runPush = () => push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "deleted", path: "gone.md" },
        ], "base");
        let caught: unknown;
        const originalError = console.error;
        console.error = () => {};
        try { await runPush(); }
        catch (error) { caught = error; }
        finally { console.error = originalError; }
        check(caught instanceof TreeReachabilityRetirementError && caught.cleanupCause === f.cleanupFailure,
            "push lost its original chunk retirement error");
        check(roots === 0 && f.saves() === 0 && f.commitCalls() === 0,
            "failed chunk cleanup crossed root/base commit");
        check(f.entries.has("gone.md") && f.treePaths().join(",") === "gone.md,keep.md",
            "failed chunk cleanup changed committed state");
        check(f.beginCalls() === 1 && f.abortCalls() === 0 && f.tree.has_candidate(),
            "failed cleanup forgot its exact still-open candidate");
        check(hasPendingTreeReachabilityRetirement(f.tree), "failed push dropped the retained native owner");
        check(f.nativeCursor()?.token === 2 && f.nativeCursor()?.kind === "chunks" &&
            f.nativeCursor()?.completed === 256, "failed push lost its partially drained chunk cursor");
        const oldRevision = f.revision();
        f.allowCleanup();

        if (replaceAfterAbortFailure) {
            // A fully finalized cursor can still own an abort that threw.
            // A subsequent external candidate is not that abort's generation.
            f.failAbort(true);
            caught = undefined;
            try { await drainTreeReachabilityRetirement(f.tree); }
            catch (error) { caught = error; }
            check(caught instanceof TreeReachabilityRetirementError && caught.cleanupCause === f.abortFailure,
                "failed candidate abort became false retirement completion");
            check(f.nativeCursor() === null && hasPendingTreeReachabilityRetirement(f.tree),
                "abort failure did not retain ownership after native finalization");
            f.failAbort(false);
            f.tree.abort_candidate(); f.tree.begin_candidate();
            const newerRevision = f.revision(), aborts = f.abortCalls();
            await drainTreeReachabilityRetirement(f.tree);
            check(f.tree.has_candidate() && f.revision() === newerRevision && f.abortCalls() === aborts,
                "retained old cleanup rolled back a newer candidate");
            check(!hasPendingTreeReachabilityRetirement(f.tree), "settled stale abort retained a phantom owner");
            f.tree.abort_candidate(); // Explicitly dispose the fixture-owned newer candidate.
        }

        const result = await runPush();
        check(result.published && roots === 1 && f.saves() === 1 && f.commitCalls() === 1,
            "later actual push did not publish/save exactly once");
        check(!f.tree.has_candidate() && !hasPendingTreeReachabilityRetirement(f.tree) && f.nativeCursor() === null,
            "successful retry left a candidate or native cleanup owner");
        check(!f.entries.has("gone.md") && f.treePaths().join(",") === "keep.md",
            "successful retry did not adopt the accepted deletion");
        check(f.events.indexOf("finalize:2") < f.events.indexOf(`abort:${oldRevision}`) &&
            f.events.indexOf(`abort:${oldRevision}`) < f.events.indexOf("begin:3:begin") &&
            f.events.indexOf("begin:3:begin") < f.events.indexOf("put-root"),
            "retry did not finalize/abort the original owner before its next candidate and root");
        if (!replaceAfterAbortFailure) {
            check(f.abortCalls() === 1 && f.abortedRevisions[0] === oldRevision && f.beginCalls() === 2,
                "same-tree retry did not retire exactly the old candidate generation");
        }
    }
}

async function postMutationGuardAbortsTheJustMutatedCandidateRevision(): Promise<void> {
    const f = deferredPushFixture(); f.allowCleanup();
    const controller = new AbortController(), failure = new Error("post-mutation source ownership changed");
    const mutate = f.tree.candidate_delete_batch;
    f.tree.candidate_delete_batch = (json: string) => {
        mutate(json);
        controller.abort(failure);
    };
    let caught: unknown, roots = 0;
    const api = { ensureTransportReady: async () => {}, putRoot: async () => {
        roots++; return { root_hash: "unexpected", conflicts: [] };
    } } as any;
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "deleted", path: "gone.md" },
        ], "base", undefined, undefined, undefined, undefined, controller.signal);
    } catch (error) { caught = error; }
    check(caught === failure, "post-mutation guard did not preserve its primary failure");
    check(roots === 0 && f.saves() === 0 && f.commitCalls() === 0,
        "post-mutation guard crossed root/base publication");
    check(!f.tree.has_candidate() && f.abortCalls() === 1,
        "push skipped abort of the candidate revision mutated before its final guard");
    check(f.abortedRevisions[0] === 2,
        "push retained the pre-mutation candidate revision witness");
    check(f.treePaths().join(",") === "gone.md,keep.md" && f.entries.has("gone.md"),
        "failed post-mutation guard changed committed tree/base state");
}

async function indeterminatePostMutationRevisionNeverBlindlyAbortsANewerCandidate(): Promise<void> {
    const f = deferredPushFixture(); f.allowCleanup();
    const revision = f.tree.candidate_revision;
    let replaceOnRead = false;
    f.tree.candidate_revision = () => {
        if (replaceOnRead) {
            replaceOnRead = false;
            f.tree.abort_candidate();
            f.tree.begin_candidate();
            throw new Error("candidate revision witness unavailable after replacement");
        }
        return revision();
    };
    const mutate = f.tree.candidate_delete_batch;
    f.tree.candidate_delete_batch = (json: string) => { mutate(json); replaceOnRead = true; };
    let caught: unknown, roots = 0;
    const api = { ensureTransportReady: async () => {}, putRoot: async () => {
        roots++; return { root_hash: "unexpected", conflicts: [] };
    } } as any;
    const originalWarn = console.warn; console.warn = () => {};
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "deleted", path: "gone.md" },
        ], "base");
    } catch (error) { caught = error; }
    finally { console.warn = originalWarn; }
    check(caught instanceof Error && caught.message.includes("revision witness unavailable"),
        "indeterminate candidate revision lost its primary failure");
    check(roots === 0 && f.saves() === 0 && f.commitCalls() === 0,
        "indeterminate candidate revision crossed root/base publication");
    check(f.tree.has_candidate() && f.abortCalls() === 1 && f.beginCalls() === 2,
        "push blindly aborted the candidate generation installed by the failing witness");
    check(f.treePaths().join(",") === "gone.md,keep.md" && f.entries.has("gone.md"),
        "indeterminate witness changed committed tree/base state");
    f.tree.abort_candidate(); // Fixture owner disposes the preserved newer candidate.
}

async function throwingCleanupPresenceNeverReplacesThePushFailure(): Promise<void> {
    const f = fixture(), controller = new AbortController();
    const primary = new Error("push stopped after candidate mutation");
    const mutate = f.tree.candidate_delete_batch, hasCandidate = f.tree.has_candidate;
    f.tree.candidate_delete_batch = (json: string) => {
        mutate(json); controller.abort(primary);
        f.tree.has_candidate = () => { throw new Error("candidate presence unavailable during cleanup"); };
    };
    let caught: unknown;
    const originalError = console.error; console.error = () => {};
    try {
        await push({ ensureTransportReady: async () => {} } as any,
            f.io, f.syncBase, f.wasm, f.tree, "vault", [
                { action: "deleted", path: "gone.md" },
            ], "base", undefined, undefined, undefined, undefined, controller.signal);
    } catch (error) { caught = error; }
    finally { console.error = originalError; }
    check(caught === primary, "throwing candidate presence replaced the primary push failure");
    check(f.abortCalls() === 0 && f.commitCalls() === 0 && f.saves() === 0,
        "uncertain cleanup falsely claimed candidate/root/base completion");
    f.tree.has_candidate = hasCandidate;
    check(f.tree.has_candidate(), "throwing cleanup getter lost the still-owned candidate");
    f.tree.abort_candidate();
}

async function stoppedPushDoesNotStartRootPublication(): Promise<void> {
    const cases = [
        "preflight", "read", "content-check", "content-upload",
        "index-check", "index-upload", "before-root",
    ] as const;
    for (const boundary of cases) {
        const f = fixture();
        const controller = new AbortController();
        const replacementHash = "9".repeat(64);
        const indexHash = "8".repeat(64);
        let rootRequests = 0;
        let hashCalls = 0;
        f.io.getAbsolutePath = () => null;
        f.io.stat = async () => ({ size: 3, mtime: 3 });
        f.io.readFile = async () => {
            controller.abort();
            return new Uint8Array([1, 2, 3]);
        };
        f.wasm.wasm_hash_batch = () => { hashCalls++; return [replacementHash]; };
        if (boundary.startsWith("index-")) {
            f.wasm.wasm_tree_candidate_chunk_hashes = () => [indexHash];
            f.wasm.wasm_tree_new_candidate_chunk_hashes = () => [indexHash];
            f.wasm.wasm_tree_get_chunk = () => new Uint8Array([1]);
        }
        const api = {
            ensureTransportReady: async () => {
                if (boundary === "preflight") controller.abort();
            },
            checkContent: async () => {
                if (boundary === "content-check") controller.abort();
                return boundary === "content-upload" ? [replacementHash] : [];
            },
            checkChunks: async () => {
                if (boundary === "index-check") controller.abort();
                return boundary === "index-upload" ? [indexHash] : [];
            },
            putObjects: async () => { controller.abort(); },
            putRoot: async () => {
                rootRequests++;
                return { root_hash: "accepted", conflicts: [] };
            },
        } as any;
        const trace = new PerfTrace({ monitorEventLoop: false });
        const operation = trace.begin("push");
        let caught: unknown;
        try {
            await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
                action: "modified",
                path: "keep.md",
                hash: boundary === "read" ? undefined : replacementHash,
                data: boundary === "content-upload" ? new Uint8Array([1, 2, 3]) : undefined,
                mtime: 3,
                size: 3,
            }], "base", (progress) => {
                if (boundary === "before-root" && progress === "↑ pushing root...") {
                    controller.abort();
                }
            }, operation, undefined, undefined, controller.signal);
        } catch (error) {
            caught = error;
        } finally {
            operation.finish("cancelled");
        }
        const record = trace.recent()[0];
        check(record.rootCommittedPaths === 0 && record.rootCommittedCuts === 0 &&
            record.trackedDeletesCommitted === 0,
        `${boundary}: pre-publication cancellation emitted a root milestone`);
        check((caught as Error)?.name === "AbortError", `${boundary}: cancellation was lost`);
        check(rootRequests === 0, `${boundary}: stopped work started root publication`);
        check(f.commitCalls() === 0, `${boundary}: stopped work committed its candidate`);
        check(f.abortCalls() === (boundary === "preflight" ? 0 : 1), `${boundary}: wrong candidate cleanup`);
        check(f.entries.get("keep.md")?.hash === keepHash, `${boundary}: cancellation changed sync-base`);
        check(f.saves() === 0, `${boundary}: cancellation saved uncommitted metadata`);
        if (boundary === "read") check(hashCalls === 0, "cancelled read started expensive hashing");
    }
}

async function rootAcceptedDuringStopStillCommitsLocally(): Promise<void> {
    const f = fixture();
    const controller = new AbortController();
    const trace = new PerfTrace({ monitorEventLoop: false });
    const operation = trace.begin("push");
    const api = {
        ensureTransportReady: async () => {},
        putRoot: async () => {
            controller.abort();
            return { root_hash: "accepted", conflicts: [] };
        },
    } as any;
    const result = await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
        { action: "deleted", path: "gone.md" },
    ], "base", undefined, operation, undefined, undefined, controller.signal);
    operation.finish("cancelled");
    check(result.newRootHash === "accepted", "accepted in-flight root was discarded on stop");
    check(f.commitCalls() === 1, "accepted in-flight candidate was not committed locally");
    check(f.abortCalls() === 0, "accepted in-flight candidate was aborted on stop");
    check(!f.entries.has("gone.md"), "accepted in-flight deletion was not adopted");
    check(f.saves() === 1, "accepted in-flight root did not checkpoint sync-base");
    const record = trace.recent()[0];
    check(record.rootCommittedPaths === 1 && record.rootCommittedCuts === 1 &&
        record.trackedDeletesCommitted === 1,
    "accepted in-flight root was hidden by local cancellation diagnostics");
}

async function failedRootDoesNotCommitUpsert(): Promise<void> {
    const f = fixture();
    const replacementHash = "c".repeat(64);
    let putRootCalled = false;
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async () => [],
        putRoot: async () => {
            putRootCalled = true;
            throw new Error("root rejected");
        },
    } as any;

    let caught: unknown;
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            {
                action: "modified",
                path: "keep.md",
                hash: replacementHash,
                mtime: 3,
                size: 3,
            },
        ], "base");
    } catch (error) {
        caught = error;
    }

    check(putRootCalled, `upsert failed before putRoot: ${String(caught)}`);
    check(caught instanceof Error && caught.message === "root rejected", "wrong upsert failure surfaced");
    check(f.entries.get("keep.md")?.hash === keepHash, "failed upsert changed sync-base");
    check(f.saves() === 0, "failed upsert saved sync-base");
}

async function everyPostCandidateFailureAbortsWithoutFullSnapshot(): Promise<void> {
    const replacementHash = "d".repeat(64);
    const cases = ["content-check", "content-upload", "index-check", "index-upload"] as const;
    for (const failure of cases) {
        const f = fixture();
        const api: any = {
            ensureTransportReady: async () => {},
            checkContent: async () => [],
            checkChunks: async () => [],
            putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
        };
        if (failure === "content-check") {
            api.checkContent = async () => { throw new Error(failure); };
        } else if (failure === "content-upload") {
            api.checkContent = async () => [replacementHash];
            api.putObjects = async () => { throw new Error(failure); };
        } else {
            const indexHash = "8".repeat(64);
            f.wasm.wasm_tree_candidate_chunk_hashes = () => [indexHash];
            f.wasm.wasm_tree_new_candidate_chunk_hashes = () => [indexHash];
            f.wasm.wasm_tree_get_chunk = () => new Uint8Array([1]);
            if (failure === "index-check") {
                api.checkChunks = async () => { throw new Error(failure); };
            } else {
                api.checkChunks = async () => [indexHash];
                api.putObjects = async () => { throw new Error(failure); };
            }
        }

        let caught: unknown;
        try {
            await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
                action: "modified",
                path: "keep.md",
                hash: replacementHash,
                data: new Uint8Array([4, 5, 6]),
                mtime: 4,
                size: 3,
            }], "base");
        } catch (error) {
            caught = error;
        }

        check(caught instanceof Error && caught.message === failure, `${failure}: wrong error`);
        check(f.abortCalls() === 1, `${failure}: candidate was not aborted`);
        check(f.commitCalls() === 0, `${failure}: candidate was committed`);
        check(f.treePaths().join(",") === "gone.md,keep.md", `${failure}: root changed`);
        check(f.entries.get("keep.md")?.hash === keepHash, `${failure}: sync-base changed`);
        check(f.allPathsCalls() === 0, `${failure}: full sync-base snapshot was built`);
        check(f.saves() === 0, `${failure}: sync-base was saved`);
    }
}

async function incrementalPushChecksOnlyNewCandidateChunks(): Promise<void> {
    const f = fixture();
    const trace = new PerfTrace({ monitorEventLoop: false });
    const operation = trace.begin("push");
    const replacementHash = "e".repeat(64);
    const oldIndexHash = "1".repeat(64), newIndexHash = "2".repeat(64);
    f.wasm.wasm_tree_committed_chunk_hashes = () => [oldIndexHash];
    f.wasm.wasm_tree_candidate_chunk_hashes = () => [oldIndexHash, newIndexHash];
    f.wasm.wasm_tree_new_candidate_chunk_hashes = () => [newIndexHash];
    let checked: string[] = [];
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async () => [],
        checkChunks: async (hashes: string[]) => {
            checked = hashes;
            return [];
        },
        putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
    } as any;

    await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
        action: "modified",
        path: "keep.md",
        hash: replacementHash,
        mtime: 5,
        size: 3,
    }], "base", undefined, operation);
    operation.finish();

    check(checked.join(",") === newIndexHash, "incremental push checked historical index chunks");
    check(f.commitCalls() === 1, "incremental push did not commit its candidate");
    check(f.abortCalls() === 0, "incremental push aborted after success");
    const record = trace.recent()[0];
    check(record.preparedFiles === 1, "authoritative upsert preparation was not counted");
    check(record.serverConfirmedFiles === 1, "server-confirmed upsert was not counted");
    check(record.rootCommittedPaths === 1 && record.rootCommittedCuts === 1,
        "accepted upsert root milestone was not counted");
    check(record.trackedDeletesCommitted === 0, "upsert invented a committed deletion");
}

async function parentlessPushChecksEveryCandidateChunk(): Promise<void> {
    const f = fixture();
    let prepared = 0;
    f.tree.build_from_entries = () => { throw new Error("engine push used compatibility bootstrap"); };
    const oldIndexHash = "1".repeat(64), newIndexHash = "2".repeat(64);
    f.wasm.wasm_tree_candidate_chunk_hashes = () => [oldIndexHash, newIndexHash];
    f.wasm.wasm_tree_new_candidate_chunk_hashes = () => {
        throw new Error("bootstrap must not enumerate only fresh chunks");
    };
    let checked: string[] = [];
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async () => [],
        checkChunks: async (hashes: string[]) => { checked = hashes; return []; },
        putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
    } as any;

    await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
        { action: "deleted", path: "gone.md" },
    ], null, undefined, undefined, undefined, undefined, undefined, undefined, undefined, {
        prepare: async () => { prepared++; },
    });

    check(checked.join(",") === `${oldIndexHash},${newIndexHash}`,
        "parentless push did not check the complete candidate graph");
    check(prepared === 1 && f.allPathsCalls() === 0 && f.rebuildCalls() === 0,
        "push rebuilt the committed graph instead of requiring its admitted owner");
    check(f.commitCalls() === 1 && f.abortCalls() === 0,
        "parentless push did not commit its candidate");
}

async function pagedCandidateChunksDriveActualPushSafely(): Promise<void> {
    const all = Array.from({ length: 257 }, (_, index) => (index + 1).toString(16).padStart(64, "0"));
    const fresh = all.filter((_, index) => index % 2 === 0);

    const incremental = pagedChunkPushFixture(all, fresh);
    const incrementalChecks: string[][] = []; let incrementalUploads = 0;
    await push({
        ensureTransportReady: async () => {}, checkContent: async () => [],
        checkChunks: async (hashes: string[]) => { incrementalChecks.push(hashes); return hashes; },
        putObjects: async () => { incrementalUploads++; },
        putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
    } as any, incremental.io, incremental.syncBase, incremental.wasm, incremental.tree, "vault", [
        { action: "deleted", path: "gone.md" },
    ], "base");
    check(incrementalChecks.length === 2 && incrementalChecks[0].length === 128 &&
        incrementalChecks[1].length === 1 && incrementalChecks.flat().join(",") === fresh.join(","),
    "incremental paged push did not check only page-local fresh hashes");
    check(incremental.pageFinishes() === 1 && incremental.chunkReads() === fresh.length &&
        incrementalUploads > 0 && incremental.commitCalls() === 1 && incremental.abortCalls() === 0,
    "incremental paged push did not retire, upload and commit in order");
    check(transientMemorySnapshot().usedBytes === 0,
        "incremental paged push retained its radix workspace admission");

    const bootstrap = pagedChunkPushFixture(all, fresh);
    const bootstrapChecks: string[][] = []; let prepared = 0;
    await push({
        ensureTransportReady: async () => {}, checkContent: async () => [],
        checkChunks: async (hashes: string[]) => { bootstrapChecks.push(hashes); return []; },
        putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
    } as any, bootstrap.io, bootstrap.syncBase, bootstrap.wasm, bootstrap.tree, "vault", [
        { action: "deleted", path: "gone.md" },
    ], null, undefined, undefined, undefined, undefined, undefined, undefined, undefined, {
        prepare: async () => { prepared++; },
    });
    check(bootstrapChecks.length === 2 && bootstrapChecks[0].length === 256 &&
        bootstrapChecks[1].length === 1 && bootstrapChecks.flat().join(",") === all.join(","),
    "bootstrap paged push did not check every page-local candidate hash");
    check(prepared === 1 && bootstrap.pageFinishes() === 1 && bootstrap.commitCalls() === 1,
        "bootstrap paged push did not use the admitted parentless transaction");

    const stopped = pagedChunkPushFixture(all, fresh), stop = new AbortController();
    let stoppedChecks = 0, stoppedUploads = 0, stoppedRoots = 0;
    await (async () => {
        let caught: unknown;
        try {
            await push({
                ensureTransportReady: async () => {}, checkContent: async () => [],
                checkChunks: async () => {
                    if (++stoppedChecks === 2) stop.abort(new Error("paged index check stopped"));
                    return [];
                },
                putObjects: async () => { stoppedUploads++; },
                putRoot: async () => { stoppedRoots++; return { root_hash: "unexpected", conflicts: [] }; },
            } as any, stopped.io, stopped.syncBase, stopped.wasm, stopped.tree, "vault", [
                { action: "deleted", path: "gone.md" },
            ], "base", undefined, undefined, undefined, undefined, stop.signal);
        } catch (error) { caught = error; }
        check(caught instanceof Error && caught.message === "paged index check stopped",
            "paged push cancellation lost its primary reason");
    })();
    check(stoppedChecks === 2 && stoppedUploads === 0 && stoppedRoots === 0 &&
        stopped.pageFinishes() === 0 && stopped.commitCalls() === 0 && stopped.abortCalls() === 1,
    "cancelled paged checks uploaded, published, or leaked their candidate");
    check(stopped.nativeCursor() === null && transientMemorySnapshot().usedBytes === 0,
        "cancelled paged checks retained native or admission ownership");

    for (const failure of ["outside-page", "cross-page-duplicate"] as const) {
        const rejected = pagedChunkPushFixture(all, all);
        let calls = 0, uploads = 0, roots = 0, first = "";
        let caught: unknown;
        try {
            await push({
                ensureTransportReady: async () => {}, checkContent: async () => [],
                checkChunks: async (hashes: string[]) => {
                    calls++;
                    if (calls === 1) {
                        first = hashes[0];
                        return failure === "outside-page" ? [all[256]] : [first];
                    }
                    return [first];
                },
                putObjects: async () => { uploads++; },
                putRoot: async () => { roots++; return { root_hash: "unexpected", conflicts: [] }; },
            } as any, rejected.io, rejected.syncBase, rejected.wasm, rejected.tree, "vault", [
                { action: "deleted", path: "gone.md" },
            ], "base");
        } catch (error) { caught = error; }
        check(caught instanceof TypeError, `${failure}: invalid needed page was accepted`);
        check(uploads === 0 && roots === 0 && rejected.pageFinishes() === 0 &&
            rejected.commitCalls() === 0 && rejected.abortCalls() === 1,
        `${failure}: invalid needed page crossed upload/root publication`);
        check(rejected.nativeCursor() === null && transientMemorySnapshot().usedBytes === 0,
            `${failure}: invalid needed page retained native or admission ownership`);
    }
}

async function rejectedCommittedBootstrapNeverFallsBackOrOpensCandidate(): Promise<void> {
    const f = fixture();
    const refusal = new Error("injected committed bootstrap refusal");
    f.tree.root_hash_hex = () => null;
    f.tree.build_from_entries = () => { throw new Error("bootstrap refusal used compatibility fallback"); };
    let contentChecks = 0, chunkChecks = 0, rootPuts = 0;
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async () => { contentChecks++; return []; },
        checkChunks: async () => { chunkChecks++; return []; },
        putRoot: async () => { rootPuts++; return { root_hash: "accepted", conflicts: [] }; },
    } as any;
    let caught: unknown;
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "deleted", path: "gone.md" },
        ], null, undefined, undefined, undefined, undefined, undefined, undefined, undefined, {
            prepare: async () => { throw refusal; },
        });
    } catch (error) { caught = error; }
    check(caught === refusal, "committed bootstrap refusal lost its identity");
    check(f.beginCalls() === 0 && f.commitCalls() === 0 && f.abortCalls() === 0,
        "committed bootstrap refusal opened a candidate");
    check(contentChecks === 0 && chunkChecks === 0 && rootPuts === 0,
        "committed bootstrap refusal reached content/index/root work");
    check(f.allPathsCalls() === 0 && f.rebuildCalls() === 0 && f.entries.has("gone.md"),
        "committed bootstrap refusal fell back or changed sync-base");
}

async function workerManifestAvoidsRendererFileBytes(): Promise<void> {
    const f = fixture();
    const fileHash = "f".repeat(64);
    const firstChunkHash = "1".repeat(64);
    const secondChunkHash = "2".repeat(64);
    const size = 2 * 1024 * 1024;
    const directory = await mkdtemp(join(tmpdir(), "obsetync-ranged-push-"));
    const absolutePath = join(directory, "large.bin");
    try {
        const handle = await open(absolutePath, "w");
        try {
            await handle.truncate(size);
            await handle.write(new Uint8Array([7, 8, 9]), 0, 3, 1024 * 1024);
        } finally {
            await handle.close();
        }
        const info = await stat(absolutePath);
        const fingerprint = {
            size: Number(info.size),
            mtime: Number(info.mtimeMs),
            ctime: Number(info.ctimeMs),
            device: Number(info.dev),
            inode: Number(info.ino),
        };
        f.wasm.wasm_should_chunk = (bytes: number) => bytes >= 1024 * 1024;
        let readCalls = 0, absolutePathCalls = 0;
        f.io.getAbsolutePath = () => { absolutePathCalls++; return absolutePath; };
        f.io.readFile = async () => {
            readCalls++;
            throw new Error("renderer read should not happen");
        };
        let workerInput: any;
        const workers = {
            run: async (input: any) => {
                workerInput = input;
                return {
                    type: "result",
                    job_id: "test",
                    mode: "manifest",
                    manifest: {
                        file_hash: fileHash,
                        total_size: size,
                        chunks: [
                            { hash: firstChunkHash, offset: 0, size: 1024 * 1024 },
                            { hash: secondChunkHash, offset: 1024 * 1024, size: 1024 * 1024 },
                        ],
                    },
                    size,
                    mtime: fingerprint.mtime,
                    fingerprint,
                    read_ms: 1,
                    hash_ms: 2,
                };
            },
        } as any;
        let manifestUploads = 0;
        let contentChunkUploads = 0;
        let uploadedRangeLength = -1;
        let uploadedRangeFirst = -1;
        let uploadedRangeThird = -1;
        const api = {
            ensureTransportReady: async () => {},
            checkContentChunks: async () => [secondChunkHash],
            putObjects: async (records: Array<{
                kind: BulkObjectKind;
                data: Uint8Array;
            }>) => {
                manifestUploads += records.filter((record) =>
                    record.kind === BulkObjectKind.Manifest).length;
                for (const record of records) {
                    if (record.kind === BulkObjectKind.ContentChunk) {
                        contentChunkUploads++;
                        uploadedRangeLength = record.data.byteLength;
                        uploadedRangeFirst = record.data[0];
                        uploadedRangeThird = record.data[2];
                    }
                }
            },
            putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
        } as any;

        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
            action: "created",
            path: "large.bin",
            mtime: fingerprint.mtime,
            size,
        }], "base", undefined, undefined, workers);

        check(workerInput.absolutePath === absolutePath, "worker did not receive absolute path");
        check(absolutePathCalls === 1, "worker path was resolved more than once across admission and dispatch");
        check(workerInput.mode === "manifest", "large file did not request a manifest job");
        check(!("data" in workerInput), "file bytes crossed the worker boundary");
        check(readCalls === 0, "desktop ranged upload called renderer readFile");
        check(contentChunkUploads === 1, "missing bitmap did not select one range");
        check(uploadedRangeLength === 1024 * 1024, "ranged read returned the wrong size");
        check(uploadedRangeFirst === 7 && uploadedRangeThird === 9, "ranged read used the wrong offset");
        check(manifestUploads === 1, "worker manifest was not uploaded");
        check(f.entries.get("large.bin")?.hash === fileHash, "worker hash was not committed");
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}

async function rendererRangeFallbackChunksWithoutWorkerPool(): Promise<void> {
    const previous = getHashTuning();
    configureHashTuning(hashTuningForRuntime("desktop"));
    const f = fixture();
    const mib = 1024 * 1024;
    const size = 2 * mib;
    const fileHash = "d".repeat(64);
    const firstChunkHash = "3".repeat(64);
    const secondChunkHash = "4".repeat(64);
    const directory = await mkdtemp(join(tmpdir(), "obsetync-renderer-range-"));
    const absolutePath = join(directory, "large.gif");
    try {
        const handle = await open(absolutePath, "w");
        try {
            await handle.truncate(size);
            await handle.write(new Uint8Array([11, 12, 13]), 0, 3, mib);
        } finally { await handle.close(); }
        const info = await stat(absolutePath);
        const mtime = Number(info.mtimeMs);
        let wholeReads = 0;
        let totalChunked = 0;
        let largestFeed = 0;
        f.io.stat = async () => ({ size, mtime });
        f.io.getAbsolutePath = () => absolutePath;
        f.io.readFile = async () => {
            wholeReads++;
            throw new Error("renderer fallback used a whole-file read");
        };
        f.wasm.wasm_should_chunk = (bytes: number) => bytes >= mib;
        f.wasm.WasmChunker = class {
            update(data: Uint8Array) {
                totalChunked += data.byteLength;
                largestFeed = Math.max(largestFeed, data.byteLength);
            }
            finish() {
                return { file_hash: fileHash, total_size: size, chunks: [
                    { hash: firstChunkHash, offset: 0, size: mib },
                    { hash: secondChunkHash, offset: mib, size: mib },
                ] };
            }
            free() {}
        };
        let chunkUploads = 0;
        let manifestUploads = 0;
        let uploadedFirstByte = -1;
        const api = {
            ensureTransportReady: async () => {},
            checkContentChunks: async () => [secondChunkHash],
            putObjects: async (records: Array<{ kind: BulkObjectKind; data: Uint8Array }>) => {
                for (const record of records) {
                    if (record.kind === BulkObjectKind.ContentChunk) {
                        chunkUploads++;
                        uploadedFirstByte = record.data[0];
                    } else if (record.kind === BulkObjectKind.Manifest) manifestUploads++;
                }
            },
            putRoot: async () => ({ root_hash: "accepted-renderer-range", conflicts: [] }),
        } as any;

        const outcome = await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
            action: "created", path: "large.gif", size, mtime,
        }], "base");

        check(outcome.published === true && outcome.deferred?.length !== 1,
            "renderer ranged fallback deferred a chunkable desktop file");
        check(wholeReads === 0 && totalChunked === size,
            "renderer ranged fallback did not stream the complete source");
        check(largestFeed > 0 && largestFeed <= getHashTuning().maxFeedBytes,
            "renderer ranged fallback exceeded its admitted feed ceiling");
        check(chunkUploads === 1 && uploadedFirstByte === 11 && manifestUploads === 1,
            "renderer ranged fallback ignored the missing bitmap or source offset");
        check(f.entries.get("large.gif")?.hash === fileHash && f.commitCalls() === 1,
            "renderer ranged fallback did not commit the manifest identity");
    } finally {
        configureHashTuning(previous);
        await rm(directory, { recursive: true, force: true });
    }
}

async function interruptedRangedUploadResumesFromServerBitmap(): Promise<void> {
    const f = fixture();
    const mib = 1024 * 1024;
    const size = 12 * mib;
    const fileHash = "3".repeat(64);
    const chunkHashes = ["4".repeat(64), "5".repeat(64), "6".repeat(64)];
    const directory = await mkdtemp(join(tmpdir(), "obsetync-ranged-resume-"));
    const absolutePath = join(directory, "resume.bin");
    try {
        const handle = await open(absolutePath, "w");
        try {
            await handle.truncate(size);
        } finally {
            await handle.close();
        }
        const info = await stat(absolutePath);
        const fingerprint = {
            size: Number(info.size),
            mtime: Number(info.mtimeMs),
            ctime: Number(info.ctimeMs),
            device: Number(info.dev),
            inode: Number(info.ino),
        };
        f.wasm.wasm_should_chunk = (bytes: number) => bytes >= mib;
        f.io.getAbsolutePath = () => absolutePath;
        let rendererReads = 0;
        f.io.readFile = async () => {
            rendererReads++;
            throw new Error("renderer read should not happen");
        };
        const manifest = {
            file_hash: fileHash,
            total_size: size,
            chunks: chunkHashes.map((hash, index) => ({
                hash,
                offset: index * 4 * mib,
                size: 4 * mib,
            })),
        };
        const workers = {
            run: async () => ({
                type: "result",
                job_id: "resume",
                mode: "manifest",
                manifest,
                size,
                mtime: fingerprint.mtime,
                fingerprint,
                read_ms: 1,
                hash_ms: 2,
            }),
        } as any;
        const change = {
            action: "created" as const,
            path: "resume.bin",
            mtime: fingerprint.mtime,
            size,
        };

        const stored = new Set<string>();
        const firstAttempted: string[] = [];
        let chunkPutCalls = 0;
        const firstApi = {
            ensureTransportReady: async () => {},
            checkContentChunks: async () => chunkHashes,
            putObjects: async (records: Array<{
                kind: BulkObjectKind;
                hash: string;
            }>) => {
                const chunks = records.filter((record) =>
                    record.kind === BulkObjectKind.ContentChunk);
                if (chunks.length === 0) return;
                chunkPutCalls++;
                firstAttempted.push(...chunks.map((record) => record.hash));
                if (chunkPutCalls === 2) throw new Error("injected ranged disconnect");
                for (const record of chunks) stored.add(record.hash);
            },
        } as any;
        let interrupted = false;
        try {
            await push(
                firstApi,
                f.io,
                f.syncBase,
                f.wasm,
                f.tree,
                "vault",
                [change],
                "base",
                undefined,
                undefined,
                workers,
            );
        } catch (error) {
            interrupted = (error as Error).message === "injected ranged disconnect";
        }
        check(interrupted, "ranged interruption was hidden");
        check(f.abortCalls() === 1 && f.commitCalls() === 0, "interruption did not abort candidate");
        check(stored.size === 1, "first ACKed memory-bounded range pack differs");
        check(
            firstAttempted.join(",") === chunkHashes.slice(0, 2).join(","),
            "first attempt did not stop on the failing range pack",
        );
        check(!f.entries.has("resume.bin"), "interrupted range upload committed metadata");

        const retryContent: string[] = [];
        let retryManifest = 0;
        const retryApi = {
            ensureTransportReady: async () => {},
            checkContentChunks: async () => chunkHashes.filter((hash) => !stored.has(hash)),
            putObjects: async (records: Array<{
                kind: BulkObjectKind;
                hash: string;
            }>) => {
                for (const record of records) {
                    if (record.kind === BulkObjectKind.ContentChunk) {
                        retryContent.push(record.hash);
                        stored.add(record.hash);
                    } else if (record.kind === BulkObjectKind.Manifest) {
                        retryManifest++;
                    }
                }
            },
            putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
        } as any;
        await push(
            retryApi,
            f.io,
            f.syncBase,
            f.wasm,
            f.tree,
            "vault",
            [change],
            "base",
            undefined,
            undefined,
            workers,
        );
        check(retryContent.join(",") === chunkHashes.slice(1).join(","), "retry re-uploaded an ACKed range");
        check(retryManifest === 1, "retry did not upload the dependent manifest once");
        check(stored.size === 3, "retry left content ranges missing");
        check(f.abortCalls() === 1 && f.commitCalls() === 1, "retry candidate outcome differs");
        check(f.entries.get("resume.bin")?.hash === fileHash, "retry did not commit metadata");
        check(rendererReads === 0, "ranged resume called renderer readFile");
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}

async function rangedDriftAfterTransferAbortsCandidate(): Promise<void> {
    const f = fixture();
    const mib = 1024 * 1024;
    const size = 2 * mib;
    const fileHash = "7".repeat(64);
    const chunkHash = "8".repeat(64);
    const directory = await mkdtemp(join(tmpdir(), "obsetync-ranged-drift-"));
    const absolutePath = join(directory, "drift.bin");
    try {
        const initial = await open(absolutePath, "w");
        try {
            await initial.truncate(size);
        } finally {
            await initial.close();
        }
        const info = await stat(absolutePath);
        const fingerprint = {
            size: Number(info.size),
            mtime: Number(info.mtimeMs),
            ctime: Number(info.ctimeMs),
            device: Number(info.dev),
            inode: Number(info.ino),
        };
        f.wasm.wasm_should_chunk = (bytes: number) => bytes >= mib;
        f.io.getAbsolutePath = () => absolutePath;
        const workers = {
            run: async () => ({
                type: "result",
                job_id: "drift",
                mode: "manifest",
                manifest: {
                    file_hash: fileHash,
                    total_size: size,
                    chunks: [{ hash: chunkHash, offset: 0, size }],
                },
                size,
                mtime: fingerprint.mtime,
                fingerprint,
                read_ms: 1,
                hash_ms: 2,
            }),
        } as any;
        let manifestUploads = 0;
        let putRootCalled = false;
        const api = {
            ensureTransportReady: async () => {},
            checkContentChunks: async () => [chunkHash],
            putObjects: async (records: Array<{ kind: BulkObjectKind }>) => {
                if (records.some((record) => record.kind === BulkObjectKind.ContentChunk)) {
                    const changed = await open(absolutePath, "r+");
                    try {
                        await changed.truncate(size - 1);
                    } finally {
                        await changed.close();
                    }
                }
                manifestUploads += records.filter((record) =>
                    record.kind === BulkObjectKind.Manifest).length;
            },
            putRoot: async () => {
                putRootCalled = true;
                return { root_hash: "accepted", conflicts: [] };
            },
        } as any;

        let drifted = false;
        try {
            await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
                action: "created",
                path: "drift.bin",
                mtime: fingerprint.mtime,
                size,
            }], "base", undefined, undefined, workers);
        } catch (error) {
            drifted = error instanceof HashWorkerFileDriftError;
        }
        check(drifted, "post-transfer file drift was hidden");
        check(manifestUploads === 1, "final drift check ran before transfer completion");
        check(!putRootCalled, "drifted ranged upload reached root commit");
        check(f.abortCalls() === 1 && f.commitCalls() === 0, "drift did not abort candidate");
        check(!f.entries.has("drift.bin"), "drifted range metadata reached sync-base");
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}

async function workerDriftAbortsCandidate(): Promise<void> {
    const f = fixture();
    f.io.getAbsolutePath = () => "/vault/drift.md";
    const workers = {
        run: async () => { throw new HashWorkerFileDriftError(); },
    } as any;
    const api = { ensureTransportReady: async () => {} } as any;
    let drifted = false;
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
            action: "modified",
            path: "drift.md",
            mtime: 12,
            size: 12,
        }], "base", undefined, undefined, workers);
    } catch (error) {
        drifted = error instanceof HashWorkerFileDriftError;
    }
    check(drifted, "worker stat drift was hidden");
    check(f.abortCalls() === 1, "worker stat drift did not abort candidate");
}

async function smallFilesReachTransportAsPacksNotPerFilePuts(): Promise<void> {
    const f = fixture();
    const changes = Array.from({ length: 600 }, (_, index) => ({
        action: "created" as const,
        path: `bulk/${index}.md`,
        hash: index.toString(16).padStart(64, "0"),
        data: new Uint8Array([index & 0xff]),
        mtime: 100 + index,
        size: 1,
    }));
    let checkCalls = 0;
    let packedCalls = 0;
    let packedRecords = 0;
    let legacyPuts = 0;
    let heavyBatchPermits = 0;
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async (hashes: string[]) => {
            checkCalls++;
            return hashes;
        },
        putObjects: async (records: unknown[]) => {
            packedCalls++;
            packedRecords += records.length;
        },
        putContent: async () => { legacyPuts++; },
        putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
    } as any;

    await push(
        api,
        f.io,
        f.syncBase,
        f.wasm,
        f.tree,
        "vault",
        changes,
        "base",
        undefined,
        undefined,
        undefined,
        async () => { heavyBatchPermits++; },
    );
    check(checkCalls <= 10, `600 files expanded to ${checkCalls} check batches`);
    check(packedCalls === checkCalls, "each stream batch did not become one packed upload call");
    check(packedRecords === 600, "packed push lost content records");
    check(legacyPuts === 0, "packed push issued a per-file content PUT");
    check(heavyBatchPermits >= packedCalls, "push bypassed a heavy-batch permit");
    check(f.commitCalls() === 1, "packed push did not commit its candidate");
}

async function pushUsesNewByteAndCountLimitsAfterEachBatchAck(): Promise<void> {
    const previous = getHashTuning();
    const f = fixture();
    const changes = Array.from({ length: 11 }, (_, index) => ({
        action: "created" as const,
        path: `adaptive/${index}.md`,
        hash: (100 + index).toString(16).padStart(64, "0"),
        data: new Uint8Array([index, index + 1, index + 2]),
        mtime: 500 + index,
        size: 3,
    }));
    const limits = [
        { maxBatchFiles: 4, maxBatchBytes: 12 },
        { maxBatchFiles: 2, maxBatchBytes: 30 }, // Count shrinks independently of bytes.
        { maxBatchFiles: 10, maxBatchBytes: 6 }, // Bytes shrink independently of count.
        { maxBatchFiles: 10, maxBatchBytes: 30 },
    ];
    const selectLimits = (index: number) => configureHashTuning({
        ...previous,
        ...limits[Math.min(index, limits.length - 1)],
        maxSingleBatchFileBytes: 30,
        maxBatchHoldMs: 60_000,
    });
    const uploaded: string[] = [];
    const batchLengths: number[] = [];
    const checkedLengths: number[] = [];
    let rootRequests = 0;
    let completedProgress = 0;
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async (hashes: string[]) => {
            checkedLengths.push(hashes.length);
            return hashes;
        },
        putObjects: async (records: Array<{ kind: BulkObjectKind; hash: string; data: Uint8Array }>) => {
            const batchIndex = batchLengths.length;
            const currentLimits = limits[Math.min(batchIndex, limits.length - 1)];
            const bytes = records.reduce((sum, record) => sum + record.data.length, 0);
            check(records.length <= currentLimits.maxBatchFiles, `adaptive batch ${batchIndex}: count limit ignored`);
            check(bytes <= currentLimits.maxBatchBytes, `adaptive batch ${batchIndex}: byte limit ignored`);
            for (const record of records) {
                const expected = changes[uploaded.length];
                check(record.kind === BulkObjectKind.Content, "adaptive small-file batch changed object kind");
                check(record.hash === expected.hash, "adaptive batch lost or reordered a file");
                check(record.data.join(",") === expected.data.join(","), "adaptive batch changed content bytes");
                uploaded.push(record.hash);
            }
            batchLengths.push(records.length);
            // Simulate a governor decision arriving with this transport ACK.
            // A whole-operation eager plan would keep using the first limit.
            selectLimits(batchIndex + 1);
        },
        putRoot: async () => {
            rootRequests++;
            check(uploaded.length === changes.length, "adaptive push published root before all content ACKs");
            return { root_hash: "accepted", conflicts: [] };
        },
    } as any;
    try {
        selectLimits(0);
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", changes, "base", (progress) => {
            if (/^↑ \d+\/11 /.test(progress)) completedProgress++;
        });
        check(batchLengths.join(",") === "4,2,2,3", "push reused stale byte/count limits after ACK");
        check(checkedLengths.join(",") === batchLengths.join(","),
            `content checks did not follow adaptive batches: ${checkedLengths.join(",")} vs ${batchLengths.join(",")}`);
        check(new Set(uploaded).size === changes.length, "adaptive batches uploaded a file more than once");
        check(completedProgress === changes.length, "adaptive batches lost or duplicated completed progress");
        check(rootRequests === 1 && f.commitCalls() === 1 && f.saves() === 1, "adaptive push did not commit exactly once");
        for (const change of changes) {
            const entry = f.entries.get(change.path);
            check(entry?.hash === change.hash && entry?.size === change.size && entry?.mtime === change.mtime,
                "adaptive batching changed committed metadata");
        }
    } finally {
        configureHashTuning(previous);
    }
}

async function pushReadGroupsUseLiveConcurrencyWithoutSkippingFiles(): Promise<void> {
    const previous = getHashTuning();
    const f = fixture();
    const changes = Array.from({ length: 10 }, (_, index) => ({
        action: "created" as const,
        path: `read-groups/${index}.md`,
        mtime: 700 + index,
        size: 1,
    }));
    const expectedHash = (index: number) => (index + 1).toString(16).padStart(64, "0");
    const readGroups: string[][] = [];
    const observedReads: string[] = [];
    let pendingReads: Array<{ path: string; resolve: (data: Uint8Array) => void }> = [];
    let activeReads = 0;
    let peakReads = 0;
    let hashCalls = 0;
    let rootRequests = 0;
    const selectConcurrency = (readConcurrency: number) => configureHashTuning({
        ...previous,
        readConcurrency,
        maxBatchFiles: 10,
        maxBatchBytes: 100,
        maxSingleBatchFileBytes: 100,
        maxBatchHoldMs: 60_000,
    });
    f.io.getAbsolutePath = () => null;
    f.io.stat = async (path: string) => {
        const change = changes.find(change => change.path === path);
        return change ? { size: change.size, mtime: change.mtime } : null;
    };
    f.io.readFile = (path: string) => new Promise<Uint8Array>((resolve) => {
        activeReads++;
        peakReads = Math.max(peakReads, activeReads);
        observedReads.push(path);
        pendingReads.push({ path, resolve });
        if (pendingReads.length !== 1) return;
        queueMicrotask(() => {
            const group = pendingReads;
            pendingReads = [];
            readGroups.push(group.map((read) => read.path));
            // Change the next group while the current group's reads complete.
            // Its cursor must advance by the actual group length, not the new limit.
            selectConcurrency([1, 4, 2][readGroups.length - 1] ?? 2);
            for (const read of group) {
                activeReads--;
                const index = changes.findIndex((change) => change.path === read.path);
                read.resolve(new Uint8Array([index + 1]));
            }
        });
    });
    f.wasm.wasm_hash_batch = (data: Uint8Array, offsets: Uint32Array, sizes: Uint32Array) => {
        hashCalls++;
        return [...offsets].map((offset, index) => {
            check(sizes[index] === 1, "live read groups changed the hash input size");
            return expectedHash(data[offset] - 1);
        });
    };
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async (hashes: string[]) => {
            check(hashes.join(",") === changes.map((_, index) => expectedHash(index)).join(","),
                "live read groups lost or reordered hashes");
            return []; // Already-present content must not trigger a second source read.
        },
        putRoot: async () => {
            rootRequests++;
            return { root_hash: "accepted", conflicts: [] };
        },
    } as any;
    try {
        selectConcurrency(3);
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", changes, "base");
        check(readGroups.map((group) => group.length).join(",") === "3,1,4,2",
            "push did not refresh read concurrency at group boundaries");
        check(observedReads.join(",") === changes.map((change) => change.path).join(","),
            "changing read concurrency skipped, duplicated or reordered source reads");
        check(activeReads === 0 && peakReads === 4, "read group concurrency or cleanup differs");
        check(hashCalls === 1, "read concurrency unexpectedly fragmented the byte-bounded hash call");
        check(rootRequests === 1 && f.commitCalls() === 1 && f.saves() === 1,
            "live read groups did not commit exactly once");
        changes.forEach((change, index) => {
            const entry = f.entries.get(change.path);
            check(entry?.hash === expectedHash(index) && entry?.size === 1 && entry?.mtime === change.mtime,
                "live read concurrency changed committed metadata");
        });
    } finally {
        configureHashTuning(previous);
    }
}

async function oversizedSourceDoesNotBlockIndependentNote(): Promise<void> {
    const f = fixture();
    const bytes = new Uint8Array([7, 8, 9]);
    const hash = "c".repeat(64);
    let reads = 0;
    let uploaded = 0;
    let scope: TransientWorkScope | undefined;
    f.io.getAbsolutePath = () => null;
    f.io.stat = async (path: string) => path === "new.md" ? { size: 3, mtime: 7 } : null;
    f.io.readFile = async (path: string) => {
        check(path === "new.md", "oversized source reached a whole-file read");
        check(transientMemorySnapshot().activeReservations > 0, "source allocated before admission");
        reads++;
        return bytes;
    };
    f.wasm.wasm_hash_batch = () => [hash];
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async (hashes: string[]) => hashes,
        putObjects: async (records: Array<{ data: Uint8Array }>, _perf: unknown, memory: TransientWorkScope) => {
            scope = memory;
            check(!memory.snapshot().parentReleased, "upload source lease was released before ACK");
            check(records.length === 1 && records[0].data === bytes, "small-file upload reread/copied its owned source");
            await Promise.resolve();
            check(!memory.snapshot().ownerClosed, "source owner closed while upload awaited ACK");
            uploaded++;
        },
        putRoot: async () => {
            check(scope?.snapshot().parentReleased, "settled content lease survived into root commit");
            check(f.entries.has("gone.md"), "dependent deletion committed before new path");
            return { root_hash: "accepted", conflicts: [] };
        },
    } as any;
    const result = await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
        { action: "created", path: "huge.bin", mtime: 5, size: 512 * 1024 * 1024 },
        { action: "created", path: "new.md", mtime: 7, size: 3 },
        { action: "deleted", path: "gone.md" },
    ], "base");
    check(reads === 1 && uploaded === 1, "independent note did not read/upload exactly once");
    check(result.published === true && f.entries.get("new.md")?.hash === hash, "independent note was not committed");
    check(result.deferred?.map(row => row.reason).join(",") === "source-too-large,dependent-delete",
        "oversized source and dependent deletion were not returned to WAL owner");
    check(!f.entries.has("huge.bin") && f.entries.has("gone.md"), "deferred paths mutated sync-base");
    check(f.treePaths().includes("gone.md") && !f.treePaths().includes("huge.bin"), "deferred paths mutated committed tree");
}

async function onlyOversizedWorkDoesNotPublishRoot(): Promise<void> {
    const f = fixture();
    const api = { ensureTransportReady: async () => {} } as any;
    const result = await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
        { action: "created", path: "huge.bin", size: 512 * 1024 * 1024, mtime: 8 },
        { action: "deleted", path: "gone.md" },
    ], "base");
    check(result.published === false && result.deferred?.length === 2, "all-deferred result claimed a root publication");
    check(f.abortCalls() === 1 && f.commitCalls() === 0 && f.saves() === 0, "all-deferred attempt advanced committed state");
    check(f.entries.has("gone.md"), "all-deferred attempt deleted the old path");
}

async function freshStatDriftStopsBeforeSourceAllocation(): Promise<void> {
    const f = fixture();
    let reads = 0;
    f.io.getAbsolutePath = () => null;
    f.io.stat = async () => ({ size: 4, mtime: 9 });
    f.io.readFile = async () => { reads++; return new Uint8Array(4); };
    let caught: unknown;
    try {
        await push({ ensureTransportReady: async () => {} } as any,
            f.io, f.syncBase, f.wasm, f.tree, "vault", [
                { action: "created", path: "growing.md", size: 3, mtime: 9 },
            ], "base");
    } catch (error) { caught = error; }
    check(caught instanceof HashWorkerFileDriftError, "admission stat drift lost retry classification");
    check(reads === 0 && f.abortCalls() === 1 && f.saves() === 0, "fresh stat drift allocated or committed source");
}

async function workerFailureUsesBoundedDesktopRangeFallback(): Promise<void> {
    const f = fixture();
    const size = 2 * 1024 * 1024;
    const fileHash = "5".repeat(64);
    const chunkHash = "6".repeat(64);
    const directory = await mkdtemp(join(tmpdir(), "obsetync-worker-range-fallback-"));
    const absolutePath = join(directory, "large.bin");
    try {
        const handle = await open(absolutePath, "w");
        try { await handle.truncate(size); }
        finally { await handle.close(); }
        const info = await stat(absolutePath);
        const mtime = Number(info.mtimeMs);
        let workerCalls = 0, wholeReads = 0, chunkedBytes = 0;
        f.wasm.wasm_should_chunk = () => true;
        f.wasm.WasmChunker = class {
            update(data: Uint8Array) { chunkedBytes += data.byteLength; }
            finish() { return { file_hash: fileHash, total_size: size,
                chunks: [{ hash: chunkHash, offset: 0, size }] }; }
            free() {}
        };
        f.io.stat = async () => ({ size, mtime });
        f.io.getAbsolutePath = () => absolutePath;
        f.io.readFile = async () => { wholeReads++; throw new Error("unexpected whole-file fallback"); };
        const workers = { run: async () => { workerCalls++; throw new Error("test worker unavailable"); } } as any;
        const api = {
            ensureTransportReady: async () => {},
            checkContentChunks: async () => [],
            putObjects: async () => {},
            putRoot: async () => ({ root_hash: "accepted-worker-fallback", conflicts: [] }),
        } as any;
        const result = await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "created", path: "large.bin", size, mtime },
        ], "base", undefined, undefined, workers);
        check(workerCalls === 1 && wholeReads === 0 && chunkedBytes === size,
            "failed worker did not hand off to the bounded ranged fallback");
        check(result.published === true && f.entries.get("large.bin")?.hash === fileHash,
            "bounded worker fallback did not commit the manifest");
    } finally { await rm(directory, { recursive: true, force: true }); }
}

async function failedReadKeepsNativeSiblingCharged(): Promise<void> {
    const f = fixture();
    const previous = getHashTuning();
    let rejectFirst!: (error: Error) => void;
    let finishSecond!: (bytes: Uint8Array) => void;
    let secondStarted!: () => void;
    const started = new Promise<void>(resolve => { secondStarted = resolve; });
    f.io.getAbsolutePath = () => null;
    f.io.stat = async () => ({ size: 3, mtime: 12 });
    f.io.readFile = (path: string) => path === "first.md"
        ? new Promise<Uint8Array>((_resolve, reject) => { rejectFirst = reject; })
        : new Promise<Uint8Array>(resolve => { finishSecond = resolve; secondStarted(); });
    try {
        configureHashTuning({ ...previous, readConcurrency: 2, maxBatchFiles: 2,
            maxBatchBytes: 1024, maxSingleBatchFileBytes: 1024, maxBatchHoldMs: 60_000 });
        const pending = push({ ensureTransportReady: async () => {} } as any,
            f.io, f.syncBase, f.wasm, f.tree, "vault", [
                { action: "created", path: "first.md", size: 3, mtime: 12 },
                { action: "created", path: "second.md", size: 3, mtime: 12 },
            ], "base").catch(error => error);
        await started;
        rejectFirst(new Error("injected source IO failure"));
        const error = await pending;
        check(error instanceof Error && error.message === "injected source IO failure", "read failure lost original error");
        check(f.abortCalls() === 1 && f.commitCalls() === 0, "failed parallel read published candidate");
        // An all-capacity waiter cannot overtake the still-live native read,
        // even though Promise.all and push's finally have already returned.
        let granted = false;
        const probe = reserveTransientWorkset(transientMemorySnapshot().capacityBytes)
            .then(lease => { granted = true; return lease; });
        await new Promise<void>(resolve => setImmediate(resolve));
        check(!granted, "failed push refunded the still-running native sibling");
        finishSecond(new Uint8Array(3));
        const lease = await probe;
        check(granted, "settled native sibling did not refund its admission");
        lease.release();
    } finally {
        finishSecond?.(new Uint8Array(3));
        configureHashTuning(previous);
    }
}

async function knownSmallReadGroupsFollowLiveLimitsAndPreserveUploadOrder(): Promise<void> {
    const previous = getHashTuning(), f = fixture();
    const changes = Array.from({ length: 10 }, (_, index) => ({ action: "created" as const,
        path: `lazy-${index}.md`, hash: (index + 16).toString(16).padStart(64, "0"), size: 1, mtime: 20 + index }));
    const groups: string[][] = [], reads: string[] = [];
    let pending: Array<{ path: string; resolve: (bytes: Uint8Array) => void }> = [];
    let active = 0, peak = 0, uploads = 0, roots = 0, absolutePathCalls = 0;
    const select = (readConcurrency: number) => configureHashTuning({ ...previous, readConcurrency,
        maxBatchFiles: 10, maxBatchBytes: 100, maxSingleBatchFileBytes: 100, maxBatchHoldMs: 60000 });
    f.io.getAbsolutePath = () => { absolutePathCalls++; return "/unused/native/path"; };
    f.io.stat = async (path: string) => {
        const change = changes.find(change => change.path === path)!;
        return { size: change.size, mtime: change.mtime };
    };
    f.io.readFile = (path: string) => new Promise<Uint8Array>(resolve => {
        check(transientMemorySnapshot().usedBytes > 0, "lazy read started before source budget admission");
        reads.push(path); active++; peak = Math.max(peak, active); pending.push({ path, resolve });
        if (pending.length !== 1) return;
        queueMicrotask(() => {
            const group = pending; pending = []; groups.push(group.map(item => item.path));
            select([1, 4, 2][groups.length - 1] ?? 2);
            // Reverse completion may not reorder records or select a duplicate.
            for (const item of [...group].reverse()) {
                active--; item.resolve(new Uint8Array([changes.findIndex(change => change.path === item.path) + 1]));
            }
        });
    });
    const api = { ensureTransportReady: async () => {}, checkContent: async (hashes: string[]) => hashes,
        putObjects: async (records: Array<{ kind: BulkObjectKind; hash: string; data: Uint8Array }>, _perf: unknown, memory: TransientWorkScope) => {
            uploads++; check(active === 0, "upload began before native source preparation joined");
            check(memory.snapshot().ownerBytes >= changes.length, "source buffers exceeded admitted owner bytes");
            check(records.length === changes.length, "lazy preparation split/omitted the original content pack");
            records.forEach((record, index) => {
                check(record.kind === BulkObjectKind.Content && record.hash === changes[index].hash && record.data[0] === index + 1,
                    "lazy preparation changed upload order, object kind or actual bytes");
            });
        }, putRoot: async () => { roots++; return { root_hash: "accepted", conflicts: [] }; } } as any;
    const unusedWorkers = { run: async () => { throw new Error("known-hash small source reached hash worker"); } } as any;
    try {
        select(3); await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", changes, "base",
            undefined, undefined, unusedWorkers);
        check(groups.map(group => group.length).join(",") === "3,1,4,2", "lazy reads did not use live bounded concurrency");
        check(reads.join(",") === changes.map(change => change.path).join(","), "lazy reads skipped/duplicated a path");
        check(peak === 4 && active === 0, "lazy read group exceeded limits or retained a native read");
        check(absolutePathCalls === 0, "known-hash renderer reads resolved unused native paths");
        check(uploads === 1 && roots === 1 && f.commitCalls() === 1 && f.saves() === 1, "lazy batched reads changed commit semantics");
        check(transientMemorySnapshot().usedBytes === 0, "successful lazy read pack retained its source admission");
    } finally { configureHashTuning(previous); }
}

async function identityVerifiedDesktopReadPreservesAdmissionAndPortableFallback(): Promise<void> {
    const hash = "d".repeat(64);
    const change = { action: "created" as const, path: "verified.md", hash, size: 3, mtime: 42 };
    const bytes = Uint8Array.from([4, 5, 6]);
    const api = (onUpload: (data: Uint8Array) => void) => ({
        ensureTransportReady: async () => {},
        checkContent: async () => [hash],
        putObjects: async (records: Array<{ data: Uint8Array }>) => {
            check(records.length === 1, "verified source changed upload record count");
            onUpload(records[0].data);
        },
        putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
    }) as any;

    {
        const f = fixture();
        let verifiedReads = 0, adapterStats = 0, adapterReads = 0, uploads = 0;
        f.io.readFileIdentityVerified = async (path: string, expected: { size: number; mtime: number }, signal?: AbortSignal) => {
            verifiedReads++;
            check(path === change.path && expected.size === change.size && expected.mtime === change.mtime,
                "verified reader did not receive the captured source identity");
            check(!signal?.aborted && transientMemorySnapshot().usedBytes >= change.size,
                "verified reader started outside source memory admission");
            return bytes;
        };
        f.io.stat = async () => { adapterStats++; throw new Error("verified source used adapter stat"); };
        f.io.readFile = async () => { adapterReads++; throw new Error("verified source used adapter whole read"); };
        await push(api(data => {
            uploads++;
            check(data === bytes, "verified reader bytes were copied or replaced before transport");
        }), f.io, f.syncBase, f.wasm, f.tree, "vault", [change], "base");
        check(verifiedReads === 1 && adapterStats === 0 && adapterReads === 0 && uploads === 1,
            "qualified desktop read did not replace one portable stat/read/stat sequence");
        check(transientMemorySnapshot().usedBytes === 0, "qualified desktop read retained its source admission");
    }

    {
        const f = fixture();
        let capabilityCalls = 0, adapterStats = 0, adapterReads = 0;
        f.io.readFileIdentityVerified = async () => { capabilityCalls++; return null; };
        f.io.stat = async () => { adapterStats++; return { size: change.size, mtime: change.mtime }; };
        f.io.readFile = async () => { adapterReads++; return bytes; };
        await push(api(data => check(data === bytes, "portable fallback changed source bytes")),
            f.io, f.syncBase, f.wasm, f.tree, "vault", [change], "base");
        check(capabilityCalls === 1 && adapterStats === 2 && adapterReads === 1,
            "unavailable desktop capability changed the portable stat/read/stat fallback");
    }

    {
        const f = fixture();
        const drift = new HashWorkerFileDriftError("injected identity drift");
        let adapterCalls = 0;
        f.io.readFileIdentityVerified = async () => { throw drift; };
        f.io.stat = async () => { adapterCalls++; return { size: change.size, mtime: change.mtime }; };
        f.io.readFile = async () => { adapterCalls++; return bytes; };
        const error = await push(api(() => { throw new Error("drifted bytes reached transport"); }),
            f.io, f.syncBase, f.wasm, f.tree, "vault", [change], "base").then(() => null, value => value);
        check(error === drift && adapterCalls === 0, "identity drift fell through to the portable source path");
        check(f.abortCalls() === 1 && f.commitCalls() === 0 && f.saves() === 0,
            "identity drift committed candidate or sync-base state");
    }
}

async function knownSmallReadsOnlyFirstMissingRepresentative(): Promise<void> {
    const previous = getHashTuning(), f = fixture();
    const h = (n: number) => (n + 16).toString(16).padStart(64, "0");
    const changes = [
        { action: "created" as const, path: "lazy.md", hash: h(1), size: 1, mtime: 10 },
        { action: "created" as const, path: "duplicate.md", hash: h(1), size: 1, mtime: 10 },
        { action: "created" as const, path: "present.md", hash: h(2), size: 1, mtime: 10 },
        { action: "created" as const, path: "preloaded.md", hash: h(3), size: 1, mtime: 10, data: new Uint8Array([3]) },
        { action: "created" as const, path: "preloaded-duplicate.md", hash: h(3), size: 1, mtime: 10 },
        { action: "created" as const, path: "unknown.md", size: 1, mtime: 10 },
    ];
    const reads: string[] = []; let uploads = 0;
    f.io.getAbsolutePath = () => { throw new Error("renderer-only small source resolved a native path"); };
    f.io.stat = async () => ({ size: 1, mtime: 10 });
    f.io.readFile = async (path: string) => { reads.push(path); return new Uint8Array([path === "unknown.md" ? 4 : 1]); };
    f.wasm.wasm_hash_batch = () => [h(4)];
    const api = { ensureTransportReady: async () => {}, checkContent: async () => [h(1), h(3), h(4)],
        putObjects: async (records: Array<{ hash: string; data: Uint8Array }>) => {
            uploads++; check(records.length === 3, "duplicate/preloaded sources changed deduplicated pack size");
            check(records.map(record => record.hash).join(",") === [h(1), h(3), h(4)].join(","), "small source representative order changed");
            check(records.map(record => record.data[0]).join(",") === "1,3,4", "small source reuse changed uploaded bytes");
        }, putRoot: async () => ({ root_hash: "accepted", conflicts: [] }) } as any;
    try {
        configureHashTuning({ ...previous, readConcurrency: 4, maxBatchFiles: 10, maxBatchBytes: 100,
            maxSingleBatchFileBytes: 100, maxBatchHoldMs: 60000 });
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", changes, "base");
        check(reads.join(",") === "unknown.md,lazy.md", "lazy stage reread a duplicate, present, preloaded or phase-A source");
        check(uploads === 1 && f.commitCalls() === 1, "deduplicated lazy read did not commit once");
    } finally { configureHashTuning(previous); }
}

async function failedKnownSmallReadJoinsEveryNativeSibling(): Promise<void> {
    for (const mode of ["async-error", "sync-error", "abort"] as const) {
        const previous = getHashTuning(), f = fixture(), controller = new AbortController();
        const expected = new Error("injected lazy source failure");
        let rejectFirst: ((error: Error) => void) | undefined, finishFirst: ((bytes: Uint8Array) => void) | undefined;
        let finishSecond: ((bytes: Uint8Array) => void) | undefined, secondStarted!: () => void;
        const started = new Promise<void>(resolve => { secondStarted = resolve; });
        const reads: string[] = [], stats: string[] = []; let settled = false, uploads = 0, roots = 0;
        f.io.getAbsolutePath = () => null;
        f.io.stat = async (path: string) => { stats.push(path); return { size: 1, mtime: 12 }; };
        f.io.readFile = (path: string) => {
            reads.push(path);
            if (path === "first.md") {
                if (mode === "sync-error") throw expected;
                return new Promise<Uint8Array>((resolve, reject) => { finishFirst = resolve; rejectFirst = reject; });
            }
            return new Promise<Uint8Array>(resolve => { finishSecond = resolve; secondStarted(); });
        };
        const api = { ensureTransportReady: async () => {}, checkContent: async (hashes: string[]) => hashes,
            putObjects: async () => { uploads++; }, putRoot: async () => { roots++; throw new Error("unexpected root"); } } as any;
        let pending: Promise<unknown> | undefined;
        try {
            configureHashTuning({ ...previous, readConcurrency: 2, maxBatchFiles: 3, maxBatchBytes: 100,
                maxSingleBatchFileBytes: 100, maxBatchHoldMs: 60000 });
            pending = push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", ["first.md", "second.md", "not-started.md"]
                .map((path, index) => ({ action: "created" as const, path, hash: (index + 10).toString(16).padStart(64, "0"), size: 1, mtime: 12 })),
                "base", undefined, undefined, null, undefined, controller.signal)
                .then(value => { settled = true; return value; }, error => { settled = true; return error; });
            await started;
            if (mode === "abort") { controller.abort(); finishFirst!(new Uint8Array([1])); }
            else rejectFirst?.(expected);
            await new Promise<void>(resolve => setImmediate(resolve));
            check(!settled && f.abortCalls() === 0, `${mode}: lazy source returned/aborted tree before its native sibling completed`);
            check(transientMemorySnapshot().usedBytes > 0, `${mode}: lazy native sibling lost its memory charge`);
            check(reads.join(",") === "first.md,second.md" && !stats.includes("not-started.md"), `${mode}: lazy group refilled after failure/abort`);
            check(uploads === 0 && roots === 0 && f.saves() === 0, `${mode}: incomplete native preparation published content or metadata`);
            finishSecond!(new Uint8Array([2])); const error = await pending;
            check(mode === "abort" ? error instanceof Error && error.name === "AbortError" : error === expected,
                `${mode}: joined lazy source lost the original error/cancellation`);
            check(f.abortCalls() === 1 && f.commitCalls() === 0 && f.saves() === 0, `${mode}: failed lazy group committed a candidate`);
            check(transientMemorySnapshot().usedBytes === 0, `${mode}: joined lazy source left memory admission live`);
        } finally {
            finishFirst?.(new Uint8Array([1])); finishSecond?.(new Uint8Array([2]));
            await pending; configureHashTuning(previous);
        }
    }
}

async function cancelledKnownReadDoesNotCrossHeldStatBoundary(): Promise<void> {
    for (const phase of ["before-read", "after-read"] as const) {
        const previous = getHashTuning(), f = fixture(), controller = new AbortController();
        let entered!: () => void, finishStat!: (value: { size: number; mtime: number }) => void;
        const started = new Promise<void>(resolve => { entered = resolve; });
        let stats = 0, reads = 0, uploads = 0, settled = false;
        f.io.getAbsolutePath = () => null;
        f.io.stat = () => {
            stats++;
            if (stats === (phase === "before-read" ? 1 : 2)) return new Promise(resolve => { finishStat = resolve; entered(); });
            return Promise.resolve({ size: 1, mtime: 10 });
        };
        f.io.readFile = async () => { reads++; return new Uint8Array([1]); };
        const api = { ensureTransportReady: async () => {}, checkContent: async (hashes: string[]) => hashes,
            putObjects: async () => { uploads++; }, putRoot: async () => { throw new Error("unexpected root"); } } as any;
        let pending: Promise<unknown> | undefined;
        try {
            configureHashTuning({ ...previous, readConcurrency: 1 });
            pending = push(api, f.io, f.syncBase, f.wasm, f.tree, "vault",
                [{ action: "created", path: "held-stat.md", hash: "c".repeat(64), mtime: 10, size: 1 }],
                "base", undefined, undefined, null, undefined, controller.signal)
                .then(value => { settled = true; return value; }, error => { settled = true; return error; });
            await started; controller.abort(); await new Promise<void>(resolve => setImmediate(resolve));
            check(!settled && transientMemorySnapshot().usedBytes > 0, `${phase}: cancellation forgot a native stat owner`);
            finishStat({ size: 1, mtime: 10 }); const error = await pending;
            check(error instanceof Error && error.name === "AbortError", `${phase}: stat completion did not preserve cancellation`);
            check(reads === (phase === "before-read" ? 0 : 1) && uploads === 0, `${phase}: stopped stat admitted a new read/upload`);
            check(f.abortCalls() === 1 && f.saves() === 0 && transientMemorySnapshot().usedBytes === 0,
                `${phase}: cancelled source escaped candidate/native cleanup`);
        } finally { finishStat?.({ size: 1, mtime: 10 }); await pending; configureHashTuning(previous); }
    }
}

async function driftedKnownReadJoinsHeldNativeSibling(): Promise<void> {
    for (const mode of ["post-stat", "oversized-backing"] as const) {
        const previous = getHashTuning(), f = fixture();
        let finishSecond!: (bytes: Uint8Array) => void, secondStarted!: () => void;
        const started = new Promise<void>(resolve => { secondStarted = resolve; });
        const stats = new Map<string, number>();
        let settled = false, uploads = 0, roots = 0;
        f.io.getAbsolutePath = () => null;
        f.io.stat = async (path: string) => {
            stats.set(path, (stats.get(path) ?? 0) + 1);
            if (mode === "post-stat" && path === "drifted.md" && stats.get(path) === 2) {
                return { size: 2, mtime: 10 };
            }
            return { size: 1, mtime: 10 };
        };
        f.io.readFile = (path: string) => {
            if (path === "drifted.md") {
                if (mode === "oversized-backing") return Promise.resolve(new Uint8Array(new ArrayBuffer(2), 0, 1));
                return Promise.resolve(new Uint8Array([1]));
            }
            if (path === "held.md") return new Promise<Uint8Array>(resolve => {
                finishSecond = resolve; secondStarted();
            });
            throw new Error(`unexpected refill read: ${path}`);
        };
        const api = { ensureTransportReady: async () => {}, checkContent: async (hashes: string[]) => hashes,
            putObjects: async () => { uploads++; }, putRoot: async () => { roots++; throw new Error("unexpected root"); } } as any;
        let pending: Promise<unknown> | undefined;
        try {
            configureHashTuning({ ...previous, readConcurrency: 2, maxBatchFiles: 3, maxBatchBytes: 100,
                maxSingleBatchFileBytes: 100, maxBatchHoldMs: 60000 });
            pending = push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", ["drifted.md", "held.md", "not-started.md"]
                .map((path, index) => ({ action: "created" as const, path,
                    hash: (index + 20).toString(16).padStart(64, "0"), size: 1, mtime: 10 })), "base")
                .then(value => { settled = true; return value; }, error => { settled = true; return error; });
            await started;
            await new Promise<void>(resolve => setImmediate(resolve));
            check(!settled && f.abortCalls() === 0, `${mode}: drift returned before its native sibling completed`);
            check(transientMemorySnapshot().usedBytes > 0, `${mode}: drift refunded a still-live native owner`);
            check(!stats.has("not-started.md"), `${mode}: failed group refilled from the next source`);
            check(uploads === 0 && roots === 0 && f.saves() === 0, `${mode}: drifted bytes reached upload/root metadata`);
            finishSecond(new Uint8Array([2]));
            const error = await pending;
            check(error instanceof HashWorkerFileDriftError, `${mode}: source drift lost its retry classification`);
            check(f.abortCalls() === 1 && f.commitCalls() === 0 && f.saves() === 0,
                `${mode}: drifted lazy group committed a candidate`);
            check(transientMemorySnapshot().usedBytes === 0, `${mode}: joined native sibling retained its admission`);
        } finally {
            finishSecond?.(new Uint8Array([2]));
            await pending;
            configureHashTuning(previous);
        }
    }
}

async function boundedLookaheadOverlapsPreparationWithUpload(): Promise<void> {
    const previous = getHashTuning(), f = fixture();
    const changes = ["first.md", "second.md"].map(path => ({
        action: "created" as const, path, size: 1, mtime: 20,
    }));
    let releaseLookahead!: (data: Uint8Array) => void;
    let lookaheadEntered!: () => void;
    const lookaheadStarted = new Promise<void>(resolve => { lookaheadEntered = resolve; });
    let releaseUpload!: () => void;
    let uploadEntered!: () => void;
    const uploadStarted = new Promise<void>(resolve => { uploadEntered = resolve; });
    let secondCheckEntered!: () => void;
    const secondCheckStarted = new Promise<void>(resolve => { secondCheckEntered = resolve; });
    const reads = new Map<string, number>();
    const checked: string[][] = [], uploaded: Array<Array<{ hash: string; byte: number }>> = [];
    let checks = 0, uploads = 0, firstUploadActive = false, secondByte = 2;
    let activeNetwork = 0, peakNetwork = 0;
    f.io.getAbsolutePath = () => { throw new Error("renderer lookahead resolved a desktop worker path"); };
    f.io.stat = async () => ({ size: 1, mtime: 20 });
    f.io.readFile = (path: string) => {
        const count = (reads.get(path) ?? 0) + 1; reads.set(path, count);
        if (path === "second.md" && count === 1) {
            lookaheadEntered();
            return new Promise<Uint8Array>(resolve => { releaseLookahead = resolve; });
        }
        return Promise.resolve(new Uint8Array([path === "first.md" ? 1 : secondByte]));
    };
    f.wasm.wasm_hash = (data: Uint8Array) => data[0].toString(16).repeat(64);
    f.wasm.wasm_hash_batch = (data: Uint8Array) => [data[0].toString(16).repeat(64)];
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async (hashes: string[]) => {
            activeNetwork++; peakNetwork = Math.max(peakNetwork, activeNetwork);
            try {
                checks++;
                checked.push([...hashes]);
                if (checks === 2) secondCheckEntered();
                return hashes;
            } finally { activeNetwork--; }
        },
        putObjects: async (records: Array<{ hash: string; data: Uint8Array }>) => {
            activeNetwork++; peakNetwork = Math.max(peakNetwork, activeNetwork);
            uploads++;
            uploaded.push(records.map(record => ({ hash: record.hash, byte: record.data[0] })));
            try {
                if (uploads !== 1) return;
                firstUploadActive = true; uploadEntered();
                await new Promise<void>(resolve => { releaseUpload = resolve; });
                firstUploadActive = false;
            } finally { activeNetwork--; }
        },
        putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
    } as any;
    try {
        configureHashTuning({ ...previous, readConcurrency: 1, maxBatchFiles: 1,
            maxBatchBytes: 16, maxSingleBatchFileBytes: 16, maxBatchHoldMs: 60000 });
        const pending = push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", changes, "base");
        await lookaheadStarted;
        await uploadStarted;
        check(firstUploadActive, "held first upload was not active while lookahead waited");
        releaseLookahead(new Uint8Array([2]));
        await secondCheckStarted;
        check(firstUploadActive, "next batch read/hash/check did not overlap the previous upload");
        check(checks === 2, "lookahead added duplicate server checks before consumption");
        // Preserve size+mtime while changing bytes after speculative hashing.
        // The owning batch must hash again and ignore the old presence bit.
        secondByte = 3;
        releaseUpload();
        await pending;
        check((reads.get("first.md") ?? 0) === 1 && (reads.get("second.md") ?? 0) === 2,
            "missing-content lookahead did not preserve explicit reread semantics");
        check(uploads === 2 && f.commitCalls() === 1 && transientMemorySnapshot().usedBytes === 0,
            "overlapped pipeline changed publication or retained admission");
        check(checked.map(row => row[0]?.[0]).join("") === "123",
            "same-stat byte drift reused the speculative hash/check as authority");
        check(uploaded[1]?.[0]?.hash === "3".repeat(64) && uploaded[1]?.[0]?.byte === 3,
            "same-stat drift uploaded stale lookahead bytes or hash");
        check(peakNetwork === 2 && activeNetwork === 0,
            "lookahead exceeded the bounded current-upload plus one-check network window");
    } finally {
        releaseLookahead?.(new Uint8Array([2])); releaseUpload?.();
        configureHashTuning(previous);
    }
}

async function failedConsumerCancelsAndJoinsLookahead(): Promise<void> {
    const previous = getHashTuning(), f = fixture(), expected = new Error("injected current upload failure");
    let releaseLookahead!: (data: Uint8Array) => void, entered!: () => void;
    const lookaheadStarted = new Promise<void>(resolve => { entered = resolve; });
    let settled = false, checks = 0;
    f.io.getAbsolutePath = () => { throw new Error("renderer lookahead resolved a desktop worker path"); };
    f.io.stat = async () => ({ size: 1, mtime: 30 });
    f.io.readFile = (path: string) => path === "future.md"
        ? new Promise<Uint8Array>(resolve => { releaseLookahead = resolve; entered(); })
        : Promise.resolve(new Uint8Array([1]));
    f.wasm.wasm_hash = () => "2".repeat(64);
    f.wasm.wasm_hash_batch = () => ["1".repeat(64)];
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async (hashes: string[]) => { checks++; return hashes; },
        putObjects: async () => { await lookaheadStarted; throw expected; },
        putRoot: async () => { throw new Error("unexpected root"); },
    } as any;
    let pending: Promise<unknown> | undefined;
    try {
        configureHashTuning({ ...previous, readConcurrency: 1, maxBatchFiles: 1,
            maxBatchBytes: 16, maxSingleBatchFileBytes: 16, maxBatchHoldMs: 60000 });
        pending = push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "created", path: "current.md", size: 1, mtime: 30 },
            { action: "created", path: "future.md", size: 1, mtime: 30 },
        ], "base").then(value => { settled = true; return value; }, error => { settled = true; return error; });
        await lookaheadStarted;
        await new Promise<void>(resolve => setImmediate(resolve));
        check(!settled && f.abortCalls() === 0,
            "failed consumer returned or aborted candidate before lookahead native read joined");
        releaseLookahead(new Uint8Array([2]));
        const error = await pending;
        check(error === expected, "lookahead cancellation replaced the owning upload failure");
        check(checks === 1, "cancelled lookahead issued a late speculative server check");
        check(f.abortCalls() === 1 && f.commitCalls() === 0 && transientMemorySnapshot().usedBytes === 0,
            "failed pipeline retained admission or published before lookahead retirement");
    } finally {
        releaseLookahead?.(new Uint8Array([2])); await pending; configureHashTuning(previous);
    }
}

async function speculativeCheckFailureFallsBackToOwningBatch(): Promise<void> {
    const previous = getHashTuning(), f = fixture();
    let checks = 0, uploads = 0;
    const requested: string[][] = [];
    f.io.getAbsolutePath = () => null;
    f.io.stat = async () => ({ size: 1, mtime: 40 });
    f.io.readFile = async (path: string) => new Uint8Array([path === "first.md" ? 1 : 2]);
    f.wasm.wasm_hash = (data: Uint8Array) => data[0].toString(16).repeat(64);
    f.wasm.wasm_hash_batch = (data: Uint8Array) => [data[0].toString(16).repeat(64)];
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async (hashes: string[]) => {
            checks++;
            requested.push([...hashes]);
            if (checks === 2) throw new Error("injected speculative check failure");
            return hashes;
        },
        putObjects: async () => { uploads++; },
        putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
    } as any;
    try {
        configureHashTuning({ ...previous, readConcurrency: 1, maxBatchFiles: 1,
            maxBatchBytes: 16, maxSingleBatchFileBytes: 16, maxBatchHoldMs: 60000 });
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "created", path: "first.md", size: 1, mtime: 40 },
            { action: "created", path: "second.md", size: 1, mtime: 40 },
        ], "base");
        check(checks === 3 && requested.map(row => row[0]?.[0]).join("") === "122",
            "failed speculative check was not retried by the authoritative owner");
        check(uploads === 2 && f.commitCalls() === 1 && f.abortCalls() === 0,
            "speculative failure aborted or changed the successful publication");
        check(transientMemorySnapshot().usedBytes === 0,
            "speculative failure fallback retained transient admission");
    } finally {
        configureHashTuning(previous);
    }
}

async function rangedLookaheadOverlapsUploadAndReusesPreparedManifest(): Promise<void> {
    const previous = getHashTuning(), f = fixture();
    const size = 2 * 1024 * 1024;
    const fileHash = "d".repeat(64), firstChunk = "e".repeat(64), secondChunk = firstChunk;
    const directory = await mkdtemp(join(tmpdir(), "obsetync-ranged-lookahead-"));
    const absolutePath = join(directory, "large.bin");
    let releaseUpload!: () => void, uploadEntered!: () => void;
    const uploadStarted = new Promise<void>(resolve => { uploadEntered = resolve; });
    let lookaheadCheckEntered!: () => void;
    const lookaheadCheckStarted = new Promise<void>(resolve => { lookaheadCheckEntered = resolve; });
    let firstUploadActive = false, networkActive = 0, peakNetwork = 0, putCalls = 0;
    let rendererLargeReads = 0, retainCalls = 0, chunkChecks = 0;
    const workerModes: string[] = [];
    let retained: any = null;
    try {
        const handle = await open(absolutePath, "w");
        try { await handle.truncate(size); }
        finally { await handle.close(); }
        const info = await stat(absolutePath);
        const mtime = Number(info.mtimeMs);
        const fingerprint = { size, mtime, ctime: Number(info.ctimeMs),
            device: Number(info.dev), inode: Number(info.ino) };
        const manifest = { file_hash: fileHash, total_size: size, chunks: [
            { hash: firstChunk, offset: 0, size: 1024 * 1024 },
            { hash: secondChunk, offset: 1024 * 1024, size: 1024 * 1024 },
        ] };
        f.wasm.wasm_should_chunk = (bytes: number) => bytes >= 1024 * 1024;
        f.wasm.wasm_hash_batch = () => ["c".repeat(64)];
        f.wasm.Hasher = class {
            update(): void {}
            finalize(): string { return firstChunk; }
            free(): void {}
        };
        f.io.getAbsolutePath = (path: string) => path === "large.bin" ? absolutePath : null;
        f.io.stat = async (path: string) => path === "large.bin"
            ? ({ size, mtime }) : ({ size: 1, mtime: 50 });
        f.io.readFile = async (path: string) => {
            if (path === "large.bin") { rendererLargeReads++; throw new Error("unexpected renderer large read"); }
            return new Uint8Array([3]);
        };
        const workers = {
            run: async (input: any) => {
                workerModes.push(input.mode);
                if (input.mode === "manifest") {
                    // Keep native preparation alive until the current small
                    // upload owns the network, proving real cross-file overlap.
                    await uploadStarted;
                    return { type: "result", job_id: "lookahead", mode: "manifest", manifest,
                        size, mtime, fingerprint, read_ms: 2, hash_ms: 3 };
                }
                return { type: "result", job_id: "owner-verify", mode: "hash", hash: fileHash,
                    size, mtime, fingerprint, read_ms: 2, hash_ms: 1 };
            },
        } as any;
        const prepared = {
            scopeHash: "9".repeat(64),
            journalThroughId: () => 7,
            assertApplicable: () => {},
            plan: {
                lookup: () => retained,
                retain: async (input: any) => {
                    retainCalls++;
                    retained = { ...input, mutationId: 11 };
                    return { retained: true, mutationId: 11 };
                },
                discard: async () => { throw new Error("unexpected prepared discard"); },
            },
        } as any;
        const api = {
            ensureTransportReady: async () => {},
            checkContent: async (hashes: string[]) => hashes,
            checkContentChunks: async (hashes: string[]) => {
                networkActive++; peakNetwork = Math.max(peakNetwork, networkActive);
                try {
                    chunkChecks++;
                    check(firstUploadActive, "ranged lookahead check did not overlap current upload");
                    check(hashes.join(",") === firstChunk,
                        "ranged lookahead checked the wrong bounded manifest window");
                    lookaheadCheckEntered();
                    return hashes;
                } finally { networkActive--; }
            },
            putObjects: async () => {
                networkActive++; peakNetwork = Math.max(peakNetwork, networkActive);
                try {
                    putCalls++;
                    if (putCalls !== 1) return;
                    firstUploadActive = true;
                    uploadEntered();
                    await new Promise<void>(resolve => { releaseUpload = resolve; });
                    firstUploadActive = false;
                } finally { networkActive--; }
            },
            putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
        } as any;
        configureHashTuning({ ...previous, readConcurrency: 1, maxBatchFiles: 1,
            maxBatchBytes: 8 * 1024 * 1024, maxSingleBatchFileBytes: 8 * 1024 * 1024,
            maxBatchHoldMs: 60000 });
        const pending = push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "created", path: "first.md", size: 1, mtime: 50 },
            { action: "created", path: "large.bin", size, mtime },
        ], "base", undefined, undefined, workers, undefined, undefined, prepared);
        await lookaheadCheckStarted;
        check(firstUploadActive, "current upload ended before ranged lookahead check");
        releaseUpload();
        const outcome = await pending;
        check(workerModes.join(",") === "manifest,hash",
            "authoritative owner did not full-hash the speculative prepared manifest");
        check(retainCalls === 1 && chunkChecks === 1,
            "ranged lookahead was duplicated or owning batch repeated its missing check");
        check(rendererLargeReads === 0 && peakNetwork === 2 && networkActive === 0,
            "ranged overlap buffered the full source or exceeded its network cap");
        check(outcome.preparedCleanup?.[0]?.expectedMutationId === 11 &&
            f.commitCalls() === 1 && transientMemorySnapshot().usedBytes === 0,
        "ranged lookahead lost prepared ownership, publication, or admission cleanup");
    } finally {
        releaseUpload?.();
        configureHashTuning(previous);
        await rm(directory, { recursive: true, force: true });
    }
}

function mobileRangedFixture(f: ReturnType<typeof fixture>, failure?: "protocol" | "source" | "abort",
    onBorrowed?: (signal: AbortSignal | undefined) => void) {
    const mib = 1024 * 1024;
    const size = 4 * mib + 3;
    const fileHash = "c".repeat(64);
    const chunkHashes = ["d".repeat(64), "e".repeat(64)];
    let manifestFinished = false;
    let chunkerRuns = 0;
    let fullHashes = 0;
    let wholeReads = 0;
    let opens = 0;
    let closes = 0;
    const rangeSizes: number[] = [];
    const borrowedOwners: TransientWorkScope[] = [];
    f.wasm.wasm_should_chunk = (bytes: number) => bytes >= mib;
    f.wasm.WasmChunker = class {
        total = 0;
        constructor() { chunkerRuns++; }
        update(data: Uint8Array) { this.total += data.byteLength; }
        finish() {
            check(this.total === size, "mobile manifest did not consume the exact source length");
            manifestFinished = true;
            return { file_hash: fileHash, total_size: size, chunks: [
                { hash: chunkHashes[0], offset: 0, size: 4 * mib },
                { hash: chunkHashes[1], offset: 4 * mib, size: 3 },
            ] };
        }
        free() {}
    };
    f.wasm.Hasher = class {
        total = 0;
        update(data: Uint8Array) { this.total += data.byteLength; }
        finalize() {
            if (this.total === size) { fullHashes++; return fileHash; }
            return this.total === 4 * mib ? chunkHashes[0] : chunkHashes[1];
        }
        free() {}
    };
    f.io.getAbsolutePath = () => { throw new Error("mobile ranged source resolved a desktop worker path"); };
    f.io.readFile = async () => { wholeReads++; throw new Error("mobile ranged source used whole read"); };
    f.io.stat = async () => ({ size, mtime: 7 });
    f.io.openMobileRangeReader = async (_path: string, expected: { size: number; mtime: number },
        signal: AbortSignal | undefined, owner: TransientWorkScope | undefined): Promise<MobileRangeReader> => {
        check(expected.size === size && expected.mtime === 7 && !!owner,
            "mobile opener lost source identity or parent admission");
        opens++;
        manifestFinished = false;
        let closed = false;
        return {
            size,
            resourceVersion: "9".repeat(64),
            async read() { throw new Error("push used standalone nested-admission range read"); },
            async readBorrowed(offset, length, borrowed, readSignal) {
                check(!closed && borrowed === owner && readSignal === signal,
                    "mobile range escaped its push owner/signal");
                check(length > 0 && length <= 4 * mib && offset >= 0 && offset <= size - length,
                    "mobile push requested an unbounded/invalid range");
                borrowedOwners.push(borrowed);
                rangeSizes.push(length);
                onBorrowed?.(readSignal);
                if (manifestFinished && failure) {
                    if (failure === "abort") {
                        const error = new Error("synthetic mobile abort"); error.name = "AbortError"; throw error;
                    }
                    throw new MobileRangeReadError(failure === "protocol" ? "PROTOCOL" : "SOURCE_CHANGED");
                }
                return new Uint8Array(length);
            },
            async verify() { if (closed) throw new Error("verified closed mobile reader"); },
            close() {
                if (closed) throw new Error("mobile range reader closed twice");
                closed = true;
                closes++;
            },
        };
    };
    return { size, fileHash, chunkHashes, rangeSizes, borrowedOwners,
        wholeReads: () => wholeReads, opens: () => opens, closes: () => closes,
        chunkerRuns: () => chunkerRuns, fullHashes: () => fullHashes };
}

async function mobileLifecycleHideDuringRangedHashIsRetrySafe(): Promise<void> {
    const previous = getHashTuning();
    configureHashTuning(hashTuningForRuntime("mobile"));
    const baseline = transientMemorySnapshot().usedBytes;
    const visibility = new ResourceVisibilityGate("mobile", true);
    const engine = new AbortController();
    const work = visibility.beginHeavyWork(engine.signal);
    try {
        const f = fixture();
        let hidden = false;
        const mobile = mobileRangedFixture(f, undefined, signal => {
            if (hidden) return;
            hidden = true;
            visibility.setVisible(false, "pagehide");
            check(signal === work.signal && signal.aborted,
                "ranged source did not observe its mobile lifecycle epoch");
        });
        let rootCalls = 0;
        const error = await push({ ensureTransportReady: async () => {}, checkContent: async () => [],
            checkContentChunks: async () => mobile.chunkHashes, putObjects: async () => {},
            putRoot: async () => { rootCalls++; return { root_hash: "bad" }; } } as any,
        f.io, f.syncBase, f.wasm, f.tree, "vault", [{ action: "created", path: "hidden-mobile.bin",
            size: mobile.size, mtime: 7 }], "base", undefined, undefined, undefined, undefined,
        work.signal).then(() => null, value => value);
        check(error === work.signal.reason && rootCalls === 0 && f.commitCalls() === 0 && f.abortCalls() === 1,
            "mobile lifecycle cancellation lost retry-safe candidate authority");
        check(mobile.wholeReads() === 0 && mobile.closes() === 1 &&
            transientMemorySnapshot().usedBytes === baseline,
        "hidden ranged hash fell back to whole-read or released incompletely");
        check(visibility.snapshot().activeWork === 1,
            "lifecycle signal released its caller before the push tail settled");
    } finally {
        work.release();
        visibility.dispose();
        configureHashTuning(previous);
    }
    check(visibility.snapshot().activeWork === 0, "ranged lifecycle owner leaked after settlement");
}

async function mobileRangedUploadResumesWithoutWholeRead(): Promise<void> {
    const previous = getHashTuning();
    configureHashTuning(hashTuningForRuntime("mobile"));
    const baseline = transientMemorySnapshot().usedBytes;
    try {
        const f = fixture();
        const mobile = mobileRangedFixture(f);
        const change = { action: "created" as const, path: "mobile-large.bin",
            size: mobile.size, mtime: 7 };
        const stored = new Set<string>();
        let contentPuts = 0;
        const firstApi = {
            ensureTransportReady: async () => {},
            checkContent: async () => [],
            checkContentChunks: async () => mobile.chunkHashes,
            putObjects: async (records: Array<{ kind: BulkObjectKind; hash: string }>) => {
                const chunks = records.filter(row => row.kind === BulkObjectKind.ContentChunk);
                if (!chunks.length) return;
                contentPuts++;
                if (contentPuts === 2) throw new Error("synthetic mobile disconnect");
                for (const row of chunks) stored.add(row.hash);
            },
        } as any;
        const first = await push(firstApi, f.io, f.syncBase, f.wasm, f.tree, "vault",
            [change], "base").then(() => null, error => error);
        check((first as Error)?.message === "synthetic mobile disconnect" && stored.size === 1,
            "mobile interruption did not preserve its first bounded object ACK");
        check(f.commitCalls() === 0 && f.abortCalls() === 1 && !f.entries.has(change.path),
            "interrupted mobile upload published file metadata");

        const retried: string[] = [];
        let manifests = 0;
        const retryApi = {
            ensureTransportReady: async () => {},
            checkContent: async () => [],
            checkContentChunks: async () => mobile.chunkHashes.filter(hash => !stored.has(hash)),
            putObjects: async (records: Array<{ kind: BulkObjectKind; hash: string }>) => {
                for (const row of records) {
                    if (row.kind === BulkObjectKind.ContentChunk) { retried.push(row.hash); stored.add(row.hash); }
                    if (row.kind === BulkObjectKind.Manifest) manifests++;
                }
            },
            putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
        } as any;
        const outcome = await push(retryApi, f.io, f.syncBase, f.wasm, f.tree, "vault",
            [change], "base");
        check(outcome.newRootHash === "accepted" && retried.join(",") === mobile.chunkHashes[1] && manifests === 1,
            "mobile retry ignored the server bitmap or dependent manifest order");
        check(f.entries.get(change.path)?.hash === mobile.fileHash && f.commitCalls() === 1,
            "mobile retry did not commit the equivalent manifest hash");
        check(mobile.wholeReads() === 0 && mobile.rangeSizes.every(bytes => bytes <= 4 * 1024 * 1024),
            "mobile ranged pipeline allocated a whole source or oversized range");
        check(mobile.opens() === 2 && mobile.closes() === 2 && mobile.borrowedOwners.length > 0,
            "mobile reader/admission ownership did not settle once per attempt");
        check(transientMemorySnapshot().usedBytes === baseline, "mobile retry leaked transient admission");
    } finally { configureHashTuning(previous); }
}

async function mobilePreparedRestartSkipsFastCdcButRehashesEveryByte(): Promise<void> {
    const previous = getHashTuning();
    configureHashTuning(hashTuningForRuntime("mobile"));
    const disk = new MemorySegmentedIO(), scopeHash = "7".repeat(64);
    const context = (plan: MobilePreparedTransferPlan, memoryArbiter: SyncMemoryArbiter) => ({
        plan, memoryArbiter, scopeHash, journalThroughId: () => 1, assertApplicable: () => {},
    });
    try {
        const f = fixture(), mobile = mobileRangedFixture(f);
        const change = { action: "created" as const, path: "restart-mobile.bin",
            size: mobile.size, mtime: 7 };
        const stored = new Set<string>();
        const firstMemory = new SyncMemoryArbiter({ capacityBytes: 64 * 1024 * 1024 });
        const firstPlan = new MobilePreparedTransferPlan(disk, {}, firstMemory); await firstPlan.load();
        let chunkPuts = 0;
        const firstApi = {
            ensureTransportReady: async () => {}, checkContent: async () => [],
            checkContentChunks: async () => mobile.chunkHashes,
            putObjects: async (records: Array<{ kind: BulkObjectKind; hash: string }>,
                _perf?: unknown, _scope?: unknown, _signal?: AbortSignal, options?: { priority?: string }) => {
                check(options === undefined, "ranged chunk/manifest upload inherited urgent priority");
                check(firstMemory.snapshot().owners["prepared-manifest"].activeLeases >= 2,
                    "prepared manifest owner was released before ranged upload settlement");
                const chunks = records.filter(record => record.kind === BulkObjectKind.ContentChunk);
                if (!chunks.length) return;
                if (++chunkPuts === 2) throw new Error("restart cut");
                for (const row of chunks) stored.add(row.hash);
            },
        } as any;
        const preparedBase = "b".repeat(64);
        const failure = await push(firstApi, f.io, f.syncBase, f.wasm, f.tree, "vault",
            [change], preparedBase, undefined, undefined, undefined, undefined, undefined,
            context(firstPlan, firstMemory)).then(() => null, error => error);
        check((failure as Error)?.message === "restart cut" && mobile.chunkerRuns() === 1,
            "interrupted mobile owner did not persist one prepared FastCDC layout");
        check(mobile.fullHashes() === 0 && mobile.wholeReads() === 0,
            "fresh mobile preparation performed a redundant/full adapter hash");
        await firstPlan.closeAndDrain(); firstMemory.close();
        check(firstMemory.snapshot().usedBytes === 0, "interrupted prepared owner leaked across shutdown");

        const restartMemory = new SyncMemoryArbiter({ capacityBytes: 64 * 1024 * 1024 });
        const restartPlan = new MobilePreparedTransferPlan(disk, {}, restartMemory); await restartPlan.load();
        const retryApi = {
            ensureTransportReady: async () => {}, checkContent: async () => [],
            checkContentChunks: async () => mobile.chunkHashes.filter(hash => !stored.has(hash)),
            putObjects: async (records: Array<{ kind: BulkObjectKind; hash: string }>,
                _perf?: unknown, _scope?: unknown, _signal?: AbortSignal, options?: { priority?: string }) => {
                check(options === undefined, "restarted ranged upload inherited urgent priority");
                check(restartMemory.snapshot().owners["prepared-manifest"].activeLeases >= 2,
                    "reused prepared manifest owner was released before ACK");
                for (const row of records) if (row.kind === BulkObjectKind.ContentChunk) stored.add(row.hash);
            },
            putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
        } as any;
        const outcome = await push(retryApi, f.io, f.syncBase, f.wasm, f.tree, "vault",
            [change], preparedBase, undefined, undefined, undefined, undefined, undefined,
            context(restartPlan, restartMemory));
        check(outcome.newRootHash === "accepted" && mobile.chunkerRuns() === 1,
            "restart reused source authority incorrectly or reran FastCDC");
        check(mobile.fullHashes() === 1 && mobile.wholeReads() === 0,
            "restart did not fully ranged-hash the current source before reuse");
        await restartPlan.closeAndDrain(); restartMemory.close();
        check(restartMemory.snapshot().usedBytes === 0, "restart prepared owner leaked after ACK/cleanup");
    } finally { configureHashTuning(previous); }
}

async function mobilePreparedAdmissionRefusalNeverFallsBackToWholeRead(): Promise<void> {
    const previous = getHashTuning(); configureHashTuning(hashTuningForRuntime("mobile"));
    const disk = new MemorySegmentedIO(), scopeHash = "6".repeat(64), baseRoot = "b".repeat(64);
    try {
        const f = fixture(), mobile = mobileRangedFixture(f), path = "pressured-mobile.bin";
        const seedMemory = new SyncMemoryArbiter({ capacityBytes: 64 * 1024 * 1024 });
        const seed = new MobilePreparedTransferPlan(disk, {}, seedMemory); await seed.load();
        const retained = await seed.retain({ scopeHash, path, journalThroughId: 1, baseRoot,
            source: { size: mobile.size, mtime: 7, fingerprint: { kind: "mobile-resource-v1",
                size: mobile.size, mtime: 7, resourceVersion: "9".repeat(64) } },
            manifest: { file_hash: mobile.fileHash, total_size: mobile.size, chunks: [
                { hash: mobile.chunkHashes[0], offset: 0, size: 4 * 1024 * 1024 },
                { hash: mobile.chunkHashes[1], offset: 4 * 1024 * 1024, size: 3 },
            ] } });
        check(retained.retained, "failed to seed durable mobile prepared record");
        await seed.closeAndDrain(); seedMemory.close();

        const tightMemory = new SyncMemoryArbiter({ capacityBytes:
            PREPARED_TRANSFER_LIMITS.retainedBytes + PREPARED_STORE_OPERATION_BYTES });
        const plan = new MobilePreparedTransferPlan(disk, {}, tightMemory); await plan.load();
        const pressure = await tightMemory.reserve("root-review", PREPARED_STORE_OPERATION_BYTES - 1);
        let roots = 0;
        const api = { ensureTransportReady: async () => {}, checkContent: async () => [],
            checkContentChunks: async () => [], putObjects: async () => {},
            putRoot: async () => { roots++; return { root_hash: "bad", conflicts: [] }; } } as any;
        const error = await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
            action: "created", path, size: mobile.size, mtime: 7,
        }], baseRoot, undefined, undefined, undefined, undefined, undefined,
        { plan, memoryArbiter: tightMemory, scopeHash, journalThroughId: () => 1,
            assertApplicable: () => {} }).then(() => null, value => value);
        check(error instanceof PreparedMobileManifestError && error.stage === "retention",
            "temporary prepared admission refusal lost its retryable classification");
        check(roots === 0 && f.commitCalls() === 0 && f.abortCalls() === 1,
            "prepared admission refusal published or committed the candidate");
        check(mobile.wholeReads() === 0 && mobile.chunkerRuns() === 0,
            "prepared admission refusal entered renderer whole-read/FastCDC fallback");
        pressure.release();
        await plan.closeAndDrain(); tightMemory.close();
        check(tightMemory.snapshot().usedBytes === 0, "admission-refused mobile plan leaked ownership");
    } finally { configureHashTuning(previous); }
}

async function mobileCapabilityFailureDefersWithoutBlocking(): Promise<void> {
    const previous = getHashTuning();
    configureHashTuning(hashTuningForRuntime("mobile"));
    try {
        const f = fixture();
        const mib = 1024 * 1024, largeSize = 4 * mib + 1, smallHash = "f".repeat(64);
        f.wasm.wasm_should_chunk = (bytes: number) => bytes >= mib;
        let wholeReads = 0, opens = 0, chunkers = 0;
        f.wasm.WasmChunker = class { constructor() { chunkers++; } update() {} finish() { return {}; } free() {} };
        f.io.getAbsolutePath = () => null;
        f.io.openMobileRangeReader = async () => { opens++; return null; };
        f.io.readFile = async () => { wholeReads++; throw new Error("whole fallback"); };
        const api = { ensureTransportReady: async () => {}, checkContent: async () => [],
            checkContentChunks: async () => [], putObjects: async () => {},
            putRoot: async () => ({ root_hash: "accepted", conflicts: [] }) } as any;
        const outcome = await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "created", path: "unsupported-large.bin", size: largeSize, mtime: 7 },
            { action: "created", path: "independent.md", size: 1, mtime: 8, hash: smallHash },
        ], "base");
        check(outcome.deferred?.length === 1 && outcome.deferred[0].path === "unsupported-large.bin" &&
            outcome.deferred[0].reason === "range-unavailable" &&
            outcome.newRootHash === "accepted", "mobile capability failure blocked independent publication");
        check(f.entries.has("independent.md") && !f.entries.has("unsupported-large.bin") &&
            wholeReads === 0 && opens === 1 && chunkers === 0,
        "mobile capability failure used a whole-file/chunker fallback or published stale metadata");

        const safe = fixture();
        const safeSize = mib;
        const safeFileHash = "1".repeat(64), safeChunkHash = "2".repeat(64);
        let safeOpens = 0, safeReads = 0;
        safe.wasm.wasm_should_chunk = (bytes: number) => bytes >= mib;
        safe.wasm.WasmChunker = class {
            total = 0; update(data: Uint8Array) { this.total += data.byteLength; }
            finish() { return { file_hash: safeFileHash, total_size: this.total,
                chunks: [{ hash: safeChunkHash, offset: 0, size: this.total }] }; }
            free() {}
        };
        safe.io.getAbsolutePath = () => null;
        safe.io.openMobileRangeReader = async () => { safeOpens++; return null; };
        safe.io.stat = async () => ({ size: safeSize, mtime: 9 });
        safe.io.readFile = async () => { safeReads++; return new Uint8Array(safeSize); };
        const safeApi = { ensureTransportReady: async () => {}, checkContent: async () => [],
            checkContentChunks: async () => [], putObjects: async () => {},
            putRoot: async () => ({ root_hash: "accepted-safe", conflicts: [] }) } as any;
        const safeOutcome = await push(safeApi, safe.io, safe.syncBase, safe.wasm, safe.tree,
            "vault", [{ action: "created", path: "bounded.bin", size: safeSize, mtime: 9 }], "base");
        check(safeOutcome.newRootHash === "accepted-safe" && safeReads === 1 && safeOpens === 0,
            "bounded mobile whole-file path was unnecessarily gated on range capability");
    } finally { configureHashTuning(previous); }
}

async function mobileMidReadFailuresRemainRetrySafe(): Promise<void> {
    const previous = getHashTuning();
    configureHashTuning(hashTuningForRuntime("mobile"));
    try {
        for (const failure of ["protocol", "source", "abort"] as const) {
            const f = fixture();
            const mobile = mobileRangedFixture(f, failure);
            let rootCalls = 0;
            const api = { ensureTransportReady: async () => {}, checkContent: async () => [],
                checkContentChunks: async () => mobile.chunkHashes,
                putObjects: async () => {}, putRoot: async () => { rootCalls++; return { root_hash: "bad" }; } } as any;
            const baseline = transientMemorySnapshot().usedBytes;
            const error = await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
                action: "created", path: "drifting-mobile.bin", size: mobile.size, mtime: 7,
            }], "base").then(() => null, value => value);
            if (failure === "abort") check((error as Error)?.name === "AbortError", "mobile abort lost identity");
            else if (failure === "source") {
                check(error instanceof HashWorkerFileDriftError,
                    "mobile source drift did not enter retry-safe generation handling");
            } else {
                check(error instanceof MobileRangeReadError && error.code === "PROTOCOL",
                    "protocol lost fixed range classification");
            }
            check(rootCalls === 0 && f.commitCalls() === 0 && f.abortCalls() === 1 &&
                !f.entries.has("drifting-mobile.bin"), `${failure} published an unverified mobile source`);
            check(mobile.wholeReads() === 0 && mobile.closes() === 1 &&
                transientMemorySnapshot().usedBytes === baseline, `${failure} fell back or leaked source admission`);
        }
    } finally { configureHashTuning(previous); }
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed && !process.exitCode) {
        console.error("push-transaction.test: unfinished native-operation test"); process.exitCode = 1;
    }
});

void terminalPreflightDoesNotMutate()
    .then(failedRootRestoresCandidate)
    .then(failedRootDoesNotCommitUpsert)
    .then(everyPostCandidateFailureAbortsWithoutFullSnapshot)
    .then(incrementalPushChecksOnlyNewCandidateChunks)
    .then(parentlessPushChecksEveryCandidateChunk)
    .then(pagedCandidateChunksDriveActualPushSafely)
    .then(rejectedCommittedBootstrapNeverFallsBackOrOpensCandidate)
    .then(workerManifestAvoidsRendererFileBytes)
    .then(rendererRangeFallbackChunksWithoutWorkerPool)
    .then(interruptedRangedUploadResumesFromServerBitmap)
    .then(rangedDriftAfterTransferAbortsCandidate)
    .then(workerDriftAbortsCandidate)
    .then(smallFilesReachTransportAsPacksNotPerFilePuts)
    .then(acceptedRootCommitsMetadata)
    .then(candidateOpenAdmissionIsWiredBeforeNativeAndFileWork)
    .then(candidateOpenAdmissionSurvivesRetirementAndTerminalAbort)
    .then(failedChunkRetirementRetainsExactAbortUntilPushRetry)
    .then(postMutationGuardAbortsTheJustMutatedCandidateRevision)
    .then(indeterminatePostMutationRevisionNeverBlindlyAbortsANewerCandidate)
    .then(throwingCleanupPresenceNeverReplacesThePushFailure)
    .then(stoppedPushDoesNotStartRootPublication)
    .then(rootAcceptedDuringStopStillCommitsLocally)
    .then(pushUsesNewByteAndCountLimitsAfterEachBatchAck)
    .then(pushReadGroupsUseLiveConcurrencyWithoutSkippingFiles)
    .then(oversizedSourceDoesNotBlockIndependentNote)
    .then(onlyOversizedWorkDoesNotPublishRoot)
    .then(freshStatDriftStopsBeforeSourceAllocation)
    .then(workerFailureUsesBoundedDesktopRangeFallback)
    .then(failedReadKeepsNativeSiblingCharged)
    .then(knownSmallReadGroupsFollowLiveLimitsAndPreserveUploadOrder)
    .then(identityVerifiedDesktopReadPreservesAdmissionAndPortableFallback)
    .then(knownSmallReadsOnlyFirstMissingRepresentative)
    .then(failedKnownSmallReadJoinsEveryNativeSibling)
    .then(cancelledKnownReadDoesNotCrossHeldStatBoundary)
    .then(driftedKnownReadJoinsHeldNativeSibling)
    .then(boundedLookaheadOverlapsPreparationWithUpload)
    .then(failedConsumerCancelsAndJoinsLookahead)
    .then(speculativeCheckFailureFallsBackToOwningBatch)
    .then(rangedLookaheadOverlapsUploadAndReusesPreparedManifest)
    .then(mobileRangedUploadResumesWithoutWholeRead)
    .then(mobileLifecycleHideDuringRangedHashIsRetrySafe)
    .then(mobilePreparedRestartSkipsFastCdcButRehashesEveryByte)
    .then(mobilePreparedAdmissionRefusalNeverFallsBackToWholeRead)
    .then(mobileCapabilityFailureDefersWithoutBlocking)
    .then(mobileMidReadFailuresRemainRetrySafe)
    .then(() => { completed = true; console.log(`push-transaction.test: ${assertions} assertions passed`); })
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
