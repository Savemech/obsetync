import { allSettledBounded, pull, PullTreeRebaseError } from "./pull";
import type { FileDelta } from "./api";
import { PerfTrace } from "./perf-trace";
import { ResourceBudget } from "./resource-budget";
import { reserveTransientScope, type TransientWorkScope } from "./transient-memory";
import { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { CandidateMutationOutputAdmissionDeniedError } from "./candidate-mutation-output-admission";
import { CandidateOpenRootAdmissionDeniedError } from "./candidate-open-root-admission";
import { hasPendingTreeReachabilityRetirement } from "./tree-candidate-job";
import { TREE_CANDIDATE_MUTATION_STEP_UNITS } from "./tree-candidate-mutation-job";

const check = (condition: unknown, message: string) => {
    if (!condition) throw new Error(message);
};

function transactionalTree(tree: any): any {
    let active = false;
    return {
        ...tree,
        begin_candidate: () => {
            check(!active, "test tree candidate already active");
            active = true;
        },
        has_candidate: () => active,
        candidate_delete_batch: (json: string) => {
            check(active, "candidate delete without active transaction");
            tree.delete_batch(json);
        },
        candidate_update_batch: (json: string) => {
            check(active, "candidate update without active transaction");
            tree.update_batch(json);
        },
        commit_candidate: () => {
            check(active, "candidate commit without active transaction");
            active = false;
        },
        abort_candidate: () => {
            check(active, "candidate abort without active transaction");
            active = false;
        },
    };
}

function admittedV2PullTree() {
    type Entry = { path: string; hash: string; mtime_ms: number; size: number };
    type Phase = "idle" | "candidate-begin" | "candidate-retiring" |
        "plan" | "planned" | "build" | "ready" | "retiring";
    const committed = new Map<string, Entry>([["doc.md", {
        path: "doc.md", hash: "base-hash", mtime_ms: 1, size: 4,
    }]]);
    let candidate: Map<string, Entry> | null = null;
    let phase: Phase = "idle", token = 0, candidateRevision = 0, committedRevision = 0;
    let mutationKind: "delete" | "update" | null = null, mutationPayload = "";
    const control: { retirementFailure: Error | null; afterCommit?: () => void;
        abortOnSecondPostRetirementRevisionRead?: () => void } = {
        retirementFailure: null,
    };
    const stats = { begins: 0, resumes: 0, finishes: 0, retirements: 0, commits: 0, aborts: 0 };
    const plan = {
        schema: 1,
        scope: "v2-candidate-mutation-output",
        nodePayloadBytes: 70,
        rangeEndpointPeakRequestedBytes: 20,
        rangeEndpointResidentRequestedBytes: 10,
        peakAdmissionBytes: 90,
        residentAdmissionBytes: 80,
    } as const;
    const requireToken = (value: number) => {
        check(value === token && token > 0, "admitted pull used a stale mutation token");
    };
    const tree = {
        tree_version: () => 2,
        committed_revision: () => committedRevision,
        root_hash_hex: () => committed.has("doc.md") ? "base-root" : "server-root",
        candidate_revision: () => {
            if (control.abortOnSecondPostRetirementRevisionRead && stats.retirements > 0) {
                postRetirementRevisionReads++;
                if (postRetirementRevisionReads === 2) {
                    control.abortOnSecondPostRetirementRevisionRead();
                }
            }
            return candidateRevision;
        },
        has_candidate: () => candidate !== null,
        begin_candidate() { throw new Error("admitted pull used synchronous candidate begin"); },
        begin_candidate_job() {
            check(phase === "idle" && candidate === null, "candidate begin overlapped native work");
            phase = "candidate-begin"; token++; return token;
        },
        step_tree_job(value: number, units: number) {
            requireToken(value); check(units === 1 && phase === "candidate-begin", "invalid candidate begin step");
            return { done: true, units: 1, completed: 1, remaining: 0, reachable: 0 };
        },
        finish_candidate_job(value: number) {
            requireToken(value); check(phase === "candidate-begin", "invalid candidate begin finish");
            candidate = new Map(committed); candidateRevision++; phase = "idle"; return 0;
        },
        cancel_tree_job(value: number) {
            requireToken(value); check(phase === "candidate-begin", "invalid candidate begin cancel"); phase = "idle";
        },
        begin_candidate_delete_job(payload: string) {
            check(phase === "idle" && candidate !== null, "delete mutation opened without candidate ownership");
            mutationKind = "delete"; mutationPayload = payload; phase = "plan"; token++; stats.begins++; return token;
        },
        begin_candidate_update_job(payload: string) {
            check(phase === "idle" && candidate !== null, "update mutation opened without candidate ownership");
            mutationKind = "update"; mutationPayload = payload; phase = "plan"; token++; stats.begins++; return token;
        },
        finish_candidate_mutation_job() { throw new Error("admitted V2 pull used consuming mutation finish"); },
        step_candidate_mutation_output_memory_v1_job(value: number, units: number) {
            requireToken(value);
            check(units === TREE_CANDIDATE_MUTATION_STEP_UNITS, "invalid admitted mutation step budget");
            if (phase === "plan") {
                phase = "planned";
                return { done: false, units: 1, completed: 1, remaining: 1, reachable: 0, phase: "plan ready" };
            }
            check(phase === "build", "mutation output stepped before admission"); phase = "ready";
            return { done: true, units: 1, completed: 2, remaining: 0, reachable: 1, phase: "ready" };
        },
        candidate_mutation_output_memory_plan_v1_job(value: number) {
            requireToken(value); check(phase === "planned", "mutation plan read in wrong phase"); return { ...plan };
        },
        resume_candidate_mutation_output_memory_v1_job(value: number, node: number, peak: number, resident: number) {
            requireToken(value); check(phase === "planned", "mutation resumed in wrong phase");
            check(node === 70 && peak === 20 && resident === 10, "mutation resume witnesses changed");
            stats.resumes++; phase = "build";
        },
        candidate_mutation_output_memory_ready_v1_job(value: number) {
            requireToken(value); check(phase === "ready", "mutation Ready read in wrong phase");
            return { schema: 1, scope: "v2-candidate-mutation-output",
                stagedNodePayloadBytes: 40, rangeEndpointResidentRequestedBytes: 10,
                residentAdmissionBytes: 50 };
        },
        finish_candidate_mutation_job_deferred(value: number) {
            requireToken(value); check(phase === "ready" && candidate !== null, "mutation finish lost candidate");
            const target = candidate;
            if (!target) throw new Error("mutation finish lost candidate");
            if (mutationKind === "delete") {
                for (const path of JSON.parse(mutationPayload)) target.delete(path);
            } else {
                for (const entry of JSON.parse(mutationPayload)) target.set(entry.path, entry);
            }
            candidateRevision++; stats.finishes++; phase = "retiring";
        },
        cancel_candidate_mutation_job_deferred(value: number) {
            requireToken(value); check(["plan", "planned", "build", "ready"].includes(phase),
                "mutation cancel ran in wrong phase"); phase = "retiring";
        },
        step_tree_retirement(value: number, units: number) {
            requireToken(value); check(phase === "retiring" && units === 256, "invalid mutation retirement");
            stats.retirements++;
            if (control.retirementFailure) throw control.retirementFailure;
            phase = "idle";
            return { done: true, units: 1, completed: 1 };
        },
        candidate_delete_batch() { throw new Error("admitted V2 pull used legacy delete"); },
        candidate_update_batch() { throw new Error("admitted V2 pull used legacy update"); },
        commit_candidate() {
            check(phase === "idle" && candidate !== null, "candidate committed before mutation retirement");
            const target = candidate;
            if (!target) throw new Error("candidate committed without an owner");
            committed.clear(); for (const [path, entry] of target) committed.set(path, entry);
            candidate = null; candidateRevision++; stats.commits++; control.afterCommit?.();
        },
        abort_candidate() {
            check(phase === "idle" && candidate !== null, "candidate aborted before mutation retirement");
            candidate = null; candidateRevision++; stats.aborts++;
        },
    };
    let postRetirementRevisionReads = 0;
    const replaceCandidateForTest = () => {
        check(phase === "idle" && candidate !== null, "test replacement crossed active native work");
        candidate = new Map(candidate!);
        candidateRevision++;
    };
    const enableSettlementOutput = (admission: RootTreeResidentAdmission,
        targets: { commit: number; abort: number } = { commit: 0, abort: 100 }) => {
        const commit = tree.commit_candidate.bind(tree), abort = tree.abort_candidate.bind(tree);
        const settlement = { commits: 0, aborts: 0, legacy: 0 };
        const report = (outcome: "commit" | "abort", target: number, after: number) => {
            const endpoint = Math.min(10, target), before = Math.max(2, after + 1);
            return { schema: 1, scope: "v2-stable-tree-output", outcome, treeVersion: 2,
                before, reachable: after, removed: before - after, after, bytesRemoved: 0,
                committedRevision, candidateRevision, countersValid: true,
                nodePayloadBytes: target - endpoint,
                rangeEndpointResidentRequestedBytes: endpoint,
                residentAdmissionBytes: target };
        };
        (tree as any).commit_candidate_output_settlement_v1 = () => {
            commit(); committedRevision++; settlement.commits++;
            return report("commit", targets.commit, committed.size);
        };
        (tree as any).abort_candidate_output_settlement_v1 = () => {
            abort(); settlement.aborts++;
            return report("abort", targets.abort, committed.size);
        };
        tree.commit_candidate = () => { settlement.legacy++; throw new Error("atomic pull commit used legacy ABI"); };
        tree.abort_candidate = () => { settlement.legacy++; throw new Error("atomic pull abort used legacy ABI"); };
        admission.markV2GraphComplete(tree, committedRevision, candidateRevision);
        return settlement;
    };
    const enableOpenRoot = (admission: RootTreeResidentAdmission) => {
        const begin = tree.begin_candidate_job.bind(tree), finish = tree.finish_candidate_job.bind(tree);
        const open = { begins: 0, plans: 0, resumes: 0, finishes: 0, cancels: 0, retirements: 0,
            total: 30, identity: 8 };
        let validated = false, prepared = false, retiringCompleted = 0;
        Object.assign(tree, {
            begin_candidate_job() {
                const value = begin(); open.begins++;
                validated = false; prepared = false; retiringCompleted = 0;
                return value;
            },
            step_reachability_job_deferred(value: number, units: number) {
                requireToken(value);
                check(phase === "candidate-begin" && !validated && units === 1,
                    "invalid candidate-open validation step");
                validated = true;
                return { done: true, units: 1, completed: 1, remaining: 0, reachable: 1 };
            },
            candidate_open_memory_plan_v1_job(value: number) {
                requireToken(value);
                check(phase === "candidate-begin" && validated && !prepared,
                    "root plan did not precede native allocation");
                open.plans++;
                return { schema: 1, scope: "v2-candidate-open-root", residentChunkCount: 1,
                    rootStringCount: 4, rootIdentityRequestedBytes: open.identity,
                    rootEndpointRequestedBytes: open.total - open.identity,
                    rootStringRequestedBytes: open.total, baselineKeySnapshotRequestedBytes: 0,
                    peakAdmissionBytes: open.total, baselineStrategy: "insertion-generation-v1" };
            },
            resume_candidate_open_memory_v1_job(value: number, identity: number, endpoint: number, total: number) {
                requireToken(value);
                check(phase === "candidate-begin" && validated && !prepared,
                    "candidate-open resume occurred outside its ready input");
                check(identity === open.identity && endpoint === open.total - open.identity && total === open.total,
                    "candidate-open exact request witnesses changed");
                const memory = admission.snapshot();
                check(memory.privateBytes === open.total && memory.residentBytes === 100 &&
                    memory.ledger.usedBytes === 100 + open.total,
                "native candidate clone started without its separate exact root allowance");
                prepared = true; open.resumes++;
            },
            finish_candidate_job_deferred(value: number) {
                requireToken(value);
                check(prepared && admission.snapshot().privateBytes === open.total,
                    "candidate-open finish lost prepared root ownership");
                finish(value); phase = "candidate-retiring"; open.finishes++;
                return 1;
            },
            finish_candidate_chunks_job_deferred() { throw new Error("pull unexpectedly collected upload chunks"); },
            cancel_reachability_job_deferred(value: number) {
                requireToken(value);
                check(phase === "candidate-begin" || phase === "candidate-retiring", "invalid open cancellation");
                phase = "candidate-retiring"; open.cancels++;
            },
            step_reachability_retirement(value: number, expected: number, units: number) {
                requireToken(value);
                check(phase === "candidate-retiring" && expected === 0 && retiringCompleted === 0 && units === 256,
                    "candidate-open retirement overlapped or replayed another native job");
                const memory = admission.snapshot();
                check(memory.privateBytes === 0 && memory.residentBytes === 100 + (candidate ? open.total : 0),
                    "candidate-open publication did not attach its allowance before retirement");
                retiringCompleted = 1;
                return { done: true, units: 1, completed: 1 };
            },
            finish_reachability_retirement(value: number, expected: number) {
                requireToken(value);
                check(phase === "candidate-retiring" && expected === 1 && retiringCompleted === 1,
                    "candidate-open retirement acknowledged before actual completion");
                phase = "idle"; open.retirements++;
            },
            free() {
                check(phase === "idle" && candidate === null, "free crossed active candidate/native retirement");
                committed.clear();
            },
        });
        return open;
    };
    return { tree: tree as any, stats, plan, committed, control, replaceCandidateForTest,
        enableSettlementOutput, enableOpenRoot };
}

async function boundedApplyNeverExceedsTheSelectedLaneCount(): Promise<void> {
    let active = 0;
    let peak = 0;
    const results = await allSettledBounded([0, 1, 2, 3, 4, 5], 2, async (value) => {
        active++;
        peak = Math.max(peak, active);
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        active--;
        if (value === 3) throw new Error("injected apply failure");
        return value * 2;
    });
    check(peak === 2, `bounded apply used ${peak} lanes instead of 2`);
    check(results.length === 6, "bounded apply lost result slots");
    check(results[3].status === "rejected", "bounded apply lost an individual failure");
    check(results[4].status === "fulfilled" && results[4].value === 8, "result order drifted");
}

async function locallyEditedDeltaKeepsHonestBase(): Promise<void> {
    const remote: FileDelta = {
        action: "modified",
        path: "doc.md",
        hash: "remote-hash",
        size: 6,
        mtime_ms: 2,
    };
    const writes: unknown[] = [];
    const ioWrites: unknown[] = [];
    let saved = false;

    const api = {
        getDiff: async () => [remote],
        getRoot: async () => new Uint8Array([1]),
        getContent: async () => {
            throw new Error("locally edited content must not be downloaded");
        },
    } as any;
    const io = {
        renameFile: async (...args: unknown[]) => ioWrites.push(args),
        deleteFile: async (...args: unknown[]) => ioWrites.push(args),
        writeFile: async (...args: unknown[]) => ioWrites.push(args),
        stat: async () => null,
    } as any;
    const syncBase = {
        removeEntry: () => {},
        setEntry: () => {},
        setLastSyncTimestamp: () => {},
        save: async () => { saved = true; },
        allPaths: () => ["doc.md"],
        getEntry: () => ({ hash: "base-hash", mtime: 1, size: 4 }),
        getTreeMtime: () => 1,
    } as any;
    const tree = transactionalTree({
        root_hash_hex: () => "base-root",
        delete_batch: () => { throw new Error("deferred path reached tree delete"); },
        update_batch: () => { throw new Error("deferred path reached tree upsert"); },
    });
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
    } as any;

    const result = await pull(
        api,
        io,
        syncBase,
        "vault",
        "base-root",
        wasm,
        tree,
        undefined,
        (planned) => writes.push(...planned),
        new Set(["doc.md"]),
    );

    check(ioWrites.length === 0, "locally edited delta touched disk");
    check(writes.length === 0, "locally edited delta registered a pull echo");
    check(result.deferredCount === 1, "local delta was not reported deferred");
    check(result.localDeferredCount === 1, "local deferral reason was lost");
    check(result.treeParity === false, "deferred tree incorrectly reached server parity");
    check(saved, "sync-base checkpoint was not saved");
}

async function renameAfterCrashIsIdempotent(): Promise<void> {
    let renameCalls = 0;
    let checkpoints = 0;
    const removed: string[] = [];
    const set: string[] = [];
    const api = {
        getDiff: async () => [{
            action: "renamed",
            old_path: "old.md",
            path: "new.md",
            hash: "same-hash",
            size: 4,
            mtime_ms: 3,
        }],
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        exists: async (path: string) => path === "new.md",
        renameFile: async () => { renameCalls++; },
        deleteFile: async () => {},
        stat: async () => ({ mtime: 3, size: 4 }),
    } as any;
    const syncBase = {
        getEntry: (path: string) => path === "old.md"
            ? { hash: "same-hash", mtime: 1, size: 4 }
            : path === "new.md"
                ? { hash: "same-hash", mtime: 3, size: 4 }
                : null,
        getTreeMtime: () => 1,
        removeEntry: (path: string) => removed.push(path),
        setEntry: (path: string) => set.push(path),
        checkpoint: async () => { checkpoints++; },
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = transactionalTree({
        root_hash_hex: () => "tree-root",
        delete_batch: () => {},
        update_batch: () => {},
    });
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        wasm_hash: () => "remote-hash",
    } as any;

    await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    check(renameCalls === 0, "crash-resumed rename tried to move the missing source again");
    check(removed.includes("old.md"), "crash-resumed rename did not remove old base entry");
    check(set.includes("new.md"), "crash-resumed rename did not install target base entry");
    check(checkpoints === 1, "rename was not checkpointed immediately");
}

async function sameSizeUnverifiedRenameTargetIsDeferred(): Promise<void> {
    let removed = 0;
    let installed = 0;
    const api = {
        getDiff: async () => [{
            action: "renamed",
            old_path: "old.md",
            path: "new.md",
            hash: "expected-hash",
            size: 4,
            mtime_ms: 3,
        }],
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        exists: async (path: string) => path === "new.md",
        stat: async () => ({ mtime: 3, size: 4 }),
        readFile: async () => new Uint8Array([9, 9, 9, 9]),
        getAbsolutePath: () => null,
        deleteFile: async () => {},
    } as any;
    const syncBase = {
        getEntry: (path: string) => path === "old.md"
            ? { hash: "expected-hash", mtime: 1, size: 4 }
            : null,
        getTreeMtime: () => 1,
        removeEntry: () => { removed++; },
        setEntry: () => { installed++; },
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = transactionalTree({
        root_hash_hex: () => "tree-root",
        delete_batch: () => {},
        update_batch: () => {},
    });
    class Hasher {
        update(): void {}
        finalize(): string { return "wrong-hash"; }
        free(): void {}
    }
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        Hasher,
    } as any;

    const result = await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    check(result.deferredCount === 1, "unverified rename target was accepted by size alone");
    check(removed === 0, "unverified rename removed the source base entry");
    check(installed === 0, "unverified rename installed a false target hash");
}

async function firstSyncAdoptsAnExistingEmptyServerRoot(): Promise<void> {
    let rootFetches = 0;
    let treeBuilds = 0;
    const api = {
        getDiff: async () => [],
        getRoot: async () => {
            rootFetches++;
            return new Uint8Array([1]);
        },
    } as any;
    const io = {} as any;
    const syncBase = {
        allPaths: () => [],
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    let treeRoot: string | null = null;
    const tree = transactionalTree({
        root_hash_hex: () => treeRoot,
        build_from_entries: (json: string) => {
            check(json === "[]", "empty first sync built a non-empty tree");
            treeBuilds++;
            treeRoot = "empty-server-root";
        },
    });
    const wasm = {
        wasm_root_hash_from_bytes: () => "empty-server-root",
    } as any;

    const result = await pull(api, io, syncBase, "vault", null, wasm, tree);
    check(rootFetches === 1, "empty server root was not fetched on first sync");
    check(treeBuilds === 1, "empty local tree was not bootstrapped");
    check(result.newRootHash === "empty-server-root", "empty server root hash was not adopted");
    check(result.treeParity === true, "empty first sync did not establish exact parity");
}

async function ignoredUpsertsRemainExplicitRemoteOmissions(): Promise<void> {
    for (const mixed of [false, true]) {
        const ignored: FileDelta = {
            action: "added", path: "ignored/blob.bin", hash: "ignored-hash", size: 4, mtime_ms: 2,
        };
        const kept: FileDelta = {
            action: "added", path: "kept.md", hash: "kept-hash", size: 4, mtime_ms: 3,
        };
        let contentFetches = 0;
        let fence = false;
        let checkpoints = 0;
        const api = {
            getDiff: async () => mixed ? [ignored, kept] : [ignored],
            getRoot: async () => new Uint8Array([1]),
            getContent: async () => { contentFetches++; throw new Error("omitted/cache-hit path fetched"); },
        } as any;
        const io = {
            stat: async (path: string) => path === kept.path ? { mtime: 3, size: 4 } : null,
            getAbsolutePath: () => null,
            readFile: async () => new Uint8Array([1, 2, 3, 4]),
        } as any;
        const syncBase = {
            setVerifiedBaseRequired(value: boolean) {
                if (fence === value) return false;
                fence = value;
                return true;
            },
            checkpoint: async () => { checkpoints++; },
            getEntry: (path: string) => path === kept.path
                ? { hash: kept.hash, mtime: 3, size: 4 }
                : null,
            getTreeMtime: () => 3,
            setEntry: () => {},
            removeEntry: () => {},
            setLastSyncTimestamp: () => {},
            save: async () => {},
        } as any;
        const wasm = { wasm_root_hash_from_bytes: () => "server-root" } as any;
        const result = await pull(
            api, io, syncBase, "vault", null, wasm, null,
            undefined, undefined, undefined,
            (path) => path.startsWith("ignored/"),
        );
        check(result.remoteOmissionCount === 1,
            `${mixed ? "mixed" : "all-ignored"} pull lost its remote omission`);
        check(result.deferredCount === 0, "ignored omission was confused with a fetch failure");
        check(result.applied === (mixed ? 1 : 0), "kept cache hit was not accounted independently");
        check(result.downloaded === 0 && contentFetches === 0, "ignored/cache-hit pull fetched content");
        check(fence && checkpoints >= 1, "fresh ignored pull lacked a durable base fence");
    }
}

async function editArrivingDuringDownloadDefersRemoteWrite(): Promise<void> {
    let dirty = false;
    let writes = 0;
    const api = {
        getDiff: async () => [{
            action: "added",
            path: "live.md",
            hash: "remote-hash",
            size: 4,
            mtime_ms: 7,
        }],
        getContent: async () => {
            dirty = true; // editor event landed while the request was in flight
            return new Uint8Array([1, 2, 3, 4]);
        },
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        stat: async () => null,
        writeFile: async () => { writes++; },
        exists: async () => false,
        deleteFile: async () => {},
        getAbsolutePath: () => null,
    } as any;
    const syncBase = {
        getEntry: () => null,
        getTreeMtime: () => null,
        setEntry: () => {},
        removeEntry: () => {},
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = transactionalTree({
        root_hash_hex: () => "local-root",
        delete_batch: () => {},
        update_batch: () => {},
    });
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        wasm_hash: () => "remote-hash",
    } as any;

    const result = await pull(
        api,
        io,
        syncBase,
        "vault",
        "base-root",
        wasm,
        tree,
        undefined,
        undefined,
        { has: () => dirty },
    );
    check(writes === 0, "remote bytes overwrote an edit that arrived during download");
    check(result.deferredCount === 1, "mid-download local edit was not deferred");
    check(result.localDeferredCount === 1, "mid-download deferral lost its local reason");
}

async function untrackedLocalCollisionIsPreservedBeforePullWrite(): Promise<void> {
    const moves: Array<[string, string]> = [];
    let writes = 0;
    const api = {
        getDiff: async () => [{
            action: "added",
            path: "doc.md",
            hash: "remote-hash",
            size: 4,
            mtime_ms: 7,
        }],
        getContent: async () => new Uint8Array([1, 2, 3, 4]),
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        stat: async (path: string) => path === "doc.md" ? { mtime: 5, size: 4 } : null,
        readFile: async () => new Uint8Array([9, 9, 9, 9]),
        writeFile: async () => { writes++; },
        renameFile: async (from: string, to: string) => { moves.push([from, to]); },
        exists: async (path: string) => path === "doc.md",
        deleteFile: async () => {},
        getAbsolutePath: () => null,
    } as any;
    const syncBase = {
        getEntry: () => null,
        getTreeMtime: () => null,
        setEntry: () => {},
        removeEntry: () => {},
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = transactionalTree({
        root_hash_hex: () => "local-root",
        delete_batch: () => {},
        update_batch: () => {},
    });
    class Hasher {
        update(): void {}
        finalize(): string { return "local-hash"; }
        free(): void {}
    }
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        wasm_hash: () => "remote-hash",
        Hasher,
    } as any;

    await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    check(moves.length === 1 && moves[0][0] === "doc.md", "untracked local file was not preserved");
    check(moves[0][1].includes("(conflict local-before-pull "), "preserved file used an invisible name");
    check(writes === 1, "remote file was not installed after preserving local bytes");
}

async function unchangedBaseIsReplacedWithoutConflictCopy(): Promise<void> {
    let moves = 0;
    let writes = 0;
    const trace = new PerfTrace({ monitorEventLoop: false });
    const operation = trace.begin("pull");
    const api = {
        getDiff: async () => [{
            action: "modified",
            path: "doc.md",
            hash: "remote-hash",
            size: 4,
            mtime_ms: 8,
        }],
        getContent: async () => new Uint8Array([1, 2, 3, 4]),
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        stat: async () => ({ mtime: 5, size: 4 }),
        readFile: async () => new Uint8Array([5, 5, 5, 5]),
        writeFile: async () => { writes++; },
        renameFile: async () => { moves++; },
        exists: async (path: string) => path === "doc.md",
        deleteFile: async () => {},
        getAbsolutePath: () => null,
    } as any;
    const syncBase = {
        getEntry: () => ({ hash: "base-hash", mtime: 5, size: 4 }),
        getTreeMtime: () => 5,
        setEntry: () => {},
        removeEntry: () => {},
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = transactionalTree({
        root_hash_hex: () => "local-root",
        delete_batch: () => {},
        update_batch: () => {},
    });
    class Hasher {
        update(): void {}
        finalize(): string { return "base-hash"; }
        free(): void {}
    }
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        wasm_hash: () => "remote-hash",
        Hasher,
    } as any;

    await pull(
        api,
        io,
        syncBase,
        "vault",
        "base-root",
        wasm,
        tree,
        undefined,
        undefined,
        undefined,
        undefined,
        operation,
    );
    operation.finish();
    check(moves === 0, "unchanged sync base was preserved as a false conflict");
    check(writes === 1, "remote modification did not replace the unchanged base");
    const record = trace.recent()[0];
    check(record.filesTotal === 1, "pull trace lost delta count");
    check(record.bytesTotal === 4, "pull trace lost delta bytes");
    check(record.filesCompleted === 1, "pull trace lost applied file count");
    check(record.bytesTransferred === 4, "pull trace lost downloaded bytes");
    check(record.phases.download !== undefined, "pull trace missed download phase");
    check(record.phases.apply !== undefined, "pull trace missed apply phase");
}

async function offlineEditSurvivesRemoteDelete(): Promise<void> {
    let diskDeletes = 0;
    let baseRemovals = 0;
    let treeDeletes = 0;
    const api = {
        getDiff: async () => [{ action: "deleted", path: "doc.md" }],
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        stat: async () => ({ mtime: 2, size: 4 }),
        readFile: async () => new Uint8Array([9, 9, 9, 9]),
        getAbsolutePath: () => null,
        deleteFile: async () => { diskDeletes++; },
    } as any;
    const syncBase = {
        getEntry: () => ({ hash: "base-hash", mtime: 1, size: 4 }),
        getTreeMtime: () => 1,
        removeEntry: () => { baseRemovals++; },
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = transactionalTree({
        root_hash_hex: () => "base-root",
        delete_batch: () => { treeDeletes++; },
        update_batch: () => {},
    });
    class Hasher {
        update(): void {}
        finalize(): string { return "local-hash"; }
        free(): void {}
    }
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        Hasher,
    } as any;

    const result = await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    check(diskDeletes === 0, "remote delete erased an offline local edit");
    check(baseRemovals === 0, "deferred delete removed the honest sync base");
    check(treeDeletes === 0, "deferred delete advanced the Merkle tree");
    check(result.deferredCount === 1, "offline edit was not deferred from remote delete");
    check(result.localDeferredCount === 1, "offline delete deferral lost its local reason");
}

async function unchangedBaseAllowsRemoteDelete(): Promise<void> {
    let diskDeletes = 0;
    let baseRemovals = 0;
    let treeDeletes = 0;
    const api = {
        getDiff: async () => [{ action: "deleted", path: "doc.md" }],
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        stat: async () => ({ mtime: 1, size: 4 }),
        deleteFile: async () => { diskDeletes++; },
    } as any;
    const syncBase = {
        getEntry: () => ({ hash: "base-hash", mtime: 1, size: 4 }),
        getTreeMtime: () => 1,
        removeEntry: () => { baseRemovals++; },
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = transactionalTree({
        root_hash_hex: () => "base-root",
        delete_batch: () => { treeDeletes++; },
        update_batch: () => {},
    });
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
    } as any;

    const result = await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    check(diskDeletes === 1, "unchanged synced file was not deleted");
    check(baseRemovals === 1, "applied delete did not update sync-base");
    check(treeDeletes === 1, "applied delete did not update the Merkle tree");
    check(result.applied === 1, "applied delete was not counted");
    check(result.deferredCount === 0, "unchanged delete was falsely deferred");
}

async function failedTreeRebaseAbortsCandidate(): Promise<void> {
    let active = false;
    let begins = 0;
    let aborts = 0;
    let commits = 0;
    let candidateDeletes = 0;
    let rootFetches = 0;
    let baseSaves = 0;
    const failure = new Error("injected candidate rebase failure");
    const api = {
        getDiff: async () => [{ action: "deleted", path: "doc.md" }],
        getRoot: async () => { rootFetches++; return new Uint8Array([1]); },
    } as any;
    const io = {
        stat: async () => ({ mtime: 1, size: 4 }),
        deleteFile: async () => {},
    } as any;
    const syncBase = {
        getEntry: () => ({ hash: "base-hash", mtime: 1, size: 4 }),
        getTreeMtime: () => 1,
        removeEntry: () => {},
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => { baseSaves++; },
    } as any;
    const tree = {
        root_hash_hex: () => "base-root",
        begin_candidate: () => {
            check(!active, "candidate was already active");
            begins++;
            active = true;
        },
        has_candidate: () => active,
        candidate_delete_batch: () => {
            candidateDeletes++;
            throw failure;
        },
        candidate_update_batch: () => {},
        commit_candidate: () => {
            commits++;
            active = false;
        },
        abort_candidate: () => {
            aborts++;
            active = false;
        },
        delete_batch: () => { throw new Error("legacy committed delete used"); },
        update_batch: () => { throw new Error("legacy committed update used"); },
    } as any;
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
    } as any;

    let rejected: unknown;
    try {
        await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    } catch (error) {
        rejected = error;
    }
    check(rejected instanceof PullTreeRebaseError, "failed pull rebase did not propagate its typed repair fence");
    check((rejected as PullTreeRebaseError).rebaseCause === failure,
        "failed pull rebase lost its original failure witness");
    check(begins === 1, "pull rebase did not open exactly one candidate");
    check(candidateDeletes === 1, "pull rebase did not mutate the candidate tree");
    check(aborts === 1, "failed pull rebase did not abort its candidate");
    check(commits === 0, "failed pull rebase committed a partial tree");
    check(!active, "failed pull rebase leaked an active candidate");
    check(rootFetches === 0, "failed pull rebase fetched a root that could be falsely adopted");
    check(baseSaves === 0, "failed pull rebase checkpointed an unverified root/base state");
}

async function admittedV2RebaseRefusalRetiresAndRetriesFromCapacity(): Promise<void> {
    const native = admittedV2PullTree();
    const admission = new RootTreeResidentAdmission({ capacityBytes: native.plan.peakAdmissionBytes - 1 });
    let present = true, diffCalls = 0, rootFetches = 0, checkpoints = 0, saves = 0, timestamps = 0;
    let statCalls = 0, hostTurns = 0, liveEditArrived = false;
    const api = {
        getDiff: async () => { diffCalls++; return [{ action: "deleted", path: "doc.md" }]; },
        getRoot: async () => { rootFetches++; return new Uint8Array([2]); },
    } as any;
    const io = {
        stat: async () => { statCalls++; return present ? { mtime: 1, size: 4 } : null; },
        deleteFile: async () => { present = false; },
    } as any;
    let baseEntryPresent = true;
    const syncBase = {
        getEntry: () => baseEntryPresent ? { hash: "base-hash", mtime: 1, size: 4 } : null,
        getTreeMtime: () => 1,
        removeEntry: () => { baseEntryPresent = false; },
        checkpoint: async () => { checkpoints++; },
        setLastSyncTimestamp: () => { timestamps++; },
        save: async () => { saves++; },
    } as any;
    const wasm = { wasm_root_hash_from_bytes: () => "server-root" } as any;
    let ownerCurrent = true;
    let holdRetirement = false;
    let retirementEnteredResolve!: () => void;
    let releaseRetirementResolve!: () => void;
    const retirementEntered = new Promise<void>(resolve => { retirementEnteredResolve = resolve; });
    const releaseRetirement = new Promise<void>(resolve => { releaseRetirementResolve = resolve; });
    const context = {
        residentAdmission: admission,
        cooperate: async () => {
            hostTurns++;
            // This models a newer editor event after remote disk/base apply.
            // Rebase must not re-read disk and fold that live overlay into its
            // remote-base candidate.
            liveEditArrived = true;
            await Promise.resolve();
        },
        cooperateRetirement: async () => {
            if (!holdRetirement) return;
            retirementEnteredResolve();
            await releaseRetirement;
        },
        assertCurrent: () => { check(ownerCurrent, "admitted pull resumed a stale tree owner"); },
    };
    const run = () => pull(api, io, syncBase, "vault", "base-root", wasm, native.tree,
        undefined, undefined, undefined, undefined, undefined, undefined, context);
    try {
        const cleanupFailure = new Error("injected retained pull mutation cleanup");
        native.control.retirementFailure = cleanupFailure;
        let refusal: unknown;
        try { await run(); } catch (error) { refusal = error; }
        check(refusal instanceof PullTreeRebaseError, "V2 admission refusal escaped the pull repair fence");
        check((refusal as PullTreeRebaseError).rebaseCause instanceof CandidateMutationOutputAdmissionDeniedError,
            "V2 pull did not fail at the output admission barrier");
        check(native.committed.has("doc.md"), "refused V2 mutation changed the committed tree");
        check(native.tree.has_candidate() === true,
            "failed retirement discarded the quarantined candidate before exact cleanup retry");
        check(admission.snapshot().ledger.usedBytes === 0, "refused V2 mutation leaked its reservation");
        check(rootFetches === 0 && saves === 0 && timestamps === 0,
            "refused V2 mutation advanced root/timestamp persistence");
        check(checkpoints === 1 && !baseEntryPresent && !present,
            "refusal test did not retain the legitimately applied remote disk/base row");
        check(native.stats.begins === 1 && native.stats.resumes === 0 &&
            native.stats.retirements >= 1 && native.stats.aborts === 0,
        "failed cleanup did not retain the exact candidate abort debt");

        native.control.retirementFailure = null;
        admission.setCapacity(native.plan.peakAdmissionBytes);
        holdRetirement = true;
        const retry = run();
        await retirementEntered;
        check(diffCalls === 1 && rootFetches === 0 && statCalls === 1,
            "retry crossed transport or disk before retained mutation cleanup");
        holdRetirement = false;
        releaseRetirementResolve();
        const result = await retry;
        check(result.treeParity === true, "capacity retry did not reproduce the server root");
        check(native.committed.size === 0 && native.tree.has_candidate() === false,
            "capacity retry did not atomically commit the remote deletion");
        check(native.stats.begins === 2 && native.stats.resumes === 1 && native.stats.finishes === 1 &&
            native.stats.retirements >= 3 && native.stats.commits === 1 && native.stats.aborts === 1,
        "capacity retry did not preserve exact V2 mutation lifecycle counts");
        check(admission.snapshot().residentBytes === 50,
            "successful V2 pull did not retain the Ready-sized resident cohort");
        check(diffCalls === 2 && rootFetches === 1 && saves === 1 && timestamps === 1,
            "capacity retry advanced pull bookkeeping more than once");
        check(statCalls === 2 && liveEditArrived && hostTurns >= 4,
            "admitted mutation reread disk after a live edit or failed to yield");
        ownerCurrent = false;
        admission.releaseResidentAfterFree(native.tree);
    } finally {
        releaseRetirementResolve();
        admission.close();
    }
}

async function admittedPullExactSettlementReleasesEmptyGraph(): Promise<void> {
    const native = admittedV2PullTree();
    const admission = new RootTreeResidentAdmission({ capacityBytes: 190 });
    const initial = admission.reserve(native.tree, { peakBytes: 100, residentBytes: 100 })!;
    admission.ready(initial); admission.releaseRetired(admission.publish(initial));
    const settlement = native.enableSettlementOutput(admission);
    let present = true, baseEntryPresent = true, rootFetches = 0, saves = 0;
    const result = await pull({
        getDiff: async () => [{ action: "deleted", path: "doc.md" }],
        getRoot: async () => { rootFetches++; return new Uint8Array([2]); },
    } as any, {
        stat: async () => present ? { mtime: 1, size: 4 } : null,
        deleteFile: async () => { present = false; },
    } as any, {
        getEntry: () => baseEntryPresent ? { hash: "base-hash", mtime: 1, size: 4 } : null,
        getTreeMtime: () => 1,
        removeEntry: () => { baseEntryPresent = false; },
        checkpoint: async () => {}, setLastSyncTimestamp: () => {},
        save: async () => { saves++; },
    } as any, "vault", "base-root", {
        wasm_root_hash_from_bytes: () => "server-root",
    } as any, native.tree, undefined, undefined, undefined, undefined, undefined, undefined, {
        residentAdmission: admission,
        cooperate: async () => { await Promise.resolve(); },
        cooperateRetirement: async () => { await Promise.resolve(); },
        assertCurrent: () => {},
    });
    check(result.treeParity === true && native.committed.size === 0,
        "proof-enabled pull did not commit the exact remote graph");
    check(rootFetches === 1 && saves === 1 && !present && !baseEntryPresent,
        "proof-enabled pull skipped or repeated durable remote bookkeeping");
    check(settlement.commits === 1 && settlement.aborts === 0 && settlement.legacy === 0,
        "proof-enabled pull did not use exactly one atomic commit");
    const snapshot = admission.snapshot();
    check(snapshot.residentBytes === 0 && snapshot.ledger.usedBytes === 0 &&
        snapshot.ledger.activeLeases === 0,
    "empty committed V2 graph retained its obsolete admitted output charge");
    admission.releaseResidentAfterFree(native.tree); admission.close();
}

async function candidateOpenRootAdmissionGatesActualPullAndSettlesExactly(): Promise<void> {
    for (const denied of [true, false]) {
        const native = admittedV2PullTree();
        const admission = new RootTreeResidentAdmission({ capacityBytes: denied ? 129 : 220 });
        const initial = admission.reserve(native.tree, { peakBytes: 100, residentBytes: 100 })!;
        admission.ready(initial); admission.releaseRetired(admission.publish(initial));
        const settlement = native.enableSettlementOutput(admission);
        const open = native.enableOpenRoot(admission);
        let present = true, baseEntryPresent = true, rootFetches = 0, saves = 0;
        let checkpoints = 0, timestamps = 0, cooperativeTurns = 0;
        let result: Awaited<ReturnType<typeof pull>> | undefined, rejected: unknown;
        try {
            try {
                result = await pull({
                    getDiff: async () => [{ action: "deleted", path: "doc.md" }],
                    getRoot: async () => { rootFetches++; return new Uint8Array([2]); },
                } as any, {
                    stat: async () => present ? { mtime: 1, size: 4 } : null,
                    deleteFile: async () => { present = false; },
                } as any, {
                    getEntry: () => baseEntryPresent ? { hash: "base-hash", mtime: 1, size: 4 } : null,
                    getTreeMtime: () => 1,
                    removeEntry: () => { baseEntryPresent = false; },
                    checkpoint: async () => { checkpoints++; },
                    setLastSyncTimestamp: () => { timestamps++; },
                    save: async () => { saves++; },
                } as any, "vault", "base-root", {
                    wasm_root_hash_from_bytes: () => "server-root",
                } as any, native.tree, undefined, undefined, undefined, undefined, undefined, undefined, {
                    residentAdmission: admission,
                    cooperate: async () => { cooperativeTurns++; await Promise.resolve(); },
                    cooperateRetirement: async () => { cooperativeTurns++; await Promise.resolve(); },
                    assertCurrent: () => {},
                });
            } catch (error) { rejected = error; }
            check(open.begins === 1 && open.plans === 1 && open.retirements === 1,
                "actual pull did not use and drain the new candidate-open ABI exactly once");
            check(!hasPendingTreeReachabilityRetirement(native.tree) && !native.tree.has_candidate(),
                "actual pull retained candidate-open cleanup or a live candidate after its terminal result");
            check(checkpoints === 1 && !present && !baseEntryPresent,
                "candidate-open outcome lost the legitimately applied remote disk/base row");
            if (denied) {
                check(rejected instanceof PullTreeRebaseError &&
                    rejected.rebaseCause instanceof CandidateOpenRootAdmissionDeniedError,
                "candidate-open denial did not propagate the exact typed pull repair gate");
                const cause = (rejected as PullTreeRebaseError).rebaseCause as CandidateOpenRootAdmissionDeniedError;
                check(cause.requestedBytes === open.total && cause.usedBytes === 100 && cause.capacityBytes === 129,
                    "pull root admission refused the wrong requested owner");
                check(result === undefined && open.resumes === 0 && open.finishes === 0 && open.cancels === 1,
                    "denied root clone reached native resume or rebase publication");
                check(native.stats.begins === 0 && native.stats.commits === 0 && native.stats.aborts === 0 &&
                    settlement.commits === 0 && settlement.aborts === 0 && native.committed.has("doc.md"),
                "root denial changed committed state or reached downstream mutation/settlement");
                check(rootFetches === 0 && saves === 0 && timestamps === 0,
                    "root denial fetched/adopted/checkpointed an unverified server root");
                check(admission.snapshot().ledger.usedBytes === 100 && admission.snapshot().residentBytes === 100,
                    "root denial released old resident ownership or leaked a new reservation");
            } else {
                check(rejected === undefined && result?.treeParity === true && native.committed.size === 0,
                    "admitted root open did not complete actual pull's exact rebase");
                check(open.resumes === 1 && open.finishes === 1 && open.cancels === 0 &&
                    native.stats.resumes === 1 && native.stats.finishes === 1 && native.stats.retirements === 1,
                "successful pull skipped/repeated root allocation or mutation cleanup");
                check(settlement.commits === 1 && settlement.aborts === 0 && settlement.legacy === 0,
                    "opened root did not pass through exactly one atomic terminal settlement");
                const memory = admission.snapshot();
                check(memory.privateAttempts === 0 && memory.retiringOwners === 0 &&
                    memory.privateBytes === 0 && memory.retiringBytes === 0 &&
                    memory.residentBytes === open.identity && memory.ledger.usedBytes === open.identity &&
                    memory.ledger.activeLeases === 1 && memory.ledger.peakUsedBytes === 220,
                "empty stable graph did not retain exactly its surviving root IDs after old+root+mutation admission");
                check(rootFetches === 1 && saves === 1 && timestamps === 1 && cooperativeTurns >= 4,
                    "successful admitted open skipped final pull bookkeeping or cooperation");
            }
            native.tree.free();
            admission.releaseResidentAfterFree(native.tree);
            const freed = admission.snapshot();
            check(freed.ledger.usedBytes === 0 && freed.ledger.activeLeases === 0 &&
                freed.privateAttempts === 0 && freed.retiringOwners === 0 && freed.residentTrees === 0,
            "candidate-open pull outcome leaked charged ownership after proven free");
        } finally { admission.close(); }
    }
}

async function stoppedAfterAdmittedCommitCannotFetchOrAdoptRoot(): Promise<void> {
    const native = admittedV2PullTree();
    const admission = new RootTreeResidentAdmission({ capacityBytes: native.plan.peakAdmissionBytes });
    const controller = new AbortController();
    const stopped = new Error("stopped after admitted pull commit");
    native.control.afterCommit = () => { queueMicrotask(() => controller.abort(stopped)); };
    let present = true, rootFetches = 0, saves = 0, timestamps = 0;
    const api = {
        getDiff: async () => [{ action: "deleted", path: "doc.md" }],
        getRoot: async () => { rootFetches++; return new Uint8Array([2]); },
    } as any;
    const io = {
        stat: async () => present ? { mtime: 1, size: 4 } : null,
        deleteFile: async () => { present = false; },
    } as any;
    let baseEntryPresent = true, checkpoints = 0;
    const syncBase = {
        getEntry: () => baseEntryPresent ? { hash: "base-hash", mtime: 1, size: 4 } : null,
        getTreeMtime: () => 1,
        removeEntry: () => { baseEntryPresent = false; },
        checkpoint: async () => { checkpoints++; },
        setLastSyncTimestamp: () => { timestamps++; },
        save: async () => { saves++; },
    } as any;
    try {
        let rejected: unknown;
        try {
            await pull(api, io, syncBase, "vault", "base-root",
                { wasm_root_hash_from_bytes: () => "server-root" } as any, native.tree,
                undefined, undefined, undefined, undefined, undefined, undefined, {
                    signal: controller.signal,
                    residentAdmission: admission,
                    cooperate: async () => { await Promise.resolve(); },
                    cooperateRetirement: async () => { await Promise.resolve(); },
                    assertCurrent: () => {},
                });
        } catch (error) { rejected = error; }
        check(rejected === stopped, "post-commit stop did not preserve its exact operation reason");
        check(native.stats.commits === 1 && native.stats.aborts === 0 && native.committed.size === 0,
            "post-commit stop rolled back or lost the valid committed remote graph");
        check(rootFetches === 0 && saves === 0 && timestamps === 0,
            "stale post-commit continuation fetched or adopted a server root");
        check(checkpoints === 1 && !baseEntryPresent && !present,
            "post-commit stop lost the already durable remote disk/base change");
        check(admission.snapshot().residentBytes === 50,
            "post-commit stop released candidate output still owned by the committed tree");
        admission.releaseResidentAfterFree(native.tree);
    } finally {
        admission.close();
    }
}

async function synchronousStopDuringFinalCandidateWitnessCannotCommit(): Promise<void> {
    const native = admittedV2PullTree();
    const admission = new RootTreeResidentAdmission({ capacityBytes: native.plan.peakAdmissionBytes });
    const controller = new AbortController();
    const stopped = new Error("stopped inside final pull candidate witness");
    native.control.abortOnSecondPostRetirementRevisionRead = () => controller.abort(stopped);
    let present = true, rootFetches = 0, saves = 0, timestamps = 0;
    const api = {
        getDiff: async () => [{ action: "deleted", path: "doc.md" }],
        getRoot: async () => { rootFetches++; return new Uint8Array([2]); },
    } as any;
    const io = {
        stat: async () => present ? { mtime: 1, size: 4 } : null,
        deleteFile: async () => { present = false; },
    } as any;
    let baseEntryPresent = true, checkpoints = 0;
    const syncBase = {
        getEntry: () => baseEntryPresent ? { hash: "base-hash", mtime: 1, size: 4 } : null,
        getTreeMtime: () => 1,
        removeEntry: () => { baseEntryPresent = false; },
        checkpoint: async () => { checkpoints++; },
        setLastSyncTimestamp: () => { timestamps++; },
        save: async () => { saves++; },
    } as any;
    try {
        let rejected: unknown;
        try {
            await pull(api, io, syncBase, "vault", "base-root",
                { wasm_root_hash_from_bytes: () => "server-root" } as any, native.tree,
                undefined, undefined, undefined, undefined, undefined, undefined, {
                    signal: controller.signal,
                    residentAdmission: admission,
                    cooperate: async () => { await Promise.resolve(); },
                    cooperateRetirement: async () => { await Promise.resolve(); },
                    assertCurrent: () => {},
                });
        } catch (error) { rejected = error; }
        check(rejected instanceof PullTreeRebaseError && rejected.rebaseCause === stopped,
            "synchronous final-witness stop lost its typed pull repair fence");
        check(native.stats.commits === 0 && native.stats.aborts === 1 && native.committed.has("doc.md"),
            "synchronous final-witness stop committed or leaked its stale candidate");
        check(rootFetches === 0 && saves === 0 && timestamps === 0,
            "synchronous final-witness stop advanced root bookkeeping");
        check(checkpoints === 1 && !baseEntryPresent && !present,
            "synchronous final-witness stop lost the durable remote base change");
        check(admission.snapshot().residentBytes === 50,
            "synchronous final-witness stop released resident output still owned by the tree store");
        admission.releaseResidentAfterFree(native.tree);
    } finally {
        admission.close();
    }
}

async function admittedPullNeverAbortsAReplacementCandidate(): Promise<void> {
    const native = admittedV2PullTree();
    const admission = new RootTreeResidentAdmission({ capacityBytes: native.plan.peakAdmissionBytes });
    let replaced = false, rootFetches = 0, saves = 0;
    const api = {
        getDiff: async () => [{ action: "deleted", path: "doc.md" }],
        getRoot: async () => { rootFetches++; return new Uint8Array([2]); },
    } as any;
    let present = true, baseEntryPresent = true;
    const io = {
        stat: async () => present ? { mtime: 1, size: 4 } : null,
        deleteFile: async () => { present = false; },
    } as any;
    const syncBase = {
        getEntry: () => baseEntryPresent ? { hash: "base-hash", mtime: 1, size: 4 } : null,
        getTreeMtime: () => 1,
        removeEntry: () => { baseEntryPresent = false; },
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => { saves++; },
    } as any;
    try {
        let rejected: unknown;
        try {
            await pull(api, io, syncBase, "vault", "base-root",
                { wasm_root_hash_from_bytes: () => "server-root" } as any, native.tree,
                undefined, undefined, undefined, undefined, undefined, undefined, {
                    residentAdmission: admission,
                    cooperate: async () => { await Promise.resolve(); },
                    cooperateRetirement: async () => {
                        if (!replaced && native.stats.finishes === 1) {
                            replaced = true;
                            native.replaceCandidateForTest();
                        }
                    },
                    assertCurrent: () => {},
                });
        } catch (error) { rejected = error; }
        check(rejected instanceof PullTreeRebaseError,
            "candidate replacement did not stop the stale pull transaction");
        check(replaced && native.tree.has_candidate() === true,
            "ABA fixture did not leave the replacement candidate owned");
        check(native.stats.commits === 0 && native.stats.aborts === 0,
            "stale pull committed or blindly aborted a newer candidate revision");
        check(rootFetches === 0 && saves === 0,
            "stale ABA continuation advanced root bookkeeping");
        check(native.committed.has("doc.md") && !baseEntryPresent && !present,
            "ABA handling changed the committed graph or lost applied remote state");
        native.tree.abort_candidate();
        admission.releaseResidentAfterFree(native.tree);
    } finally {
        admission.close();
    }
}

async function smallMissesUseOneVerifiedBulkDownload(): Promise<void> {
    const hashes = ["01".repeat(32), "02".repeat(32), "03".repeat(32)];
    const deltas: FileDelta[] = hashes.map((hash, index) => ({
        action: "added",
        path: `bulk-${index}.md`,
        hash,
        size: 1,
        mtime_ms: index + 1,
    }));
    let bulkCalls = 0;
    let singleCalls = 0;
    const api = {
        getDiff: async () => deltas,
        getObjects: async (_kind: number, requested: string[]) => {
            bulkCalls++;
            return new Map(requested.map((hash, index) => [hash, new Uint8Array([index + 1])]));
        },
        getContent: async () => {
            singleCalls++;
            throw new Error("single GET must not run");
        },
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const files = new Map<string, Uint8Array>();
    const io = {
        stat: async (path: string) => {
            const data = files.get(path);
            return data ? { mtime: 10, size: data.length } : null;
        },
        writeFile: async (path: string, data: Uint8Array) => files.set(path, data.slice()),
        getAbsolutePath: (path: string) => `/vault/${path}`,
        exists: async (path: string) => files.has(path),
        renameFile: async () => {},
    } as any;
    const syncBase = {
        getEntry: () => null,
        getTreeMtime: () => null,
        setEntry: () => {},
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = transactionalTree({
        root_hash_hex: () => "base-root",
        delete_batch: () => {},
        update_batch: () => {},
    });
    const wasm = {
        wasm_hash: (data: Uint8Array) => hashes[data[0] - 1],
        wasm_root_hash_from_bytes: () => "server-root",
    } as any;

    let heavyBatchPermits = 0;
    const result = await pull(
        api,
        io,
        syncBase,
        "vault",
        "base-root",
        wasm,
        tree,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        async () => { heavyBatchPermits++; },
    );
    check(bulkCalls === 1, "three small misses were not coalesced into one bulk GET");
    check(singleCalls === 0, "bulk pull fell through to a single GET");
    check(files.size === 3, "bulk pull did not apply every verified record");
    check(result.downloaded === 3, "bulk pull lost physical-file progress");
    check(heavyBatchPermits === 1, "pull bypassed its bounded download permit");
}

async function ownedSmallBatchRetainsAllNativeSiblingsAndCheckpoint(): Promise<void> {
    const hashes = ["a".repeat(64), "b".repeat(64), "c".repeat(64)];
    const deltas: FileDelta[] = hashes.map((hash, index) => ({
        action: "added", path: `owned-${index}.md`, hash, size: 1, mtime_ms: 2,
    }));
    const perApplyBytes = 64 * 1024 + 3;
    const ownerBytes = 3;
    const workBytes = 2 * perApplyBytes;
    const budget = new ResourceBudget({ capacityBytes: ownerBytes + workBytes });
    let memory: TransientWorkScope | undefined;
    let releases = 0;
    let requests = 0;
    let activeWrites = 0;
    let peakWrites = 0;
    let localRead = false;
    const deferred = () => {
        let resolve!: () => void;
        const promise = new Promise<void>((done) => { resolve = done; });
        return { promise, resolve };
    };
    const enteredWrite = deferred();
    const releaseWrites = deferred();
    const enteredCheckpoint = deferred();
    const releaseCheckpoint = deferred();
    let firstCheckpoint = true;
    const files = new Map<string, Uint8Array>([[deltas[0].path, new Uint8Array([9])]]);
    const api = {
        getDiff: async () => deltas,
        getRoot: async () => new Uint8Array([1]),
        getObjectsOwned: async (_kind: number, requested: string[]) => {
            requests++;
            check(localRead, "owned admission happened before local hash preparation");
            memory = await reserveTransientScope({ ownerBytes, workBytes }, { budget });
            const ownedMemory = memory;
            const objects = new Map(requested.map((hash) => [hash, new Uint8Array([hashes.indexOf(hash) + 1])]));
            return { objects, memory, release: () => {
                check(activeWrites === 0, "owner released while a sibling native write was alive");
                releases++;
                objects.clear();
                ownedMemory.close();
            } };
        },
        getContent: async () => { throw new Error("owned pull bypassed admission with getContent"); },
        getObjects: async () => { throw new Error("owned pull bypassed admission with getObjects"); },
    } as any;
    const io = {
        getAbsolutePath: () => null,
        stat: async (path: string) => files.has(path) ? { mtime: 1, size: files.get(path)!.length } : null,
        readFile: async (path: string) => {
            check(budget.snapshot().usedBytes === 0, "local hashing nested under complete download lease");
            localRead = true;
            return files.get(path)!;
        },
        writeFile: async (path: string, data: Uint8Array) => {
            check(budget.snapshot().usedBytes > 0, "native apply started without download ownership");
            check(memory!.snapshot().work.usedBytes >= perApplyBytes, "native apply lacked copy quota");
            if (path === deltas[1].path) throw new Error("injected native apply failure");
            activeWrites++;
            peakWrites = Math.max(peakWrites, activeWrites);
            enteredWrite.resolve();
            await releaseWrites.promise;
            files.set(path, data.slice());
            activeWrites--;
        },
        exists: async (path: string) => files.has(path),
        renameFile: async () => { throw new Error("unchanged old base must not become conflict"); },
    } as any;
    const syncBase = {
        getEntry: (path: string) => path === deltas[0].path ? { hash: "old-base", size: 1, mtime: 1 } : null,
        getTreeMtime: () => 1,
        setEntry: () => {},
        checkpoint: async () => {
            if (!firstCheckpoint) return;
            firstCheckpoint = false;
            check(activeWrites === 0 && budget.snapshot().usedBytes > 0,
                "checkpoint did not retain drained download ownership");
            enteredCheckpoint.resolve();
            await releaseCheckpoint.promise;
        },
        setLastSyncTimestamp: () => {}, save: async () => {},
    } as any;
    const wasm = {
        wasm_hash: (data: Uint8Array) => hashes[data[0] - 1],
        wasm_root_hash_from_bytes: () => "server-root",
        Hasher: class { update(): void {} finalize(): string { return "old-base"; } free(): void {} },
    } as any;
    const tree = transactionalTree({ root_hash_hex: () => "base-root", delete_batch: () => {}, update_batch: () => {} });
    const pulling = pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    await enteredWrite.promise;
    for (let i = 0; i < 12; i++) await Promise.resolve();
    check(releases === 0, "individual apply failure released another native consumer");
    check(peakWrites <= 2, "parallel apply exceeded admitted independent copy quota");
    releaseWrites.resolve();
    await enteredCheckpoint.promise;
    check(releases === 0 && budget.snapshot().usedBytes > 0, "download owner released before checkpoint settled");
    releaseCheckpoint.resolve();
    const result = await pulling;
    check(requests === 2 && releases === 2, "failed item retry did not use/release independent owned API");
    check(result.deferredCount === 1 && !files.has(deltas[1].path), "failed file was falsely applied");
    check(files.get(deltas[2].path)?.[0] === 3, "one apply failure prevented a sibling file");
    check(budget.snapshot().usedBytes === 0, "owned pull leaked admission");
}

void boundedApplyNeverExceedsTheSelectedLaneCount()
    .then(locallyEditedDeltaKeepsHonestBase)
    .then(renameAfterCrashIsIdempotent)
    .then(sameSizeUnverifiedRenameTargetIsDeferred)
    .then(firstSyncAdoptsAnExistingEmptyServerRoot)
    .then(ignoredUpsertsRemainExplicitRemoteOmissions)
    .then(editArrivingDuringDownloadDefersRemoteWrite)
    .then(untrackedLocalCollisionIsPreservedBeforePullWrite)
    .then(unchangedBaseIsReplacedWithoutConflictCopy)
    .then(offlineEditSurvivesRemoteDelete)
    .then(unchangedBaseAllowsRemoteDelete)
    .then(failedTreeRebaseAbortsCandidate)
    .then(admittedV2RebaseRefusalRetiresAndRetriesFromCapacity)
    .then(admittedPullExactSettlementReleasesEmptyGraph)
    .then(candidateOpenRootAdmissionGatesActualPullAndSettlesExactly)
    .then(stoppedAfterAdmittedCommitCannotFetchOrAdoptRoot)
    .then(synchronousStopDuringFinalCandidateWitnessCannotCommit)
    .then(admittedPullNeverAbortsAReplacementCandidate)
    .then(smallMissesUseOneVerifiedBulkDownload)
    .then(ownedSmallBatchRetainsAllNativeSiblingsAndCheckpoint)
    .then(() => console.log("pull.test: regression scenarios passed"))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
