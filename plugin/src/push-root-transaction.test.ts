import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { push, type FileChange, type PushRootContext, type PushPreparedContext,
    type PushCommittedBootstrap, type WasmTree } from "./push";
import { ObsetyncSyncBase } from "./sync-base";
import { MemorySegmentedIO } from "./segmented-store-test-io";
import { BulkObjectKind, type BulkUploadRecord } from "./bulk-codec";
import { HashWorkerFileDriftError } from "./desktop-hash-workers";
import type { RootRecoveryResult } from "./root-recovery";
import { ROOT_MAX_BYTES } from "./root-outcome";
import { getHashTuning, configureHashTuning } from "./hash-runtime";
import { TREE_CHUNK_EXPORT_PAGE_BYTES as EXPORT_PAGE } from "./tree-chunk-export-job";
import { TREE_ROOT_EXPORT_PAGE_BYTES as ROOT_EXPORT_PAGE } from "./tree-root-export-job";
import { transientMemorySnapshot, type TransientWorkScope } from "./transient-memory";
import { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { CandidateMutationOutputAdmissionDeniedError } from "./candidate-mutation-output-admission";
import { hasPendingTreeCandidateMutationRetirement,
    TREE_CANDIDATE_MUTATION_STEP_UNITS } from "./tree-candidate-mutation-job";
import { TREE_CANDIDATE_JOB_STEP_UNITS } from "./tree-candidate-job";

(globalThis as any).window ??= globalThis;
let assertions = 0;
const check = (value: unknown, message: string) => { assertions++; assert.ok(value, message); };
const same = (actual: unknown, expected: unknown, message: string) => { assertions++; assert.deepEqual(actual, expected, message); };
async function rejects(work: Promise<unknown>, message: RegExp, label: string) {
    assertions++; await assert.rejects(work, message, label);
}
const hash = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const h = (value: number) => value.toString(16).padStart(64, "0");
const bytes = (...values: number[]) => new Uint8Array(values);
type Candidate = Parameters<PushRootContext["publish"]>[0];
type CapturedCandidate = Omit<Candidate, "rootExport"> & { rootBytes: Uint8Array };
type TreeEntry = { path: string; hash: string; mtime_ms: number; size: number };
const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value));
function gate() {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
}

/** Actual push + actual segmented sync-base. The synthetic tree/hash/network
 * ports expose ordering and ownership, not native/WASM/transport conformance. */
async function fixture() {
    const disk = new MemorySegmentedIO(), base = new ObsetyncSyncBase({ vault: { adapter: disk } } as any);
    await base.load();
    const files = new Map<string, { bytes: Uint8Array; mtime: number; declaredSize?: number }>([
        ["keep.md", { bytes: bytes(1), mtime: 1 }], ["gone.md", { bytes: bytes(2, 3), mtime: 2 }],
        ["old.md", { bytes: bytes(4, 5, 6), mtime: 3 }],
    ]);
    let committed = new Map<string, TreeEntry>();
    for (const [path, source] of files) {
        const row = { path, hash: hash(source.bytes), mtime_ms: source.mtime, size: source.bytes.length };
        committed.set(path, row); base.setEntry(path, row.hash, row.mtime_ms, row.size);
    }
    // Keep the synthetic root above two pages so every durable transaction
    // exercises the actual paged root-export bridge rather than a one-shot mock.
    const rootPadding = "r".repeat(2 * ROOT_EXPORT_PAGE + 37);
    const serialize = (entries: Map<string, TreeEntry>) => new TextEncoder().encode(JSON.stringify({ version: 2,
        entries: [...entries.values()].sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0),
        padding: rootPadding }));
    const initialRoot = hash(serialize(committed));
    base.setTreeBaseRoot(initialRoot); await base.save();
    let legacyCalls = 0;
    const legacy = () => { legacyCalls++; throw new Error("unexpected legacy base mutation/save"); };
    base.setEntry = legacy; base.removeEntry = legacy; base.setLastSyncTimestamp = legacy;
    base.setTreeBaseRoot = legacy; base.save = async () => legacy();
    let candidate: Map<string, TreeEntry> | null = null, candidateRevision = 0, committedRevision = 0;
    const events: string[] = [], checks: string[] = [], reads: string[] = [];
    const uploaded: BulkUploadRecord[] = [];
    const counts = { begin: 0, commit: 0, abort: 0, delete: 0, update: 0,
        candidateRootBytes: 0, legacyRoot: 0, publish: 0, indexRequests: 0,
        contentCheckRequests: 0, contentCheckedObjects: 0,
        contentPutRequests: 0, contentPutObjects: 0, contentPutBytes: 0,
        maxContentCheckObjects: 0, maxContentPutObjects: 0, maxContentPutBytes: 0 };
    const jobCounts = { begin: 0, step: 0, finish: 0, cancel: 0 };
    const chunkJobCounts = { begin: 0, step: 0, finish: 0, cancel: 0 };
    const mutationJobCounts = { begin: 0, step: 0, finish: 0, cancel: 0, kinds: [] as string[] };
    const exportCounts = { begin: 0, read: 0, finish: 0, cancel: 0, legacy: 0,
        offsets: [] as number[], lengths: [] as number[] };
    const rootExportCounts = { begin: 0, plan: 0, build: 0, read: 0, finish: 0, cancel: 0,
        offsets: [] as number[], lengths: [] as number[] };
    let beforeMutation: (() => void) | undefined, beforeMutationStep: (() => void) | undefined;
    let beforeChunkStep: (() => void) | undefined;
    const tree: WasmTree = {
        tree_version: () => 2,
        set_tree_version: () => { throw new Error("unexpected tree version switch"); },
        root_hash_hex: () => hash(serialize(committed)), root_bytes: () => serialize(committed),
        total_files: () => committed.size,
        begin_candidate: () => {
            counts.begin++; check(candidate === null, "a second candidate was opened");
            candidate = new Map([...committed].map(([path, row]) => [path, { ...row }]));
            candidateRevision++;
        },
        has_candidate: () => candidate !== null,
        candidate_revision: () => candidateRevision,
        candidate_root_hash_hex: () => candidate ? hash(serialize(candidate)) : null,
        candidate_root_bytes: () => { counts.candidateRootBytes++; return candidate ? serialize(candidate) : null; },
        candidate_total_files: () => candidate?.size ?? 0,
        candidate_delete_batch: value => {
            beforeMutation?.(); events.push("candidate-delete"); counts.delete++;
            assert.ok(candidate); const paths = JSON.parse(value) as string[];
            for (const path of paths) candidate.delete(path);
            if (paths.length) candidateRevision++;
        },
        candidate_update_batch: value => {
            beforeMutation?.(); events.push("candidate-update"); counts.update++;
            assert.ok(candidate); const entries = JSON.parse(value) as TreeEntry[];
            for (const row of entries) candidate.set(row.path, { ...row });
            if (entries.length) candidateRevision++;
        },
        commit_candidate: () => {
            check(activeJob === null, "candidate committed while an export/tree job still owned it");
            events.push("candidate-commit"); counts.commit++; assert.ok(candidate);
            committed = candidate; candidate = null;
            candidateRevision++;
            return { before: 3, reachable: committed.size, after: committed.size, removed: 0, bytes_removed: 0 };
        },
        abort_candidate: () => {
            check(activeJob === null, "candidate aborted before the active export/tree job was cancelled");
            events.push("candidate-abort"); counts.abort++; assert.ok(candidate); candidate = null;
            candidateRevision++;
            return { before: 3, reachable: committed.size, after: committed.size, removed: 0, bytes_removed: 0 };
        },
        update_entry: legacy, delete_entry: legacy, update_batch: legacy, delete_batch: legacy,
        build_from_entries: legacy, rebuild_from_entries_in_version: legacy, load_root: legacy,
    };
    let indexBytes = bytes(91, 92, 93), indexHash = hash(indexBytes);
    let withIndex = true;
    let activeJob: number | null = null;
    let activeJobKind: "begin" | "chunks" | "update" | "delete" | "export" | "root-export" | null = null;
    let activeMutationPayload = "", nextJob = 1, jobStep = 0;
    let candidateJobEnabled = false;
    let rootExportBytes = new Uint8Array(), rootExportOffset = 0;
    let rootExportPhase: "plan" | "planned" | "build" | "built" | "read" | null = null;
    tree.begin_candidate_root_export_job = maxArenaBytes => {
        check(activeJob === null && activeJobKind === null && candidate !== null,
            "candidate root export began without exclusive candidate ownership");
        rootExportBytes = serialize(candidate!); rootExportOffset = 0; rootExportPhase = "plan";
        check(maxArenaBytes >= rootExportBytes.byteLength,
            "candidate root export was admitted below its synthetic arena");
        rootExportCounts.begin++; events.push("root-export-begin");
        activeJobKind = "root-export"; activeJob = nextJob++; return activeJob;
    };
    tree.begin_committed_root_export_job = () => { throw new Error("unexpected committed root export"); };
    tree.step_root_export_job = (token, maxUnits, maxBytes) => {
        check(token === activeJob && activeJobKind === "root-export" && candidate !== null &&
            maxUnits >= 1 && maxBytes >= 256,
        "root export step lost token, operation/byte budget or candidate ownership");
        if (rootExportPhase === "plan") {
            rootExportCounts.plan++; rootExportPhase = "planned";
        } else if (rootExportPhase === "build") {
            rootExportCounts.build++; rootExportPhase = "built";
        } else throw new Error("root export stepped outside plan/build");
        return { done: true, units: 1, bytes: 256, completed: 1, processed: 256 };
    };
    tree.root_export_workset = token => {
        check(token === activeJob && activeJobKind === "root-export" && rootExportPhase === "planned",
            "root export workset read before completed plan");
        return { max_length: rootExportBytes.byteLength, offset_bytes: 0 };
    };
    tree.start_root_export_build_job = token => {
        check(token === activeJob && activeJobKind === "root-export" && rootExportPhase === "planned",
            "root export build started without its planned owner");
        rootExportPhase = "build";
    };
    tree.root_export_info = token => {
        check(token === activeJob && activeJobKind === "root-export" && rootExportPhase === "built",
            "root export info read before completed build");
        rootExportPhase = "read";
        return { length: rootExportBytes.byteLength, version: 2, hash: hash(rootExportBytes) };
    };
    tree.read_root_export_job = (token, offset, maxBytes) => {
        check(token === activeJob && activeJobKind === "root-export" && rootExportPhase === "read" &&
            candidate !== null && offset === rootExportOffset && maxBytes === ROOT_EXPORT_PAGE,
        "root export page read lost token, offset, cap or candidate ownership");
        const part = rootExportBytes.slice(offset, Math.min(rootExportBytes.byteLength, offset + maxBytes));
        rootExportCounts.read++; rootExportCounts.offsets.push(offset); rootExportCounts.lengths.push(part.byteLength);
        rootExportOffset += part.byteLength; events.push(`root-export-read:${offset}`); return part;
    };
    tree.finish_root_export_job = token => {
        check(token === activeJob && activeJobKind === "root-export" && rootExportPhase === "read" &&
            candidate !== null && rootExportOffset === rootExportBytes.byteLength,
        "root export finished without EOF or its candidate owner");
        rootExportCounts.finish++; events.push("root-export-finish");
        activeJob = null; activeJobKind = null; rootExportPhase = null; rootExportBytes = new Uint8Array();
    };
    tree.cancel_tree_job = token => {
        check(token === activeJob && activeJobKind === "root-export" && candidate !== null,
            "root export cancellation lost its candidate owner");
        rootExportCounts.cancel++; events.push("root-export-cancel");
        activeJob = null; activeJobKind = null; rootExportPhase = null; rootExportBytes = new Uint8Array();
    };
    const enableCandidateJob = () => {
        if (candidateJobEnabled) return;
        candidateJobEnabled = true;
        tree.begin_candidate_job = () => {
            check(activeJob === null && activeJobKind === null && candidate === null,
                "candidate job overlapped provisional tree state");
            jobCounts.begin++; jobStep = 0; activeJobKind = "begin"; activeJob = nextJob++; return activeJob;
        };
        tree.begin_candidate_chunks_job = () => {
            check(activeJob === null && activeJobKind === null && candidate !== null,
                "candidate chunk job lost candidate ownership");
            chunkJobCounts.begin++; jobStep = 0; activeJobKind = "chunks"; activeJob = nextJob++; return activeJob;
        };
        const beginMutation = (kind: "update" | "delete", payload: string) => {
            check(activeJob === null && activeJobKind === null && candidate !== null,
                "candidate mutation job lost candidate ownership");
            mutationJobCounts.begin++; mutationJobCounts.kinds.push(kind); jobStep = 0;
            activeJobKind = kind; activeMutationPayload = payload; activeJob = nextJob++; return activeJob;
        };
        tree.begin_candidate_update_job = payload => beginMutation("update", payload);
        tree.begin_candidate_delete_job = payload => beginMutation("delete", payload);
        tree.step_tree_job = (token, maxUnits) => {
            const candidateStateMatches = activeJobKind === "begin" ? candidate === null : candidate !== null;
            const stepUnits = activeJobKind === "update" || activeJobKind === "delete"
                ? TREE_CANDIDATE_MUTATION_STEP_UNITS : TREE_CANDIDATE_JOB_STEP_UNITS;
            check(token === activeJob && maxUnits === stepUnits &&
                activeJobKind !== null && activeJobKind !== "export" && candidateStateMatches,
                "candidate job step lost token, budget or isolation");
            const counters = activeJobKind === "begin" ? jobCounts : activeJobKind === "chunks"
                ? chunkJobCounts : mutationJobCounts;
            counters.step++; jobStep++;
            if (activeJobKind === "update" || activeJobKind === "delete") beforeMutationStep?.();
            if (activeJobKind === "chunks") beforeChunkStep?.();
            const reachable = activeJobKind === "chunks" && withIndex ? 1 : 0;
            return jobStep < 3
                ? { done: false, units: 1, completed: jobStep, remaining: 3 - jobStep, reachable: 0 }
                : { done: true, units: 1, completed: jobStep, remaining: 0, reachable };
        };
        tree.finish_candidate_job = token => {
            check(token === activeJob && activeJobKind === "begin" && jobStep === 3 && candidate === null,
                "candidate job finished before validation or over an existing candidate");
            jobCounts.finish++; activeJob = null; activeJobKind = null;
            candidate = new Map([...committed].map(([path, row]) => [path, { ...row }]));
            candidateRevision++;
            return 0;
        };
        tree.finish_candidate_chunks_job = token => {
            check(token === activeJob && activeJobKind === "chunks" && jobStep === 3 && candidate !== null,
                "candidate chunk job finished before traversal or without a candidate");
            chunkJobCounts.finish++; activeJob = null; activeJobKind = null;
            const hashes = withIndex ? [indexHash] : [];
            return { all: hashes, fresh: hashes };
        };
        tree.finish_candidate_mutation_job = token => {
            check(token === activeJob && (activeJobKind === "update" || activeJobKind === "delete") &&
                jobStep === 3 && candidate !== null,
            "candidate mutation job finished before traversal or without a candidate");
            const kind = activeJobKind;
            mutationJobCounts.finish++;
            if (kind === "delete") {
                events.push("candidate-delete"); counts.delete++;
                for (const path of JSON.parse(activeMutationPayload) as string[]) candidate!.delete(path);
            } else {
                events.push("candidate-update"); counts.update++;
                for (const row of JSON.parse(activeMutationPayload) as TreeEntry[]) candidate!.set(row.path, { ...row });
            }
            candidateRevision++;
            activeJob = null; activeJobKind = null; activeMutationPayload = "";
        };
        const cancelRootExport = tree.cancel_tree_job!;
        tree.cancel_tree_job = token => {
            if (activeJobKind === "root-export") { cancelRootExport(token); return; }
            const candidateStateMatches = activeJobKind === "begin" ? candidate === null : candidate !== null;
            check(token === activeJob && activeJobKind !== null && activeJobKind !== "export" && candidateStateMatches,
                "candidate job cancel lost ownership");
            const counters = activeJobKind === "begin" ? jobCounts : activeJobKind === "chunks"
                ? chunkJobCounts : mutationJobCounts;
            counters.cancel++;
            activeJob = null; activeJobKind = null; activeMutationPayload = "";
        };
    };
    const enableIndexExport = (payload: Uint8Array) => {
        enableCandidateJob();
        indexBytes = payload.slice(); indexHash = hash(indexBytes);
        let exportOffset = 0;
        tree.begin_tree_chunk_export_job = requested => {
            check(requested === indexHash && activeJob === null && activeJobKind === null && candidate !== null,
                "index export began without the selected candidate/hash owner");
            exportCounts.begin++; events.push("export-begin"); exportOffset = 0;
            activeJobKind = "export"; activeJob = nextJob++;
            return { token: activeJob, length: indexBytes.length };
        };
        tree.read_tree_chunk_export_job = (token, at, maxBytes) => {
            check(token === activeJob && activeJobKind === "export" && candidate !== null &&
                at === exportOffset && maxBytes === EXPORT_PAGE,
            "index page read lost token, offset, cap or candidate ownership");
            const part = indexBytes.slice(at, Math.min(indexBytes.length, at + maxBytes));
            exportCounts.read++; exportCounts.offsets.push(at); exportCounts.lengths.push(part.length);
            events.push(`export-read:${at}`); exportOffset += part.length;
            return part;
        };
        tree.finish_tree_chunk_export_job = token => {
            check(token === activeJob && activeJobKind === "export" && candidate !== null &&
                exportOffset === indexBytes.length, "index export finished without EOF or current owner");
            exportCounts.finish++; events.push("export-finish"); activeJob = null; activeJobKind = null;
        };
        const cancelPrevious = tree.cancel_tree_job!;
        tree.cancel_tree_job = token => {
            if (activeJobKind !== "export") { cancelPrevious(token); return; }
            check(token === activeJob && candidate !== null, "index export cancellation lost its live candidate owner");
            exportCounts.cancel++; events.push("export-cancel"); activeJob = null; activeJobKind = null;
        };
    };
    /** Synthetic native ABI only: real push/driver/admission own all policy
     * transitions. The fixture never reserves, shrinks or releases a lease. */
    const enableMutationOutput = (admission: RootTreeResidentAdmission) => {
        enableCandidateJob();
        const plans = {
            delete: { nodePayloadBytes: 600, rangeEndpointPeakRequestedBytes: 400,
                rangeEndpointResidentRequestedBytes: 100, peakAdmissionBytes: 1000, residentAdmissionBytes: 700 },
            update: { nodePayloadBytes: 500, rangeEndpointPeakRequestedBytes: 400,
                rangeEndpointResidentRequestedBytes: 50, peakAdmissionBytes: 900, residentAdmissionBytes: 550 },
        };
        const actualNodes = { delete: 400, update: 300 };
        type Kind = keyof typeof plans;
        const points: { kind: Kind; stage: string; snapshot: ReturnType<RootTreeResidentAdmission["snapshot"]> }[] = [];
        const witnesses: number[][] = [];
        let resumed = false, retiring = false, retirementCompleted = 0;
        let retirementFailure: unknown, postResumeFailure: unknown;
        let replaceAfterRetirement = false;
        const kind = (): Kind => {
            check(activeJobKind === "delete" || activeJobKind === "update", "output ABI lost its mutation kind");
            return activeJobKind as Kind;
        };
        const observe = (stage: string) => {
            points.push({ kind: kind(), stage, snapshot: admission.snapshot() });
            events.push(`mutation-output:${kind()}:${stage}`);
        };
        const beginUpdate = tree.begin_candidate_update_job!, beginDelete = tree.begin_candidate_delete_job!;
        const begin = (beginJob: (payload: string) => number, payload: string) => {
            const token = beginJob(payload); resumed = false; retiring = false; retirementCompleted = 0;
            return token;
        };
        tree.begin_candidate_update_job = payload => begin(beginUpdate, payload);
        tree.begin_candidate_delete_job = payload => begin(beginDelete, payload);
        tree.step_candidate_mutation_output_memory_v1_job = (token, maxUnits) => {
            check(token === activeJob && candidate !== null && !retiring &&
                maxUnits === TREE_CANDIDATE_MUTATION_STEP_UNITS,
                "output step lost its token, candidate or unit budget");
            check(jobStep === 0 || resumed, "output allocation preceded its admitted resume");
            if (resumed && kind() === "update" && postResumeFailure !== undefined) throw postResumeFailure;
            mutationJobCounts.step++; jobStep++;
            return { done: jobStep === 3, units: 1, completed: jobStep,
                remaining: jobStep === 3 ? 0 : 1, reachable: jobStep - 1,
                phase: jobStep === 1 ? "plan ready" : jobStep === 2 ? "emit" : "ready" };
        };
        tree.candidate_mutation_output_memory_plan_v1_job = token => {
            check(token === activeJob && !resumed && !retiring && jobStep === 1,
                "output plan read outside its zero-output barrier");
            observe("plan");
            return { schema: 1, scope: "v2-candidate-mutation-output", ...plans[kind()] };
        };
        tree.resume_candidate_mutation_output_memory_v1_job = (token, node, peak, resident) => {
            check(token === activeJob && !resumed && !retiring && jobStep === 1,
                "output resumed outside its plan barrier");
            const plan = plans[kind()];
            same([node, peak, resident], [plan.nodePayloadBytes, plan.rangeEndpointPeakRequestedBytes,
                plan.rangeEndpointResidentRequestedBytes], "push changed the exact native admission witnesses");
            witnesses.push([node, peak, resident]); observe("resume"); resumed = true;
        };
        tree.candidate_mutation_output_memory_ready_v1_job = token => {
            check(token === activeJob && resumed && !retiring && jobStep === 3,
                "actual output summary read before Ready");
            observe("ready");
            const selected = kind(), endpoint = plans[selected].rangeEndpointResidentRequestedBytes;
            return { schema: 1, scope: "v2-candidate-mutation-output", stagedNodePayloadBytes: actualNodes[selected],
                rangeEndpointResidentRequestedBytes: endpoint, residentAdmissionBytes: actualNodes[selected] + endpoint };
        };
        const finishMutation = tree.finish_candidate_mutation_job!;
        tree.finish_candidate_mutation_job_deferred = token => {
            check(token === activeJob && resumed && !retiring && jobStep === 3,
                "output finished without its admitted Ready owner");
            observe("finish");
            const selected = kind();
            finishMutation(token); // Apply the actual synthetic candidate delete/update, not just its counters.
            activeJob = token; activeJobKind = selected; retiring = true;
        };
        tree.cancel_candidate_mutation_job_deferred = token => {
            check(token === activeJob && candidate !== null, "deferred cancellation lost its exact candidate owner");
            kind();
            if (!retiring) { mutationJobCounts.cancel++; observe("cancel"); retiring = true; }
        };
        tree.step_tree_retirement = (token, maxUnits) => {
            check(token === activeJob && candidate !== null && retiring && maxUnits === 256,
                "retirement lost its still-live candidate/token/budget");
            if (retirementFailure !== undefined && kind() === "update") throw retirementFailure;
            retirementCompleted++; observe(`retire-${retirementCompleted}`);
            const done = retirementCompleted === 2;
            if (done) {
                activeJob = null; activeJobKind = null; activeMutationPayload = ""; retiring = false;
                if (replaceAfterRetirement) {
                    candidate = new Map([...candidate!].map(([path, row]) => [path, { ...row }]));
                    candidateRevision += 2; replaceAfterRetirement = false; events.push("mutation-retirement-aba");
                }
            }
            return { done, units: 1, completed: retirementCompleted };
        };
        // Keep old entrypoints as strict tripwires: merely exposing the new
        // methods must route production mutations through the whole family.
        tree.finish_candidate_mutation_job = () => { throw new Error("output mutation used synchronous finish"); };
        const genericStep = tree.step_tree_job!;
        tree.step_tree_job = (token, units) => {
            check(activeJobKind !== "delete" && activeJobKind !== "update", "output mutation used generic stepping");
            return genericStep(token, units);
        };
        return { points, witnesses,
            setRetirementFailure(value: unknown) { retirementFailure = value; },
            setPostResumeFailure(value: unknown) { postResumeFailure = value; },
            replaceCandidateAfterRetirement() { replaceAfterRetirement = true; },
        };
    };
    const enableSettlementOutput = (admission: RootTreeResidentAdmission, options: {
        commitBytes?: number; abortBytes?: number; malformedCommit?: boolean;
    } = {}) => {
        const commitBytes = options.commitBytes ?? 350, abortBytes = options.abortBytes ?? 300;
        const nativeCommit = tree.commit_candidate.bind(tree), nativeAbort = tree.abort_candidate.bind(tree);
        const settlement = { commits: 0, aborts: 0, legacy: 0 };
        const report = (outcome: "commit" | "abort", target: number, before: number, after: number) => {
            const endpoint = Math.min(50, target);
            return { schema: 1, scope: "v2-stable-tree-output", outcome, treeVersion: 2,
                before, reachable: after, removed: before - after, after, bytesRemoved: 0,
                committedRevision, candidateRevision, countersValid: true,
                nodePayloadBytes: target - endpoint,
                rangeEndpointResidentRequestedBytes: endpoint,
                residentAdmissionBytes: target };
        };
        tree.committed_revision = () => committedRevision;
        tree.commit_candidate_output_settlement_v1 = () => {
            const before = Math.max(4, (candidate?.size ?? 0) + 1);
            nativeCommit(); committedRevision++; settlement.commits++;
            if (options.malformedCommit) return null;
            return report("commit", commitBytes, before, committed.size);
        };
        tree.abort_candidate_output_settlement_v1 = () => {
            const before = Math.max(4, committed.size + 1);
            nativeAbort(); settlement.aborts++;
            return report("abort", abortBytes, before, committed.size);
        };
        tree.commit_candidate = () => { settlement.legacy++; throw new Error("atomic commit fell back to legacy ABI"); };
        tree.abort_candidate = () => { settlement.legacy++; throw new Error("atomic abort fell back to legacy ABI"); };
        admission.markV2GraphComplete(tree, committedRevision, candidateRevision);
        return settlement;
    };
    const wasm = {
        wasm_hash: hash, wasm_should_chunk: () => false,
        wasm_root_hash_from_bytes: (value: Uint8Array) => hash(value),
        wasm_root_version_from_bytes: (value: Uint8Array) => JSON.parse(new TextDecoder().decode(value)).version,
        wasm_hash_batch: (data: Uint8Array, offsets: Uint32Array, sizes: Uint32Array) =>
            [...offsets].map((offset, index) => hash(data.subarray(offset, offset + sizes[index]))),
        wasm_tree_committed_chunk_hashes: () => [],
        wasm_tree_candidate_chunk_hashes: () => withIndex ? [indexHash] : [],
        wasm_tree_new_candidate_chunk_hashes: () => withIndex ? [indexHash] : [],
        wasm_tree_chunk_byte_length: () => indexBytes.length,
        wasm_tree_get_chunk: () => { exportCounts.legacy++; return indexBytes.slice(); },
    } as any;
    const hooks: { beforeUpload?: (records: BulkUploadRecord[], memory?: TransientWorkScope) => Promise<void>;
        afterContentCheck?: (hashes: readonly string[]) => void | Promise<void>;
        afterSourceRead?: (path: string) => void | Promise<void>;
        afterIndexAck?: () => void;
        legacyRoot?: (rootBytes: Uint8Array, parentRoot: string) => Promise<never> } = {};
    const io = {
        getAbsolutePath: () => null,
        stat: async (path: string) => {
            const source = files.get(path); return source ? { mtime: source.mtime, size: source.declaredSize ?? source.bytes.length } : null;
        },
        readFile: async (path: string) => {
            reads.push(path); const source = files.get(path);
            assert.ok(source, "unexpected missing source read");
            check(source.declaredSize === undefined, "oversized source reached an actual whole-file read");
            await hooks.afterSourceRead?.(path);
            return source.bytes;
        },
    } as any;
    const api = {
        ensureTransportReady: async () => { events.push("transport"); },
        checkContent: async (hashes: string[]) => {
            counts.contentCheckRequests++; counts.contentCheckedObjects += hashes.length;
            counts.maxContentCheckObjects = Math.max(counts.maxContentCheckObjects, hashes.length);
            checks.push(...hashes); await hooks.afterContentCheck?.(hashes); return hashes;
        },
        checkChunks: async (hashes: string[]) => hashes,
        putObjects: async (records: BulkUploadRecord[], _perf?: unknown, memory?: TransientWorkScope) => {
            if (records.some(record => record.kind === BulkObjectKind.IndexChunk)) counts.indexRequests++;
            const content = records.filter(record => record.kind !== BulkObjectKind.IndexChunk);
            if (content.length > 0) {
                const contentBytes = content.reduce((sum, record) => sum + record.data.byteLength, 0);
                counts.contentPutRequests++; counts.contentPutObjects += content.length;
                counts.contentPutBytes += contentBytes;
                counts.maxContentPutObjects = Math.max(counts.maxContentPutObjects, content.length);
                counts.maxContentPutBytes = Math.max(counts.maxContentPutBytes, contentBytes);
            }
            await hooks.beforeUpload?.(records, memory);
            for (const record of records) {
                check(record.hash === hash(record.data), "root candidate depended on bytes not named by actual uploaded hash");
                uploaded.push({ ...record, data: record.data.slice() });
                events.push(record.kind === BulkObjectKind.IndexChunk ? "index-ack" : "content-ack");
            }
            if (records.some(record => record.kind === BulkObjectKind.IndexChunk)) hooks.afterIndexAck?.();
        },
        putRoot: async (_vault: string, rootBytes: Uint8Array, parentRoot: string) => {
            counts.legacyRoot++;
            if (hooks.legacyRoot) return hooks.legacyRoot(rootBytes, parentRoot);
            throw new Error("unexpected legacy putRoot");
        },
    } as any;
    const context = (publish: PushRootContext["publish"], groups: readonly (readonly string[])[] = [],
        slice?: PushRootContext["slice"]): PushRootContext => ({
        groups, rootByteLimit: ROOT_MAX_BYTES, slice,
        assertApplicable(path) { events.push(`guard:${path}`); },
        async publish(value, assertCandidateCurrent) {
            counts.publish++; events.push("publish"); return publish(value, assertCandidateCurrent);
        },
    });
    const apply = async (value: Candidate) => {
        await base.commitRootPublication({ identity: { scopeHash: h(1), sequence: 1, mutationId: "1".repeat(32), requestHash: h(2) },
            candidateRoot: value.candidateRoot, entries: value.entries, committedAt: 123 });
        events.push("durable-base");
    };
    const submit = (changes: FileChange[], root: PushRootContext, signal?: AbortSignal,
        prepared?: PushPreparedContext, beforeHeavyBatch?: () => Promise<void>, admission?: RootTreeResidentAdmission,
        bootstrap?: PushCommittedBootstrap) =>
        push(api, io, base, wasm, tree, "vault", changes, initialRoot,
            undefined, undefined, undefined, beforeHeavyBatch, signal, prepared, root, bootstrap, admission);
    const submitLegacy = (changes: FileChange[], signal?: AbortSignal) =>
        push(api, io, base, wasm, tree, "vault", changes, initialRoot,
            undefined, undefined, undefined, undefined, signal);
    const replaceCandidateABA = () => {
        assert.ok(candidate, "candidate ABA requires a live candidate");
        const beforeHash = hash(serialize(candidate)), beforeFiles = candidate.size;
        candidate = new Map([...candidate].map(([path, row]) => [path, { ...row }]));
        // Model two visible replacements A -> B -> A while preserving the
        // semantic hash and count observed by the legacy fallback.
        candidateRevision += 2; events.push("candidate-aba");
        check(hash(serialize(candidate)) === beforeHash && candidate.size === beforeFiles,
            "candidate ABA helper changed semantic identity");
    };
    // `cancel_tree_job` is shared by every native job family, so exposing the
    // complete root exporter also requires a complete candidate job surface.
    enableCandidateJob();
    return { disk, base, files, tree, wasm, io, api, hooks, counts, jobCounts, chunkJobCounts, mutationJobCounts,
        events, checks, reads, uploaded,
        context, apply, submit, submitLegacy, initialRoot, enableCandidateJob, enableIndexExport, exportCounts,
        rootExportCounts, replaceCandidateABA, enableMutationOutput, enableSettlementOutput,
        exportActive: () => activeJobKind === "export",
        rootExportActive: () => activeJobKind === "root-export",
        legacyCalls: () => legacyCalls, treeEntries: () => [...committed.values()], disableIndex: () => { withIndex = false; },
        beforeMutation: (hook: () => void) => { beforeMutation = hook; },
        beforeMutationStep: (hook: () => void) => { beforeMutationStep = hook; },
        beforeChunkStep: (hook: () => void) => { beforeChunkStep = hook; } };
}
const accepted = (candidate: Candidate): RootRecoveryResult => ({ status: "accepted", candidateRoot: candidate.candidateRoot, observedRoot: h(99) });
function noLegacy(f: Awaited<ReturnType<typeof fixture>>): void {
    check(f.counts.candidateRootBytes === 0 && f.counts.legacyRoot === 0 && f.legacyCalls() === 0,
        "durable branch invoked legacy candidate bytes/root/base mutation/save");
}
function released(owner: Candidate["rootExport"] | undefined, label: string): void {
    check(owner !== undefined, `${label}: publication did not expose an export owner`);
    assertions++; assert.throws(() => owner!.bytes, /released/, `${label}: raw root alias remained readable`);
    const memory = owner!.memory.snapshot();
    check(memory.ownerClosed && memory.parentReleased, `${label}: export admission remained live`);
}
function modify(f: Awaited<ReturnType<typeof fixture>>, path: string, data = bytes(10, 11, 12), mtime = 40): FileChange {
    f.files.set(path, { bytes: data, mtime });
    return { path, action: "modified", size: data.length, mtime };
}

async function exactAcceptedCandidateAndBaseOwnership(): Promise<void> {
    const f = await fixture(), changed = modify(f, "keep.md"), created = modify(f, "z-new.md", bytes(20, 21), 50);
    created.action = "created";
    const changes: FileChange[] = [created, { action: "deleted", path: "gone.md" }, changed];
    let received: CapturedCandidate | undefined, owner: Candidate["rootExport"] | undefined;
    f.beforeMutation(() => check(changes.every(change => f.events.includes(`guard:${change.path}`)),
        "candidate changed before every selected path's applicability guard"));
    const root = f.context(async value => {
        owner = value.rootExport;
        const { rootExport: _, ...metadata } = value;
        received = { ...metadata, rootBytes: value.rootExport.bytes.slice(), entries: copy(value.entries) };
        check(!value.rootExport.memory.snapshot().parentReleased, "accepted callback received an already released root owner");
        check(f.events.includes("index-ack") && f.uploaded.filter(record => record.kind === BulkObjectKind.Content).length === 2,
            "root request preceded content/index ACKs");
        check(f.base.getHash("keep.md") !== hash(f.files.get("keep.md")!.bytes) && f.base.getEntry("gone.md") !== null,
            "push applied ordinary base metadata before coordinator publication");
        check(changes.every(change => f.events.filter(event => event === `guard:${change.path}`).length >= 2),
            "repeated all-path applicability guard did not precede request");
        await f.apply(value); return accepted(value);
    });
    const prepared: PushPreparedContext = { plan: {} as any, scopeHash: h(4), journalThroughId: () => 1,
        assertApplicable() { throw new Error("unprepared small source incorrectly used prepared-only guard"); } };
    const result = await f.submit(changes, root, undefined, prepared);
    assert.ok(received);
    released(owner, "accepted publication");
    same(received.entries, [
        { action: "delete", path: "gone.md" },
        { action: "upsert", path: "keep.md", hash: hash(bytes(10, 11, 12)), mtime: 40, size: 3 },
        { action: "upsert", path: "z-new.md", hash: hash(bytes(20, 21)), mtime: 50, size: 2 },
    ], "root context did not receive exact sorted applied states and actual content hashes");
    check(hash(received.rootBytes) === received.candidateRoot && received.parentRoot === f.initialRoot && received.treeVersion === 2,
        "root bytes/hash/version/parent were not the exact candidate");
    same(received.rootBytes, f.tree.root_bytes(), "committed candidate bytes differed from bytes handed to publication");
    check(result.published === true && result.newRootHash === h(99) && result.rootSettlement?.status === "accepted",
        "accepted result lost historical observed root or terminal disposition");
    check(f.tree.root_hash_hex() === received.candidateRoot && f.base.treeBaseRoot === received.candidateRoot,
        "merged outcome root replaced local candidate/base");
    check(f.base.getEntry("gone.md") === null && f.base.getHash("keep.md") === hash(bytes(10, 11, 12)),
        "actual atomic base publication did not match candidate states");
    check(f.events.indexOf("durable-base") < f.events.indexOf("candidate-commit"), "candidate committed before publication callback completed");
    check(f.counts.commit === 1 && f.counts.abort === 0 && f.counts.publish === 1, "accepted candidate had wrong lifetime");
    noLegacy(f);
    same(f.rootExportCounts, { begin: 1, plan: 1, build: 1, read: 3, finish: 1, cancel: 0,
        offsets: [0, ROOT_EXPORT_PAGE, 2 * ROOT_EXPORT_PAGE],
        lengths: [ROOT_EXPORT_PAGE, ROOT_EXPORT_PAGE, received.rootBytes.byteLength - 2 * ROOT_EXPORT_PAGE] },
    "durable candidate root did not use the complete paged export API exactly once");
    const restored = new ObsetyncSyncBase({ vault: { adapter: f.disk.clone() } } as any); await restored.load();
    check(restored.treeBaseRoot === received.candidateRoot && restored.lastAppliedPublication?.candidateRoot === received.candidateRoot,
        "atomic candidate publication did not survive actual store reload");
}

async function nonAcceptanceAlwaysAborts(): Promise<void> {
    const dispositions: RootRecoveryResult[] = [{ status: "cancelled" }, { status: "unresolved" }, { status: "conflicts-pending" },
        { status: "deferred", reason: "closing" }, { status: "deferred", reason: "unsupported" }, { status: "deferred", reason: "limits" }];
    for (const disposition of dispositions) {
        const f = await fixture(), disk = f.disk.snapshot();
        let owner: Candidate["rootExport"] | undefined;
        const result = await f.submit([modify(f, "keep.md"), { action: "deleted", path: "gone.md" }], f.context(async candidate => {
            owner = candidate.rootExport;
            check(!owner.memory.snapshot().parentReleased, "deferred callback received a released root owner");
            return disposition;
        }));
        released(owner, `nonaccepted ${disposition.status}`);
        same(result.rootSettlement, disposition, "nonaccepted coordinator result was changed");
        check(result.published === false && f.counts.commit === 0 && f.counts.abort === 1,
            "cancelled/deferred candidate was committed or not aborted");
        check(f.tree.root_hash_hex() === f.initialRoot && f.base.treeBaseRoot === f.initialRoot,
            "nonaccepted result advanced committed tree/base");
        same(f.disk.snapshot(), disk, "nonaccepted push made an ordinary durable base write");
        noLegacy(f);
    }
}

async function ambiguousPublicationDoesNotInventSuccess(): Promise<void> {
    for (const persisted of [false, true]) {
        const f = await fixture(); let serverAccepted = false, candidateRoot = "";
        let owner: Candidate["rootExport"] | undefined;
        await rejects(f.submit([modify(f, "keep.md"), { action: "deleted", path: "gone.md" }], f.context(async candidate => {
            owner = candidate.rootExport;
            serverAccepted = true; candidateRoot = candidate.candidateRoot;
            if (persisted) await f.apply(candidate);
            throw new Error(persisted ? "failure after durable accepted base" : "lost accepted response");
        })), /accepted/, "ambiguous publication was turned into a completed push");
        released(owner, `rejected wrapper persisted=${persisted}`);
        check(serverAccepted && f.counts.commit === 0 && f.counts.abort === 1, "failed acceptance tail committed or leaked the candidate");
        check(f.tree.root_hash_hex() === f.initialRoot, "failed accepted tail rewrote local committed graph");
        check(f.base.treeBaseRoot === (persisted ? candidateRoot : f.initialRoot),
            "push rolled back coordinator-owned durable base or invented one before persistence");
        check((f.base.lastAppliedPublication !== null) === persisted, "ambiguous tail fabricated an applied marker");
        noLegacy(f);
    }
    const wrong = await fixture(); let wrongOwner: Candidate["rootExport"] | undefined;
    await rejects(wrong.submit([{ action: "deleted", path: "gone.md" }], wrong.context(async candidate => {
        wrongOwner = candidate.rootExport;
        return { status: "accepted", candidateRoot: h(7), observedRoot: h(8) };
    })), /another candidate/, "foreign candidate acceptance was adopted");
    released(wrongOwner, "foreign accepted identity");
    check(wrong.counts.abort === 1 && wrong.counts.commit === 0, "foreign accepted identity did not abort its candidate"); noLegacy(wrong);
}

async function lateDeferralClosesSelectedGroupsAfterGlobalDeleteHold(): Promise<void> {
    for (const independent of [false, true]) {
        const f = await fixture();
        f.files.set("huge.bin", { bytes: bytes(), mtime: 90, declaredSize: 512 * 1024 * 1024 });
        const changes: FileChange[] = [
            { action: "created", path: "huge.bin" }, // exact size discovered only at push admission
            modify(f, "chain-peer.md", bytes(20)), modify(f, "rename-dest.md", bytes(21)),
            modify(f, "rename-tail.md", bytes(22)), { action: "deleted", path: "old.md" },
            { action: "deleted", path: "gone.md" },
        ];
        if (independent) changes.push(modify(f, "independent.md", bytes(23)));
        const groups = [["huge.bin", "chain-peer.md"], ["old.md", "rename-dest.md", "rename-tail.md"]];
        let entries: Candidate["entries"] = [];
        const root = f.context(async candidate => {
            entries = copy(candidate.entries); await f.apply(candidate); return accepted(candidate);
        }, groups);
        const result = await f.submit(changes, root);
        same(entries.map(entry => entry.path), independent ? ["independent.md"] : [],
            "late admission/global deletion hold left a linked upsert in publication");
        same(result.deferred?.map(row => [row.path, row.reason]).sort(), [
            ["huge.bin", "source-too-large"], ["chain-peer.md", "dependent-delete"],
            ["old.md", "dependent-delete"], ["gone.md", "dependent-delete"],
            ["rename-dest.md", "dependent-delete"], ["rename-tail.md", "dependent-delete"],
        ].sort(), "runtime closure lost held peers or duplicated deferred states");
        check(!f.reads.includes("huge.bin"), "oversized source allocated whole bytes before deferral");
        check(f.counts.delete === 0 && f.base.getEntry("old.md") !== null && f.base.getEntry("gone.md") !== null,
            "global held deletion touched candidate/base");
        check(f.counts.publish === Number(independent) && f.counts.commit === Number(independent) && f.counts.abort === Number(!independent),
            "dependency closure blocked independent notes or published an all-deferred root");
        check(result.published === independent, "late dependency result claimed incorrect publication disposition");
        for (const path of ["chain-peer.md", "rename-dest.md", "rename-tail.md"]) {
            check(!f.treeEntries().some(row => row.path === path) && f.base.getEntry(path) === null,
                "uploaded-but-held component metadata leaked into committed state");
        }
        check(f.counts.update === Number(independent), "all-deferred candidate performed an update before returning");
        noLegacy(f);
    }
}

async function rootPublicationSlicesAtAckSafeByteTimeAndDependencyBoundaries(): Promise<void> {
    for (const reason of ["bytes", "time"] as const) {
        const f = await fixture();
        const changes = ["slice-a.md", "slice-b.md", "slice-c.md", "slice-d.md"]
            .map((path, index) => modify(f, path, bytes(20 + index), 50 + index));
        let clock = 0, entries: Candidate["entries"] = [];
        const slice: NonNullable<PushRootContext["slice"]> = {
            maxSourceBytes: reason === "bytes" ? 1 : 1024,
            maxElapsedMs: reason === "time" ? 50 : 60_000,
            maxBoundaryFiles: 1,
            shouldPreempt: () => false,
            now: () => reason === "time" ? (clock++ === 0 ? 0 : 60) : 0,
        };
        const result = await f.submit(changes, f.context(async candidate => {
            entries = copy(candidate.entries); await f.apply(candidate); return accepted(candidate);
        }, [], slice));
        same(entries.map(entry => entry.path), [changes[0].path], `${reason} slice crossed its first ACK-safe prefix`);
        same(result.continuation?.paths, changes.slice(1).map(change => change.path),
            `${reason} slice lost or reordered its immediate continuation`);
        same(result.continuation?.reason, reason, `${reason} slice reported another trigger`);
        check(result.continuation?.sourceBytes === 1 && result.published === true,
            `${reason} slice counters or publication disposition were not truthful`);
        noLegacy(f);
    }

    const grouped = await fixture();
    const changes = ["group-a.md", "middle-b.md", "group-c.md", "tail-d.md", "tail-e.md"]
        .map((path, index) => modify(grouped, path, bytes(40 + index), 70 + index));
    let entries: Candidate["entries"] = [];
    const result = await grouped.submit(changes, grouped.context(async candidate => {
        entries = copy(candidate.entries); await grouped.apply(candidate); return accepted(candidate);
    }, [[changes[0].path, changes[2].path]], {
        maxSourceBytes: 1, maxElapsedMs: 60_000, maxBoundaryFiles: 1,
        shouldPreempt: () => false, now: () => 0,
    }));
    same(entries.map(entry => entry.path), changes.slice(0, 3).map(change => change.path).sort(),
        "interleaved dependency component was rolled back or split at the soft byte ceiling");
    same(result.continuation?.paths, changes.slice(3).map(change => change.path),
        "dependency extension did not resume at the first component-complete boundary");
    check(result.continuation?.sourceBytes === 3 && grouped.counts.publish === 1 && grouped.counts.commit === 1,
        "oversized dependency component did not make one bounded forward publication");
    noLegacy(grouped);

    for (const reason of ["bytes", "time"] as const) {
        const f = await fixture();
        const changes = ["routine-a.md", "routine-b.md", "routine-c.md", "routine-d.md"]
            .map((path, index) => modify(f, path, bytes(60 + index), 90 + index));
        let clock = 0, entries: Candidate["entries"] = [];
        const result = await f.submit(changes, f.context(async candidate => {
            entries = copy(candidate.entries); await f.apply(candidate); return accepted(candidate);
        }, [], {
            maxSourceBytes: reason === "bytes" ? 1 : 1024,
            maxElapsedMs: reason === "time" ? 50 : 60_000,
            maxBoundaryFiles: 1,
            allowRoutineSplit: false,
            shouldPreempt: () => false,
            now: () => reason === "time" ? (clock++ === 0 ? 0 : 60) : 0,
        }));
        same(entries.map(entry => entry.path), changes.map(change => change.path),
            `${reason} split consumed a reviewed suffix without requeue ownership`);
        check(result.continuation === undefined && result.published === true,
            `${reason} split escaped an explicitly indivisible reviewed selection`);
        noLegacy(f);
    }
}

async function contentPacksCoalesceAcrossResponsivePredispatchBoundaries(): Promise<void> {
    const previous = getHashTuning();
    configureHashTuning({ ...previous, readConcurrency: 1, maxBatchFiles: 6,
        maxBatchBytes: 1024, maxSingleBatchFileBytes: 1024 });
    const makeChanges = async (f: Awaited<ReturnType<typeof fixture>>, count: number) => {
        const changes: FileChange[] = [];
        for (let index = 0; index < count; index++) {
            const data = bytes(100 + index), change = modify(f, `pack-${index}.md`, data, 500 + index);
            change.hash = hash(data); // exercise phase-C lazy source reads
            changes.push(change);
        }
        return changes;
    };
    const slice = (urgent: () => boolean): NonNullable<PushRootContext["slice"]> => ({
        maxSourceBytes: 1024,
        maxElapsedMs: 60_000,
        maxBoundaryFiles: 2,
        allowRoutineSplit: false,
        shouldPreempt: urgent,
        now: () => 0,
    });
    try {
        {
            const f = await fixture(), changes = await makeChanges(f, 5);
            const result = await f.submit(changes, f.context(async candidate => {
                await f.apply(candidate); return accepted(candidate);
            }, [], slice(() => false)));
            same([f.counts.contentCheckRequests, f.counts.contentCheckedObjects,
                f.counts.contentPutRequests, f.counts.contentPutObjects, f.counts.contentPutBytes],
            [1, 5, 1, 5, 5], "responsive windows remained coupled to content network requests");
            same([f.counts.maxContentCheckObjects, f.counts.maxContentPutObjects, f.counts.maxContentPutBytes],
                [5, 5, 5], "coalesced content pack occupancy instrumentation was incorrect");
            check(result.continuation === undefined && result.published === true,
                "quiet coalesced pack invented a continuation");
            noLegacy(f);
        }

        for (const trigger of ["check", "lazy-read"] as const) {
            const f = await fixture(), changes = await makeChanges(f, 5);
            let urgent = false, entries: Candidate["entries"] = [];
            if (trigger === "check") f.hooks.afterContentCheck = () => { urgent = true; };
            else f.hooks.afterSourceRead = () => { urgent = true; };
            const result = await f.submit(changes, f.context(async candidate => {
                entries = copy(candidate.entries); await f.apply(candidate); return accepted(candidate);
            }, [], slice(() => urgent)));
            same(entries.map(entry => entry.path), changes.slice(0, 2).map(change => change.path),
                `${trigger} urgent event crossed the prepared two-file prefix`);
            same(result.continuation?.paths, changes.slice(2).map(change => change.path),
                `${trigger} urgent event lost the untouched suffix`);
            same([f.counts.contentCheckRequests, f.counts.contentCheckedObjects,
                f.counts.contentPutRequests, f.counts.contentPutObjects, f.counts.contentPutBytes],
            [1, 5, 1, 2, 2], `${trigger} urgent cut fabricated a partial request ACK`);
            check(result.continuation?.reason === "urgent" && result.continuation.sourceBytes === 2,
                `${trigger} urgent cut reported incorrect durable-prefix accounting`);
            noLegacy(f);
        }

        {
            const f = await fixture(), changes = await makeChanges(f, 6);
            let urgent = false, dispatched = 0;
            f.hooks.beforeUpload = async records => {
                if (records.some(record => record.kind !== BulkObjectKind.IndexChunk)) {
                    dispatched = records.length; urgent = true;
                }
            };
            const result = await f.submit(changes, f.context(async candidate => {
                await f.apply(candidate); return accepted(candidate);
            }, [], slice(() => urgent)));
            same([dispatched, f.counts.contentPutRequests, f.counts.contentPutObjects], [6, 1, 6],
                "urgent event after PUT dispatch split or partially acknowledged the request");
            check(result.continuation === undefined && result.published === true,
                "post-dispatch urgent event rolled back an already indivisible request");
            noLegacy(f);
        }

        {
            const f = await fixture(), change = (await makeChanges(f, 1))[0];
            await rejects(f.submit([change], f.context(async candidate => accepted(candidate), [], {
                ...slice(() => false), maxBoundaryFiles: 0,
            })), /invalid slice limits/, "zero responsive boundary reached preparation");
            same([f.counts.begin, f.counts.contentCheckRequests, f.counts.contentPutRequests], [0, 0, 0],
                "invalid responsive boundary acquired candidate or network ownership");
        }
    } finally {
        configureHashTuning(previous);
    }
}

async function applicabilityGuardsEverySelectedPath(): Promise<void> {
    for (const stage of ["before-candidate", "before-request"] as const) {
        for (const stalePath of ["keep.md", "gone.md"]) {
            const f = await fixture(), changes: FileChange[] = [modify(f, "keep.md"), { action: "deleted", path: "gone.md" }];
            let stale = stage === "before-candidate";
            f.hooks.afterIndexAck = () => { stale = true; };
            const root = f.context(async value => accepted(value));
            root.assertApplicable = path => {
                f.events.push(`guard:${path}`);
                if (stale && path === stalePath) throw new HashWorkerFileDriftError("selected path changed");
            };
            await rejects(f.submit(changes, root), /selected path changed/, "changed selected path reached publication");
            check(f.counts.publish === 0 && f.counts.commit === 0 &&
                f.counts.abort === (stage === "before-candidate" ? 0 : 1),
                "failed all-path applicability committed or leaked a candidate");
            if (stage === "before-candidate") check(f.counts.update === 0 && f.counts.delete === 0,
                "stale selected path was applied to candidate before its guard");
            else check(f.counts.update === 1 && f.counts.delete === 1 && f.events.includes("index-ack"),
                "request guard failure did not exercise the post-index boundary");
            check(f.tree.root_hash_hex() === f.initialRoot && f.base.treeBaseRoot === f.initialRoot,
                "failed selected-path guard advanced committed metadata"); noLegacy(f);
        }
    }
}

async function contentAckFailureAndAcceptedStop(): Promise<void> {
    const failed = await fixture();
    failed.hooks.beforeUpload = async records => {
        if (records.some(record => record.kind === BulkObjectKind.Content)) throw new Error("content ACK failed");
    };
    await rejects(failed.submit([modify(failed, "keep.md")], failed.context(async value => accepted(value))),
        /content ACK failed/, "failed content was allowed into a root request");
    check(failed.counts.publish === 0 && failed.counts.update === 0 && failed.counts.abort === 1,
        "content rejection built/published unacknowledged candidate entries"); noLegacy(failed);
    const stopped = await fixture(), abort = new AbortController();
    const result = await stopped.submit([{ action: "deleted", path: "gone.md" }], stopped.context(async candidate => {
        abort.abort(); await stopped.apply(candidate); return accepted(candidate);
    }), abort.signal);
    check(result.published === true && stopped.counts.commit === 1 && stopped.counts.abort === 0,
        "stop discarded an actually completed accepted publication tail");
    check(stopped.base.getEntry("gone.md") === null, "accepted stop lost its coordinator-owned atomic base"); noLegacy(stopped);
}

async function finalSendCandidateRevisionRejectsABAAndReleasesOwner(): Promise<void> {
    const f = await fixture(), beforeDisk = f.disk.snapshot();
    let owner: Candidate["rootExport"] | undefined, exactBytes: Uint8Array | undefined, expectedRoot = "";
    await rejects(f.submit([modify(f, "keep.md"), { action: "deleted", path: "gone.md" }],
        f.context(async (candidate, assertCandidateCurrent) => {
            owner = candidate.rootExport; exactBytes = owner.bytes.slice();
            const semanticHash = candidate.candidateRoot, fileCount = f.tree.candidate_total_files();
            expectedRoot = semanticHash;
            f.replaceCandidateABA();
            check(f.tree.candidate_root_hash_hex() === semanticHash && f.tree.candidate_total_files() === fileCount,
                "final-send ABA did not preserve the legacy hash/count identity");
            assertCandidateCurrent();
            throw new Error("candidate revision guard unexpectedly accepted ABA");
        })), /push candidate changed during index publication/,
    "final-send closure accepted candidate ABA with unchanged hash/count");
    check(exactBytes !== undefined && hash(exactBytes) === expectedRoot,
        "final-send closure did not receive the exact exported candidate bytes");
    released(owner, "final-send ABA rejection");
    check(f.counts.publish === 1 && f.counts.commit === 0 && f.counts.abort === 0 && f.tree.has_candidate(),
        "candidate ABA rejection rolled back a newer provisional generation");
    check(f.events.indexOf("root-export-finish") < f.events.indexOf("candidate-aba") &&
        !f.events.includes("candidate-abort"),
    "candidate ABA did not remain isolated at the post-export final-send boundary");
    same(f.disk.snapshot(), beforeDisk, "candidate ABA rejection changed durable base state");
    noLegacy(f);
    f.tree.abort_candidate(); // Fixture owner explicitly retires the newer generation.
}

async function legacyRootStillUsesOnlyLegacyCandidateGetter(): Promise<void> {
    const f = await fixture(); let received: Uint8Array | undefined, parent = "";
    f.hooks.legacyRoot = async (rootBytes, parentRoot) => {
        received = rootBytes.slice(); parent = parentRoot;
        check(hash(rootBytes) === f.tree.candidate_root_hash_hex(),
            "legacy putRoot did not receive the exact current candidate bytes");
        throw new Error("stop after legacy root capture");
    };
    await rejects(f.submitLegacy([modify(f, "keep.md"), { action: "deleted", path: "gone.md" }]),
        /stop after legacy root capture/, "legacy root branch did not reach putRoot");
    check(received !== undefined && parent === f.initialRoot && f.counts.candidateRootBytes === 1 &&
        f.counts.legacyRoot === 1, "legacy root branch changed getter/putRoot ownership");
    same(f.rootExportCounts, { begin: 0, plan: 0, build: 0, read: 0, finish: 0, cancel: 0,
        offsets: [], lengths: [] }, "legacy root branch accidentally used durable paged export");
    check(f.counts.abort === 1 && f.counts.commit === 0 && !f.tree.has_candidate(),
        "failed legacy root request committed or leaked its candidate");
}

async function candidateJobPushLifecycle(): Promise<void> {
    const completed = await fixture(); completed.enableCandidateJob();
    const result = await completed.submit([modify(completed, "keep.md"), { action: "deleted", path: "gone.md" }],
        completed.context(async candidate => {
        check(completed.jobCounts.finish === 1 && completed.mutationJobCounts.finish === 2 &&
            completed.chunkJobCounts.finish === 1 &&
            completed.tree.has_candidate(), "root publication ran before candidate job ownership handoff");
        await completed.apply(candidate); return accepted(candidate);
    }), undefined, undefined, async () => {});
    check(result.published === true && completed.counts.begin === 0 && completed.jobCounts.begin === 1 &&
        completed.jobCounts.step === 3 && completed.jobCounts.finish === 1 && completed.jobCounts.cancel === 0,
    "successful push bypassed or repeated resumable candidate validation");
    check(completed.chunkJobCounts.begin === 1 && completed.chunkJobCounts.step === 3 &&
        completed.chunkJobCounts.finish === 1 && completed.chunkJobCounts.cancel === 0,
    "successful push bypassed or repeated resumable candidate chunk collection");
    check(completed.mutationJobCounts.begin === 2 && completed.mutationJobCounts.step === 6 &&
        completed.mutationJobCounts.finish === 2 && completed.mutationJobCounts.cancel === 0 &&
        completed.mutationJobCounts.kinds.join(",") === "delete,update" &&
        completed.counts.delete === 1 && completed.counts.update === 1,
    "successful push combined, reordered, bypassed or repeated resumable candidate mutations");
    check(completed.counts.commit === 1 && completed.counts.abort === 0 && !completed.tree.has_candidate(),
        "successful candidate job did not transfer cleanup ownership through commit");
    noLegacy(completed);

    const cancelled = await fixture(); cancelled.enableCandidateJob();
    const stop = new AbortController(); let gates = 0;
    await rejects(cancelled.submit([{ action: "deleted", path: "gone.md" }],
        cancelled.context(async candidate => { await cancelled.apply(candidate); return accepted(candidate); }),
        stop.signal, undefined, async () => {
            if (++gates === 2) stop.abort(new Error("candidate validation stopped"));
        }), /candidate validation stopped/, "cancelled validation reached candidate publication");
    check(cancelled.jobCounts.begin === 1 && cancelled.jobCounts.step === 1 && cancelled.jobCounts.finish === 0 &&
        cancelled.jobCounts.cancel === 1 && cancelled.counts.begin === 0,
    "push did not cancel exactly the active native validation job");
    check(cancelled.chunkJobCounts.begin === 0 && cancelled.chunkJobCounts.step === 0 &&
        cancelled.chunkJobCounts.finish === 0 && cancelled.chunkJobCounts.cancel === 0,
    "candidate chunk collection began after candidate validation was cancelled");
    check(cancelled.mutationJobCounts.begin === 0 && cancelled.mutationJobCounts.step === 0 &&
        cancelled.mutationJobCounts.finish === 0 && cancelled.mutationJobCounts.cancel === 0,
    "candidate mutation began after candidate validation was cancelled");
    check(!cancelled.tree.has_candidate() && cancelled.counts.abort === 0 && cancelled.counts.commit === 0 &&
        cancelled.counts.delete === 0 && cancelled.counts.update === 0 && cancelled.counts.publish === 0,
    "cancelled validation leaked candidate mutation, publication or cleanup ownership");
    check(cancelled.tree.root_hash_hex() === cancelled.initialRoot && cancelled.base.treeBaseRoot === cancelled.initialRoot &&
        cancelled.uploaded.length === 0, "cancelled validation changed root/base or emitted an upload ACK");
    noLegacy(cancelled);

    const mutationCancelled = await fixture(); mutationCancelled.enableCandidateJob();
    const mutationStop = new AbortController();
    mutationCancelled.beforeMutationStep(() => {
        if (mutationCancelled.mutationJobCounts.step === 1) {
            mutationStop.abort(new Error("candidate mutation stopped"));
        }
    });
    await rejects(mutationCancelled.submit([{ action: "deleted", path: "gone.md" }],
        mutationCancelled.context(async candidate => { await mutationCancelled.apply(candidate); return accepted(candidate); }),
        mutationStop.signal, undefined, async () => {}),
        /candidate mutation stopped/, "cancelled mutation reached candidate publication");
    check(mutationCancelled.jobCounts.finish === 1 && mutationCancelled.mutationJobCounts.begin === 1 &&
        mutationCancelled.mutationJobCounts.step === 1 && mutationCancelled.mutationJobCounts.finish === 0 &&
        mutationCancelled.mutationJobCounts.cancel === 1,
    "push did not cancel exactly the active candidate mutation job");
    check(mutationCancelled.chunkJobCounts.begin === 0 && mutationCancelled.counts.delete === 0 &&
        mutationCancelled.counts.abort === 1 && mutationCancelled.counts.commit === 0 &&
        mutationCancelled.counts.publish === 0 && !mutationCancelled.tree.has_candidate(),
    "cancelled mutation leaked staged state or reached chunk/root publication");
    check(mutationCancelled.tree.root_hash_hex() === mutationCancelled.initialRoot &&
        mutationCancelled.base.treeBaseRoot === mutationCancelled.initialRoot && mutationCancelled.uploaded.length === 0,
    "cancelled mutation changed root/base or emitted an upload ACK");
    noLegacy(mutationCancelled);

    const updateCancelled = await fixture(); updateCancelled.enableCandidateJob();
    const updateStop = new AbortController();
    updateCancelled.beforeMutationStep(() => {
        if (updateCancelled.mutationJobCounts.begin === 2 &&
            updateCancelled.mutationJobCounts.step === 4 &&
            updateCancelled.mutationJobCounts.finish === 1) {
            updateStop.abort(new Error("candidate update stopped"));
        }
    });
    await rejects(updateCancelled.submit([modify(updateCancelled, "keep.md"), { action: "deleted", path: "gone.md" }],
        updateCancelled.context(async candidate => { await updateCancelled.apply(candidate); return accepted(candidate); }),
        updateStop.signal, undefined, async () => {}),
        /candidate update stopped/, "cancelled update reached candidate publication");
    check(updateCancelled.jobCounts.finish === 1 && updateCancelled.mutationJobCounts.begin === 2 &&
        updateCancelled.mutationJobCounts.step === 4 && updateCancelled.mutationJobCounts.finish === 1 &&
        updateCancelled.mutationJobCounts.cancel === 1 &&
        updateCancelled.mutationJobCounts.kinds.join(",") === "delete,update",
    "push did not retain the completed delete while cancelling exactly the active update job");
    check(updateCancelled.chunkJobCounts.begin === 0 && updateCancelled.counts.delete === 1 &&
        updateCancelled.counts.update === 0 && updateCancelled.counts.abort === 1 &&
        updateCancelled.counts.commit === 0 && updateCancelled.counts.publish === 0 &&
        !updateCancelled.tree.has_candidate(),
    "cancelled update leaked its staged state or the intermediate delete candidate");
    check(updateCancelled.tree.root_hash_hex() === updateCancelled.initialRoot &&
        updateCancelled.base.treeBaseRoot === updateCancelled.initialRoot &&
        updateCancelled.uploaded.length === 1 &&
        updateCancelled.uploaded.every(record => record.kind === BulkObjectKind.Content),
    "cancelled update changed root/base or emitted an index upload ACK");
    noLegacy(updateCancelled);

    const chunksCancelled = await fixture(); chunksCancelled.enableCandidateJob();
    const chunksStop = new AbortController();
    chunksCancelled.beforeChunkStep(() => {
        if (chunksCancelled.chunkJobCounts.step === 1) {
            chunksStop.abort(new Error("candidate chunk collection stopped"));
        }
    });
    await rejects(chunksCancelled.submit([{ action: "deleted", path: "gone.md" }],
        chunksCancelled.context(async candidate => { await chunksCancelled.apply(candidate); return accepted(candidate); }),
        chunksStop.signal, undefined, async () => {}),
        /candidate chunk collection stopped/, "cancelled chunk traversal reached candidate publication");
    check(chunksCancelled.jobCounts.begin === 1 && chunksCancelled.jobCounts.step === 3 &&
        chunksCancelled.jobCounts.finish === 1 && chunksCancelled.jobCounts.cancel === 0,
    "chunk cancellation changed completed candidate validation ownership");
    check(chunksCancelled.chunkJobCounts.begin === 1 && chunksCancelled.chunkJobCounts.step === 1 &&
        chunksCancelled.chunkJobCounts.finish === 0 && chunksCancelled.chunkJobCounts.cancel === 1,
    "push did not cancel exactly the active candidate chunk job");
    check(chunksCancelled.mutationJobCounts.begin === 1 && chunksCancelled.mutationJobCounts.step === 3 &&
        chunksCancelled.mutationJobCounts.finish === 1 && chunksCancelled.mutationJobCounts.cancel === 0,
    "chunk cancellation changed completed candidate mutation ownership");
    check(!chunksCancelled.tree.has_candidate() && chunksCancelled.counts.abort === 1 &&
        chunksCancelled.counts.commit === 0 && chunksCancelled.counts.delete === 1 &&
        chunksCancelled.counts.update === 0 && chunksCancelled.counts.publish === 0,
    "cancelled chunk traversal leaked candidate state, publication or wrong mutation ordering");
    check(chunksCancelled.tree.root_hash_hex() === chunksCancelled.initialRoot &&
        chunksCancelled.base.treeBaseRoot === chunksCancelled.initialRoot && chunksCancelled.uploaded.length === 0,
    "cancelled chunk traversal changed root/base or emitted an upload ACK");
    noLegacy(chunksCancelled);
}

function indexPayload(): Uint8Array {
    return Uint8Array.from({ length: 2 * EXPORT_PAGE + 13 }, (_, index) => (index * 131 ^ index >>> 5) & 255);
}

async function pagedIndexExportCompletesBeforeIndexAckAndRoot(): Promise<void> {
    const f = await fixture(), payload = indexPayload(), beforeMemory = transientMemorySnapshot();
    f.enableIndexExport(payload);
    let pageGates = 0;
    const result = await f.submit([modify(f, "keep.md"), { action: "deleted", path: "gone.md" }],
        f.context(async candidate => {
            check(f.exportCounts.finish === 1 && !f.exportActive() && f.events.includes("index-ack"),
                "root publication started before paged index export/actual index ACK");
            await f.apply(candidate); return accepted(candidate);
        }), undefined, undefined, async () => {
            if (!f.exportActive()) return;
            pageGates++;
            check(f.exportCounts.read === pageGates && f.exportCounts.finish === 0,
                "host cooperation was not between actual completed index pages");
            check(!f.events.includes("index-ack") && f.counts.publish === 0 && f.tree.has_candidate(),
                "partial export exposed an index/root publication or lost candidate ownership");
            check(transientMemorySnapshot().usedBytes > beforeMemory.usedBytes,
                "index export allocated before retaining its admitted pack budget");
        });
    same(f.exportCounts, { begin: 1, read: 3, finish: 1, cancel: 0, legacy: 0,
        offsets: [0, EXPORT_PAGE, 2 * EXPORT_PAGE], lengths: [EXPORT_PAGE, EXPORT_PAGE, 13] },
    "actual push did not consume the optional export API exactly once in bounded pages");
    same(pageGates, 2, "three-page index export did not provide two host boundaries");
    const indexes = f.uploaded.filter(record => record.kind === BulkObjectKind.IndexChunk);
    same(indexes.map(record => [record.hash, record.data]), [[hash(payload), payload]],
        "paged index bytes/order/hash changed before upload");
    check(f.events.indexOf("export-finish") < f.events.indexOf("index-ack") &&
        f.events.indexOf("index-ack") < f.events.indexOf("publish") &&
        f.events.indexOf("publish") < f.events.indexOf("candidate-commit"),
    "export/index ACK/root/candidate commit ordering changed");
    check(result.published === true && f.counts.commit === 1 && f.counts.abort === 0 && f.counts.indexRequests === 1,
        "successful paged push failed its candidate lifetime");
    same(transientMemorySnapshot().usedBytes, beforeMemory.usedBytes, "accepted index pack retained its admission");
    noLegacy(f);
}

async function indexExportPageInterruptionCancelsBeforeCandidateAbort(): Promise<void> {
    for (const mode of ["abort", "ownership"] as const) {
        const f = await fixture(), beforeDisk = f.disk.snapshot(), beforeMemory = transientMemorySnapshot();
        f.enableIndexExport(indexPayload());
        const stop = new AbortController(), entered = gate(), resume = gate();
        const failure = mode === "abort" ? new Error("paged index stopped")
            : new HashWorkerFileDriftError("paged index selected generation changed");
        let current = true, settled = false, gated = false;
        const root = f.context(async candidate => { await f.apply(candidate); return accepted(candidate); });
        root.assertApplicable = path => {
            f.events.push(`guard:${path}`);
            if (!current) throw failure;
        };
        const work = f.submit([modify(f, "keep.md"), { action: "deleted", path: "gone.md" }], root,
            stop.signal, undefined, async () => {
                if (!gated && f.exportActive() && f.exportCounts.read === 1) {
                    gated = true; entered.resolve(); await resume.promise;
                }
            });
        void work.then(() => { settled = true; }, () => { settled = true; });
        try {
            await entered.promise;
            check(!settled && f.exportActive() && f.exportCounts.read === 1 && f.exportCounts.finish === 0 &&
                f.counts.abort === 0 && f.tree.has_candidate(), "page gate did not retain actual unfinished push ownership");
            check(transientMemorySnapshot().usedBytes > beforeMemory.usedBytes,
                "held page cooperation released the index pack admission");
            if (mode === "abort") stop.abort(failure); else current = false;
            resume.resolve();
            await rejects(work, mode === "abort" ? /paged index stopped/ : /paged index selected generation changed/,
                "page interruption did not reject actual push");
            same(f.exportCounts, { begin: 1, read: 1, finish: 0, cancel: 1, legacy: 0,
                offsets: [0], lengths: [EXPORT_PAGE] }, "failed page boundary read more data, finished, or lost cancellation ownership");
            check(f.events.indexOf("export-cancel") < f.events.indexOf("candidate-abort"),
                "outer push aborted before export cleanup released the tree job");
            check(f.counts.abort === 1 && f.counts.commit === 0 && f.counts.publish === 0 && !f.tree.has_candidate(),
                "page interruption leaked or committed a candidate/root");
            check(f.counts.indexRequests === 0 && !f.uploaded.some(record => record.kind === BulkObjectKind.IndexChunk) && !f.events.includes("index-ack"),
                "partial index export reached an index upload/ACK");
            check(f.uploaded.length === 1 && f.uploaded[0].kind === BulkObjectKind.Content,
                "page-boundary fixture did not reach completed content preparation first");
            check(f.tree.root_hash_hex() === f.initialRoot && f.base.treeBaseRoot === f.initialRoot,
                "page interruption advanced the committed root/base");
            same(f.disk.snapshot(), beforeDisk, "page interruption acknowledged local durable state");
            same(transientMemorySnapshot().usedBytes, beforeMemory.usedBytes, "failed export leaked index pack admission");
            noLegacy(f);
        } finally { resume.resolve(); }
    }
}

async function indexAckKeepsActualPushAndAdmissionAliveAfterStop(): Promise<void> {
    const f = await fixture(), beforeDisk = f.disk.snapshot(), beforeMemory = transientMemorySnapshot();
    const payload = indexPayload(); f.enableIndexExport(payload);
    const stop = new AbortController(), entered = gate(), acknowledge = gate();
    let admitted: TransientWorkScope | undefined, settled = false;
    f.hooks.beforeUpload = async (records, memory) => {
        if (!records.some(record => record.kind === BulkObjectKind.IndexChunk)) return;
        admitted = memory;
        check(f.exportCounts.finish === 1 && !f.exportActive(), "index request began before native export finished");
        check(memory !== undefined && !memory.snapshot().ownerClosed && !memory.snapshot().parentReleased,
            "actual index request did not receive the live shared pack scope");
        same(records[0].data, payload, "held request did not own the exact exported bytes");
        entered.resolve(); await acknowledge.promise;
        check(memory !== undefined && !memory.snapshot().ownerClosed && !memory.snapshot().parentReleased,
            "stop released the actual index request's bytes before its ACK settled");
        same(records[0].data, payload, "held request bytes changed while the caller stopped");
    };
    const work = f.submit([{ action: "deleted", path: "gone.md" }],
        f.context(async candidate => { await f.apply(candidate); return accepted(candidate); }), stop.signal,
        undefined, async () => {});
    void work.then(() => { settled = true; }, () => { settled = true; });
    try {
        await entered.promise;
        stop.abort(new Error("stopped during actual index ACK"));
        await Promise.resolve(); await Promise.resolve();
        check(!settled && f.tree.has_candidate() && f.counts.abort === 0 && f.counts.publish === 0 && f.counts.indexRequests === 1,
            "push virtually completed or aborted its candidate before actual index ACK settlement");
        check(admitted !== undefined && !admitted.snapshot().ownerClosed && !admitted.snapshot().parentReleased &&
            transientMemorySnapshot().usedBytes > beforeMemory.usedBytes,
        "stop prematurely released shared index pack ownership");
        acknowledge.resolve();
        await rejects(work, /stopped during actual index ACK/, "stopped index ACK tail reached root publication");
        check(f.events.includes("index-ack") && f.events.indexOf("index-ack") < f.events.indexOf("candidate-abort"),
            "candidate cleanup did not follow the actual index request completion");
        check(f.exportCounts.cancel === 0 && f.exportCounts.finish === 1 && f.exportCounts.legacy === 0,
            "post-export stop cancelled or repeated an already released native export token");
        check(f.counts.abort === 1 && f.counts.commit === 0 && f.counts.publish === 0 && !f.tree.has_candidate(),
            "stopped index ACK tail published or leaked its candidate");
        check(admitted?.snapshot().ownerClosed && admitted.snapshot().parentReleased,
            "settled index request failed to release its pack ownership");
        same(f.disk.snapshot(), beforeDisk, "index ACK alone acknowledged local base/journal metadata");
        same(transientMemorySnapshot().usedBytes, beforeMemory.usedBytes, "actual ACK tail leaked admission");
        noLegacy(f);
    } finally { acknowledge.resolve(); }
}

function seedResidentAdmission(f: Awaited<ReturnType<typeof fixture>>, capacityBytes: number): RootTreeResidentAdmission {
    const admission = new RootTreeResidentAdmission({ capacityBytes });
    // A previously admitted replacement owns 300 requested bytes. This is an
    // explicit synthetic component allowance, not an estimate of the JSON tree.
    const initial = admission.reserve(f.tree, { peakBytes: 300, residentBytes: 300 });
    check(initial !== null, "fixture could not admit its initial resident cohort");
    admission.ready(initial!); admission.releaseRetired(admission.publish(initial!));
    same(admission.snapshot().ledger.usedBytes, 300, "fixture initial resident was not retained");
    return admission;
}

async function admittedMutationActualsSurviveRetirementAndAcceptedRoot(
    settlementMode: "none" | "exact" | "malformed",
): Promise<void> {
    const f = await fixture(), admission = seedResidentAdmission(f, 1700);
    const output = f.enableMutationOutput(admission), beforeDisk = f.disk.snapshot();
    const settlement = settlementMode === "none" ? null : f.enableSettlementOutput(admission, {
        commitBytes: 350, malformedCommit: settlementMode === "malformed",
    });
    const entered = gate(), acknowledge = gate();
    let settled = false;
    const changes: FileChange[] = [{ action: "deleted", path: "gone.md" }, modify(f, "keep.md")];
    const work = f.submit(changes, f.context(async value => {
        check(!hasPendingTreeCandidateMutationRetirement(f.tree), "root entered before mutation retirement drained");
        check(f.counts.indexRequests === 1 && f.events.includes("index-ack"), "accepted fixture skipped index publication");
        same(admission.snapshot().ledger.usedBytes, 1150, "root dispatch released or overcharged actual mutation cohorts");
        entered.resolve(); await acknowledge.promise;
        same(admission.snapshot().residentBytes, 1150, "awaited root ACK lost actual attached output ownership");
        await f.apply(value); return accepted(value);
    }), undefined, undefined, async () => {}, admission);
    void work.then(() => { settled = true; }, () => { settled = true; });
    try {
        await entered.promise;
        check(!settled && f.counts.commit === 0 && f.counts.abort === 0 && f.tree.has_candidate(),
            "held root request virtually committed or aborted the admitted candidate");
        same(f.disk.snapshot(), beforeDisk, "mutation finish alone acknowledged durable base state");
        check(f.tree.root_hash_hex() === f.initialRoot && f.base.treeBaseRoot === f.initialRoot,
            "mutation finish changed committed root/base before root acceptance");
        same(output.witnesses, [[600, 400, 100], [500, 400, 50]], "delete/update lost their separate exact plan witnesses");
        same(output.points.map(({ kind, stage, snapshot: s }) =>
            [kind, stage, s.ledger.usedBytes, s.privateBytes, s.residentBytes]), [
            ["delete", "plan", 300, 0, 300],
            ["delete", "resume", 1300, 1000, 300],
            ["delete", "ready", 1300, 1000, 300],
            ["delete", "finish", 800, 500, 300],
            ["delete", "retire-1", 800, 0, 800],
            ["delete", "retire-2", 800, 0, 800],
            ["update", "plan", 800, 0, 800],
            ["update", "resume", 1700, 900, 800],
            ["update", "ready", 1700, 900, 800],
            ["update", "finish", 1150, 350, 800],
            ["update", "retire-1", 1150, 0, 1150],
            ["update", "retire-2", 1150, 0, 1150],
        ], "actual push did not reserve exact peaks before resume, shrink on Ready, then attach before retirement");
        acknowledge.resolve();
        const result = await work;
        check(result.published === true && f.counts.commit === 1 && f.counts.abort === 0 && !f.tree.has_candidate(),
            "accepted admitted candidate did not commit exactly once");
        check(f.base.getEntry("gone.md") === null && f.base.getHash("keep.md") === hash(f.files.get("keep.md")!.bytes),
            "accepted admitted root did not commit exact base entries");
        check(f.treeEntries().every(row => row.path !== "gone.md") && f.base.treeBaseRoot === f.tree.root_hash_hex(),
            "accepted candidate/base diverged after actual delete+update jobs");
        const snapshot = admission.snapshot();
        same([snapshot.ledger.usedBytes, snapshot.ledger.peakUsedBytes, snapshot.ledger.activeLeases,
            snapshot.residentBytes, snapshot.privateBytes, snapshot.retiringBytes,
            snapshot.privateAttempts, snapshot.retiringOwners], [
                settlementMode === "exact" ? 350 : 1150, 1700, 1,
                settlementMode === "exact" ? 350 : 1150, 0, 0, 0, 0,
            ],
        "commit or cursor retirement released attached cohorts, or retained planned instead of actual output");
        if (settlement) same([settlement.commits, settlement.aborts, settlement.legacy], [1, 0, 0],
            "accepted push did not use exactly one atomic settlement commit");
        same(f.mutationJobCounts.kinds, ["delete", "update"], "production changed mutation ordering");
        same([f.mutationJobCounts.finish, f.mutationJobCounts.cancel], [2, 0], "accepted mutation cleanup repeated/cancelled publication");
        noLegacy(f);
    } finally { acknowledge.resolve(); }
}

async function updateAdmissionRefusalAfterDeleteAbortsOuterCandidate(exactSettlement: boolean): Promise<void> {
    const f = await fixture(), admission = seedResidentAdmission(f, 1699), output = f.enableMutationOutput(admission);
    const settlement = exactSettlement ? f.enableSettlementOutput(admission, { abortBytes: 300 }) : null;
    const beforeDisk = f.disk.snapshot(), beforeRows = copy(f.treeEntries()), beforeIO = f.disk.events.length;
    const changes: FileChange[] = [{ action: "deleted", path: "gone.md" }, modify(f, "keep.md")];
    let failure: unknown;
    try {
        await f.submit(changes, f.context(async () => { throw new Error("refused update reached root publication"); }),
            undefined, undefined, async () => {}, admission);
    } catch (error) { failure = error; }
    check(failure instanceof CandidateMutationOutputAdmissionDeniedError, "actual update admission refusal did not reject push");
    const refusal = failure as CandidateMutationOutputAdmissionDeniedError;
    same([refusal.requestedBytes, refusal.usedBytes, refusal.capacityBytes], [900, 800, 1699],
        "update admission failed for a reason other than resident + delete cohort + new peak");
    same(output.witnesses, [[600, 400, 100]], "refused update resumed or used a rewritten native plan");
    same([f.counts.delete, f.counts.update, f.mutationJobCounts.finish, f.mutationJobCounts.cancel], [1, 0, 1, 1],
        "fixture did not finish delete before refusing/cancelling the separate update job");
    same(output.points.filter(point => point.kind === "update").map(({ stage, snapshot: s }) =>
        [stage, s.ledger.usedBytes, s.privateAttempts, s.residentBytes]), [
        ["plan", 800, 0, 800], ["cancel", 800, 0, 800],
        ["retire-1", 800, 0, 800], ["retire-2", 800, 0, 800],
    ], "refused update allocated output or released the previously attached delete cohort");
    check(f.events.indexOf("candidate-delete") < f.events.indexOf("mutation-output:update:plan") &&
        f.events.indexOf("mutation-output:update:retire-2") < f.events.indexOf("candidate-abort"),
    "outer abort did not follow actual delete mutation and failed-update retirement");
    check(f.counts.abort === 1 && f.counts.commit === 0 && !f.tree.has_candidate() &&
        !hasPendingTreeCandidateMutationRetirement(f.tree), "admission refusal lost the exact outer candidate cleanup");
    same([f.counts.indexRequests, f.counts.publish, f.rootExportCounts.begin, f.chunkJobCounts.begin], [0, 0, 0, 0],
        "admission refusal reached index/root preparation or publication");
    check(!f.events.includes("index-ack") && !f.events.includes("durable-base") &&
        f.uploaded.every(record => record.kind === BulkObjectKind.Content), "refusal acknowledged index or durable metadata");
    check(f.uploaded.length === 1, "refusal fixture did not first complete ordinary source preparation");
    same(f.treeEntries(), beforeRows, "refusal published the intermediate delete into the committed tree");
    check(f.tree.root_hash_hex() === f.initialRoot && f.base.treeBaseRoot === f.initialRoot,
        "refusal changed the committed root/base witness");
    same(f.disk.snapshot(), beforeDisk, "refusal changed durable base bytes");
    check(!f.disk.events.slice(beforeIO).some(event => ["write", "rename", "remove"].includes(event.method)),
        "refusal performed durable base writes hidden by equal final snapshots");
    const snapshot = admission.snapshot();
    same([snapshot.ledger.usedBytes, snapshot.ledger.peakUsedBytes, snapshot.ledger.activeLeases,
        snapshot.ledger.refusedReservations, snapshot.residentBytes, snapshot.privateAttempts, snapshot.retiringOwners],
    [exactSettlement ? 300 : 800, 1300, 1, 1, exactSettlement ? 300 : 800, 0, 0],
    "candidate abort incorrectly reconciled shared-store output or leaked refused private ownership");
    if (settlement) same([settlement.commits, settlement.aborts, settlement.legacy], [0, 1, 0],
        "refused push did not use exactly one atomic settlement abort");
    noLegacy(f);
}

async function retainedAdmittedMutationRetiresBeforeExactAbort(): Promise<void> {
    const f = await fixture(), admission = seedResidentAdmission(f, 1_700);
    const output = f.enableMutationOutput(admission);
    const settlement = f.enableSettlementOutput(admission, { abortBytes: 300 });
    const primary = new Error("post-resume mutation failed"), cleanup = new Error("held admitted retirement");
    const originalError = console.error; console.error = () => {};
    try {
        output.setPostResumeFailure(primary); output.setRetirementFailure(cleanup);
        let first: unknown;
        try {
            await f.submit([{ action: "deleted", path: "gone.md" }, modify(f, "keep.md")],
                f.context(async () => { throw new Error("failed admitted mutation reached root"); }),
                undefined, undefined, async () => {}, admission);
        } catch (error) { first = error; }
        check(first === primary, "retained admitted cleanup replaced the primary mutation error");
        check(hasPendingTreeCandidateMutationRetirement(f.tree) && f.tree.has_candidate() &&
            settlement.aborts === 0, "failed admitted retirement lost or prematurely aborted its candidate");
        const held = admission.snapshot();
        same([held.ledger.usedBytes, held.residentBytes, held.retiringBytes, held.retiringOwners],
            [1700, 800, 900, 1], "failed admitted retirement did not retain both exact native owners");

        output.setPostResumeFailure(undefined); output.setRetirementFailure(undefined);
        const afterDrain = new Error("stop after admitted abort ordering witness");
        let prepares = 0;
        await assert.rejects(f.submit([{ action: "deleted", path: "gone.md" }],
            f.context(async () => { throw new Error("admitted retry reached root"); }),
            undefined, undefined, async () => {}, admission, {
                async prepare() {
                    prepares++;
                    check(!hasPendingTreeCandidateMutationRetirement(f.tree) && !f.tree.has_candidate(),
                        "bootstrap ran before admitted retirement and exact abort completed");
                    const snapshot = admission.snapshot();
                    same([snapshot.ledger.usedBytes, snapshot.residentBytes, snapshot.retiringBytes,
                        snapshot.retiringOwners], [300, 300, 0, 0],
                    "exact abort ran before private retirement release or failed to shrink the stable graph");
                    throw afterDrain;
                },
            }), error => error === afterDrain);
        assertions++;
        same([prepares, settlement.commits, settlement.aborts, settlement.legacy], [1, 0, 1, 0],
            "retained admitted abort debt did not settle exactly once before retry work");
    } finally {
        output.setPostResumeFailure(undefined); output.setRetirementFailure(undefined);
        console.error = originalError;
    }
}

async function retryDrainsMutationAbortDebtBeforeBootstrapValidation(): Promise<void> {
    const f = await fixture(), admission = seedResidentAdmission(f, 1_699), output = f.enableMutationOutput(admission);
    const cleanupFailure = new Error("held native mutation retirement");
    const originalError = console.error; console.error = () => {};
    try {
        output.setRetirementFailure(cleanupFailure);
        const changes: FileChange[] = [{ action: "deleted", path: "gone.md" }, modify(f, "keep.md")];
        let first: unknown;
        try {
            await f.submit(changes, f.context(async () => { throw new Error("failed cleanup reached root"); }),
                undefined, undefined, async () => {}, admission);
        } catch (error) { first = error; }
        check(first instanceof CandidateMutationOutputAdmissionDeniedError,
            "cleanup failure replaced the primary update admission refusal");
        check(hasPendingTreeCandidateMutationRetirement(f.tree) && f.tree.has_candidate() && f.counts.abort === 0,
            "failed retirement forgot or prematurely aborted the exact outer candidate");

        output.setRetirementFailure(undefined);
        const bootstrapFailure = new Error("stop after bootstrap ordering witness");
        let prepares = 0;
        const retry = f.submit([{ action: "deleted", path: "gone.md" }],
            f.context(async () => { throw new Error("bootstrap witness reached root"); }),
            undefined, undefined, async () => {}, admission, {
                async prepare() {
                    prepares++;
                    check(!hasPendingTreeCandidateMutationRetirement(f.tree) && !f.tree.has_candidate(),
                        "bootstrap ran before exact mutation retirement and abort debt drained");
                    throw bootstrapFailure;
                },
            });
        await assert.rejects(retry, error => error === bootstrapFailure);
        assertions++;
        same([prepares, f.counts.abort, f.counts.commit, f.counts.publish], [1, 1, 0, 0],
            "retry did not drain/abort once before invoking the engine bootstrap hook");
        check(!hasPendingTreeCandidateMutationRetirement(f.tree) && !f.tree.has_candidate(),
            "retry left the prior mutation owner or candidate quarantined");
        noLegacy(f);
    } finally {
        output.setRetirementFailure(undefined);
        console.error = originalError;
    }
}

async function finalRetirementYieldRejectsForeignCandidateRevision(): Promise<void> {
    const f = await fixture(), admission = seedResidentAdmission(f, 1_300), output = f.enableMutationOutput(admission);
    const beforeDisk = f.disk.snapshot(), beforeRows = copy(f.treeEntries());
    output.replaceCandidateAfterRetirement();
    await rejects(f.submit([{ action: "deleted", path: "gone.md" }],
        f.context(async () => { throw new Error("foreign candidate reached root publication"); }),
        undefined, undefined, async () => {}, admission), /candidate ownership changed/,
    "foreign revision after final retirement yield was accepted");
    check(output.points.some(point => point.stage === "retire-2") &&
        f.events.includes("mutation-retirement-aba"), "fixture did not replace candidate at the final retirement boundary");
    check(!hasPendingTreeCandidateMutationRetirement(f.tree) && f.tree.has_candidate(),
        "stale continuation leaked cleanup or aborted the foreign candidate");
    same([f.counts.delete, f.counts.abort, f.counts.commit, f.counts.publish], [1, 0, 0, 0],
        "stale mutation continuation reached publication or rolled back a newer candidate revision");
    same(f.treeEntries(), beforeRows, "foreign candidate revision changed the committed tree");
    same(f.disk.snapshot(), beforeDisk, "stale continuation acknowledged durable base state");
    same([admission.snapshot().ledger.usedBytes, admission.snapshot().residentBytes], [800, 800],
        "completed mutation output lost its shared-store cohort after stale continuation rejection");
    noLegacy(f);
}

async function run() {
    const tuning = getHashTuning();
    try {
        configureHashTuning({ ...tuning, readConcurrency: 1, maxBatchFiles: 2 });
        await exactAcceptedCandidateAndBaseOwnership(); await nonAcceptanceAlwaysAborts();
        await ambiguousPublicationDoesNotInventSuccess(); await lateDeferralClosesSelectedGroupsAfterGlobalDeleteHold();
        await rootPublicationSlicesAtAckSafeByteTimeAndDependencyBoundaries();
        await contentPacksCoalesceAcrossResponsivePredispatchBoundaries();
        await applicabilityGuardsEverySelectedPath(); await contentAckFailureAndAcceptedStop();
        await finalSendCandidateRevisionRejectsABAAndReleasesOwner();
        await legacyRootStillUsesOnlyLegacyCandidateGetter();
        await candidateJobPushLifecycle();
        await pagedIndexExportCompletesBeforeIndexAckAndRoot();
        await indexExportPageInterruptionCancelsBeforeCandidateAbort();
        await indexAckKeepsActualPushAndAdmissionAliveAfterStop();
        await admittedMutationActualsSurviveRetirementAndAcceptedRoot("none");
        await admittedMutationActualsSurviveRetirementAndAcceptedRoot("exact");
        await admittedMutationActualsSurviveRetirementAndAcceptedRoot("malformed");
        await updateAdmissionRefusalAfterDeleteAbortsOuterCandidate(false);
        await updateAdmissionRefusalAfterDeleteAbortsOuterCandidate(true);
        await retainedAdmittedMutationRetiresBeforeExactAbort();
        await retryDrainsMutationAbortDebtBeforeBootstrapValidation();
        await finalRetirementYieldRejectsForeignCandidateRevision();
        console.log(`push-root-transaction: ${assertions} assertions passed`);
    } finally { configureHashTuning(tuning); }
}
let suiteCompleted = false;
process.once("beforeExit", () => {
    if (!suiteCompleted) { console.error("push-root-transaction: unfinished asynchronous suite"); process.exitCode = 1; }
});
void run().then(() => { suiteCompleted = true; }, error => {
    suiteCompleted = true; console.error(error); process.exitCode = 1;
});
