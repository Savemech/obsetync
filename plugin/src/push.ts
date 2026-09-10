import type { ObsetyncApi } from "./api";
import {
    BulkObjectKind,
    bulkPackEncodedLength,
    type BulkUploadRecord,
} from "./bulk-codec";
import { PlatformIO } from "./platform";
import { ObsetyncSyncBase } from "./sync-base";
import type { PerfOperation } from "./perf-trace";
import {
    getHashTuning,
    observeHashFeedback,
    planByteBoundedBatches,
    type HashTuning,
} from "./hash-runtime";
import {
    HashWorkerFileDriftError,
    HashWorkerPoolError,
    waitForHashWorkerCleanup,
    type DesktopHashWorkerPool,
} from "./desktop-hash-workers";
import {
    uploadDesktopMissingRanges,
    uploadMissingRangesWithReader,
    type DesktopRangedUploadResult,
    type DesktopRangeSource,
} from "./desktop-ranged-upload";
import { MobileRangeReadError, type MobileRangeReader } from "./mobile-ranged-source";
import { throwIfWorkAborted, yieldWork, type WorkLane } from "./work-scheduler";
import { adaptiveBatches } from "./adaptive-batches";
import { planPushMemory, pushGroupingByteLimit, PUSH_CHUNKER_WORK_BYTES, type PushMemoryFile } from "./push-memory";
import { reserveTransientScope, transientMemorySnapshot,
    type TransientWorkScope } from "./transient-memory";
import { ResourceBudgetOversizedError } from "./resource-budget";
import { MAX_FASTCDC_CHUNK_BYTES } from "./hash-worker-protocol";
import { withHashSource, withStreamingHashSource } from "./hash-source-budget";
import { reserveCompactIndexHashSpool, uploadIndexChunks,
    type CompactIndexHashSpool } from "./index-upload";
import { MobilePreparedTransferPlan, type PreparedTransferPlan } from "./transfer-plan";
import type { ConfirmedObject, ConfirmedObjectKind, ObjectConfirmationStore } from "./object-confirmation-store";
import { PreparedDesktopManifestError, resolvePreparedDesktopManifest } from "./prepared-desktop-manifest";
import { resolvePreparedMobileManifest } from "./prepared-mobile-manifest";
import type { SyncMemoryArbiter } from "./sync-memory-arbiter";
import type { HashWorkerMode } from "./hash-worker-protocol";
import {
    BrowserHashWorkerError,
    type BrowserHashWorkerRuntime,
} from "./browser-hash-workers";
import { BROWSER_HASH_WORKER_MAX_INPUT_BYTES } from "./browser-hash-worker-protocol";
import type { RootCandidatePublication } from "./root-sync-runtime";
import type { RootRecoveryResult } from "./root-recovery";
import { abortTreeCandidateAfterReachabilityRetirement, beginTreeCandidate,
    collectTreeCandidateChunkPages, type TreeJobProgress } from "./tree-candidate-job";
import {
    abortTreeCandidateAfterMutationRetirement,
    applyTreeCandidateMutation,
    drainTreeCandidateMutationRetirement,
    hasPendingTreeCandidateMutationRetirement,
} from "./tree-candidate-mutation-job";
import { exportTreeRoot, TREE_ROOT_EXPORT_MAX_STEP_BYTES,
    TREE_ROOT_EXPORT_MAX_STEP_UNITS } from "./tree-root-export-job";
import { estimateRootIntentPreparationWorkset, rootPublicationArenaLimit } from "./root-publication-memory";
import type { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { admitCandidateMutationOutput, attestCandidateMutationRefusalCut,
    CandidateMutationOutputAdmissionDeniedError } from "./candidate-mutation-output-admission";
import { admitCandidateOpenRoot } from "./candidate-open-root-admission";
import { settleTreeCandidateOutputWithAdmission,
    type V2TreeOutputSettlementOutcome } from "./tree-output-settlement";

const URGENT_FILE_PACK_MAX_BYTES = 64 * 1024;
const DEFAULT_PUSH_RESPONSIVE_BOUNDARY_FILES = 32;
function urgentPutOptions(records: readonly BulkUploadRecord[], lane: WorkLane) {
    return lane === "urgent-file" && records.length === 1 &&
        records[0].kind === BulkObjectKind.Content &&
        bulkPackEncodedLength(records) <= URGENT_FILE_PACK_MAX_BYTES
        ? { priority: "urgent-file" as const }
        : undefined;
}

export interface PushRootContext {
    readonly groups: readonly (readonly string[])[];
    readonly rootByteLimit: number;
    /** Soft root-slice ceilings. Preparation cooperates at maxBoundaryFiles
     * before a coalesced request is dispatched. Publication still observes
     * only a complete content presence/PUT ACK; one dependency component may
     * exceed the soft limits, but is never split across root publication. */
    readonly slice?: {
        readonly maxSourceBytes: number;
        readonly maxElapsedMs: number;
        readonly maxBoundaryFiles: number;
        /** Defaults to true. A caller whose reviewed selection cannot requeue
         * a consumed suffix may disable routine byte/time publication splits
         * while retaining bounded source batches and urgent preemption. */
        readonly allowRoutineSplit?: boolean;
        shouldPreempt(): boolean;
        /** Test seam; production omits this and uses the monotonic host clock. */
        now?: () => number;
    };
    assertApplicable(path: string): void;
    publish(candidate: Omit<RootCandidatePublication, "journalEpoch" | "journalCuts">,
        assertCandidateCurrent: () => void): Promise<RootRecoveryResult>;
}

export interface PushPreparedContext {
    plan: PreparedTransferPlan;
    memoryArbiter?: SyncMemoryArbiter;
    confirmations?: ObjectConfirmationStore;
    serverGeneration?: string;
    scopeHash: string;
    journalThroughId(path: string): number | undefined;
    assertApplicable(path: string): void;
}

function hasDurableObjectConfirmation(prepared: PushPreparedContext | undefined,
    kind: ConfirmedObjectKind, hash: string): boolean {
    if (!prepared?.confirmations || !prepared.serverGeneration) return false;
    try {
        return prepared.confirmations.has(prepared.scopeHash, prepared.serverGeneration, { kind, hash });
    } catch {
        // Positive hints are optional. A poisoned/unloaded store never blocks
        // authoritative network checks or publication.
        return false;
    }
}

async function retainDurableObjectConfirmations(prepared: PushPreparedContext | undefined,
    values: readonly ConfirmedObject[]): Promise<void> {
    if (!values.length || !prepared?.confirmations || !prepared.serverGeneration) return;
    try {
        await prepared.confirmations.retain(prepared.scopeHash, prepared.serverGeneration, values);
    } catch {
        // Server presence/upload ACK already succeeded. Losing this cache write
        // costs a later check; it cannot restore or acknowledge journal work.
        console.warn("[obsetync] object-presence confirmation awaits validated recovery");
    }
}

function confirmedObjectsFromUploadRecords(records: readonly BulkUploadRecord[]): ConfirmedObject[] {
    const values: ConfirmedObject[] = [];
    for (const record of records) {
        if (record.kind === BulkObjectKind.ContentChunk) values.push({ kind: "content-chunk", hash: record.hash });
        else if (record.kind === BulkObjectKind.Content) values.push({ kind: "content", hash: record.hash });
    }
    return values;
}

async function invalidateDurableObjectConfirmations(prepared: PushPreparedContext | undefined): Promise<void> {
    if (!prepared?.confirmations || !prepared.serverGeneration) return;
    try { await prepared.confirmations.invalidate(prepared.scopeHash, prepared.serverGeneration); }
    catch { console.warn("[obsetync] object-presence invalidation awaits validated recovery"); }
}

/** Engine-owned committed-tree preparation. Production uses this to route a
 * rootless/pending-retirement wrapper through admitted replacement before any
 * candidate exists. Direct compatibility callers may omit it. */
export interface PushCommittedBootstrap {
    prepare(): Promise<void>;
}

/** Streaming Blake3 hasher — feed bounded slices, call finalize(), then free(). */
export interface WasmHasher {
    update(chunk: Uint8Array): void;
    update_and_hash(chunk: Uint8Array): string;
    finalize(): string;
    free(): void;
}

export interface WasmChunker {
    update(chunk: Uint8Array): void;
    finish(): any;
    free(): void;
}

export interface WasmModule {
    /** Optional per-instance requested-allocation telemetry; not RSS or admission. */
    wasm_memory_snapshot?(): unknown;
    wasm_hash(data: Uint8Array): string;
    wasm_should_chunk(size: number): boolean;
    wasm_chunk_file(data: Uint8Array): any;
    wasm_get_file_chunk(data: Uint8Array, offset: number, size: number): Uint8Array;
    wasm_tree_get_chunk(tree: any, hash: string): Uint8Array | null;
    wasm_tree_chunk_byte_length(tree: any, hash: string): number | undefined;
    wasm_tree_chunk_hashes(tree: any): string[];
    wasm_tree_committed_chunk_hashes(tree: any): string[];
    wasm_tree_candidate_chunk_hashes(tree: any): string[];
    wasm_tree_new_candidate_chunk_hashes(tree: any): string[];
    wasm_root_hash_from_bytes(bytes: Uint8Array): string | undefined;
    wasm_root_version_from_bytes(bytes: Uint8Array): number | undefined;
    /** Streaming Blake3 hasher. Peak WASM heap = feed size, not file size. */
    Hasher: new () => WasmHasher;
    WasmChunker: new () => WasmChunker;
    /**
     * Hash N files in one WASM call. data = concatenated bytes of all files.
     * offsets[i] = byte offset where file i starts. sizes[i] = byte length of file i.
     * Returns hex hashes, one per file. ONE WASM boundary crossing for the whole group.
     */
    wasm_hash_batch(data: Uint8Array, offsets: Uint32Array, sizes: Uint32Array): string[];
    WasmTree: {
        new (vaultId: string, deviceId: string): WasmTree;
    };
}

export interface WasmTree {
    /** Optional node-store component counters, not a total tree/native heap budget. */
    chunk_memory_snapshot?(): unknown;
    /** Optional root/identity/baseline metadata counters, not total heap or admission. */
    metadata_memory_snapshot?(): unknown;
    /** Optional exact accepted replacement-input Vec/path components. */
    replacement_input_memory_snapshot?(): unknown;
    /** Optional exact original FileEntry redistribution components for Tree v1. */
    replacement_v1_entries_memory_snapshot?(): unknown;
    /** Optional exact V1 graph-assembly Vec/String ownership components. */
    replacement_v1_graph_memory_snapshot?(): unknown;
    /** Optional exact top-level Vec backing for replacement indirect sorters. */
    replacement_sort_memory_snapshot?(): unknown;
    /** Optional exact POD descriptor Vec backing for Tree v2 replacement planning. */
    replacement_v2_planning_memory_snapshot?(): unknown;
    /** Optional exact V2 replacement range Vec/endpoint String components. */
    replacement_v2_ranges_memory_snapshot?(): unknown;
    /** Optional exact V2 entry Vec/path components after replacement sorting. */
    replacement_v2_post_sort_memory_snapshot?(): unknown;
    /** Packaged wasm-bindgen trees expose explicit disposal. Optional here for
     * non-owning test/host ports; plugin ownership validates it before release. */
    free?(): void;
    set_tree_version(version: number): void;
    tree_version(): number;
    rebuild_from_entries_in_version(version: number, entriesJson: string): void;
    begin_replacement_rebuild_job?(version: number, expectedEntries: number, expectedJsonBytes: number): number;
    append_replacement_rebuild_job?(token: number, expectedOffset: number, entriesJson: string): number;
    start_replacement_rebuild_job?(token: number): void;
    /** Optional exact V2 sorter index allocation requests, read after input is complete. */
    replacement_rebuild_sort_memory_plan_v1_job?(token: number): unknown;
    /** Echo both admitted index requests before allocating the private sorter. */
    start_replacement_rebuild_sort_memory_v1_job?(token: number,
        expectedSourceIndexBytes: number, expectedTargetIndexBytes: number): void;
    step_replacement_rebuild_job?(token: number, maxUnits: number): unknown;
    /** Opt-in step family which exposes the output admission barrier. */
    step_replacement_rebuild_output_memory_v1_job?(token: number, maxUnits: number): unknown;
    replacement_rebuild_plan_job?(token: number): unknown;
    resume_replacement_rebuild_job?(token: number, expectedNodePayloadBytes: number): void;
    /** Optional versioned V2 node-payload/endpoint memory plan; not a total heap quota. */
    replacement_rebuild_output_memory_plan_v1_job?(token: number): unknown;
    /** Echo all scoped witnesses for this token before output construction resumes. */
    resume_replacement_rebuild_output_memory_v1_job?(token: number, expectedNodePayloadBytes: number,
        expectedRangeEndpointPeakRequestedBytes: number,
        expectedRangeEndpointResidentRequestedBytes: number): void;
    finish_replacement_rebuild_job?(token: number): void;
    cancel_replacement_rebuild_job_deferred?(token: number): void;
    finish_replacement_rebuild_job_deferred?(token: number): void;
    step_tree_retirement?(token: number, maxUnits: number): unknown;
    load_root(rootBytes: Uint8Array): void;
    root_hash_hex(): string | null;
    /** O(1), monotonic only within this exact WasmTree wrapper. */
    committed_revision?(): number;
    /** O(1), monotonic witness for visible candidate replacements. */
    candidate_revision?(): number;
    root_bytes(): Uint8Array | null;
    total_files(): number;
    begin_candidate(): void;
    begin_candidate_job?(): number;
    begin_candidate_chunks_job?(): number;
    begin_candidate_update_job?(entriesJson: string): number;
    begin_candidate_delete_job?(pathsJson: string): number;
    step_candidate_mutation_output_memory_v1_job?(token: number, maxUnits: number): unknown;
    candidate_mutation_output_memory_plan_v1_job?(token: number): unknown;
    resume_candidate_mutation_output_memory_v1_job?(token: number, expectedNodePayloadBytes: number,
        expectedRangeEndpointPeakRequestedBytes: number,
        expectedRangeEndpointResidentRequestedBytes: number): void;
    candidate_mutation_output_memory_ready_v1_job?(token: number): unknown;
    finish_candidate_mutation_job_deferred?(token: number): void;
    cancel_candidate_mutation_job_deferred?(token: number): void;
    begin_tree_chunk_export_job?(hash: string): unknown;
    read_tree_chunk_export_job?(token: number, expectedOffset: number, maxBytes: number): unknown;
    finish_tree_chunk_export_job?(token: number): void;
    begin_candidate_root_export_job?(maxArenaBytes: number): number;
    begin_committed_root_export_job?(maxArenaBytes: number): number;
    step_root_export_job?(token: number, maxUnits: number, maxBytes: number): unknown;
    root_export_workset?(token: number): unknown;
    start_root_export_build_job?(token: number): void;
    root_export_info?(token: number): unknown;
    read_root_export_job?(token: number, expectedOffset: number, maxBytes: number): unknown;
    finish_root_export_job?(token: number): void;
    step_tree_job?(token: number, maxUnits: number): TreeJobProgress;
    step_reachability_job_deferred?(token: number, maxUnits: number): TreeJobProgress;
    finish_candidate_job?(token: number): number;
    finish_candidate_job_deferred?(token: number): number;
    candidate_open_memory_plan_v1_job?(token: number): unknown;
    resume_candidate_open_memory_v1_job?(token: number, identityBytes: number,
        endpointBytes: number, rootStringBytes: number): void;
    finish_candidate_chunks_job?(token: number): unknown;
    finish_candidate_chunks_job_deferred?(token: number): unknown;
    candidate_chunks_sort_memory_plan_v1_job?(token: number): unknown;
    resume_candidate_chunks_sort_memory_v1_job?(token: number,
        sourceHashesBytes: number, scratchHashesBytes: number): void;
    step_candidate_chunks_sort_v1_job?(token: number, maxUnits: number): unknown;
    candidate_chunks_plan_info_v1_job?(token: number): unknown;
    read_candidate_chunks_page_v1_job?(token: number, expectedOffset: number,
        maxHashes: number): unknown;
    finish_candidate_chunks_plan_v1_job?(token: number): void;
    finish_candidate_mutation_job?(token: number): void;
    cancel_tree_job?(token: number): void;
    cancel_reachability_job_deferred?(token: number): void;
    step_reachability_retirement?(token: number, expectedCompleted: number, maxUnits: number): unknown;
    finish_reachability_retirement?(token: number, expectedCompleted: number): void;
    has_candidate(): boolean;
    candidate_root_hash_hex(): string | null;
    candidate_root_bytes(): Uint8Array | null;
    candidate_total_files(): number;
    candidate_update_batch(entriesJson: string): void;
    candidate_delete_batch(pathsJson: string): void;
    /** Additive atomic V2 commit/abort receipts. Missing pair keeps the old
     * conservative resident charge; a partial pair is rejected before use. */
    commit_candidate_output_settlement_v1?(): unknown;
    abort_candidate_output_settlement_v1?(): unknown;
    commit_candidate(): WasmTreeGcStats;
    abort_candidate(): WasmTreeGcStats;
    update_entry(path: string, hash: string, mtime: number, size: number): void;
    delete_entry(path: string): void;
    build_from_entries(entriesJson: string): void;
    /** Upsert N entries in ONE update_tree call. JSON: [{path,hash,mtime_ms,size},...] */
    update_batch(entriesJson: string): void;
    /** Delete N paths in ONE update_tree call. JSON: ["path/a.md","path/b.md",...] */
    delete_batch(pathsJson: string): void;
}

export interface WasmTreeGcStats {
    before: number;
    reachable: number;
    removed: number;
    after: number;
    bytes_removed: number;
}

export interface FileChange {
    action: "created" | "modified" | "deleted";
    path: string;
    hash?: string;
    data?: Uint8Array; // only populated for single-file vault events
    mtime?: number;
    size?: number;
}

export interface DeferredPushChange {
    path: string;
    reason: "source-too-large" | "dependent-delete";
    requiredBytes: number;
    capacityBytes: number;
}

export interface PushOutcome {
    newRootHash: string | null;
    conflicts: any[];
    deferred?: DeferredPushChange[];
    published?: boolean;
    /** Prepared hints may be retired only after the caller's journal ACK. */
    preparedCleanup?: Array<{ scopeHash: string; path: string; expectedMutationId: number }>;
    /** Present only for the new durable branch; never repeat legacy base/ACK. */
    rootSettlement?: RootRecoveryResult;
    /** Selected, reviewed work intentionally left out at a safe root boundary.
     * Paths remain owned by dirty/WAL state and receive no cooldown. */
    continuation?: {
        paths: string[];
        reason: "urgent" | "bytes" | "time";
        sourceBytes: number;
        elapsedMs: number;
    };
}

type RootSliceReason = NonNullable<PushOutcome["continuation"]>["reason"];

function validateRootSlice(rootContext: PushRootContext | undefined): PushRootContext["slice"] {
    const slice = rootContext?.slice;
    if (!slice) return undefined;
    if (!Number.isSafeInteger(slice.maxSourceBytes) || slice.maxSourceBytes < 1 ||
        !Number.isFinite(slice.maxElapsedMs) || slice.maxElapsedMs <= 0 ||
        !Number.isSafeInteger(slice.maxBoundaryFiles) || slice.maxBoundaryFiles < 1 ||
        slice.maxBoundaryFiles > 256 || typeof slice.shouldPreempt !== "function" ||
        (slice.allowRoutineSplit !== undefined && typeof slice.allowRoutineSplit !== "boolean") ||
        (slice.now !== undefined && typeof slice.now !== "function")) {
        throw new Error("durable publication has invalid slice limits");
    }
    return slice;
}

/** Return a deterministic suffix only when the processed prefix is a complete
 * set of dependency components. A crossing group returns null so the caller
 * extends work FORWARD through the component; soft limits never roll a
 * completed member back and cannot livelock on a large/interleaved group. */
function rootSliceSuffixAtSafeBoundary(
    changes: readonly FileChange[],
    groups: readonly (readonly string[])[],
    processedPaths: ReadonlySet<string>,
    deferredPaths: ReadonlySet<string>,
): { held: Set<string>; continuation: string[] } | null {
    const indexByPath = new Map<string, number>();
    for (let index = 0; index < changes.length; index++) {
        const path = changes[index].path;
        if (indexByPath.has(path)) throw new Error("durable publication contains a duplicate path");
        indexByPath.set(path, index);
    }
    let cut = changes.length;
    for (let index = 0; index < changes.length; index++) {
        const change = changes[index];
        if (change.action !== "deleted" && !processedPaths.has(change.path) &&
            !deferredPaths.has(change.path)) {
            cut = index;
            break;
        }
    }
    if (cut === changes.length) return { held: new Set(), continuation: [] };
    for (const group of groups) {
        let first = changes.length, last = -1, deferred = false;
        for (const path of group) {
            const index = indexByPath.get(path);
            if (index === undefined) throw new Error("durable publication group contains an unknown path");
            first = Math.min(first, index); last = Math.max(last, index);
            deferred ||= deferredPaths.has(path);
        }
        // An intrinsically deferred group is already withheld by the existing
        // dependency closure and does not require more source work here.
        if (!deferred && first < cut && last >= cut) return null;
    }
    const held = new Set<string>();
    const continuation: string[] = [];
    for (let index = cut; index < changes.length; index++) {
        const path = changes[index].path;
        held.add(path);
        if (!deferredPaths.has(path)) continuation.push(path);
    }
    return { held, continuation };
}

/**
 * Per-file state held while a push batch is in flight. A named class (vs an
 * object literal) so DevTools heap snapshots attribute these — and any
 * `largeData` blobs they retain — to obsetync: filter the Constructor column
 * by "Obsetync" and read Retained Size.
 */
class ObsetyncBatchFile {
    constructor(
        public change: FileChange,
        public size: number,
        public mtime: number,
        /** kept only for large files (needed for chunk upload) */
        public chunkInfo?: any,
        /** Source retained through ACK; small unknown files reuse their read. */
        public largeData?: Uint8Array,
        /** exact pass-1 source identity for desktop ranged pass 2 */
        public rangedSource?: DesktopRangeSource,
        /** Cached manifest ranges require a local content check before send. */
        public reusedPrepared = false,
        /** Runtime-qualified public resource reader on mobile. */
        public mobileRangeReader?: MobileRangeReader,
        /** Detached prepared manifest remains admitted through object ACK. */
        public preparedMemoryOwner?: { release(): void },
    ) {}
}

interface ObsetyncRead {
    change: FileChange;
    data: Uint8Array;
}

function hashUnknownSmallReadsRenderer(
    wasm: WasmModule,
    reads: ObsetyncRead[],
    tuning: HashTuning,
    perf?: PerfOperation,
    additionalResidentBytes = 0,
): ObsetyncBatchFile[] {
    const result: ObsetyncBatchFile[] = [];
    const batches = planByteBoundedBatches(reads, (read) => read.data.length, {
        maxFiles: tuning.maxBatchFiles,
        maxBytes: tuning.maxBatchBytes,
        maxSingleBytes: tuning.maxSingleBatchFileBytes,
        maxHoldMs: tuning.maxBatchHoldMs,
    });
    const residentBytes = additionalResidentBytes +
        reads.reduce((sum, read) => sum + read.data.length, 0);

    for (const batch of batches) {
        const totalBytes = batch.reduce((sum, read) => sum + read.data.length, 0);
        perf?.observePeakBatchBytes(residentBytes + totalBytes);
        const flat = new Uint8Array(totalBytes);
        const offsets = new Uint32Array(batch.length);
        const sizes = new Uint32Array(batch.length);
        let offset = 0;
        for (let index = 0; index < batch.length; index++) {
            flat.set(batch[index].data, offset);
            offsets[index] = offset;
            sizes[index] = batch[index].data.length;
            offset += batch[index].data.length;
        }
        const endHash = perf?.phase("hash");
        const hashStarted = monotonicNow();
        let hashes: string[];
        try {
            hashes = wasm.wasm_hash_batch(flat, offsets, sizes);
        } finally {
            observeHashStep(totalBytes, hashStarted);
            endHash?.();
        }
        for (let index = 0; index < batch.length; index++) {
            const read = batch[index];
            read.change.hash = hashes[index];
            result.push(new ObsetyncBatchFile(
                read.change,
                read.data.length,
                read.change.mtime ?? Date.now(),
                undefined,
                read.data,
            ));
        }
    }
    return result;
}

/** Hash already-admitted exact-owned bytes outside the renderer. The returned
 * allocation replaces the detached input and remains owned by the caller's
 * existing source lease. */
export async function hashAdmittedBytesWithBrowserWorker(
    wasm: WasmModule,
    data: Uint8Array,
    perf: PerfOperation | undefined,
    signal: AbortSignal | undefined,
    admittedFeedCeiling: number,
    runtime?: BrowserHashWorkerRuntime | null,
    perfWeight = 1,
): Promise<{ hash: string; data: Uint8Array; offloaded: boolean }> {
    const renderer = async () => ({
        hash: await hashAdmittedRange(wasm, data, perf, signal, admittedFeedCeiling, perfWeight),
        data,
        offloaded: false,
    });
    if (!runtime) return renderer();
    runtime.assertCurrent();
    const stats = runtime.pool.stats();
    if (stats.state !== "ready") {
        if (!stats.fallbackSafe) {
            const diagnostics = runtime.pool.diagnostics();
            throw new BrowserHashWorkerError("browser hash worker cleanup is unconfirmed",
                "UNAVAILABLE", false, diagnostics.unconfirmedReleaseBytes);
        }
        return renderer();
    }
    if (!(data.buffer instanceof ArrayBuffer) || data.byteOffset !== 0 ||
        data.byteLength !== data.buffer.byteLength || data.byteLength > BROWSER_HASH_WORKER_MAX_INPUT_BYTES) {
        return renderer();
    }
    try {
        const result = await runtime.pool.run(data, {
            feedBytes: Math.min(getHashTuning().feedBytes, admittedFeedCeiling), signal,
        });
        runtime.assertCurrent();
        perf?.addPhase("hash", result.hashMs * perfWeight);
        return { hash: result.hash, data: result.returnedBytes, offloaded: true };
    } catch (error) {
        if ((error as Error)?.name === "AbortError") throw error;
        if (!(error instanceof BrowserHashWorkerError)) throw error;
        if (!error.fallbackSafe) throw error;
        runtime.assertCurrent();
        if (data.byteLength === 0) throw new BrowserHashFreshReadRequired();
        return renderer();
    }
}

export class BrowserHashFreshReadRequired extends Error {
    constructor() {
        super("browser hash worker requires a freshly admitted source read");
        this.name = "BrowserHashFreshReadRequired";
    }
}

async function hashUnknownSmallReads(
    wasm: WasmModule,
    reads: ObsetyncRead[],
    tuning: HashTuning,
    perf?: PerfOperation,
    signal?: AbortSignal,
    browserHash?: BrowserHashWorkerRuntime | null,
): Promise<ObsetyncBatchFile[]> {
    if (!browserHash) return hashUnknownSmallReadsRenderer(wasm, reads, tuning, perf);
    const result: ObsetyncBatchFile[] = [];
    const rendererReads: ObsetyncRead[] = [];
    for (const read of reads) {
        throwIfWorkAborted(signal);
        const hashed = await hashAdmittedBytesWithBrowserWorker(
            wasm, read.data, perf, signal, tuning.maxFeedBytes, browserHash,
        );
        if (!hashed.offloaded) {
            rendererReads.push(read);
            continue;
        }
        read.data = hashed.data;
        read.change.hash = hashed.hash;
        result.push(new ObsetyncBatchFile(read.change, read.data.length,
            read.change.mtime ?? Date.now(), undefined, read.data));
    }
    const offloadedResidentBytes = result.reduce((sum, file) => sum + (file.largeData?.byteLength ?? 0), 0);
    result.push(...hashUnknownSmallReadsRenderer(
        wasm, rendererReads, tuning, perf, offloadedResidentBytes,
    ));
    return result;
}

type PushLookaheadResult = { ok: true } | { ok: false; error: unknown };
const PUSH_LOOKAHEAD_MAX_CHUNK_HASHES = 256;

function validateLookaheadMissingHashes(requestedHashes: readonly string[], value: unknown): Set<string> {
    if (!Array.isArray(value)) throw new Error("lookahead content check returned a non-array");
    const requested = new Set(requestedHashes);
    const missing = new Set<string>();
    for (const hash of value) {
        if (typeof hash !== "string" || !/^[0-9a-f]{64}$/.test(hash) ||
            !requested.has(hash) || missing.has(hash)) {
            throw new Error("lookahead content check returned an invalid hash set");
        }
        missing.add(hash);
    }
    return missing;
}

/** Prepare one next-in-order desktop ranged source. The worker streams from
 * the native path and the durable prepared-plan owns the bounded manifest
 * after this transient scope closes. Reuse still requires the owning batch's
 * full-file hash validation; this is only CPU/check lookahead, never source
 * authority. */
async function prepareRangedPushLookahead(
    change: FileChange, api: ObsetyncApi, io: PlatformIO, wasm: WasmModule,
    hashWorkers: DesktopHashWorkerPool, prepared: PushPreparedContext,
    neededChunks: Map<string, boolean>, baseRootHash: string | null,
    perf?: PerfOperation, signal?: AbortSignal, assertApplicable?: (path: string) => void,
    checkPermit?: Promise<boolean>,
): Promise<void> {
    if (change.size === undefined || change.mtime === undefined ||
        !wasm.wasm_should_chunk(change.size)) return;
    const absolutePath = io.getAbsolutePath(change.path);
    if (!absolutePath) return;
    assertApplicable?.(change.path);
    const tuning = getHashTuning();
    let memoryPlan = planPushMemory([{
        size: change.size, chunked: true, ranged: true, worker: true,
    }], tuning);
    const capacityBytes = transientMemorySnapshot().capacityBytes;
    if (memoryPlan.totalBytes > capacityBytes) {
        memoryPlan = planPushMemory([{
            size: change.size, chunked: true, ranged: true, worker: true,
        }], tuning, MAX_FASTCDC_CHUNK_BYTES);
    }
    if (memoryPlan.totalBytes > capacityBytes) return;
    let memory: TransientWorkScope;
    try { memory = await reserveTransientScope(memoryPlan, { signal }); }
    catch (error) {
        if (error instanceof ResourceBudgetOversizedError) return;
        throw error;
    }
    try {
        throwIfWorkAborted(signal);
        assertApplicable?.(change.path);
        const endPrepare = perf?.phase("prepare_batch");
        let result: Awaited<ReturnType<DesktopHashWorkerPool["run"]>>;
        try {
            result = await memory.run(PUSH_CHUNKER_WORK_BYTES,
                context => context.track(() => hashWorkers.run({
                    absolutePath,
                    expectedSize: change.size!,
                    expectedMtime: change.mtime!,
                    mode: "manifest",
                    feedBytes: Math.min(tuning.feedBytes, tuning.maxFeedBytes),
                }, signal, context)), { signal });
        } finally { endPrepare?.(); }
        throwIfWorkAborted(signal);
        assertApplicable?.(change.path);
        if (result.mode !== "manifest") throw new Error("lookahead worker returned the wrong result mode");
        const retained = await prepared.plan.retain({
            scopeHash: prepared.scopeHash,
            path: change.path,
            journalThroughId: prepared.journalThroughId(change.path),
            baseRoot: baseRootHash,
            source: { size: result.size, mtime: result.mtime,
                fingerprint: { kind: "desktop-v1", ...result.fingerprint } },
            manifest: result.manifest,
        });
        throwIfWorkAborted(signal);
        assertApplicable?.(change.path);
        if (!retained.retained) return;
        const hashes = [...new Set(result.manifest.chunks
            .slice(0, PUSH_LOOKAHEAD_MAX_CHUNK_HASHES)
            .map(chunk => chunk.hash))];
        if (hashes.length === 0) return;
        if (checkPermit && !await checkPermit) return;
        throwIfWorkAborted(signal);
        assertApplicable?.(change.path);
        const endCheck = perf?.phase("check");
        let response: unknown;
        try { response = await api.checkContentChunks(hashes, perf, signal, memory); }
        finally { endCheck?.(); }
        throwIfWorkAborted(signal);
        assertApplicable?.(change.path);
        for (const hash of validateLookaheadMissingHashes(hashes, response)) {
            neededChunks.set(hash, true);
        }
    } finally {
        memory.close();
    }
}

/** One bounded future window, never a tail-sized promise graph. Source bytes
 * are admitted and released inside each read/hash; only missing-object hints
 * survive. The owning upload batch always resolves unknown source bytes again;
 * lookahead overlaps bounded cache-warming/hash/check work with the preceding
 * network wait without treating an unleased presence answer as authority. */
async function preparePushLookahead(
    changes: readonly FileChange[], api: ObsetyncApi, io: PlatformIO, wasm: WasmModule,
    needed: Map<string, boolean>, neededChunks: Map<string, boolean>,
    hashWorkers: DesktopHashWorkerPool | null | undefined, prepared: PushPreparedContext | undefined,
    baseRootHash: string | null, perf?: PerfOperation, beforeHeavyBatch?: () => Promise<void>,
    signal?: AbortSignal, assertApplicable?: (path: string) => void,
    checkPermit?: Promise<boolean>,
    workLane: WorkLane = "bulk",
    browserHash?: BrowserHashWorkerRuntime | null,
): Promise<void> {
    if (changes.length === 0) return;
    await beforeHeavyBatch?.();
    await yieldWork({ perf, signal, lane: workLane });
    throwIfWorkAborted(signal);
    const first = changes[0];
    if (first?.size !== undefined && wasm.wasm_should_chunk(first.size)) {
        if (hashWorkers && prepared) {
            await prepareRangedPushLookahead(first, api, io, wasm, hashWorkers, prepared,
                neededChunks, baseRootHash, perf, signal, assertApplicable, checkPermit);
        }
        return;
    }
    const tuning = getHashTuning();
    const candidates: FileChange[] = [];
    let candidateBytes = 0;
    for (const change of changes) {
        if (candidates.length >= tuning.maxBatchFiles || change.size === undefined ||
            wasm.wasm_should_chunk(change.size) || change.size > tuning.maxSingleBatchFileBytes ||
            (candidates.length > 0 && candidateBytes + change.size > tuning.maxBatchBytes)) break;
        candidates.push(change);
        candidateBytes += change.size;
    }
    const toHash = candidates.slice(0, Math.max(1, Math.min(4, tuning.readConcurrency)))
        .filter(change => !change.hash && !change.data &&
        change.size !== undefined && change.mtime !== undefined &&
        !wasm.wasm_should_chunk(change.size));
    const settled = await Promise.allSettled(toHash.map(async change => {
        assertApplicable?.(change.path);
        const before = await io.stat(change.path);
        throwIfWorkAborted(signal);
        if (!before || before.size !== change.size || Math.abs(before.mtime - change.mtime!) > 1) {
            throw new HashWorkerFileDriftError("lookahead source changed before hashing");
        }
        const hash = await withHashSource({
            sourceBytes: before.size,
            feedBytes: tuning.maxFeedBytes,
            signal,
            read: () => io.readFile(change.path),
            consume: browserHash ? async data => (await hashAdmittedBytesWithBrowserWorker(
                wasm, data, perf, signal, tuning.maxFeedBytes, browserHash,
            )).hash : data => {
                const endHash = perf?.phase("hash");
                const started = monotonicNow();
                try { return wasm.wasm_hash(data); }
                finally { observeHashStep(data.byteLength, started); endHash?.(); }
            },
        });
        const after = await io.stat(change.path);
        throwIfWorkAborted(signal);
        assertApplicable?.(change.path);
        if (!after || after.size !== before.size || Math.abs(after.mtime - before.mtime) > 1) {
            throw new HashWorkerFileDriftError("lookahead source changed while hashing");
        }
        return { change, hash };
    }));
    const failed = settled.find((row): row is PromiseRejectedResult => row.status === "rejected");
    if (failed) throw failed.reason;
    const predicted = new Map<FileChange, string>();
    for (const row of settled as PromiseFulfilledResult<{ change: FileChange; hash: string }>[]) {
        predicted.set(row.value.change, row.value.hash);
    }
    throwIfWorkAborted(signal);
    for (const change of candidates) assertApplicable?.(change.path);
    const hashes = [...new Set(candidates.map(change => change.hash ?? predicted.get(change)).filter((hash): hash is string =>
        typeof hash === "string" && /^[0-9a-f]{64}$/.test(hash)))];
    if (hashes.length === 0) return;
    // The current batch may already own two HTTP checks. Do not add a third
    // concurrent request; once those settle this single speculative check can
    // overlap current missing-source preparation/upload.
    if (checkPermit && !await checkPermit) return;
    throwIfWorkAborted(signal);
    const endCheck = perf?.phase("check");
    let response: unknown;
    try { response = await api.checkContent(hashes, perf); }
    finally { endCheck?.(); }
    throwIfWorkAborted(signal);
    const missingSet = validateLookaheadMissingHashes(hashes, response);
    for (const change of candidates) assertApplicable?.(change.path);
    // A speculative "present" answer cannot survive a server storage reset or
    // GC generation change because that capability is not yet in PR08. Cache
    // only missing=true: a later duplicate upload is harmless if another
    // client fills it first; present objects are authoritatively rechecked by
    // their owning batch.
    for (const hash of missingSet) needed.set(hash, true);
}

/**
 * Push path — streams through byte- and count-bounded batches so peak memory
 * follows the platform budget, not total vault size.
 *
 * Per batch:
 *   A. Read + hash (parallel reads; wasm_hash_batch for small unknown files per group)
 *   B. Batch-check hashes against server (2 requests)
 *   C. Upload only what's missing; collect tree updates
 *   → data released at end of batch
 *
 * After all batches:
 *   D. tree.candidate_update_batch — one update_tree call for all N upserts
 *   E. Upload index chunks + push root
 *
 * Root push and sync-base save happen once after all batches.
 */
export async function push(
    api: ObsetyncApi,
    io: PlatformIO,
    syncBase: ObsetyncSyncBase,
    wasm: WasmModule,
    tree: WasmTree,
    vaultId: string,
    changes: FileChange[],
    /** The server root this device's tree state was DERIVED from — i.e. the
     *  last root the tree verifiably reconciled with (engine.treeBaseRoot),
     *  NOT merely the last root observed on the server. The server uses it
     *  as the merge base, so lying here (e.g. sending a freshly-polled
     *  current root while the tree is stale) turns a divergence into a
     *  "fast-forward" that reverts the whole vault — incident 2026-07-13. */
    baseRootHash: string | null,
    onProgress?: (msg: string) => void,
    perf?: PerfOperation,
    hashWorkers?: DesktopHashWorkerPool | null,
    beforeHeavyBatch?: () => Promise<void>,
    signal?: AbortSignal,
    prepared?: PushPreparedContext,
    rootContext?: PushRootContext,
    committedBootstrap?: PushCommittedBootstrap,
    candidateMutationAdmission?: RootTreeResidentAdmission,
    workLane: WorkLane = "bulk",
    browserHash?: BrowserHashWorkerRuntime | null,
): Promise<PushOutcome> {
    throwIfWorkAborted(signal);
    perf?.setWorkload({ filesTotal: changes.length });
    if (changes.length === 0) {
        perf?.setWorkload({
            bytesTotal: 0,
            filesNeeded: 0,
            bytesNeeded: 0,
        });
        return { newRootHash: tree.root_hash_hex(), conflicts: [] };
    }
    const rootSlice = validateRootSlice(rootContext);
    const sliceClock = rootSlice?.now ?? monotonicNow;
    const sliceStartedAt = rootSlice ? sliceClock() : 0;
    if (rootSlice && !Number.isFinite(sliceStartedAt)) {
        throw new Error("durable publication slice clock is invalid");
    }
    let sliceSourceBytes = 0;
    let sliceDecision: { reason: RootSliceReason; elapsedMs: number } | null = null;
    let sliceSuffix: { held: Set<string>; continuation: string[] } | null = null;
    let publicationChanges: readonly FileChange[] = changes;
    let continuation: PushOutcome["continuation"];

    // Fail terminal enrollment/transport mismatches before hashing thousands
    // of files or touching the candidate tree. Authentication is still
    // checked by the first real request; this resolves the local v2 channel.
    const endPreflight = perf?.phase("check");
    try {
        await api.ensureTransportReady(perf);
    } finally {
        endPreflight?.();
    }
    throwIfWorkAborted(signal);

    // A failed prior mutation cleanup owns the only native job slot and may
    // also carry this wrapper's exact candidate-abort debt. Resolve it before
    // bootstrap inspects/rebuilds the committed owner: an old candidate must
    // not make that preparation fail forever before cleanup gets a turn.
    await drainTreeCandidateMutationRetirement(tree);
    throwIfWorkAborted(signal);

    const total = changes.length;
    console.log(`[obsetync] pushing ${total} changes`);

    // Capture parentless publication before preparation installs a committed
    // graph: it must check ALL candidate chunks because the server cannot be
    // assumed to own a baseline. The engine hook never falls back after an
    // error. Direct structural/legacy callers retain the old compatibility
    // bootstrap until pull/push compatibility APIs can become async owners.
    let bootstrapped = baseRootHash === null;
    if (committedBootstrap) {
        const expectedVersion = tree.tree_version();
        await committedBootstrap.prepare();
        throwIfWorkAborted(signal);
        if (!tree.root_hash_hex() || tree.has_candidate() || tree.tree_version() !== expectedVersion) {
            throw new Error("push committed-tree bootstrap returned an invalid owner");
        }
    } else if (!tree.root_hash_hex()) {
        const endEnumerate = perf?.phase("enumerate");
        let baseEntries: Array<{ path: string; hash: string; mtime_ms: number; size: number }>;
        try {
            baseEntries = syncBase.allPaths().map((path) => {
                const entry = syncBase.getEntry(path)!;
                return { path, hash: entry.hash,
                    mtime_ms: syncBase.getTreeMtime(path) ?? entry.mtime, size: entry.size };
            });
        } finally { endEnumerate?.(); }
        const endTree = perf?.phase("tree_update");
        try {
            tree.build_from_entries(JSON.stringify(baseEntries));
            bootstrapped = true;
        } finally { endTree?.(); }
    }

    let candidateOpen = false;
    let candidateMutationInputCount: number | null = null;
    let candidateOwnedRevision: number | undefined;
    let candidateRevisionIndeterminate = false;
    const candidateRevisionReader = tree.candidate_revision;
    const committedRevisionReader = tree.committed_revision;
    if (candidateRevisionReader !== undefined && typeof candidateRevisionReader !== "function") {
        throw new Error("push candidate revision witness is invalid");
    }
    const readOwnedCandidateRevision = candidateRevisionReader === undefined ? undefined : () => {
        const revision = candidateRevisionReader.call(tree);
        if (!Number.isSafeInteger(revision) || revision < 0) {
            throw new Error("push candidate revision witness is invalid");
        }
        return revision;
    };
    const recordOwnedCandidateRevision = (): number | undefined => {
        candidateOwnedRevision = undefined;
        if (!readOwnedCandidateRevision) return undefined;
        candidateRevisionIndeterminate = true;
        const revision = readOwnedCandidateRevision();
        candidateOwnedRevision = revision;
        candidateRevisionIndeterminate = false;
        if (candidateMutationAdmission) {
            try {
                const committed = committedRevisionReader?.call(tree);
                if (!Number.isSafeInteger(committed) || committed! < 0 ||
                    !candidateMutationAdmission.advanceV2CandidateRevision(tree, committed!, revision)) {
                    candidateMutationAdmission.invalidateV2Graph(tree);
                }
            } catch {
                try { candidateMutationAdmission.invalidateV2Graph(tree); } catch { /* optional provenance */ }
            }
        }
        return revision;
    };
    const assertOwnedCandidateRevision = (): void => {
        if (candidateOwnedRevision !== undefined && readOwnedCandidateRevision &&
            readOwnedCandidateRevision() !== candidateOwnedRevision) {
            throw new Error("push candidate ownership changed during mutation");
        }
    };
    const settleCandidateOutput = (outcome: V2TreeOutputSettlementOutcome) => {
        const settlement = settleTreeCandidateOutputWithAdmission(
            tree,
            outcome,
            candidateMutationAdmission,
            () => {
                if (!candidateOpen || tree.has_candidate() !== true) {
                    throw new Error("push candidate disappeared before settlement");
                }
                assertOwnedCandidateRevision();
            },
        );
        if (settlement.gcStats) perf?.setWasmChunks({ after: settlement.gcStats.after });
        return settlement;
    };
    try {
    // Packaged trees validate the committed graph one native unit at a time.
    // Structural test/legacy ports retain the old synchronous fallback and
    // need the old explicit count because their begin method returns none.
    const candidateJobMembers = [tree.begin_candidate_job, tree.step_tree_job,
        tree.finish_candidate_job, tree.cancel_tree_job];
    const candidateJobClaimed = candidateJobMembers.some(member => member !== undefined);
    const candidateJobAvailable = candidateJobMembers.every(member => typeof member === "function");
    const candidateOpenMemoryAvailable =
        typeof tree.candidate_open_memory_plan_v1_job === "function" &&
        typeof tree.resume_candidate_open_memory_v1_job === "function";
    const legacyCommittedCount = !candidateJobClaimed
        ? wasm.wasm_tree_committed_chunk_hashes(tree).length : null;
    const opened = await beginTreeCandidate(tree, {
        signal,
        cooperate: async () => {
            await beforeHeavyBatch?.();
            await yieldWork({ perf, signal, lane: workLane });
        },
        cooperateRetirement: async () => {
            await yieldWork({ perf, lane: "interactive", deadlineMs: 50 });
        },
        assertCurrent: candidateJobAvailable ? () => {
            if (rootContext) for (const change of changes) rootContext.assertApplicable(change.path);
        } : undefined,
        onOpenMemoryPlan: candidateMutationAdmission && candidateOpenMemoryAvailable
            ? plan => admitCandidateOpenRoot(candidateMutationAdmission, tree, plan)
            : undefined,
        abortCandidateOpened: candidateMutationAdmission && candidateOpenMemoryAvailable && readOwnedCandidateRevision
            ? expectedRevision => settleTreeCandidateOutputWithAdmission(
                tree,
                "abort",
                candidateMutationAdmission,
                () => {
                    if (tree.has_candidate() !== true || readOwnedCandidateRevision() !== expectedRevision) {
                        throw new Error("push candidate ownership changed before begin cleanup");
                    }
                },
            )
            : undefined,
        onCandidateOpened: () => {
            candidateOpen = true;
            recordOwnedCandidateRevision();
        },
    });
    perf?.setWasmChunks({ before: opened.reachable ?? legacyCommittedCount ?? 0 });

    let deleted     = changes.filter(c => c.action === "deleted");
    const nonDeleted = changes.filter(c => c.action !== "deleted");
    const deferred: DeferredPushChange[] = [];

    console.log(
        `[obsetync] push: ${changes.length} changes (${nonDeleted.length} upsert, ` +
        `${deleted.length} delete), bootstrap=${bootstrapped}, ` +
        `sync-base size=${syncBase.entryCount()}`
    );

    // Deletions wait for source admission. Old rename hints have no durable
    // group identity, so a deferred target conservatively holds ALL deletes.
    let processed = 0;
    let uploadedBytes = 0;
    let neededFiles = 0;
    let neededBytes = 0;
    const startTime = Date.now();
    // Collected here; applied in ONE update_batch call after all content batches.
    const allTreeUpdates: { path: string; hash: string; mtime_ms: number; size: number }[] = [];
    const preparedPaths = new Set<string>();
    const preparedMutations = new Map<string, number>();
    const mobileRangeMethod = io.openMobileRangeReader;
    if (mobileRangeMethod !== undefined && typeof mobileRangeMethod !== "function") {
        throw new Error("invalid mobile range capability");
    }
    const openMobileRangeReader = mobileRangeMethod?.bind(io);

    // Stream through byte- and count-bounded NETWORK packs. Large or
    // size-unknown files are singletons. Small-file CPU/read work cooperates at
    // the narrower root boundary below without coupling that responsive
    // boundary to check/PUT request occupancy.
    let workerFallbackWarned = false;
    const splitBatches: FileChange[][] = [];
    const streamBatches = adaptiveBatches(nonDeleted, (change, limits) => {
        if (change.size === undefined || wasm.wasm_should_chunk(change.size)) {
            return limits.maxSingleBytes + 1;
        }
        return change.size;
    }, () => {
        const tuning = getHashTuning();
        return {
            maxFiles: tuning.maxBatchFiles,
            maxBytes: Math.min(pushGroupingByteLimit(tuning, transientMemorySnapshot().capacityBytes),
                rootSlice?.maxSourceBytes ?? Number.MAX_SAFE_INTEGER),
            maxSingleBytes: tuning.maxSingleBatchFileBytes,
            maxHoldMs: tuning.maxBatchHoldMs,
        };
    });
    let lookahead: Promise<PushLookaheadResult> | null = null;
    const lookaheadNeeded = new Map<string, boolean>();
    const lookaheadNeededChunks = new Map<string, boolean>();
    const lookaheadAbort = new AbortController();
    const propagateLookaheadAbort = () => lookaheadAbort.abort();
    if (signal?.aborted) lookaheadAbort.abort();
    else signal?.addEventListener("abort", propagateLookaheadAbort, { once: true });
    const settleLookahead = async (): Promise<void> => {
        const pending = lookahead;
        lookahead = null;
        if (!pending) return;
        const result = await pending;
        if (!result.ok) throw result.error;
    };
    let loopFailed = false;
    try {
    for (let batched = 0; batched < nonDeleted.length || splitBatches.length > 0;) {
        await settleLookahead();
        await beforeHeavyBatch?.();
        // A real task boundary lets input/rendering run between bounded batches.
        await yieldWork({ perf, signal, lane: workLane });
        // Select AFTER the wait/yield so new limits apply before allocation.
        let selectedBatch = splitBatches.pop();
        if (!selectedBatch) {
            const nextBatch = streamBatches.next();
            if (nextBatch.done) break;
            selectedBatch = nextBatch.value;
            batched += selectedBatch.length;
        }
        // Generator exhaustion is handled by the break above. Keep the active
        // aggregate non-optional because pre-dispatch slicing may replace it
        // with a dependency-safe prefix inside nested callbacks.
        let batchChanges: FileChange[] = selectedBatch;
        const tuning = getHashTuning();
        const admittedFeedBytes = tuning.maxFeedBytes;
        const capacityBytes = transientMemorySnapshot().capacityBytes;
        for (const change of batchChanges) {
            if (change.size === undefined) {
                if (change.data) change.size = change.data.byteLength;
                else {
                    const stat = await io.stat(change.path);
                    if (!stat) throw new HashWorkerFileDriftError("source disappeared before admission");
                    change.size = stat.size;
                    change.mtime = stat.mtime;
                }
            }
            if (change.data && change.data.byteLength !== change.size) {
                throw new HashWorkerFileDriftError("preloaded source size differs from admitted metadata");
            }
        }
        const mobileRangedChanges = new Set<FileChange>();
        const workerPaths = new Map<FileChange, string>();
        const memoryFiles: PushMemoryFile[] = batchChanges.map(change => {
            const chunked = wasm.wasm_should_chunk(change.size!);
            // Resolve a native path only when a desktop worker can consume it.
            // Known-hash small files deliberately skip phase-A source IO, and
            // renderer-only hosts cannot use the path at all. Calling through
            // the adapter for every such file adds one native boundary to a
            // large drain without adding a source-identity check.
            const absolutePath = hashWorkers &&
                !(change.hash && !chunked) &&
                change.data === undefined && change.mtime !== undefined
                && typeof io.getAbsolutePath === "function" ? io.getAbsolutePath(change.path) : null;
            if (absolutePath) workerPaths.set(change, absolutePath);
            const desktopRanged = !!(hashWorkers && chunked && absolutePath);
            const mobileRanged = !!(!absolutePath && chunked && change.data === undefined &&
                change.mtime !== undefined && openMobileRangeReader &&
                planPushMemory([{ size: change.size!, chunked: true, ranged: false }], tuning).totalBytes > capacityBytes);
            if (mobileRanged) mobileRangedChanges.add(change);
            return {
                size: change.size!,
                backingBytes: change.data ? Math.max(change.size!, change.data.buffer.byteLength) : undefined,
                chunked,
                ranged: desktopRanged || mobileRanged,
                worker: !!(hashWorkers && !(change.hash && !chunked) && absolutePath),
            };
        });
        // Whole-source copies are counted for every file regardless of read
        // concurrency. Only worker feed/window concurrency needs a pinned cap.
        const admittedReadConcurrency = memoryFiles.some(file => file.worker)
            ? tuning.readConcurrency : batchChanges.length;
        let memoryPlan = planPushMemory(memoryFiles, tuning);
        if (memoryPlan.totalBytes > capacityBytes && memoryFiles.some(file => file.ranged)) {
            memoryPlan = planPushMemory(memoryFiles, tuning, MAX_FASTCDC_CHUNK_BYTES);
        }
        if (memoryPlan.totalBytes > capacityBytes) {
            if (batchChanges.length > 1) {
                const middle = Math.ceil(batchChanges.length / 2);
                splitBatches.push(batchChanges.slice(middle), batchChanges.slice(0, middle));
                continue;
            }
            for (const change of batchChanges) deferred.push({
                path: change.path, reason: "source-too-large",
                requiredBytes: memoryPlan.totalBytes, capacityBytes,
            });
            continue;
        }
        let memory: TransientWorkScope;
        try { memory = await reserveTransientScope(memoryPlan, { signal }); }
        catch (error) {
            if (!(error instanceof ResourceBudgetOversizedError)) throw error;
            // A queued admission may become oversized after a policy shrink.
            // No bytes were read yet; replan/split against the actual ceiling.
            splitBatches.push(batchChanges);
            continue;
        }
        const batchFiles: ObsetyncBatchFile[] = [];
        const unknownSmallReads: ObsetyncRead[] = [];
        const unattachedPreparedOwners = new Set<{ release(): void }>();
        const responsiveBoundaryFiles = Math.max(1, Math.min(
            rootSlice?.maxBoundaryFiles ?? DEFAULT_PUSH_RESPONSIVE_BOUNDARY_FILES,
            batchChanges.length,
        ));
        const releasePreparedFile = (file: ObsetyncBatchFile): void => {
            file.largeData = undefined;
            file.mobileRangeReader?.close();
            file.mobileRangeReader = undefined;
            file.preparedMemoryOwner?.release();
            if (file.preparedMemoryOwner) unattachedPreparedOwners.delete(file.preparedMemoryOwner);
            file.preparedMemoryOwner = undefined;
            preparedPaths.delete(file.change.path);
            preparedMutations.delete(file.change.path);
        };
        /** Select a root-slice decision using only a fully prepared source
         * prefix. If that prefix closes every dependency component, retain it
         * in this aggregate request and return the untouched suffix to the
         * local iterator. No path becomes ACKed here. */
        const selectPreparedSlice = (readyFiles: readonly ObsetyncBatchFile[]): boolean => {
            if (!rootSlice) return false;
            const readyPaths = new Set(readyFiles.map(file => file.change.path));
            const deferredPaths = new Set(deferred.map(change => change.path));
            const hasCurrentRemainder = batchChanges.some(change =>
                !readyPaths.has(change.path) && !deferredPaths.has(change.path));
            const hasRemainingSource = hasCurrentRemainder || splitBatches.length > 0 || batched < nonDeleted.length;
            if (!hasRemainingSource) return false;
            const observedAt = sliceClock();
            if (!Number.isFinite(observedAt)) throw new Error("durable publication slice clock is invalid");
            const elapsedMs = Math.max(0, observedAt - sliceStartedAt);
            if (!sliceDecision) {
                const preparedSourceBytes = readyFiles.reduce((sum, file) => sum + file.size, 0);
                const reason: RootSliceReason | null = rootSlice.shouldPreempt() ? "urgent" :
                    rootSlice.allowRoutineSplit !== false &&
                        sliceSourceBytes + preparedSourceBytes >= rootSlice.maxSourceBytes ? "bytes" :
                        rootSlice.allowRoutineSplit !== false && elapsedMs >= rootSlice.maxElapsedMs ? "time" : null;
                if (reason) sliceDecision = { reason, elapsedMs };
            }
            if (!sliceDecision) return false;
            const processedPaths = new Set(allTreeUpdates.map(update => update.path));
            for (const path of readyPaths) processedPaths.add(path);
            const boundary = rootSliceSuffixAtSafeBoundary(
                changes,
                rootContext!.groups,
                processedPaths,
                deferredPaths,
            );
            if (!boundary || boundary.continuation.length === 0) return false;
            sliceDecision.elapsedMs = elapsedMs;
            const suffix = batchChanges.filter(change =>
                boundary.held.has(change.path) && !deferredPaths.has(change.path));
            if (suffix.length === 0) return false;
            for (let index = batchFiles.length - 1; index >= 0; index--) {
                if (!boundary.held.has(batchFiles[index].change.path)) continue;
                releasePreparedFile(batchFiles[index]);
                batchFiles.splice(index, 1);
            }
            batchChanges = batchChanges.filter(change => !boundary.held.has(change.path));
            splitBatches.push(suffix);
            return true;
        };
        const cooperatePreparedBoundary = async (readyFiles: readonly ObsetyncBatchFile[]): Promise<boolean> => {
            await beforeHeavyBatch?.();
            await yieldWork({ perf, signal, lane: workLane });
            return selectPreparedSlice(readyFiles);
        };
        const selectAfterAsyncBoundary = (): boolean => {
            if (!rootSlice || batchFiles.length === 0) return false;
            let ready = sliceDecision
                ? batchFiles.length
                : Math.min(responsiveBoundaryFiles, batchFiles.length);
            while (ready > 0) {
                if (selectPreparedSlice(batchFiles.slice(0, ready))) return true;
                if (!sliceDecision || ready === batchFiles.length) return false;
                ready = Math.min(batchFiles.length, ready + responsiveBoundaryFiles);
            }
            return false;
        };
        try {

        // ------------------------------------------------------------------
        // A. Hash resolution — parallel reads, wasm_hash_batch per group.
        //
        // Small file with known hash + size: skip read entirely (lazy-read in C).
        //   For incremental syncs the server already has most content → zero reads.
        //
        // Large file (wasm_should_chunk): must read now for FastCDC.
        //   wasm_chunk_file computes the hash internally.
        //
        // Small file without known hash: read in parallel, then wasm_hash_batch
        //   for the whole sub-group — ONE WASM boundary crossing regardless of N.
        // ------------------------------------------------------------------
        const endPrepare = perf?.phase("prepare_batch");
        try {
            const flushUnknownSmallReads = async (): Promise<void> => {
                if (unknownSmallReads.length === 0) return;
                batchFiles.push(...await hashUnknownSmallReads(
                    wasm, unknownSmallReads, getHashTuning(), perf, signal, browserHash,
                ));
                unknownSmallReads.length = 0;
            };
            let phaseACut = false;
            for (let i = 0; i < batchChanges.length;) {
                if (i > 0 && i % responsiveBoundaryFiles === 0) {
                    // Unknown small reads must become named objects before this
                    // prefix can be selected. This retains the old <=32-file
                    // synchronous hash quantum even when the wire pack is 256.
                    await flushUnknownSmallReads();
                    if (await cooperatePreparedBoundary(batchFiles)) {
                        phaseACut = true;
                        break;
                    }
                }
                throwIfWorkAborted(signal);
                const tuning = getHashTuning();
                const boundaryRemaining = responsiveBoundaryFiles - (i % responsiveBoundaryFiles);
                const group = batchChanges.slice(i, i + Math.min(
                    tuning.readConcurrency,
                    admittedReadConcurrency,
                    boundaryRemaining,
                ));
                i += group.length;
                perf?.setDemand({ read: group.length + batchChanges.length - i, hash: group.length + batchChanges.length - i });

                // Partition: known-hash small files skip reading.
                const skipRead: FileChange[] = [];
                const needRead: FileChange[] = [];
                const workerEligible: Array<{ change: FileChange; absolutePath: string }> = [];
                const mobileRangeEligible: FileChange[] = [];
                for (const c of group) {
                    if (c.hash && c.size !== undefined && !wasm.wasm_should_chunk(c.size)) {
                        skipRead.push(c);
                    } else {
                        const absolutePath = workerPaths.get(c) ?? null;
                        if (hashWorkers && absolutePath) {
                            workerEligible.push({ change: c, absolutePath });
                        } else if (mobileRangedChanges.has(c)) {
                            mobileRangeEligible.push(c);
                        } else {
                            needRead.push(c);
                        }
                    }
                }

                for (const c of skipRead) {
                    batchFiles.push(new ObsetyncBatchFile(c, c.size!, c.mtime ?? Date.now()));
                }

                if (workerEligible.length > 0) {
                    const workerResults = await Promise.all(workerEligible.map(async (candidate) => {
                        try {
                            const job = {
                                absolutePath: candidate.absolutePath,
                                expectedSize: candidate.change.size!,
                                expectedMtime: candidate.change.mtime!,
                                mode: wasm.wasm_should_chunk(candidate.change.size!)
                                    ? "manifest" as const
                                    : "hash" as const,
                                feedBytes: Math.min(tuning.feedBytes, admittedFeedBytes),
                            };
                            const run = (mode: HashWorkerMode) => mode === "manifest"
                                ? memory.run(PUSH_CHUNKER_WORK_BYTES,
                                    context => context.track(() => hashWorkers!.run({ ...job, mode }, signal, context)), { signal })
                                : memory.track(() => hashWorkers!.run({ ...job, mode }, signal, memory));
                            if (job.mode === "manifest" && prepared) {
                                const { path } = candidate.change;
                                const journalThroughId = prepared.journalThroughId(path);
                                const resolution = await resolvePreparedDesktopManifest({
                                    expectedSize: job.expectedSize, expectedMtime: job.expectedMtime, signal,
                                    assertApplicable: () => prepared.assertApplicable(path),
                                    loadHint: async () => {
                                        const hint = await (prepared.memoryArbiter
                                            ? prepared.plan.lookupUnownedViewForAdmission(prepared.scopeHash, path)
                                            : prepared.plan.lookup(prepared.scopeHash, path));
                                        if (hint) preparedMutations.set(path, hint.mutationId);
                                        return hint;
                                    },
                                    discardHint: async mutationId => {
                                        await prepared.plan.discard(prepared.scopeHash, path, mutationId);
                                        preparedMutations.delete(path);
                                    },
                                    retain: async result => {
                                        const retained = await prepared.plan.retain({ scopeHash: prepared.scopeHash, path, journalThroughId,
                                            baseRoot: baseRootHash, source: { size: result.size, mtime: result.mtime,
                                                fingerprint: { kind: "desktop-v1", ...result.fingerprint } },
                                            manifest: result.manifest });
                                        if (retained.retained) preparedMutations.set(path, retained.mutationId);
                                    },
                                    run,
                                    memoryArbiter: prepared.memoryArbiter,
                                });
                                preparedPaths.add(path);
                                perf?.addPhase("read", resolution.validationReadMs);
                                perf?.addPhase("hash", resolution.validationHashMs);
                                if (resolution.memoryOwner) unattachedPreparedOwners.add(resolution.memoryOwner);
                                return { candidate, result: resolution.result, reusedPrepared: resolution.reusedPrepared,
                                    preparedMemoryOwner: resolution.memoryOwner };
                            }
                            return { candidate, result: await run(job.mode), reusedPrepared: false,
                                preparedMemoryOwner: undefined };
                        } catch (error) {
                            throwIfWorkAborted(signal);
                            if (
                                error instanceof PreparedDesktopManifestError ||
                                error instanceof HashWorkerFileDriftError ||
                                (error instanceof HashWorkerPoolError && error.code === "CLOSED") ||
                                (error as Error)?.name === "AbortError"
                            ) {
                                throw error;
                            }
                            // run() may reject promptly while its native thread
                            // still owns feed/chunker buffers. Do not reuse the
                            // scope for renderer fallback until cleanup is known.
                            if (!(await waitForHashWorkerCleanup(error))) throw error;
                            throwIfWorkAborted(signal);
                            if (!workerFallbackWarned) {
                                workerFallbackWarned = true;
                                console.warn(
                                    "[obsetync] hash worker unavailable during push; " +
                                    "using renderer fallback:",
                                    error,
                                );
                            }
                            const fallbackPlan = planPushMemory([{
                                size: candidate.change.size!, chunked: wasm.wasm_should_chunk(candidate.change.size!), ranged: false,
                            }], getHashTuning());
                            const fallbackCapacity = transientMemorySnapshot().capacityBytes;
                            if (fallbackPlan.ownerBytes > memoryPlan.ownerBytes ||
                                fallbackPlan.workBytes > memoryPlan.workBytes) {
                                if (batchChanges.length === 1 && fallbackPlan.totalBytes <= fallbackCapacity) {
                                    // The worker has already settled; no source
                                    // was read. Re-admit the whole-file fallback
                                    // before allocating, not under a range lease.
                                    memory.close();
                                    try { memory = await reserveTransientScope(fallbackPlan, { signal }); }
                                    catch (admissionError) {
                                        if (!(admissionError instanceof ResourceBudgetOversizedError)) throw admissionError;
                                        deferred.push({ path: candidate.change.path, reason: "source-too-large",
                                            requiredBytes: fallbackPlan.totalBytes, capacityBytes: transientMemorySnapshot().capacityBytes });
                                        return null;
                                    }
                                    memoryPlan = fallbackPlan;
                                } else {
                                    deferred.push({ path: candidate.change.path, reason: "source-too-large",
                                        requiredBytes: fallbackPlan.totalBytes, capacityBytes: fallbackCapacity });
                                    return null;
                                }
                            }
                            needRead.push(candidate.change);
                            return null;
                        }
                    }));
                    throwIfWorkAborted(signal);
                    perf?.observePeakBatchBytes(workerEligible.length * tuning.feedBytes);
                    for (const row of workerResults) {
                        if (!row) continue;
                        const { change } = row.candidate;
                        perf?.addPhase("read", row.result.read_ms);
                        if (row.result.mode === "hash") {
                            perf?.addPhase("hash", row.result.hash_ms);
                            change.hash = row.result.hash;
                            batchFiles.push(new ObsetyncBatchFile(
                                change,
                                row.result.size,
                                change.mtime!,
                            ));
                        } else {
                            perf?.addPhase("fastcdc", row.result.hash_ms);
                            change.hash = row.result.manifest.file_hash;
                            batchFiles.push(new ObsetyncBatchFile(
                                change,
                                row.result.size,
                                change.mtime!,
                                row.result.manifest,
                                undefined,
                                {
                                    absolutePath: row.candidate.absolutePath,
                                    fingerprint: row.result.fingerprint,
                                },
                                row.reusedPrepared,
                                undefined,
                                row.preparedMemoryOwner,
                            ));
                            if (row.preparedMemoryOwner) unattachedPreparedOwners.delete(row.preparedMemoryOwner);
                        }
                    }
                }

                for (const change of mobileRangeEligible) {
                    throwIfWorkAborted(signal);
                    rootContext?.assertApplicable(change.path);
                    prepared?.assertApplicable(change.path);
                    const wholeFallback = planPushMemory([{
                        size: change.size!, chunked: true, ranged: false,
                    }], getHashTuning());
                    let mobileReader: MobileRangeReader | null = null;
                    let preparedOwner: { release(): void } | undefined;
                    try {
                        mobileReader = await openMobileRangeReader!(change.path, {
                            size: change.size!, mtime: change.mtime!,
                        }, signal, memory);
                        throwIfWorkAborted(signal);
                        rootContext?.assertApplicable(change.path);
                        prepared?.assertApplicable(change.path);
                        if (!mobileReader) {
                            deferred.push({ path: change.path, reason: "source-too-large",
                                requiredBytes: wholeFallback.totalBytes, capacityBytes });
                            continue;
                        }
                        const resourceVersion = mobileReader.resourceVersion;
                        if (!Number.isSafeInteger(mobileReader.size) ||
                            mobileReader.size !== change.size ||
                            typeof resourceVersion !== "string" || !/^[0-9a-f]{64}$/.test(resourceVersion)) {
                            throw new MobileRangeReadError("PROTOCOL");
                        }
                        let chunkInfo: any;
                        const journalThroughId = prepared?.journalThroughId(change.path);
                        if (prepared?.plan instanceof MobilePreparedTransferPlan && journalThroughId !== undefined) {
                            const resolution = await resolvePreparedMobileManifest({
                                plan: prepared.plan,
                                scopeHash: prepared.scopeHash,
                                path: change.path,
                                journalThroughId,
                                baseRoot: baseRootHash,
                                source: {
                                    size: change.size!, mtime: change.mtime!,
                                    fingerprint: { kind: "mobile-resource-v1", size: change.size!,
                                        mtime: change.mtime!, resourceVersion },
                                },
                                hashCurrent: async () => {
                                    const endHash = perf?.phase("hash");
                                    try {
                                        return await memory.run(PUSH_CHUNKER_WORK_BYTES,
                                            () => hashMobileRangedSource(wasm, mobileReader!, memory,
                                                perf, signal, admittedFeedBytes), { signal });
                                    } finally { endHash?.(); }
                                },
                                prepareCurrent: async () => {
                                    const endChunk = perf?.phase("fastcdc");
                                    try {
                                        return await memory.run(PUSH_CHUNKER_WORK_BYTES,
                                            () => chunkMobileRangedSource(wasm, mobileReader!, memory,
                                                perf, signal, admittedFeedBytes), { signal });
                                    } finally { endChunk?.(); }
                                },
                                assertApplicable: () => prepared.assertApplicable(change.path),
                                signal,
                            });
                            chunkInfo = resolution.manifest;
                            preparedOwner = resolution.memoryOwner;
                            if (preparedOwner) unattachedPreparedOwners.add(preparedOwner);
                            preparedPaths.add(change.path);
                            preparedMutations.set(change.path, resolution.mutationId);
                        } else {
                            const endChunk = perf?.phase("fastcdc");
                            try {
                                chunkInfo = await memory.run(PUSH_CHUNKER_WORK_BYTES,
                                    () => chunkMobileRangedSource(wasm, mobileReader!, memory,
                                        perf, signal, admittedFeedBytes), { signal });
                            } finally { endChunk?.(); }
                        }
                        throwIfWorkAborted(signal);
                        rootContext?.assertApplicable(change.path);
                        prepared?.assertApplicable(change.path);
                        change.hash = chunkInfo.file_hash;
                        batchFiles.push(new ObsetyncBatchFile(
                            change,
                            change.size!,
                            change.mtime!,
                            chunkInfo,
                            undefined,
                            undefined,
                            false,
                            mobileReader,
                            preparedOwner,
                        ));
                        if (preparedOwner) unattachedPreparedOwners.delete(preparedOwner);
                        preparedOwner = undefined;
                        mobileReader = null; // batch file owns close through upload/finally
                    } catch (error) {
                        throwIfWorkAborted(signal);
                        if (error instanceof MobileRangeReadError && error.code === "SOURCE_CHANGED") {
                            throw new HashWorkerFileDriftError("mobile ranged source changed during preparation");
                        }
                        if (error instanceof MobileRangeReadError &&
                            (error.code === "UNAVAILABLE" || error.code === "PROTOCOL")) {
                            deferred.push({ path: change.path, reason: "source-too-large",
                                requiredBytes: wholeFallback.totalBytes, capacityBytes });
                            continue;
                        }
                        throw error;
                    } finally {
                        mobileReader?.close();
                        preparedOwner?.release();
                        if (preparedOwner) unattachedPreparedOwners.delete(preparedOwner);
                    }
                }

                if (needRead.length === 0) continue;

                // Read files in parallel.
                const endRead = perf?.phase("read");
                let reads: Array<{ change: FileChange; data: Uint8Array }>;
                try {
                    reads = await Promise.all(needRead.map(async c => ({
                        change: c,
                        data: c.data ?? await readAdmittedPushSource(c, io, memory, signal),
                    })));
                } finally {
                    endRead?.();
                }
                throwIfWorkAborted(signal);
                const residentReadBytes = reads.reduce((sum, row) => sum + row.data.length, 0);
                perf?.observePeakBatchBytes(residentReadBytes);

                // Large files — wasm_chunk_file hashes internally.
                for (const { change, data } of reads.filter(r => wasm.wasm_should_chunk(r.data.length))) {
                    const endChunk = perf?.phase("fastcdc");
                    let chunkInfo: any;
                    try {
                        chunkInfo = await memory.run(PUSH_CHUNKER_WORK_BYTES,
                            () => chunkFileStreaming(wasm, data, perf, signal, admittedFeedBytes), { signal });
                    } finally {
                        endChunk?.();
                    }
                    change.hash = chunkInfo.file_hash;
                    batchFiles.push(new ObsetyncBatchFile(
                        change,
                        data.length,
                        change.mtime ?? Date.now(),
                        chunkInfo,
                        data,
                    ));
                }

                // Small files — batch hash unknown-hash ones in ONE wasm_hash_batch call.
                const smallReads = reads.filter(r => !wasm.wasm_should_chunk(r.data.length));
                if (smallReads.length === 0) continue;

                // Known-hash small files that were forced to read (preloaded change.data).
                for (const { change, data } of smallReads.filter(r => r.change.hash)) {
                    batchFiles.push(new ObsetyncBatchFile(change, data.length, change.mtime ?? Date.now(), undefined, data));
                }

                unknownSmallReads.push(...smallReads.filter(r => !r.change.hash));
            }

            // Flush the final partial responsive window. A decision at the end
            // may still stop before a later adaptive/network batch.
            await flushUnknownSmallReads();
            if (!phaseACut) selectPreparedSlice(batchFiles);
        } finally {
            endPrepare?.();
            perf?.setDemand({});
        }
        // This is the authoritative owning batch, not speculative lookahead.
        // Preparation is still volatile: only the later server-confirmed
        // milestone proves its content objects survived this renderer.
        const batchOrder = new Map(batchChanges.map((change, index) => [change, index]));
        batchFiles.sort((left, right) =>
            (batchOrder.get(left.change) ?? Number.MAX_SAFE_INTEGER) -
            (batchOrder.get(right.change) ?? Number.MAX_SAFE_INTEGER));
        if (batchFiles.length > 0) perf?.increment({ preparedFiles: batchFiles.length });

        // ------------------------------------------------------------------
        // B. Two batch-check requests for this batch.
        // ------------------------------------------------------------------
        throwIfWorkAborted(signal);
        const smallHashes = batchFiles
            .filter(f => !f.chunkInfo)
            .map(f => f.change.hash!);

        const allChunkHashes: string[] = batchFiles
            .filter(f => f.chunkInfo)
            .flatMap(f => (f.chunkInfo.chunks as any[]).map((c: any) => c.hash));

        const cachedNeededSmall: string[] = [];
        const uncheckedSmall: string[] = [];
        const consumedLookahead = new Set<string>();
        for (const hash of smallHashes) {
            if (lookaheadNeeded.has(hash)) {
                if (lookaheadNeeded.get(hash)) cachedNeededSmall.push(hash);
                consumedLookahead.add(hash);
            } else if (hasDurableObjectConfirmation(prepared, "content", hash)) {
                // Exact server generation makes this positive result reusable
                // across renderer restarts. Missing is never cached.
            } else uncheckedSmall.push(hash);
        }
        for (const hash of consumedLookahead) lookaheadNeeded.delete(hash);
        const cachedNeededChunks: string[] = [];
        const uncheckedChunkHashes: string[] = [];
        const consumedChunkLookahead = new Set<string>();
        for (const hash of allChunkHashes) {
            if (lookaheadNeededChunks.has(hash)) {
                if (lookaheadNeededChunks.get(hash)) cachedNeededChunks.push(hash);
                consumedChunkLookahead.add(hash);
            } else if (hasDurableObjectConfirmation(prepared, "content-chunk", hash)) {
            } else uncheckedChunkHashes.push(hash);
        }
        for (const hash of consumedChunkLookahead) lookaheadNeededChunks.delete(hash);
        const endCheck = perf?.phase("check");
        perf?.setDemand({ network: Number(uncheckedSmall.length > 0) + Number(uncheckedChunkHashes.length > 0) });
        let neededSmall: string[];
        let neededChunks: string[];
        try {
            const checks = Promise.all([
                uncheckedSmall.length > 0
                    ? api.checkContent(uncheckedSmall, perf, signal, memory)
                    : Promise.resolve([]),
                uncheckedChunkHashes.length > 0
                    ? api.checkContentChunks(uncheckedChunkHashes, perf, signal, memory)
                    : Promise.resolve([]),
            ]);
            // Network calls above are already dispatched. Prepare only the
            // immediate future window while this batch waits; the iterator is
            // not advanced, so post-ACK adaptive limits and urgent ordering
            // remain authoritative for the next batch.
            if (!lookahead && splitBatches.length === 0 && batched < nonDeleted.length) {
                const future = nonDeleted.slice(batched,
                    batched + getHashTuning().maxBatchFiles);
                lookahead = preparePushLookahead(
                    future, api, io, wasm, lookaheadNeeded, lookaheadNeededChunks,
                    hashWorkers, prepared, baseRootHash, perf, beforeHeavyBatch, lookaheadAbort.signal,
                    rootContext || prepared ? path => {
                        rootContext?.assertApplicable(path);
                        prepared?.assertApplicable(path);
                    } : undefined,
                    checks.then(() => true, () => false),
                    workLane,
                    browserHash,
                ).then(() => ({ ok: true as const }), error => {
                    // Lookahead is never authoritative. Source drift, a failed
                    // speculative request, or a malformed speculative reply
                    // only discards this window; the owning batch will redo
                    // every check from fresh source state. Caller cancellation
                    // remains fatal, while an internal abort used to join a
                    // failed consumer is suppressed by the owning catch path.
                    if (!signal?.aborted) return { ok: true as const };
                    try { throwIfWorkAborted(signal); }
                    catch (abortError) { return { ok: false as const, error: abortError }; }
                    return { ok: false as const, error };
                });
            }
            [neededSmall, neededChunks] = await checks;
            const missingSmall = new Set(neededSmall);
            const missingChunks = new Set(neededChunks);
            await retainDurableObjectConfirmations(prepared, [
                ...uncheckedSmall.filter(hash => !missingSmall.has(hash))
                    .map(hash => ({ kind: "content" as const, hash })),
                ...uncheckedChunkHashes.filter(hash => !missingChunks.has(hash))
                    .map(hash => ({ kind: "content-chunk" as const, hash })),
            ]);
            neededSmall = [...cachedNeededSmall, ...neededSmall];
            neededChunks = [...cachedNeededChunks, ...neededChunks];
        } finally {
            endCheck?.();
            perf?.setDemand({});
        }
        throwIfWorkAborted(signal);

        const neededSmallSet  = new Set(neededSmall);
        const neededChunksSet = new Set(neededChunks);
        // A content check may have yielded long enough for an urgent edit.
        // Select the earliest dependency-safe prepared prefix before lazy
        // reads retain source bytes for the rest of the aggregate pack.
        selectAfterAsyncBoundary();

        // ------------------------------------------------------------------
        // C. Prepare missing content, then ACK one or more byte-bounded packs.
        // ------------------------------------------------------------------
        const uploadRecords: BulkUploadRecord[] = [];
        const queuedContent = new Set<string>();
        const queuedContentChunks = new Set<string>();
        const queuedManifests = new Set<string>();
        let batchContentBytes = 0;
        // Known-hash small notes were intentionally not read in phase A.
        // Read only the first missing representative of each content hash,
        // within this SAME admitted batch/source budget. Groups join every
        // actual native tail before failure/stop or the next admission.
        await readMissingSmallSources(batchFiles, neededSmallSet, io, memory,
            admittedReadConcurrency, responsiveBoundaryFiles, async readyPrefixCount => {
                // Native source reads already returned through the event loop;
                // recheck lifecycle/admission without adding a second host
                // yield for the same 32-file window.
                await beforeHeavyBatch?.();
                return selectPreparedSlice(batchFiles.slice(0, readyPrefixCount));
            }, perf, signal);
        // Preserve the final pre-dispatch scheduler/admission checkpoint. An
        // urgent event observed here may shrink the still-volatile pack, but
        // once putObjects starts its one terminal ACK remains indivisible.
        if (batchFiles.length > 0) {
            await beforeHeavyBatch?.();
            selectAfterAsyncBoundary();
        }
        for (const file of batchFiles) {
            if (file.chunkInfo) {
                let missingForFile = 0;
                for (const chunk of file.chunkInfo.chunks as any[]) {
                    if (neededChunksSet.has(chunk.hash)) missingForFile += chunk.size;
                }
                if (missingForFile > 0) {
                    neededFiles++;
                    neededBytes += missingForFile;
                }
            } else if (file.change.hash && neededSmallSet.has(file.change.hash)) {
                neededFiles++;
                neededBytes += file.size;
            }
        }
        for (const batchFile of batchFiles) {
            const {
                change,
                size,
                mtime,
                chunkInfo,
                largeData,
                rangedSource,
                reusedPrepared,
                mobileRangeReader,
            } = batchFile;
            throwIfWorkAborted(signal);
            if (preparedPaths.has(change.path)) prepared?.assertApplicable(change.path);
            if (chunkInfo) {
                const missingChunks: Array<{ hash: string; offset: number; size: number }> = [];
                for (const chunk of chunkInfo.chunks as Array<{
                    hash: string;
                    offset: number;
                    size: number;
                }>) {
                    if (
                        neededChunksSet.has(chunk.hash) &&
                        !queuedContentChunks.has(chunk.hash)
                    ) {
                        queuedContentChunks.add(chunk.hash);
                        missingChunks.push(chunk);
                    }
                }
                const uploadManifest = !queuedManifests.has(change.hash!);
                if (uploadManifest) queuedManifests.add(change.hash!);
                const makeManifestRecord = (): BulkUploadRecord => ({
                    kind: BulkObjectKind.Manifest,
                    hash: change.hash!,
                    data: new TextEncoder().encode(JSON.stringify({
                        file_hash: change.hash!,
                        total_size: chunkInfo.total_size,
                        chunks: chunkInfo.chunks,
                    })),
                });

                if (rangedSource || mobileRangeReader) {
                    const putRangedRecords = async (
                        records: readonly BulkUploadRecord[],
                    ): Promise<void> => {
                        await beforeHeavyBatch?.();
                        throwIfWorkAborted(signal);
                        rootContext?.assertApplicable(change.path);
                        if (preparedPaths.has(change.path)) prepared?.assertApplicable(change.path);
                        const endUpload = perf?.phase("upload");
                        perf?.setDemand({ network: 1 });
                        try {
                            await api.putObjects(records, perf, memory);
                        } finally {
                            endUpload?.();
                            perf?.setDemand({});
                        }
                        await retainDurableObjectConfirmations(prepared,
                            confirmedObjectsFromUploadRecords(records));
                        rootContext?.assertApplicable(change.path);
                        const bytes = records.reduce(
                            (sum, record) => sum + record.data.byteLength,
                            0,
                        );
                        uploadedBytes += bytes;
                        perf?.increment({ bytesTransferred: bytes });
                    };
                    const finalizeManifest = async () => {
                            if (!uploadManifest) return;
                            await beforeHeavyBatch?.();
                            throwIfWorkAborted(signal);
                            rootContext?.assertApplicable(change.path);
                            const manifestRecord = makeManifestRecord();
                            perf?.observePeakBatchBytes(manifestRecord.data.byteLength);
                            const endUpload = perf?.phase("upload");
                            try {
                                await api.putObjects([manifestRecord], perf, memory);
                            } finally {
                                endUpload?.();
                            }
                            rootContext?.assertApplicable(change.path);
                        };
                    const verifyRange = reusedPrepared || mobileRangeReader
                        ? async (data: Uint8Array, expectedHash: string) => {
                                rootContext?.assertApplicable(change.path);
                                prepared?.assertApplicable(change.path);
                                // Source belongs to the admitted range queue;
                                // the reusable work quota covers bounded WASM feeds.
                                const actual = await memory.run(2 * admittedFeedBytes + 64 * 1024,
                                    () => hashAdmittedRange(wasm, data, perf, signal, admittedFeedBytes), { signal });
                                if (actual !== expectedHash) throw new HashWorkerFileDriftError("ranged source content changed");
                                rootContext?.assertApplicable(change.path);
                                prepared?.assertApplicable(change.path);
                            }
                        : undefined;
                    let ranged: DesktopRangedUploadResult;
                    try {
                        ranged = rangedSource
                            ? await uploadDesktopMissingRanges(rangedSource, missingChunks,
                                putRangedRecords, finalizeManifest,
                                { maxBufferedBytes: memoryPlan.rangeQueueBytes, verifyRange })
                            : await uploadMissingRangesWithReader(size, missingChunks,
                                putRangedRecords, finalizeManifest, async () => ({
                                    read: (offset, length) => mobileRangeReader!.readBorrowed(
                                        offset, length, memory, signal),
                                    verify: async () => {
                                        rootContext?.assertApplicable(change.path);
                                        await mobileRangeReader!.verify();
                                        rootContext?.assertApplicable(change.path);
                                    },
                                    close: async () => {
                                        // The shared ACK queue owns reader shutdown.
                                        // Clear the batch fallback first so the outer
                                        // error cleanup cannot close a host reader twice.
                                        if (batchFile.mobileRangeReader === mobileRangeReader) {
                                            batchFile.mobileRangeReader = undefined;
                                        }
                                        mobileRangeReader!.close();
                                    },
                                }), { maxBufferedBytes: memoryPlan.rangeQueueBytes, verifyRange });
                    } catch (error) {
                        if (mobileRangeReader && error instanceof MobileRangeReadError &&
                            error.code === "SOURCE_CHANGED") {
                            throw new HashWorkerFileDriftError("mobile ranged source changed during upload");
                        }
                        throw error;
                    }
                    rootContext?.assertApplicable(change.path);
                    if (preparedPaths.has(change.path)) prepared?.assertApplicable(change.path);
                    perf?.addPhase("read", ranged.readMs);
                    perf?.observePeakBatchBytes(ranged.peakBufferedBytes);
                    continue;
                }

                let content = largeData;
                if (missingChunks.length > 0 && !content) {
                    const before = await io.stat(change.path);
                    throwIfWorkAborted(signal);
                    if (
                        !before || before.size !== size ||
                        Math.abs(before.mtime - mtime) > 1
                    ) {
                        throw new HashWorkerFileDriftError(
                            "file changed between manifest planning and upload",
                        );
                    }
                    const endRead = perf?.phase("read");
                    try {
                        content = await readAdmittedPushSource(change, io, memory, signal);
                    } finally {
                        endRead?.();
                    }
                    throwIfWorkAborted(signal);
                    const after = await io.stat(change.path);
                    throwIfWorkAborted(signal);
                    if (
                        content.length !== size ||
                        !after || after.size !== size ||
                        Math.abs(after.mtime - mtime) > 1
                    ) {
                        throw new HashWorkerFileDriftError(
                            "file changed while loading planned chunks",
                        );
                    }
                    perf?.observePeakBatchBytes(content.length);
                }
                for (const chunk of missingChunks) {
                    const chunkData = content!.subarray(
                        chunk.offset,
                        chunk.offset + chunk.size,
                    );
                    uploadRecords.push({
                        kind: BulkObjectKind.ContentChunk,
                        hash: chunk.hash,
                        data: chunkData,
                    });
                    batchContentBytes += chunkData.length;
                }
                if (uploadManifest) uploadRecords.push(makeManifestRecord());
            } else if (
                change.hash &&
                neededSmallSet.has(change.hash) &&
                !queuedContent.has(change.hash)
            ) {
                // Native small-source preparation already joined above;
                // unknown/preloaded sources keep their existing owned bytes.
                const data = change.data ?? largeData;
                if (!data) throw new Error("missing admitted small-source preparation");
                perf?.observePeakBatchBytes(data.length);
                queuedContent.add(change.hash);
                uploadRecords.push({
                    kind: BulkObjectKind.Content,
                    hash: change.hash,
                    data,
                });
                batchContentBytes += data.length;
            }

            // Tree/progress state is appended below only after every object
            // in this batch has a successful stored/already-present ACK.
        }

        if (uploadRecords.length > 0) {
            throwIfWorkAborted(signal);
            const endUpload = perf?.phase("upload");
            perf?.setDemand({ network: 1 });
            try {
                await api.putObjects(uploadRecords, perf, memory, signal,
                    urgentPutOptions(uploadRecords, workLane));
            } finally {
                endUpload?.();
                perf?.setDemand({});
            }
            await retainDurableObjectConfirmations(prepared,
                confirmedObjectsFromUploadRecords(uploadRecords));
            uploadedBytes += batchContentBytes;
            perf?.increment({ bytesTransferred: batchContentBytes });
        }

        // Every file in this batch now has either an authenticated presence
        // answer or a completed object PUT ACK (including ranged chunks and
        // its manifest). This deliberately precedes candidate/root mutation.
        if (batchFiles.length > 0) {
            perf?.increment({ serverConfirmedFiles: batchFiles.length });
        }

        for (const { change, size, mtime } of batchFiles) {
            processed++;
            perf?.increment({ filesCompleted: 1 });
            onProgress?.(`↑ ${processed}/${total} ${throughput(processed, uploadedBytes, startTime)}`);

            // Queue tree update — applied in ONE update_batch after all batches.
            allTreeUpdates.push({ path: change.path, hash: change.hash!, mtime_ms: mtime, size });
        }

        if (rootSlice) {
            sliceSourceBytes += batchFiles.reduce((sum, file) => sum + file.size, 0);
            const hasRemainingSource = splitBatches.length > 0 || batched < nonDeleted.length;
            if (hasRemainingSource) {
                const observedAt = sliceClock();
                if (!Number.isFinite(observedAt)) throw new Error("durable publication slice clock is invalid");
                const elapsedMs = Math.max(0, observedAt - sliceStartedAt);
                if (!sliceDecision) {
                    const reason: RootSliceReason | null = rootSlice.shouldPreempt() ? "urgent" :
                        rootSlice.allowRoutineSplit !== false && sliceSourceBytes >= rootSlice.maxSourceBytes ? "bytes" :
                            rootSlice.allowRoutineSplit !== false && elapsedMs >= rootSlice.maxElapsedMs ? "time" : null;
                    if (reason) sliceDecision = { reason, elapsedMs };
                }
                if (sliceDecision) {
                    const boundary = rootSliceSuffixAtSafeBoundary(
                        changes,
                        rootContext!.groups,
                        new Set(allTreeUpdates.map(update => update.path)),
                        new Set(deferred.map(change => change.path)),
                    );
                    if (boundary) {
                        sliceDecision.elapsedMs = elapsedMs;
                        sliceSuffix = boundary;
                        // Speculation may own native reads/tasks for the suffix.
                        // Abort and join it before this owner releases admission or
                        // starts the indivisible candidate/root transaction.
                        lookaheadAbort.abort();
                        break;
                    }
                }
            }
        }

        } finally {
            // Pending tracked sibling reads keep admission after an early
            // Promise.all rejection. Drop this batch's remaining source views.
            for (const file of batchFiles) {
                file.largeData = undefined;
                file.mobileRangeReader?.close();
                file.preparedMemoryOwner?.release();
            }
            for (const owner of unattachedPreparedOwners) owner.release();
            unattachedPreparedOwners.clear();
            batchFiles.length = 0;
            unknownSmallReads.length = 0;
            memory.close();
        }
    }
    } catch (error) {
        loopFailed = true;
        lookaheadAbort.abort();
        throw error;
    } finally {
        let settlementError: unknown;
        try { await settleLookahead(); }
        catch (error) { settlementError = error; }
        finally {
            signal?.removeEventListener("abort", propagateLookaheadAbort);
            lookaheadNeeded.clear();
            lookaheadNeededChunks.clear();
        }
        if (!loopFailed && settlementError !== undefined) throw settlementError;
    }

    if (sliceDecision && sliceSuffix) {
        if (sliceSuffix.continuation.length > 0) {
            continuation = { paths: sliceSuffix.continuation, reason: sliceDecision.reason,
                sourceBytes: sliceSourceBytes, elapsedMs: sliceDecision.elapsedMs };
            publicationChanges = changes.filter(change => !sliceSuffix!.held.has(change.path));
            for (let index = allTreeUpdates.length - 1; index >= 0; index--) {
                if (sliceSuffix.held.has(allTreeUpdates[index].path)) allTreeUpdates.splice(index, 1);
            }
            deleted = deleted.filter(change => !sliceSuffix!.held.has(change.path));
            for (const path of sliceSuffix.held) preparedPaths.delete(path);
        }
    }

    if (deferred.length > 0) {
        for (const change of deleted) deferred.push({ path: change.path, reason: "dependent-delete",
            requiredBytes: 0, capacityBytes: transientMemorySnapshot().capacityBytes });
        deleted = [];
    }
    if (rootContext) {
        // A source can become too large only after IO admission. Preserve the
        // same dependency closure selected by the root planner, including
        // upsert peers; withholding deletes alone cannot protect rename chains.
        const held = new Set(deferred.map(change => change.path));
        for (const group of rootContext.groups) if (group.some(path => held.has(path))) {
            for (const path of group) if (!held.has(path)) {
                held.add(path); deferred.push({ path, reason: "dependent-delete", requiredBytes: 0,
                    capacityBytes: transientMemorySnapshot().capacityBytes });
            }
        }
        for (let index = allTreeUpdates.length - 1; index >= 0; index--) {
            if (held.has(allTreeUpdates[index].path)) allTreeUpdates.splice(index, 1);
        }
        deleted = deleted.filter(change => !held.has(change.path));
    }
    if (allTreeUpdates.length === 0 && deleted.length === 0 &&
        (deferred.length > 0 || continuation)) {
        settleCandidateOutput("abort");
        candidateOpen = false;
        return { newRootHash: tree.root_hash_hex(), conflicts: [], deferred, published: false,
            ...(continuation ? { continuation } : {}) };
    }
    const trackedDeletesForCommit = deleted.reduce(
        (count, change) => count + Number(syncBase.getEntry(change.path) !== null),
        0,
    );
    candidateMutationInputCount = allTreeUpdates.length + deleted.length;
    if (rootContext) for (const change of publicationChanges) rootContext.assertApplicable(change.path);
    if (deleted.length > 0) {
        const endTree = perf?.phase("tree_update");
        const payload = JSON.stringify(deleted.map(change => change.path));
        try {
            await applyTreeCandidateMutation(tree, "delete", payload, {
                signal,
                cooperate: async () => {
                    await beforeHeavyBatch?.();
                    await yieldWork({ perf, signal, lane: workLane });
                },
                assertCurrent: () => {
                    assertOwnedCandidateRevision();
                    for (const path of preparedPaths) prepared?.assertApplicable(path);
                    if (rootContext) for (const change of publicationChanges) rootContext.assertApplicable(change.path);
                },
                legacy: () => tree.candidate_delete_batch(payload),
                onCandidateMutated: recordOwnedCandidateRevision,
                cooperateRetirement: async () => {
                    await yieldWork({ perf, lane: "interactive", deadlineMs: 50 });
                },
                onOutputMemoryPlan: candidateMutationAdmission
                    ? plan => admitCandidateMutationOutput(candidateMutationAdmission, tree, plan)
                    : undefined,
            });
        }
        finally { endTree?.(); }
        processed += deleted.length;
        perf?.increment({ filesCompleted: deleted.length });
    }

    // ------------------------------------------------------------------
    // D. Apply all tree updates to the candidate in one update_tree call.
    //    O(N + prefix_size) vs O(N × prefix_size) with per-file update_entry.
    // ------------------------------------------------------------------
    throwIfWorkAborted(signal);
    for (const path of preparedPaths) prepared?.assertApplicable(path);
    const beforeRoot = tree.root_hash_hex();
    const beforeFiles = tree.total_files();
    if (allTreeUpdates.length > 0) {
        const endTree = perf?.phase("tree_update");
        const payload = JSON.stringify(allTreeUpdates);
        try {
            await applyTreeCandidateMutation(tree, "update", payload, {
                signal,
                cooperate: async () => {
                    await beforeHeavyBatch?.();
                    await yieldWork({ perf, signal, lane: workLane });
                },
                assertCurrent: () => {
                    assertOwnedCandidateRevision();
                    for (const path of preparedPaths) prepared?.assertApplicable(path);
                    if (rootContext) for (const change of publicationChanges) rootContext.assertApplicable(change.path);
                },
                legacy: () => tree.candidate_update_batch(payload),
                onCandidateMutated: recordOwnedCandidateRevision,
                cooperateRetirement: async () => {
                    await yieldWork({ perf, lane: "interactive", deadlineMs: 50 });
                },
                onOutputMemoryPlan: candidateMutationAdmission
                    ? plan => admitCandidateMutationOutput(candidateMutationAdmission, tree, plan)
                    : undefined,
            });
        } finally {
            endTree?.();
        }
    }
    perf?.setWorkload({
        bytesTotal: allTreeUpdates.reduce((sum, update) => sum + update.size, 0),
        filesNeeded: neededFiles,
        bytesNeeded: neededBytes,
    });
    const afterRoot = tree.candidate_root_hash_hex();
    const afterFiles = tree.candidate_total_files();
    const candidateVersion = tree.tree_version();
    const readCandidateRevision = readOwnedCandidateRevision;
    const candidateRevision = recordOwnedCandidateRevision();
    const assertCandidateCurrent = () => {
        for (const path of preparedPaths) prepared?.assertApplicable(path);
        if (rootContext) for (const change of publicationChanges) rootContext.assertApplicable(change.path);
        if (!tree.has_candidate() || tree.tree_version() !== candidateVersion ||
            (readCandidateRevision ? readCandidateRevision() !== candidateRevision :
                tree.candidate_root_hash_hex() !== afterRoot || tree.candidate_total_files() !== afterFiles)) {
            throw new Error("push candidate changed during index publication");
        }
    };
    // Diagnostic: if batch > 0 but root didn't move, or file count didn't grow
    // by the expected delta, candidate_update_batch silently dropped entries and we want
    // to know NOW instead of watching devices mysteriously fail to sync.
    console.log(
        `[obsetync] tree update: files ${beforeFiles} → ${afterFiles}, ` +
        `root ${(beforeRoot ?? "(empty)").slice(0, 16)} → ${(afterRoot ?? "(empty)").slice(0, 16)}, ` +
        `batch=${allTreeUpdates.length} deletes=${deleted.length}`
    );
    if (allTreeUpdates.length > 0 && beforeRoot === afterRoot) {
        console.warn(
            `[obsetync] candidate_update_batch didn't move root despite ` +
            `${allTreeUpdates.length} entries — ` +
            `first 3: ${JSON.stringify(allTreeUpdates.slice(0, 3))}`
        );
    }

    // Upload index chunks (LeafChunk, InternalNode) accumulated in MemoryChunkStore.
    // Server needs these to walk the tree during merge/diff.
    // Sort/hex output stays behind one native cursor and crosses the bridge in
    // bounded pages. Check each page while it is live and retain only hashes
    // the server actually lacks; the tree job is retired before chunk export
    // claims the wrapper's single native job slot.
    let neededChunks: CompactIndexHashSpool | undefined;
    let previousNeededHash: string | undefined;
    let checkingAnnounced = false;
    try {
        const candidateChunkSummary = await collectTreeCandidateChunkPages(tree, {
            signal,
            cooperate: async () => {
                await beforeHeavyBatch?.();
                await yieldWork({ perf, signal, lane: workLane });
            },
            cooperateRetirement: async () => {
                await yieldWork({ perf, lane: "interactive", deadlineMs: 50 });
            },
            assertCurrent: assertCandidateCurrent,
            legacy: () => {
                const all = wasm.wasm_tree_candidate_chunk_hashes(tree);
                return { all, fresh: bootstrapped ? all : wasm.wasm_tree_new_candidate_chunk_hashes(tree) };
            },
            onSortMemoryPlan: async plan => {
                if (neededChunks) throw new Error("candidate index hash spool was already admitted");
                const admitted = await reserveCompactIndexHashSpool(
                    plan.hashCount, plan.peakAdmissionBytes, { signal });
                neededChunks = admitted.spool;
                return admitted.coOwner;
            },
            onPage: async (page, allCount) => {
                const hashes = bootstrapped ? page.all : page.fresh;
                if (hashes.length === 0) return;
                if (!checkingAnnounced) {
                    onProgress?.(`↑ checking ${bootstrapped ? allCount : "new"} index chunks...`);
                    checkingAnnounced = true;
                }
                const unchecked = prepared?.confirmations && prepared.serverGeneration
                    ? hashes.filter(hash => !hasDurableObjectConfirmation(prepared, "index-chunk", hash))
                    : hashes;
                const endPageCheck = perf?.phase("check");
                let rawNeeded: unknown;
                try { rawNeeded = unchecked.length ? await api.checkChunks(unchecked, perf) : []; }
                finally { endPageCheck?.(); }
                if (!Array.isArray(rawNeeded) || rawNeeded.length > unchecked.length) {
                    throw new TypeError("server returned an invalid needed index chunk page");
                }
                // Validate the complete bounded response before mutating the
                // compact spool: a malformed late member cannot leave a
                // partially appended page behind.
                let source = 0;
                let previous = previousNeededHash;
                for (const hash of rawNeeded) {
                    if (typeof hash !== "string" || !/^[0-9a-f]{64}$/.test(hash) ||
                        (previous !== undefined && previous >= hash)) {
                        throw new TypeError("server returned a non-canonical needed index chunk page");
                    }
                    while (source < unchecked.length && unchecked[source] < hash) source++;
                    if (unchecked[source] !== hash) {
                        throw new TypeError("server requested an index chunk outside the checked page");
                    }
                    source++;
                    previous = hash;
                }
                const missing = new Set(rawNeeded as string[]);
                await retainDurableObjectConfirmations(prepared, unchecked
                    .filter(hash => !missing.has(hash))
                    .map(hash => ({ kind: "index-chunk" as const, hash })));
                if (rawNeeded.length === 0) return;
                // Legacy trees have no sort-plan callback. Their full result
                // arrays remain compatibility debt, but the second retained
                // O(N) missing-hash string array is still replaced here.
                if (!neededChunks) {
                    neededChunks = (await reserveCompactIndexHashSpool(allCount, 0, { signal })).spool;
                }
                for (const hash of rawNeeded) neededChunks.append(hash);
                previousNeededHash = previous;
            },
        });
        throwIfWorkAborted(signal);
        if (rootContext) for (const change of publicationChanges) rootContext.assertApplicable(change.path);
        perf?.setWasmChunks({ reachable: candidateChunkSummary.allCount });
        if (neededChunks) {
            if (neededChunks.capacityHashes !== candidateChunkSummary.allCount) {
                throw new Error("candidate index hash spool capacity changed during planning");
            }
            neededChunks.seal();
        }
        if (neededChunks && neededChunks.length > 0) {
            onProgress?.(`↑ uploading ${neededChunks.length} index chunks...`);
            const capacity = transientMemorySnapshot().capacityBytes;
            const uploadCapacity = capacity - neededChunks.capacityBytes;
            if (uploadCapacity < 1) {
                throw new ResourceBudgetOversizedError(neededChunks.capacityBytes + 1, capacity);
            }
            const endIndexUpload = perf?.phase("tree_index_upload");
            try {
                await uploadIndexChunks(api, wasm, tree, neededChunks, perf, beforeHeavyBatch, signal,
                    assertCandidateCurrent, uploadCapacity);
            } finally {
                endIndexUpload?.();
            }
            for (let first = 0; first < neededChunks.length; first += 512) {
                const confirmed: ConfirmedObject[] = [];
                const end = Math.min(neededChunks.length, first + 512);
                for (let index = first; index < end; index++) {
                    confirmed.push({ kind: "index-chunk", hash: neededChunks.hashAt(index) });
                }
                await retainDurableObjectConfirmations(prepared, confirmed);
            }
            await yieldWork({ perf, signal, lane: workLane });
            assertCandidateCurrent();
        }
    } finally {
        neededChunks?.close();
    }

    // Publish once after this transaction's content batches. With rootContext
    // the engine has already selected a bounded dependency-safe path group.
    // parentHash = the base this tree state descends from. An honest base
    // lets the server fast-forward when we're truly current and pick the
    // correct three-way-merge base when we're not.
    const parentHash = baseRootHash ?? "";

    onProgress?.("↑ pushing root...");
    const endRootCommit = perf?.phase("root_commit");
    let result: Awaited<ReturnType<ObsetyncApi["putRoot"]>>;
    let rootSettlement: RootRecoveryResult | undefined;
    try {
        throwIfWorkAborted(signal);
        for (const path of preparedPaths) prepared?.assertApplicable(path);
        if (rootContext) {
            for (const change of publicationChanges) rootContext.assertApplicable(change.path);
            if (!afterRoot) throw new Error("durable publication has no candidate root");
            const rootByteLimit = rootContext.rootByteLimit;
            if (!Number.isSafeInteger(rootByteLimit) || rootByteLimit < 1) {
                throw new Error("durable publication has an invalid root byte limit");
            }
            const preparation = estimateRootIntentPreparationWorkset(rootByteLimit);
            const capacity = transientMemorySnapshot().capacityBytes;
            const maxArenaBytes = rootPublicationArenaLimit(capacity, preparation.totalBytes);
            const stepBytes = Math.max(1, Math.min(TREE_ROOT_EXPORT_MAX_STEP_BYTES,
                getHashTuning().feedBytes));
            const rootExport = await exportTreeRoot(tree, "candidate", {
                cooperate: async () => {
                    await beforeHeavyBatch?.();
                    await yieldWork({ perf, signal, lane: workLane });
                },
                signal,
                assertCurrent: assertCandidateCurrent,
                expectedHash: afterRoot,
                expectedVersion: candidateVersion as 1 | 2,
                stepUnits: TREE_ROOT_EXPORT_MAX_STEP_UNITS,
                stepBytes,
                maxArenaBytes,
                maxOutputBytes: rootByteLimit,
                workBytes: preparation.totalBytes,
                parse: bytes => {
                    const hash = wasm.wasm_root_hash_from_bytes(bytes);
                    const version = wasm.wasm_root_version_from_bytes(bytes);
                    if (!hash || (version !== 1 && version !== 2)) {
                        throw new Error("candidate root export produced an invalid root");
                    }
                    return { hash, version };
                },
                legacy: () => tree.candidate_root_bytes(),
            });
            try {
                console.log(
                    `[obsetync] root-commit → parent=${parentHash ? parentHash.slice(0,16) : "(empty)"} ` +
                    `new=${afterRoot.slice(0,16)} bytes=${rootExport.bytes.byteLength}`
                );
                const entries: RootCandidatePublication["entries"][number][] = [
                    ...deleted.map(change => ({ action: "delete" as const, path: change.path })),
                    ...allTreeUpdates.map(update => ({ action: "upsert" as const, path: update.path,
                        hash: update.hash, mtime: update.mtime_ms, size: update.size })),
                ];
                entries.sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0);
                rootSettlement = await rootContext.publish({ rootExport, candidateRoot: afterRoot,
                    treeVersion: candidateVersion, parentRoot: parentHash, entries }, assertCandidateCurrent);
            } finally {
                // Runtime consumes/releases on entry; this covers wrapper
                // failures before that handoff and is deliberately idempotent.
                rootExport.release();
            }
            if (rootSettlement.status !== "accepted") {
                await invalidateDurableObjectConfirmations(prepared);
                settleCandidateOutput("abort"); candidateOpen = false;
                return { newRootHash: tree.root_hash_hex(), conflicts: [], deferred, published: false, rootSettlement,
                    ...(continuation ? { continuation } : {}) };
            }
            if (rootSettlement.candidateRoot !== afterRoot) throw new Error("root settlement returned another candidate");
            result = { root_hash: rootSettlement.observedRoot, conflicts: [] };
        } else {
            const rootBytes = tree.candidate_root_bytes();
            if (!rootBytes) throw new Error("push: candidate_root_bytes() returned null — candidate uninitialised");
            console.log(
                `[obsetync] putRoot → parent=${parentHash ? parentHash.slice(0,16) : "(empty)"} ` +
                `new=${afterRoot?.slice(0,16)} bytes=${rootBytes.length}`
            );
            result = await api.putRoot(vaultId, rootBytes, parentHash, perf);
        }
    } finally {
        endRootCommit?.();
    }

    // The server accepted this semantic root transaction. Record it before
    // local candidate adoption/checkpoint: a later local failure must not hide
    // an already-authoritative remote commit or call it merely "prepared".
    perf?.increment({
        rootCommittedPaths: allTreeUpdates.length + deleted.length,
        rootCommittedCuts: 1,
        trackedDeletesCommitted: trackedDeletesForCommit,
    });

    // Server acceptance is the transaction boundary. Only now can the local
    // committed pointer advance and old immutable chunks be swept. If stop
    // arrived during this request, still adopt/save its accepted result: an
    // in-flight root publication is not made cancellable by the yield signal.
    settleCandidateOutput("commit");
    candidateOpen = false;

    // Commit local metadata only after the server accepted this root. A
    // failed check/upload/root request therefore leaves sync-base untouched.
    if (!rootContext) {
        for (const change of deleted) syncBase.removeEntry(change.path);
        for (const update of allTreeUpdates) {
            syncBase.setEntry(update.path, update.hash, update.mtime_ms, update.size);
        }
        syncBase.setLastSyncTimestamp(Date.now());
        const endCheckpoint = perf?.phase("checkpoint");
        try { await syncBase.save(); }
        finally { endCheckpoint?.(); }
    }

    const preparedCleanup: NonNullable<PushOutcome["preparedCleanup"]> = [];
    if (prepared) for (const update of allTreeUpdates) {
        const mutationId = preparedMutations.get(update.path);
        if (mutationId !== undefined) preparedCleanup.push({ scopeHash: prepared.scopeHash,
            path: update.path, expectedMutationId: mutationId });
    }
    return {
        newRootHash: result.root_hash,
        conflicts:   result.conflicts ?? [],
        deferred,
        published: true,
        ...(rootSettlement ? { rootSettlement } : {}),
        ...(preparedCleanup.length ? { preparedCleanup } : {}),
        ...(continuation ? { continuation } : {}),
    };
    } catch (error) {
        if (error instanceof CandidateMutationOutputAdmissionDeniedError &&
            candidateMutationInputCount !== null && candidateOwnedRevision !== undefined) {
            // This is the only point which knows the post-admission mutation
            // closure. The engine may suppress only if it exactly covers the
            // claimed cut; a late oversized/dependent deferral makes it smaller.
            try {
                attestCandidateMutationRefusalCut(
                    error,
                    tree,
                    candidateMutationInputCount,
                    candidateOwnedRevision,
                );
            } catch {
                // Suppression provenance is optional. Candidate abort below
                // must run and the original refusal must remain authoritative.
            }
        }
        if (candidateOpen) {
            try {
                const endTree = perf?.phase("tree_update");
                let aborted = false;
                try {
                    if (candidateOwnedRevision !== undefined) {
                        const abortCandidate = () => settleCandidateOutput("abort");
                        aborted = hasPendingTreeCandidateMutationRetirement(tree)
                            ? await abortTreeCandidateAfterMutationRetirement(tree, candidateOwnedRevision, abortCandidate)
                            : await abortTreeCandidateAfterReachabilityRetirement(tree, candidateOwnedRevision, abortCandidate);
                        if (!aborted) {
                            console.warn("[obsetync] skipped abort of a newer candidate tree");
                        }
                    } else if (candidateRevisionReader === undefined && tree.has_candidate()) {
                        settleCandidateOutput("abort");
                        aborted = true;
                    } else if (candidateRevisionIndeterminate) {
                        console.warn("[obsetync] candidate abort deferred: exact revision witness is unavailable");
                    }
                } finally {
                    endTree?.();
                }
                if (aborted) console.warn("[obsetync] failed push aborted candidate tree");
            } catch (abortError) {
                console.error("[obsetync] failed to abort candidate tree:", abortError);
            }
        }
        await invalidateDurableObjectConfirmations(prepared);
        throw error;
    }
}

const monotonicNow = (): number => globalThis.performance?.now?.() ?? Date.now();

async function readMissingSmallSources(
    files: readonly ObsetyncBatchFile[], needed: ReadonlySet<string>, io: PlatformIO,
    memory: TransientWorkScope, admittedConcurrency: number, boundaryFiles: number,
    onBoundary: ((readyPrefixCount: number) => Promise<boolean>) | undefined,
    perf?: PerfOperation, signal?: AbortSignal,
): Promise<void> {
    if (!Number.isSafeInteger(boundaryFiles) || boundaryFiles < 1) {
        throw new RangeError("invalid small-source responsive boundary");
    }
    const seen = new Set<string>();
    let requiresRead = false;
    // `files` is already a byte/count-admitted batch, not the whole backlog.
    // Preserve its first-representative and upload order, including duplicates
    // whose earlier representative already has preloaded/phase-A data.
    for (const file of files) {
        const hash = file.change.hash;
        if (file.chunkInfo || !hash || !needed.has(hash) || seen.has(hash)) continue;
        seen.add(hash);
        if (!file.change.data && !file.largeData) { requiresRead = true; break; }
    }
    // Every missing representative already owns bytes from phase A.
    if (!requiresRead) return;

    seen.clear();
    for (let windowStart = 0; windowStart < files.length;) {
        const windowEnd = Math.min(files.length, windowStart + boundaryFiles);
        const sources: ObsetyncBatchFile[] = [];
        for (let index = windowStart; index < windowEnd; index++) {
            const file = files[index], hash = file.change.hash;
            if (file.chunkInfo || !hash || !needed.has(hash) || seen.has(hash)) continue;
            seen.add(hash);
            if (!file.change.data && !file.largeData) sources.push(file);
        }
        for (let first = 0; first < sources.length;) {
            throwIfWorkAborted(signal);
            const configured = getHashTuning().readConcurrency;
            if (!Number.isSafeInteger(configured) || configured < 1 ||
                !Number.isSafeInteger(admittedConcurrency) || admittedConcurrency < 1) {
                throw new RangeError("invalid admitted small-source read concurrency");
            }
            const group = sources.slice(first, first + Math.min(configured, admittedConcurrency));
            first += group.length;
            // Capture source scalars before any native dispatch. A caller
            // mutation cannot redirect an admitted read to another path/size.
            const captured = group.map(file => ({ ...file.change, data: undefined }));
            const endRead = perf?.phase("read");
            perf?.setDemand({ read: sources.length - first + group.length });
            try {
                const results = await Promise.allSettled(captured.map(change =>
                    readAdmittedPushSource(change, io, memory, signal)));
                const failed = results.find((result): result is PromiseRejectedResult => result.status === "rejected");
                if (failed) throw failed.reason;
                throwIfWorkAborted(signal);
                for (let index = 0; index < group.length; index++) {
                    group[index].largeData = (results[index] as PromiseFulfilledResult<Uint8Array>).value;
                }
            } finally { endRead?.(); perf?.setDemand({}); }
        }
        windowStart = windowEnd;
        if (windowStart < files.length && onBoundary && await onBoundary(windowStart)) return;
    }
}

async function readAdmittedPushSource(
    change: FileChange, io: PlatformIO, memory: TransientWorkScope, signal?: AbortSignal,
): Promise<Uint8Array> {
    return memory.track(async () => {
        throwIfWorkAborted(signal);
        if (io.readFileIdentityVerified && change.size !== undefined && change.mtime !== undefined) {
            const verified = await io.readFileIdentityVerified(change.path,
                { size: change.size, mtime: change.mtime }, signal);
            throwIfWorkAborted(signal);
            if (verified !== null) {
                if (verified.byteLength !== change.size || verified.buffer.byteLength > change.size) {
                    throw new HashWorkerFileDriftError(
                        "identity-verified source changed or exceeded its admitted allocation",
                    );
                }
                return verified;
            }
        }
        const before = await io.stat(change.path);
        throwIfWorkAborted(signal);
        if (!before || before.size !== change.size ||
            (change.mtime !== undefined && Math.abs(before.mtime - change.mtime) > 1)) {
            throw new HashWorkerFileDriftError("source changed before admitted read");
        }
        const data = await io.readFile(change.path);
        throwIfWorkAborted(signal);
        if (data.byteLength !== before.size || data.buffer.byteLength > before.size) {
            throw new HashWorkerFileDriftError("source changed or backing allocation exceeded admitted size");
        }
        const after = await io.stat(change.path);
        throwIfWorkAborted(signal);
        if (!after || after.size !== before.size || Math.abs(after.mtime - before.mtime) > 1) {
            throw new HashWorkerFileDriftError("source changed during admitted read");
        }
        return data;
    });
}

function memoryPressureDetected(): boolean {
    const memory = (globalThis.performance as any)?.memory;
    return Number.isFinite(memory?.usedJSHeapSize) &&
        Number.isFinite(memory?.jsHeapSizeLimit) &&
        memory.jsHeapSizeLimit > 0 &&
        memory.usedJSHeapSize / memory.jsHeapSizeLimit >= 0.85;
}

function observeHashStep(bytes: number, startedAt: number): number {
    const durationMs = Math.max(0, monotonicNow() - startedAt);
    // A synchronous WASM call occupies the renderer for its full duration,
    // so that duration is also a conservative event-loop-lag observation.
    observeHashFeedback({
        bytes,
        durationMs,
        eventLoopLagMs: durationMs,
        memoryPressure: memoryPressureDetected(),
    });
    return durationMs;
}

/** Caller retains the range and reserves the complete feed workspace before
 * this function constructs WASM state. No nested global source admission. */
async function hashAdmittedRange(
    wasm: WasmModule, data: Uint8Array, perf: PerfOperation | undefined,
    signal: AbortSignal | undefined, admittedFeedCeiling: number, perfWeight = 1,
): Promise<string> {
    throwIfWorkAborted(signal);
    const hasher = new wasm.Hasher();
    let hashMs = 0;
    try {
        let lastYieldAt = monotonicNow();
        for (let offset = 0; offset < data.byteLength;) {
            throwIfWorkAborted(signal);
            const end = Math.min(data.byteLength, offset + Math.min(getHashTuning().feedBytes, admittedFeedCeiling));
            const started = monotonicNow();
            hasher.update(data.subarray(offset, end));
            hashMs += observeHashStep(end - offset, started);
            offset = end;
            if (monotonicNow() - lastYieldAt >= getHashTuning().yieldBudgetMs) {
                await yieldWork({ perf, signal });
                lastYieldAt = monotonicNow();
            }
        }
        throwIfWorkAborted(signal);
        const started = monotonicNow();
        const hash = hasher.finalize();
        hashMs += Math.max(0, monotonicNow() - started);
        return hash;
    } finally {
        try { hasher.free(); }
        finally { perf?.addPhase("hash", hashMs * perfWeight); }
    }
}

/**
 * Hash file bytes via the streaming WASM Hasher. Feed size is selected for the
 * active platform and adapts between 64 KiB and its platform cap. WASM linear
 * memory therefore follows the feed budget, never the full file size.
 */
export function streamingHash(wasm: WasmModule, data: Uint8Array): string {
    const hasher = new wasm.Hasher();
    try {
        let offset = 0;
        while (offset < data.length) {
            const feedBytes = getHashTuning().feedBytes;
            const end = Math.min(data.length, offset + feedBytes);
            const started = monotonicNow();
            hasher.update(data.subarray(offset, end));
            observeHashStep(end - offset, started);
            offset = end;
        }
        return hasher.finalize();
    } finally {
        hasher.free();
    }
}

/** Plan FastCDC chunks through small WASM bridge slices. The source buffer is
 *  necessarily whole-file on Obsidian mobile, but WASM never receives a
 *  second whole-file copy and retains a bounded ~4 MiB window. */
export async function chunkFileStreaming(
    wasm: WasmModule,
    data: Uint8Array,
    perf?: PerfOperation,
    signal?: AbortSignal,
    admittedFeedCeiling = getHashTuning().maxFeedBytes,
): Promise<any> {
    throwIfWorkAborted(signal);
    const chunker = new wasm.WasmChunker();
    try {
        let offset = 0;
        let lastYieldAt = monotonicNow();
        while (offset < data.length) {
            throwIfWorkAborted(signal);
            const feedBytes = Math.min(getHashTuning().feedBytes, admittedFeedCeiling);
            const end = Math.min(data.length, offset + feedBytes);
            const started = monotonicNow();
            chunker.update(data.subarray(offset, end));
            observeHashStep(end - offset, started);
            offset = end;
            const afterStep = monotonicNow();
            if (afterStep - lastYieldAt >= getHashTuning().yieldBudgetMs) {
                await yieldWork({ perf, signal });
                lastYieldAt = monotonicNow();
            }
        }
        throwIfWorkAborted(signal);
        return chunker.finish();
    } finally {
        chunker.free();
    }
}

/** Mobile FastCDC preparation from a runtime-qualified public resource URL.
 * Source ranges belong to the caller's already-admitted upload scope; no
 * whole-file DataAdapter read or nested global reservation is permitted. */
export async function chunkMobileRangedSource(
    wasm: WasmModule,
    reader: MobileRangeReader,
    memory: TransientWorkScope,
    perf?: PerfOperation,
    signal?: AbortSignal,
    admittedFeedCeiling = getHashTuning().maxFeedBytes,
): Promise<any> {
    throwIfWorkAborted(signal);
    const chunker = new wasm.WasmChunker();
    let offset = 0;
    let lastYieldAt = monotonicNow();
    try {
        while (offset < reader.size) {
            throwIfWorkAborted(signal);
            const rangeSize = Math.min(MAX_FASTCDC_CHUNK_BYTES, reader.size - offset);
            const readStarted = monotonicNow();
            const data = await reader.readBorrowed(offset, rangeSize, memory, signal);
            perf?.addPhase("read", Math.max(0, monotonicNow() - readStarted));
            if (data.byteLength !== rangeSize) {
                throw new MobileRangeReadError("PROTOCOL");
            }
            for (let rangeOffset = 0; rangeOffset < data.byteLength;) {
                throwIfWorkAborted(signal);
                const feedBytes = Math.min(getHashTuning().feedBytes, admittedFeedCeiling);
                const end = Math.min(data.byteLength, rangeOffset + feedBytes);
                const started = monotonicNow();
                chunker.update(data.subarray(rangeOffset, end));
                observeHashStep(end - rangeOffset, started);
                rangeOffset = end;
                const now = monotonicNow();
                if (now - lastYieldAt >= getHashTuning().yieldBudgetMs) {
                    await yieldWork({ perf, signal });
                    lastYieldAt = monotonicNow();
                }
            }
            offset += data.byteLength;
        }
        await reader.verify();
        throwIfWorkAborted(signal);
        return chunker.finish();
    } finally {
        chunker.free();
    }
}

/** Full authoritative mobile source hash without invoking FastCDC. Used only
 * to validate a persisted chunk layout; ranges remain owned by the enclosing
 * transient scope and the resource reader performs exact generation fences. */
export async function hashMobileRangedSource(
    wasm: WasmModule,
    reader: MobileRangeReader,
    memory: TransientWorkScope,
    perf?: PerfOperation,
    signal?: AbortSignal,
    admittedFeedCeiling = getHashTuning().maxFeedBytes,
): Promise<string> {
    throwIfWorkAborted(signal);
    const hasher = new wasm.Hasher();
    let offset = 0;
    let lastYieldAt = monotonicNow();
    try {
        while (offset < reader.size) {
            throwIfWorkAborted(signal);
            const rangeSize = Math.min(MAX_FASTCDC_CHUNK_BYTES, reader.size - offset);
            const readStarted = monotonicNow();
            const data = await reader.readBorrowed(offset, rangeSize, memory, signal);
            perf?.addPhase("read", Math.max(0, monotonicNow() - readStarted));
            if (data.byteLength !== rangeSize) throw new MobileRangeReadError("PROTOCOL");
            for (let rangeOffset = 0; rangeOffset < data.byteLength;) {
                throwIfWorkAborted(signal);
                const feedBytes = Math.min(getHashTuning().feedBytes, admittedFeedCeiling);
                const end = Math.min(data.byteLength, rangeOffset + feedBytes);
                const started = monotonicNow();
                hasher.update(data.subarray(rangeOffset, end));
                observeHashStep(end - rangeOffset, started);
                rangeOffset = end;
                const now = monotonicNow();
                if (now - lastYieldAt >= getHashTuning().yieldBudgetMs) {
                    await yieldWork({ perf, signal });
                    lastYieldAt = monotonicNow();
                }
            }
            offset += data.byteLength;
        }
        await reader.verify();
        throwIfWorkAborted(signal);
        return hasher.finalize();
    } finally { hasher.free(); }
}

/**
 * Stream-hash a file directly from disk using Node.js fs (Electron/desktop only).
 * Uses the adaptive platform feed — peak read memory follows that bounded feed
 * regardless of file size.
 * Falls back to a whole-file read followed by cooperatively yielded hash feeds
 * on mobile (no Node.js fs); this does not make the source read streaming.
 *
 * This is the nproc-ready path: each Web Worker calls this independently,
 * giving true parallel hashing across cores with zero data crossing thread boundaries.
 */
export async function hashFileStreaming(
    path: string,
    io: PlatformIO,
    wasm: WasmModule,
    perf?: PerfOperation,
    perfWeight = 1,
    signal?: AbortSignal,
    browserHash?: BrowserHashWorkerRuntime | null,
): Promise<string> {
    throwIfWorkAborted(signal);
    const absPath = io.getAbsolutePath(path);
    if (absPath) {
        const runtimeRequire = typeof require === "function"
            ? require
            : (globalThis as any).require;
        let fs: typeof import('fs') | undefined;
        try { fs = runtimeRequire?.('fs'); } catch { /* no native stream capability */ }
        if (fs?.createReadStream) {
            const nativeFs = fs;
            const streamFeedBytes = getHashTuning().feedBytes;
            return withStreamingHashSource({
                feedBytes: streamFeedBytes,
                signal,
                consume: async () => {
                    const hasher = new wasm.Hasher();
                    const started = perf ? monotonicNow() : 0;
                    let hashMs = 0;
                    let yieldMs = 0;
                    let stream: import('fs').ReadStream | undefined;
                    let closed: Promise<void> | undefined;
                    try {
                        stream = nativeFs.createReadStream(absPath, {
                            highWaterMark: streamFeedBytes, signal,
                        });
                        closed = new Promise<void>((resolve) => stream!.once("close", resolve));
                        let lastYieldAt = monotonicNow();
                        // Unlike throwing inside an EventEmitter data handler,
                        // iterator failures reach this finally and close the
                        // native stream before its byte reservation is freed.
                        for await (const chunk of stream) {
                            if (!(chunk instanceof Uint8Array)) {
                                throw new Error("native hash stream returned non-binary data");
                            }
                            for (let offset = 0; offset < chunk.byteLength;) {
                                throwIfWorkAborted(signal);
                                const feed = Math.min(getHashTuning().feedBytes, streamFeedBytes);
                                const end = Math.min(chunk.byteLength, offset + feed);
                                const hashStarted = monotonicNow();
                                hasher.update(chunk.subarray(offset, end));
                                hashMs += observeHashStep(end - offset, hashStarted);
                                offset = end;
                                if (monotonicNow() - lastYieldAt >= getHashTuning().yieldBudgetMs) {
                                    const yieldStarted = monotonicNow();
                                    await yieldWork({ perf, signal });
                                    yieldMs += Math.max(0, monotonicNow() - yieldStarted);
                                    lastYieldAt = monotonicNow();
                                }
                            }
                        }
                        throwIfWorkAborted(signal);
                        const finalizeStarted = monotonicNow();
                        const result = hasher.finalize();
                        hashMs += Math.max(0, monotonicNow() - finalizeStarted);
                        return result;
                    } finally {
                        try {
                            // A pending fs read can outlive destroy(). Keep its
                            // workset charged until the descriptor closes.
                            stream?.destroy();
                            await closed;
                        } finally {
                            hasher.free();
                            if (perf) {
                                const totalMs = Math.max(0, monotonicNow() - started);
                                perf.addPhase("hash", hashMs * perfWeight);
                                perf.addPhase("read", Math.max(0, totalMs - hashMs - yieldMs) * perfWeight);
                            }
                        }
                    }
                },
            });
        }
    }
    // Mobile / no-fs fallback: stat before admission/native allocation. The
    // adapter still cannot bound a source that grows after this stat.
    const stat = await io.stat(path);
    if (!stat) {
        const error = new Error("hash source no longer exists");
        Object.assign(error, { code: "ENOENT" });
        throw error;
    }
    throwIfWorkAborted(signal);
    const reservedFeedBytes = getHashTuning().maxFeedBytes;
    const attempt = (
        sourceStat: { size: number; mtime: number },
        runtime?: BrowserHashWorkerRuntime | null,
        assertCurrent?: () => void,
    ) => withHashSource({
        sourceBytes: sourceStat.size,
        feedBytes: reservedFeedBytes,
        signal,
        read: async () => {
            assertCurrent?.();
            const readStarted = perf ? monotonicNow() : 0;
            try {
                const data = await io.readFile(path);
                assertCurrent?.();
                return data;
            }
            finally { if (perf) perf.addPhase("read", Math.max(0, monotonicNow() - readStarted) * perfWeight); }
        },
        consume: async data => {
            assertCurrent?.();
            if (data.byteLength !== sourceStat.size) {
                throw new HashWorkerFileDriftError("hash source size changed during admitted read");
            }
            const hashed = await hashAdmittedBytesWithBrowserWorker(
                wasm, data, perf, signal, reservedFeedBytes, runtime, perfWeight,
            );
            assertCurrent?.();
            return hashed.hash;
        },
    });
    try {
        if (!browserHash || stat.size > BROWSER_HASH_WORKER_MAX_INPUT_BYTES) return await attempt(stat);
        return await attempt(stat, browserHash);
    } catch (error) {
        if (!(error instanceof BrowserHashFreshReadRequired)) throw error;
        browserHash?.assertCurrent();
        throwIfWorkAborted(signal);
        const refreshed = await io.stat(path);
        browserHash?.assertCurrent();
        if (!refreshed || refreshed.size !== stat.size || Math.abs(refreshed.mtime - stat.mtime) > 1) {
            throw new HashWorkerFileDriftError("source changed before browser hash fallback reread");
        }
        // The first withHashSource lease has settled. Re-enter without the
        // worker so a lost-but-safely-released transfer is read only under a
        // fresh admission and the same captured runtime generation fence.
        return await attempt(refreshed, null, () => browserHash?.assertCurrent());
    }
}

function throughput(files: number, bytes: number, startMs: number): string {
    const secs = Math.max((Date.now() - startMs) / 1000, 0.1);
    const fps  = (files / secs).toFixed(1);
    const bps  = bytes / secs;
    let bpsFmt: string;
    if (bps >= 1_048_576)  bpsFmt = `${(bps / 1_048_576).toFixed(1)} MB/s`;
    else if (bps >= 1024)  bpsFmt = `${(bps / 1024).toFixed(0)} KB/s`;
    else if (bytes > 0)    bpsFmt = `${bps.toFixed(0)} B/s`;
    else                   bpsFmt = "";
    return bpsFmt ? `· ${fps} f/s · ${bpsFmt}` : `· ${fps} f/s`;
}
