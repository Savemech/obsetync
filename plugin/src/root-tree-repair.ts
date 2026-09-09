import { isSafeVaultPath } from "./delta-validation";
import type { CapturedBaseTreeEntries, CapturedBaseTreeEntry, ObsetyncSyncBase } from "./sync-base";
import type { WasmTree } from "./push";
import { reserveTransientWorkset, type TransientReservationBudget } from "./transient-memory";
import type { ResourceReservation } from "./resource-budget";
import {
    RootTreeResidentAdmission,
    type RootTreeResidentAttempt,
    type RootTreeResidentRetirement,
} from "./root-tree-resident-admission";
import { throwIfWorkAborted, yieldWork } from "./work-scheduler";

export const ROOT_TREE_REPAIR_LIMITS = {
    entries: 65_536,
    serializedBytes: 8 * 1024 * 1024,
    yieldEvery: 256,
    scratchBytes: 128 * 1024,
    feedEntries: 256,
    feedBytes: 256 * 1024,
    buildUnits: 256,
    retirementUnits: 256,
} as const;

export class RootTreeRepairError extends Error {
    constructor(readonly code: "LIMIT" | "BASE_CHANGED" | "INVALID_BASE" | "INVALID_TREE", message: string) {
        super(message);
        this.name = "RootTreeRepairError";
    }
}

/** Cleanup failed with a native owner still potentially alive. The exact
 * wrapper and its admission remain quarantined until a successful drain. */
export class RootTreeRetirementError extends Error {
    readonly code = "ROOT_TREE_RETIREMENT_FAILED";
    constructor(readonly cleanupCause: unknown, readonly originalCause?: unknown) {
        super("root tree retirement failed; native owner and memory admission remain quarantined");
        this.name = "RootTreeRetirementError";
    }
}

/** Fail-fast refusal of the scoped V2 replacement-output allowance. This is
 * not an assertion about total native heap/RSS or currently unmeasured owners. */
export class RootTreeOutputAdmissionDeniedError extends Error {
    readonly code = "ROOT_TREE_OUTPUT_ADMISSION_DENIED";
    constructor(
        readonly requestedBytes: number,
        readonly capacityBytes: number,
        readonly usedBytes: number,
    ) {
        super(`V2 replacement output requires ${requestedBytes} requested bytes; ` +
            `${Math.max(0, capacityBytes - usedBytes)} are currently available`);
        this.name = "RootTreeOutputAdmissionDeniedError";
    }
}

export interface RootTreeRepairOptions {
    /** Isolated accounting/scheduling seams. Production uses the shared pool
     * and task scheduler; these options never enlarge the hard limits. */
    budget?: TransientReservationBudget;
    cooperate?: (signal?: AbortSignal) => Promise<void>;
    /** The engine can invalidate this exact tree wrapper while we yield. */
    assertCurrent?: () => void;
    /** Optional policy hook at the exact V2 pre-encode boundary. It runs while
     * the private node store is still empty. Returning normally authorizes the
     * host to echo the plan witness back to native; throwing leaves the job
     * available for deferred retirement. This hook is not itself a quota. */
    onNodePayloadPlan?: (plan: Readonly<RootTreeNodePayloadPlan>, signal?: AbortSignal) => void | Promise<void>;
    /** Independent fail-fast ownership for the versioned V2 replacement-output
     * requested-buffer subset. It does not share or queue behind `budget`. */
    residentAdmission?: RootTreeResidentAdmission;
}

export interface RootTreeNodePayloadPlan {
    nodeCount: number;
    leafCount: number;
    internalCount: number;
    nodePayloadBytes: number;
    maxNodeBytes: number;
    storedRootBytes: number;
}

export interface RootTreeOutputMemoryPlanV1 {
    schema: 1;
    scope: "v1-replacement-output" | "v2-replacement-output";
    nodePayloadBytes: number;
    rangeEndpointPeakRequestedBytes: number;
    rangeEndpointResidentRequestedBytes: number;
    peakAdmissionBytes: number;
    residentAdmissionBytes: number;
}

interface RootTreeSortMemoryPlanV1 {
    schema: 1;
    scope: "v2-replacement-sort-indices";
    entryCount: number;
    indexSizeBytes: 4;
    sourceIndexRequestedBytes: number;
    targetIndexRequestedBytes: number;
    peakAdmissionBytes: number;
}

export interface RootTreeRepairResult {
    rebuiltRoot: string;
    capturedBaseRoot: string | null;
    entryCount: number;
    serializedBytes: number;
}

type RepairTree = Pick<WasmTree, "rebuild_from_entries_in_version" | "root_hash_hex" | "tree_version" | "total_files" |
    "begin_replacement_rebuild_job" | "append_replacement_rebuild_job" | "finish_replacement_rebuild_job" |
    "start_replacement_rebuild_job" | "step_replacement_rebuild_job" |
    "step_replacement_rebuild_output_memory_v1_job" |
    "replacement_rebuild_sort_memory_plan_v1_job" | "start_replacement_rebuild_sort_memory_v1_job" |
    "replacement_rebuild_plan_job" | "resume_replacement_rebuild_job" |
    "replacement_rebuild_output_memory_plan_v1_job" | "resume_replacement_rebuild_output_memory_v1_job" |
    "cancel_replacement_rebuild_job_deferred" | "finish_replacement_rebuild_job_deferred" | "step_tree_retirement" |
    "cancel_tree_job" | "committed_revision" | "candidate_revision">;
const HASH = /^[0-9a-f]{64}$/;

interface PendingRetirement {
    token: number;
    needsCancel: boolean;
    readyForDrain: boolean;
    done: boolean;
    completed: number;
    memory: ResourceReservation;
    outputRelease?: (token: RootTreeResidentRetirement) => void;
    outputToken?: RootTreeResidentRetirement;
    cancel: (token: number) => void;
    step: (token: number, maxUnits: number) => unknown;
    cooperate: (signal?: AbortSignal) => Promise<void>;
    flight?: Promise<void>;
}

// Strong ownership is intentional: an exceptional native cleanup failure must
// not let GC lose the wrapper/token while its accounting remains reserved.
// At most one cleanup belongs to each wrapper; no per-step history is kept.
const pendingRetirements = new Map<RepairTree, PendingRetirement>();

export function hasPendingRootTreeRetirement(tree: RepairTree): boolean {
    return pendingRetirements.has(tree);
}

/** Join/retry cleanup of this exact wrapper, never a newly selected tree.
 * Call before free or admitting another repair. A rejection is NOT permission
 * to free/release an owner which may still be alive. Concurrent calls join the
 * same promise; failed calls may be retried without another reservation. */
export function drainRootTreeRetirement(tree: RepairTree): Promise<void> {
    const owner = pendingRetirements.get(tree);
    if (!owner) return Promise.resolve();
    if (!owner.readyForDrain) {
        return Promise.reject(new RootTreeRetirementError(
            new Error("root tree operation still owns its pre-registered retirement slot")));
    }
    if (owner.flight) return owner.flight;
    // Install the flight before any native call or host callback can reenter.
    const work = Promise.resolve().then(async () => {
        if (owner.needsCancel) {
            owner.cancel.call(tree, owner.token);
            owner.needsCancel = false;
        }
        while (!owner.done) {
            const progress = retirementProgress(owner.step.call(tree, owner.token,
                ROOT_TREE_REPAIR_LIMITS.retirementUnits), owner.completed);
            owner.completed = progress.completed;
            owner.done = progress.done;
            // Cleanup cannot be cancelled by the operation whose buffers it
            // retires. Even the final step crosses a real host boundary.
            await owner.cooperate(undefined);
        }
    });
    const flight = work.catch(error => {
        if (owner.done) throw error;
        throw new RootTreeRetirementError(error);
    }).finally(() => {
        owner.flight = undefined;
        if (owner.done) {
            // Native completion is the proof for both transient input and the
            // scoped output owner. If the host ownership transition itself is
            // unexpectedly rejected, leave the strong pending owner intact so
            // a later drain retries instead of silently losing its charge.
            if (owner.outputToken) {
                owner.outputRelease!(owner.outputToken);
                owner.outputToken = undefined;
                owner.outputRelease = undefined;
            }
            if (pendingRetirements.get(tree) !== owner) {
                throw new RootTreeRetirementError(
                    new Error("root tree retirement owner identity changed"));
            }
            pendingRetirements.delete(tree);
            owner.memory.release();
        }
    });
    owner.flight = flight;
    return flight;
}

/** Allowance for retained row strings, joined UTF-16 input and the UTF-8
 * wasm-bindgen input copy, plus bounded traversal/row scratch. Parsed native
 * entries, replacement graph, fixed/grown WASM heaps and GC retention are NOT
 * fully covered by this ledger. The atomic native rebuild is one synchronous
 * call: it cannot yield or be cancelled once entered. */
export function estimateRootTreeRepairWorkset(serializedBytes: number): number {
    if (!Number.isSafeInteger(serializedBytes) || serializedBytes < 2 ||
        serializedBytes > ROOT_TREE_REPAIR_LIMITS.serializedBytes) {
        throw new RootTreeRepairError("LIMIT", "root tree rebuild JSON exceeds the bounded input capacity");
    }
    return 6 * serializedBytes + ROOT_TREE_REPAIR_LIMITS.scratchBytes;
}

/** Complete input admission for the paged path: retained native path bytes,
 * entry slots, and one bounded page's strings/parse/bridge copies. When the
 * versioned V2 sort ABI is present, its two exact index-buffer requests are
 * added separately to this same outer reservation before native begin. Other
 * graph/sort/validation scratch, old graph drop, allocator/GC/RSS remain
 * outside this controlled allowance. Native begin reserves entry capacity. */
export function estimatePagedRootTreeRepairWorkset(serializedBytes: number, entryCount: number): number {
    estimateRootTreeRepairWorkset(serializedBytes); // Shared hard input bound.
    if (!Number.isSafeInteger(entryCount) || entryCount < 0 || entryCount > ROOT_TREE_REPAIR_LIMITS.entries) {
        throw new RootTreeRepairError("LIMIT", "root tree rebuild entry count exceeds the bounded input capacity");
    }
    return 2 * serializedBytes + 128 * entryCount +
        6 * Math.min(serializedBytes, ROOT_TREE_REPAIR_LIMITS.feedBytes) + ROOT_TREE_REPAIR_LIMITS.scratchBytes;
}

function captureTreeOwner(tree: RepairTree): () => void {
    const version = tree.tree_version();
    const readers = [tree.committed_revision, tree.candidate_revision];
    const read = (reader: (() => number) | undefined): number | undefined => {
        if (reader === undefined) return undefined;
        if (typeof reader !== "function") throw new RootTreeRepairError("INVALID_TREE", "invalid tree revision witness");
        const value = reader.call(tree);
        if (!Number.isSafeInteger(value) || value < 0) throw new RootTreeRepairError("INVALID_TREE", "invalid tree revision witness");
        return value;
    };
    const revisions = readers.map(read);
    const hash = readers[0] === undefined ? tree.root_hash_hex() : undefined;
    const count = readers[0] === undefined ? tree.total_files() : undefined;
    return () => {
        if (tree.tree_version() !== version || readers.some((reader, index) => read(reader) !== revisions[index]) ||
            (readers[0] === undefined && (tree.root_hash_hex() !== hash || tree.total_files() !== count))) {
            throw new RootTreeRepairError("INVALID_TREE", "tree changed during root rebuild input");
        }
    };
}

function assertCurrent(captured: CapturedBaseTreeEntries, signal?: AbortSignal): void {
    throwIfWorkAborted(signal);
    if (!captured.isCurrent()) throw new RootTreeRepairError("BASE_CHANGED", "sync-base changed during root tree rebuild preparation");
}

function buildProgress(value: unknown, previous: number): { done: boolean; completed: number; phase: string } {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement rebuild progress");
    }
    const keys = Reflect.ownKeys(value), row: Record<string, unknown> = {};
    const fields = ["done", "units", "completed", "phase"];
    if (keys.length !== fields.length || !fields.every(key => keys.includes(key))) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement rebuild progress");
    }
    for (const field of fields) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor)) {
            throw new RootTreeRepairError("INVALID_TREE", "invalid replacement rebuild progress");
        }
        row[field] = descriptor.value;
    }
    if (typeof row.done !== "boolean" || typeof row.phase !== "string" || row.phase.length < 1 || row.phase.length > 64 ||
        !Number.isSafeInteger(row.units) || (row.units as number) < 0 || (row.units as number) > ROOT_TREE_REPAIR_LIMITS.buildUnits ||
        (!row.done && row.units === 0 && row.phase !== "plan ready") || !Number.isSafeInteger(row.completed) ||
        row.completed !== previous + (row.units as number)) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement rebuild progress");
    }
    return { done: row.done, completed: row.completed as number, phase: row.phase as string };
}

function nodePayloadPlan(value: unknown): Readonly<RootTreeNodePayloadPlan> {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement node payload plan");
    }
    const fields = ["nodeCount", "leafCount", "internalCount", "nodePayloadBytes", "maxNodeBytes", "storedRootBytes"] as const;
    const keys = Reflect.ownKeys(value), row: Record<string, unknown> = {};
    if (keys.length !== fields.length || !fields.every(key => keys.includes(key))) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement node payload plan");
    }
    for (const field of fields) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor) || !Number.isSafeInteger(descriptor.value) || descriptor.value < 0) {
            throw new RootTreeRepairError("INVALID_TREE", "invalid replacement node payload plan");
        }
        row[field] = descriptor.value;
    }
    const plan = row as unknown as RootTreeNodePayloadPlan;
    if (plan.nodeCount !== plan.leafCount + plan.internalCount ||
        plan.maxNodeBytes > 256 * 1024 || plan.storedRootBytes < 64 || plan.storedRootBytes > 16 * 1024 ||
        (plan.nodeCount === 0
            ? plan.nodePayloadBytes !== 0 || plan.maxNodeBytes !== 0 || plan.leafCount !== 0 || plan.internalCount !== 0
            : plan.nodePayloadBytes < plan.maxNodeBytes || plan.maxNodeBytes < 8 || plan.leafCount < 1)) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement node payload plan");
    }
    return Object.freeze({ ...plan });
}

function outputMemoryPlanV1(value: unknown, version: 1 | 2): Readonly<RootTreeOutputMemoryPlanV1> {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement output memory plan");
    }
    const fields = ["schema", "scope", "nodePayloadBytes", "rangeEndpointPeakRequestedBytes",
        "rangeEndpointResidentRequestedBytes", "peakAdmissionBytes", "residentAdmissionBytes"] as const;
    const keys = Reflect.ownKeys(value), row: Record<string, unknown> = {};
    if (keys.length !== fields.length || !fields.every(key => keys.includes(key))) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement output memory plan");
    }
    for (const field of fields) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor)) {
            throw new RootTreeRepairError("INVALID_TREE", "invalid replacement output memory plan");
        }
        row[field] = descriptor.value;
    }
    const numeric = fields.slice(2);
    if (row.schema !== 1 || row.scope !== `${version === 1 ? "v1" : "v2"}-replacement-output` ||
        numeric.some(field => !Number.isSafeInteger(row[field]) || (row[field] as number) < 0)) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement output memory plan");
    }
    const plan = row as unknown as RootTreeOutputMemoryPlanV1;
    if (plan.rangeEndpointResidentRequestedBytes > plan.rangeEndpointPeakRequestedBytes ||
        plan.nodePayloadBytes > Number.MAX_SAFE_INTEGER - plan.rangeEndpointPeakRequestedBytes ||
        plan.peakAdmissionBytes !== plan.nodePayloadBytes + plan.rangeEndpointPeakRequestedBytes ||
        plan.nodePayloadBytes > Number.MAX_SAFE_INTEGER - plan.rangeEndpointResidentRequestedBytes ||
        plan.residentAdmissionBytes !== plan.nodePayloadBytes + plan.rangeEndpointResidentRequestedBytes ||
        plan.residentAdmissionBytes > plan.peakAdmissionBytes) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement output memory plan");
    }
    return Object.freeze({ ...plan });
}

function sortMemoryPlanV1(value: unknown, entryCount: number): Readonly<RootTreeSortMemoryPlanV1> {
    const invalid = () => new RootTreeRepairError("INVALID_TREE", "invalid replacement sort memory plan");
    if (value === null || typeof value !== "object" || Array.isArray(value)) throw invalid();
    const fields = ["schema", "scope", "entryCount", "indexSizeBytes",
        "sourceIndexRequestedBytes", "targetIndexRequestedBytes", "peakAdmissionBytes"] as const;
    const keys = Reflect.ownKeys(value), row: Record<string, unknown> = {};
    if (keys.length !== fields.length || !fields.every(key => keys.includes(key))) throw invalid();
    for (const field of fields) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor)) throw invalid();
        row[field] = descriptor.value;
    }
    if (row.schema !== 1 || row.scope !== "v2-replacement-sort-indices" ||
        fields.slice(2).some(field => !Number.isSafeInteger(row[field]) || (row[field] as number) < 0) ||
        row.entryCount !== entryCount || row.indexSizeBytes !== 4 ||
        row.sourceIndexRequestedBytes !== 4 * entryCount || row.targetIndexRequestedBytes !== 4 * entryCount ||
        row.peakAdmissionBytes !== 8 * entryCount) throw invalid();
    return Object.freeze({ ...row }) as unknown as Readonly<RootTreeSortMemoryPlanV1>;
}

function retirementProgress(value: unknown, previous: number): { done: boolean; completed: number } {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid tree retirement progress");
    }
    const fields = ["done", "units", "completed"], keys = Reflect.ownKeys(value);
    if (keys.length !== fields.length || !fields.every(key => keys.includes(key))) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid tree retirement progress");
    }
    const row: Record<string, unknown> = {};
    for (const field of fields) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor)) {
            throw new RootTreeRepairError("INVALID_TREE", "invalid tree retirement progress");
        }
        row[field] = descriptor.value;
    }
    if (typeof row.done !== "boolean" || !Number.isSafeInteger(row.units) ||
        (row.units as number) < 0 || (row.units as number) > ROOT_TREE_REPAIR_LIMITS.retirementUnits ||
        (!row.done && row.units === 0) || !Number.isSafeInteger(row.completed) ||
        row.completed !== previous + (row.units as number)) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid tree retirement progress");
    }
    return { done: row.done, completed: row.completed as number };
}

function serializeEntry(row: CapturedBaseTreeEntry, previousPath: string | null): string {
    if (!isSafeVaultPath(row.path) || (previousPath !== null && row.path <= previousPath) ||
        !HASH.test(row.hash) || !Number.isSafeInteger(row.size) || row.size < 0 ||
        !Number.isSafeInteger(row.mtime_ms) || row.mtime_ms < 0) {
        // Do not truncate/round legacy metadata into a different semantic
        // tree. The native JSON contract requires unsigned integer metadata.
        throw new RootTreeRepairError("INVALID_BASE", "sync-base has an invalid tree entry");
    }
    // Only explicit scalar fields cross the native boundary; no caller data
    // or extra fields/toJSON hooks are retained.
    return JSON.stringify({ path: row.path, hash: row.hash, mtime_ms: row.mtime_ms, size: row.size });
}

/** Count without constructing an additional encoded byte array. JSON.stringify
 * has already escaped isolated UTF-16 surrogates, so only valid pairs remain. */
function utf8Bytes(text: string): number {
    let bytes = 0;
    for (let index = 0; index < text.length; index++) {
        const code = text.charCodeAt(index);
        if (code < 0x80) bytes++;
        else if (code < 0x800) bytes += 2;
        else if (code >= 0xd800 && code <= 0xdbff && index + 1 < text.length &&
            text.charCodeAt(index + 1) >= 0xdc00 && text.charCodeAt(index + 1) <= 0xdfff) {
            bytes += 4; index++;
        } else bytes += 3;
    }
    return bytes;
}

/** Rebuild the COMPLETE immutable graph from current validated base entries,
 * never from an old intent payload or root bytes without their child chunks.
 * The semantic rebuilt root need not equal capturedBaseRoot after a partial
 * pull; callers must keep that honest parent separately. No base/cache/journal
 * write occurs here. Oversize, abort or drift fail the caller's startup gate.
 *
 * Capture is O(1). Pass one retains only one bounded row. After full input
 * admission the job path feeds bounded pages; the legacy path joins the whole
 * admitted input. Optional build and retirement APIs also step private graph
 * construction and owner teardown; individual codecs remain atomic. Older
 * ABIs retain their synchronous build/graph-drop compatibility boundaries. */
export async function rebuildRootTreeFromBase(
    base: Pick<ObsetyncSyncBase, "captureTreeEntries">,
    tree: RepairTree,
    preferredVersion: number,
    signal?: AbortSignal,
    options: RootTreeRepairOptions = {},
): Promise<RootTreeRepairResult> {
    const onNodePayloadPlan = options.onNodePayloadPlan;
    if (onNodePayloadPlan !== undefined && typeof onNodePayloadPlan !== "function") {
        throw new RootTreeRepairError("INVALID_TREE", "invalid node payload policy hook");
    }
    const residentAdmission = options.residentAdmission;
    if (residentAdmission !== undefined && !(residentAdmission instanceof RootTreeResidentAdmission)) {
        throw new RootTreeRepairError("INVALID_TREE", "invalid replacement output admission owner");
    }
    const outputAdmission = residentAdmission && {
        reserve: residentAdmission.reserve.bind(residentAdmission),
        ready: residentAdmission.ready.bind(residentAdmission),
        publish: residentAdmission.publish.bind(residentAdmission),
        detachPrivate: residentAdmission.detachPrivate.bind(residentAdmission),
        releaseBeforeOutput: residentAdmission.releaseBeforeOutput.bind(residentAdmission),
        releaseRetired: residentAdmission.releaseRetired.bind(residentAdmission),
        snapshot: residentAdmission.snapshot.bind(residentAdmission),
    };
    if (hasPendingRootTreeRetirement(tree)) await drainRootTreeRetirement(tree);
    throwIfWorkAborted(signal);
    if (preferredVersion !== 1 && preferredVersion !== 2) {
        throw new RootTreeRepairError("INVALID_TREE", "root tree rebuild requires a supported negotiated version");
    }
    const assertScope = options.assertCurrent;
    const begin = tree.begin_replacement_rebuild_job, append = tree.append_replacement_rebuild_job;
    const finish = tree.finish_replacement_rebuild_job, cancel = tree.cancel_tree_job;
    const startBuild = tree.start_replacement_rebuild_job, stepBuild = tree.step_replacement_rebuild_job;
    const stepOutputBuild = tree.step_replacement_rebuild_output_memory_v1_job;
    const readSortPlan = tree.replacement_rebuild_sort_memory_plan_v1_job;
    const startSortBuild = tree.start_replacement_rebuild_sort_memory_v1_job;
    const readPlan = tree.replacement_rebuild_plan_job, resumeBuild = tree.resume_replacement_rebuild_job;
    const readOutputPlan = tree.replacement_rebuild_output_memory_plan_v1_job;
    const resumeOutputBuild = tree.resume_replacement_rebuild_output_memory_v1_job;
    const cancelDeferred = tree.cancel_replacement_rebuild_job_deferred;
    const finishDeferred = tree.finish_replacement_rebuild_job_deferred, stepRetirement = tree.step_tree_retirement;
    const retirementClaimed = [cancelDeferred, finishDeferred, stepRetirement].some(method => method !== undefined);
    const buildClaimed = [startBuild, stepBuild].some(method => method !== undefined);
    const sortPlanClaimed = [readSortPlan, startSortBuild].some(method => method !== undefined);
    const planClaimed = [readPlan, resumeBuild].some(method => method !== undefined);
    const outputPlanClaimed = [stepOutputBuild, readOutputPlan, resumeOutputBuild]
        .some(method => method !== undefined);
    const claimed = [begin, append, finish].some(method => method !== undefined);
    if (claimed && ![begin, append, finish, cancel].every(method => typeof method === "function")) {
        throw new RootTreeRepairError("INVALID_TREE", "WASM tree exposes an incomplete replacement input API");
    }
    if (buildClaimed && (!claimed || ![startBuild, stepBuild].every(method => typeof method === "function"))) {
        throw new RootTreeRepairError("INVALID_TREE", "WASM tree exposes an incomplete replacement build API");
    }
    if (sortPlanClaimed && (!buildClaimed || ![readSortPlan, startSortBuild].every(method => typeof method === "function"))) {
        throw new RootTreeRepairError("INVALID_TREE", "WASM tree exposes an incomplete replacement sort memory API");
    }
    if (planClaimed && (!buildClaimed || ![readPlan, resumeBuild].every(method => typeof method === "function"))) {
        throw new RootTreeRepairError("INVALID_TREE", "WASM tree exposes an incomplete replacement plan API");
    }
    if (outputPlanClaimed && (!buildClaimed ||
        ![stepOutputBuild, readOutputPlan, resumeOutputBuild].every(method => typeof method === "function"))) {
        throw new RootTreeRepairError("INVALID_TREE", "WASM tree exposes an incomplete replacement output memory API");
    }
    if (preferredVersion === 2 && outputPlanClaimed && !planClaimed) {
        throw new RootTreeRepairError("INVALID_TREE", "V2 output memory API requires the node payload plan API");
    }
    if (preferredVersion === 2 && onNodePayloadPlan !== undefined && !planClaimed) {
        throw new RootTreeRepairError("INVALID_TREE", "V2 node payload policy requires the replacement plan API");
    }
    if (residentAdmission !== undefined && !outputPlanClaimed) {
        throw new RootTreeRepairError("INVALID_TREE", "replacement output admission requires the versioned memory API");
    }
    if (retirementClaimed && (!buildClaimed ||
        ![cancelDeferred, finishDeferred, stepRetirement].every(method => typeof method === "function"))) {
        throw new RootTreeRepairError("INVALID_TREE", "WASM tree exposes an incomplete retirement API");
    }
    if (residentAdmission !== undefined && claimed && !retirementClaimed) {
        throw new RootTreeRepairError("INVALID_TREE", "replacement output admission requires cooperative retirement");
    }
    const assertOwner = claimed ? captureTreeOwner(tree) : () => {};
    const captured = base.captureTreeEntries();
    const check = () => {
        assertCurrent(captured, signal); assertScope?.(); assertOwner();
        assertCurrent(captured, signal);
    };
    if (!Number.isSafeInteger(captured.entryCount) || captured.entryCount < 0 ||
        captured.entryCount > ROOT_TREE_REPAIR_LIMITS.entries) {
        throw new RootTreeRepairError("LIMIT", "root tree rebuild entry count exceeds the bounded input capacity");
    }
    if (captured.treeBaseRoot !== null && !HASH.test(captured.treeBaseRoot)) {
        throw new RootTreeRepairError("INVALID_BASE", "sync-base has an invalid parent root");
    }
    const reserve = options.budget
        ? options.budget.reserve.bind(options.budget) : reserveTransientWorkset;
    const cooperate = options.cooperate ?? ((signal?: AbortSignal) => yieldWork({ signal }));
    let serializedBytes = 2;
    // Do not keep this small lease while queueing the complete workset: nested
    // admission could deadlock when the larger request needs the whole pool.
    const scratch = await reserve(ROOT_TREE_REPAIR_LIMITS.scratchBytes, { signal });
    try {
        check();
        let count = 0, previousPath: string | null = null;
        for (const row of captured.entries()) {
            if (++count > captured.entryCount) throw new RootTreeRepairError("INVALID_BASE", "captured tree entry count differs");
            const line = serializeEntry(row, previousPath);
            previousPath = row.path;
            serializedBytes += utf8Bytes(line) + (count > 1 ? 1 : 0);
            if (serializedBytes > ROOT_TREE_REPAIR_LIMITS.serializedBytes) {
                throw new RootTreeRepairError("LIMIT", "root tree rebuild JSON exceeds the bounded input capacity");
            }
            if (count % ROOT_TREE_REPAIR_LIMITS.yieldEvery === 0) {
                await cooperate(signal); check();
            }
        }
        if (count !== captured.entryCount) throw new RootTreeRepairError("INVALID_BASE", "captured tree entry count differs");
        check();
    } finally { scratch.release(); }

    // wasm32 has two four-byte index arrays. The captured count is already
    // bounded, so this exact request is at most 512 KiB. Reserve alongside the
    // input BEFORE begin, not through a nested reservation when input is held.
    // The native plan must later confirm this amount before sorter allocation.
    const sortMemoryBytes = preferredVersion === 2 && sortPlanClaimed ? 8 * captured.entryCount : 0;
    const memory = await reserve((claimed
        ? estimatePagedRootTreeRepairWorkset(serializedBytes, captured.entryCount)
        : estimateRootTreeRepairWorkset(serializedBytes)) + sortMemoryBytes, { signal });
    const fragments: string[] = ["["];
    let json: string | undefined;
    let token: number | undefined, jobOpen = false, primary: unknown;
    let beginReturned = false;
    let finishReturned = false;
    let memoryTransferred = false;
    let outputAttempt: RootTreeResidentAttempt | undefined;
    let outputResumeAttempted = false;
    let outputReady = false;
    let preparedCleanup: PendingRetirement | undefined;
    let result: RootTreeRepairResult | undefined;
    let assertPublishedOwner: (() => void) | undefined;
    const reserveOutput = (peakBytes: number, residentBytes: number): RootTreeResidentAttempt => {
        const attempt = outputAdmission!.reserve(tree, { peakBytes, residentBytes });
        if (attempt) return attempt;
        const snapshot = outputAdmission!.snapshot().ledger;
        throw new RootTreeOutputAdmissionDeniedError(peakBytes, snapshot.capacityBytes, snapshot.usedBytes);
    };
    const prepareCleanup = (needsCancel: boolean): PendingRetirement => {
        if (preparedCleanup) {
            preparedCleanup.needsCancel = needsCancel;
            return preparedCleanup;
        }
        const owner: PendingRetirement = { token: token ?? 0, needsCancel, readyForDrain: false,
            done: false, completed: 0,
            memory, cancel: cancelDeferred!, step: stepRetirement!, cooperate,
            outputRelease: outputAdmission?.releaseRetired, outputToken: undefined };
        // Register the strong native/input owner before finish or detach can
        // consume another capability. Nothing fallible remains between native
        // publication and arming the preallocated output slot below.
        if (pendingRetirements.has(tree)) {
            throw new RootTreeRepairError("INVALID_TREE", "root tree already has an active repair owner");
        }
        pendingRetirements.set(tree, owner);
        preparedCleanup = owner;
        memoryTransferred = true;
        return owner;
    };
    const attachOutputCleanup = (output: RootTreeResidentRetirement) => {
        if (!preparedCleanup || preparedCleanup.outputToken || !preparedCleanup.outputRelease) {
            throw new RootTreeRepairError("INVALID_TREE", "replacement output cleanup owner is invalid");
        }
        preparedCleanup.outputToken = output;
    };
    try {
        check();
        if (claimed) {
            await cooperate(signal); check();
            // The registry allocation happens while no native job exists and
            // immediately before the synchronous begin call. Once begin owns a
            // token, every later cancellation/publication transition reuses
            // this strong slot without another Map insertion.
            if (retirementClaimed) prepareCleanup(true);
            token = begin!.call(tree, preferredVersion, captured.entryCount, serializedBytes);
            beginReturned = true;
            if (preparedCleanup) preparedCleanup.token = token;
            jobOpen = true;
            if (!Number.isInteger(token) || token < 1 || token > 0xffff_ffff) {
                throw new RootTreeRepairError("INVALID_TREE", "invalid replacement rebuild token");
            }
            check();
        }
        let count = 0, secondBytes = 2, previousPath: string | null = null;
        let pageEntries = 0, pageBytes = 2, fedEntries = 0;
        const flush = async () => {
            check();
            fragments.push("]"); json = fragments.join(""); fragments.length = 0;
            const received = append!.call(tree, token!, fedEntries, json);
            json = undefined;
            if (received !== fedEntries + pageEntries) {
                throw new RootTreeRepairError("INVALID_TREE", "replacement rebuild feed progress differs");
            }
            fedEntries = received; pageEntries = 0; pageBytes = 2; fragments.push("[");
            check(); await cooperate(signal); check();
        };
        for (const row of captured.entries()) {
            if (++count > captured.entryCount) throw new RootTreeRepairError("INVALID_BASE", "captured tree entry count differs");
            const line = serializeEntry(row, previousPath);
            previousPath = row.path;
            const lineBytes = utf8Bytes(line);
            secondBytes += lineBytes + (count > 1 ? 1 : 0);
            if (secondBytes > serializedBytes) throw new RootTreeRepairError("INVALID_BASE", "captured tree serialization differs");
            if (claimed) {
                if (lineBytes + 2 > ROOT_TREE_REPAIR_LIMITS.feedBytes) {
                    throw new RootTreeRepairError("LIMIT", "root rebuild row exceeds the page byte bound");
                }
                if (pageEntries > 0 && pageBytes + lineBytes + 1 > ROOT_TREE_REPAIR_LIMITS.feedBytes) await flush();
                if (pageEntries > 0) fragments.push(",");
                pageBytes += lineBytes + (pageEntries > 0 ? 1 : 0); pageEntries++;
            } else if (count > 1) fragments.push(",");
            fragments.push(line);
            if (claimed && pageEntries === ROOT_TREE_REPAIR_LIMITS.feedEntries) await flush();
            else if (!claimed && count % ROOT_TREE_REPAIR_LIMITS.yieldEvery === 0) {
                await cooperate(signal); check();
            }
        }
        if (count !== captured.entryCount || secondBytes !== serializedBytes) {
            throw new RootTreeRepairError("INVALID_BASE", "captured tree serialization differs");
        }
        check();
        if (claimed) {
            if (pageEntries > 0) await flush();
            else { await cooperate(signal); check(); }
            if (fedEntries !== captured.entryCount) throw new RootTreeRepairError("INVALID_BASE", "replacement rebuild input is incomplete");
            check();
            if (buildClaimed) {
                if (preferredVersion === 2 && sortPlanClaimed) {
                    check();
                    const sortPlan = sortMemoryPlanV1(readSortPlan!.call(tree, token!), captured.entryCount);
                    check();
                    startSortBuild!.call(tree, token!, sortPlan.sourceIndexRequestedBytes,
                        sortPlan.targetIndexRequestedBytes);
                } else {
                    startBuild!.call(tree, token!);
                }
                let completed = 0, done = false, outputBarrierHandled = false;
                while (!done) {
                    check();
                    const step = outputPlanClaimed ? stepOutputBuild! : stepBuild!;
                    const progress = buildProgress(step.call(tree, token!, ROOT_TREE_REPAIR_LIMITS.buildUnits), completed);
                    completed = progress.completed; done = progress.done;
                    if (progress.phase === "plan ready") {
                        if (done || outputBarrierHandled ||
                            (preferredVersion === 1 && !outputPlanClaimed) ||
                            (preferredVersion === 2 && !planClaimed)) {
                            throw new RootTreeRepairError("INVALID_TREE", "replacement build paused without its output plan API");
                        }
                        let nodePlan: Readonly<RootTreeNodePayloadPlan> | undefined;
                        if (preferredVersion === 2) {
                            nodePlan = nodePayloadPlan(readPlan!.call(tree, token!));
                        }
                        const outputPlan = outputPlanClaimed
                            ? outputMemoryPlanV1(readOutputPlan!.call(tree, token!), preferredVersion)
                            : undefined;
                        if (nodePlan && outputPlan && outputPlan.nodePayloadBytes !== nodePlan.nodePayloadBytes) {
                            throw new RootTreeRepairError("INVALID_TREE", "replacement output plans disagree");
                        }
                        check();
                        if (residentAdmission !== undefined && outputPlan) {
                            outputAttempt = reserveOutput(
                                outputPlan.peakAdmissionBytes,
                                outputPlan.residentAdmissionBytes,
                            );
                            check();
                        }
                        if (nodePlan) await onNodePayloadPlan?.(nodePlan, signal);
                        check();
                        // Once native resume is invoked, even a thrown result is
                        // conservatively treated as potentially owning output
                        // until deferred cancellation proves otherwise.
                        if (outputPlan) {
                            outputResumeAttempted = true;
                            resumeOutputBuild!.call(tree, token!, outputPlan.nodePayloadBytes,
                                outputPlan.rangeEndpointPeakRequestedBytes,
                                outputPlan.rangeEndpointResidentRequestedBytes);
                        } else {
                            resumeBuild!.call(tree, token!, nodePlan!.nodePayloadBytes);
                        }
                        outputBarrierHandled = true;
                        check();
                    }
                    check(); await cooperate(signal); check();
                }
                if (((preferredVersion === 1 && outputPlanClaimed) ||
                    (preferredVersion === 2 && planClaimed)) && !outputBarrierHandled) {
                    throw new RootTreeRepairError("INVALID_TREE", "replacement build omitted its output plan boundary");
                }
                // The final native step is private too. Even done must cross
                // a real host boundary and revalidate before publishing.
                check();
                if (residentAdmission !== undefined) {
                    if (!outputAttempt || !outputResumeAttempted) {
                        throw new RootTreeRepairError("INVALID_TREE", "replacement output admission was not retained");
                    }
                    outputAdmission!.ready(outputAttempt);
                    outputReady = true;
                }
            }
            if (retirementClaimed) {
                // Deferred finish retains the original job on failure. On
                // success the same token owns retirement of the old graph.
                const cleanup = prepareCleanup(true);
                finishDeferred!.call(tree, token!);
                finishReturned = true;
                jobOpen = false;
                cleanup.needsCancel = false;
                if (outputAttempt) {
                    const retiringOutput = outputAdmission!.publish(outputAttempt);
                    outputAttempt = undefined;
                    attachOutputCleanup(retiringOutput);
                }
                cleanup.readyForDrain = true;
            } else {
                // The older ready finish consumes on success OR failure.
                jobOpen = false;
                finish!.call(tree, token!);
            }
        } else {
            fragments.push("]");
            // Compatibility path retains its admitted complete JSON input.
            json = fragments.join(""); fragments.length = 0;
            check(); tree.rebuild_from_entries_in_version(preferredVersion, json);
        }
        // No fake cancellation race around the native call. Its real stack has
        // returned now. Reentrant host/test ports must not authorize a stale
        // result if they changed the base during that call.
        // Publication legitimately changes the revisions. Capture a NEW
        // witness, never compare the published tree with the pre-build cut.
        assertPublishedOwner = captureTreeOwner(tree);
        assertCurrent(captured, signal);
        assertScope?.();
        const rebuiltRoot = tree.root_hash_hex();
        if (typeof rebuiltRoot !== "string" || !HASH.test(rebuiltRoot) ||
            tree.tree_version() !== preferredVersion || tree.total_files() !== captured.entryCount) {
            throw new RootTreeRepairError("INVALID_TREE", "native root tree rebuild returned an invalid graph summary");
        }
        assertCurrent(captured, signal);
        assertScope?.();
        assertCurrent(captured, signal);
        result = { rebuiltRoot, capturedBaseRoot: captured.treeBaseRoot, entryCount: captured.entryCount, serializedBytes };
    } catch (error) {
        primary = error; throw error;
    } finally {
        fragments.length = 0;
        json = undefined;
        try {
            if (preparedCleanup && !beginReturned) {
                // begin threw before returning an owned token. Its contract is
                // consuming on error, so the inactive pre-registered slot can
                // be removed without invoking native retirement.
                if (pendingRetirements.get(tree) === preparedCleanup) pendingRetirements.delete(tree);
                preparedCleanup = undefined;
                memoryTransferred = false;
            }
            if (jobOpen) {
                if (retirementClaimed) {
                    const cleanup = prepareCleanup(true);
                    if (outputAttempt) {
                        if (outputResumeAttempted || outputReady) {
                            attachOutputCleanup(outputAdmission!.detachPrivate(outputAttempt));
                        } else {
                            // No covered native output could have been created:
                            // release the reservation before unrelated input/
                            // planning owners finish their deferred cleanup.
                            outputAdmission!.releaseBeforeOutput(outputAttempt);
                        }
                        outputAttempt = undefined;
                    }
                    cleanup.readyForDrain = true;
                } else {
                    cancel!.call(tree, token!);
                }
            } else if (finishReturned && preparedCleanup && outputAttempt) {
                // A host ownership transition unexpectedly threw after native
                // finish returned. Retry the deterministic preallocated move;
                // if it still fails the unarmed strong slot remains quarantined.
                const retiringOutput = outputAdmission!.publish(outputAttempt);
                outputAttempt = undefined;
                attachOutputCleanup(retiringOutput);
                preparedCleanup.readyForDrain = true;
            } else if (outputAttempt) {
                // Native begin failed before it returned an owned token.
                outputAdmission!.releaseBeforeOutput(outputAttempt);
                outputAttempt = undefined;
            }
            if (memoryTransferred) await drainRootTreeRetirement(tree);
        } catch (error) {
            if (error instanceof RootTreeRetirementError) {
                throw new RootTreeRetirementError(error.cleanupCause, primary);
            }
            if (primary === undefined) throw error;
            console.warn("[obsetync] replacement rebuild cleanup failed", error);
        } finally { if (!memoryTransferred) memory.release(); }
    }
    // Retirement may have crossed many host turns. Cleanup above must finish
    // unconditionally, but its completion alone does not authorize the caller
    // to update base/cache metadata from a now-stale publication result.
    // Rejection here never rolls back the already installed committed graph.
    assertCurrent(captured, signal);
    assertScope?.();
    assertPublishedOwner!();
    assertCurrent(captured, signal);
    if (preferredVersion === 2 && residentAdmission) {
        // Optional settlement provenance is installed only after native
        // publication, old-owner retirement and every final base/scope/wrapper
        // fence. Failure keeps the conservative charge and cannot invalidate
        // an otherwise successful admitted replacement.
        try {
            const committed = tree.committed_revision?.();
            const candidate = tree.candidate_revision?.();
            if (Number.isSafeInteger(committed) && committed! >= 0 &&
                Number.isSafeInteger(candidate) && candidate! >= 0) {
                residentAdmission.markV2GraphComplete(tree, committed!, candidate!);
            }
        } catch { /* Settlement is optional; replacement ownership is not. */ }
    }
    // Revision getters and the optional admission marker above are observable
    // host boundaries. Re-run the same final ownership/scope fences so a
    // reentrant stop, base change or wrapper replacement cannot turn their
    // stale publication result into a reported success.
    assertCurrent(captured, signal);
    assertScope?.();
    assertPublishedOwner!();
    assertCurrent(captured, signal);
    return result!;
}
