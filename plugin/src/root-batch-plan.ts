import type { DirtyFileChange } from "./dirty-set";
import type { FileChange } from "./push";
import { throwIfWorkAborted } from "./work-scheduler";
import type { SyncMemoryArbiter, SyncMemoryLease } from "./sync-memory-arbiter";
import {
    ROOT_BATCH_LIMITS, RootBatchError, buildRootBatchGraphSteps, emptyRootBatchDeferrals,
    resolveRootBatchLimits, rootBatchComponentHold, rootBatchPathBytesSteps,
    type RootBatchDependency, type RootBatchDeferrals, type RootBatchLimits, type ResolvedRootBatchLimits,
} from "./root-batch";

/** Deterministic cache workset model, not a vault/protocol limit or an RSS proof.
 * Caller-owned input arrays are outside this accounting. Fixed row charges are
 * experimental deterministic weights for graph/index/witness/reference work,
 * NOT measured conservative JS heap bounds. Strings are charged as UTF16 plus
 * exact escaped UTF8; output workspace has a separate model charge. Map/Set/
 * object/string/GC overhead is unmeasured. When a shared memory arbiter is
 * injected, the full configured model is reserved before graph allocation and
 * held until dispose(). No file contents are copied or admitted by this API. */
export const ROOT_BATCH_PLAN_ADMISSION = {
    maxPaths: 65_536,
    maxDependencies: 65_536,
    maxMetadataBytes: 16 * 1024 * 1024,
    queuedRowBytes: 352,
    readyRowBytes: 128,
    dependencyBytes: 256,
    workspaceBytes: ROOT_BATCH_LIMITS.metadataBytes + 256 * 512 + 64 * 1024,
    cooperationUnits: 256,
} as const;
export interface RootBatchPlanAdmission {
    maxPaths?: number;
    maxDependencies?: number;
    maxMetadataBytes?: number;
}
export interface RootBatchPlanOptions {
    /** MUST yield to the actual host. Promise.resolve alone is not UI fairness. */
    cooperate: () => Promise<void>;
    signal?: AbortSignal;
    limits?: RootBatchLimits;
    /** May lower, never raise, the experimental cache ceilings. */
    admission?: RootBatchPlanAdmission;
    /** Phase-A injection seam. Omission preserves the legacy unwired caller. */
    memoryArbiter?: SyncMemoryArbiter;
    /** Nested owners must use false to avoid waiting on their own parent lease. */
    memoryWait?: boolean;
}
export interface RootPlannedBatch {
    ready: FileChange[];
    /** Original immutable detached hint objects, retaining owner-local proof. */
    queued: DirtyFileChange[];
    selectedGroups: readonly (readonly string[])[];
    estimatedMetadataBytes: number;
    cutCount: number;
    /** Intrinsic holds plus eligible groups scheduled for later batches. */
    deferred: RootBatchDeferrals;
}
export interface RootBatchPlanSnapshot {
    disposed: boolean;
    queuedPaths: number;
    remainingEligiblePaths: number;
    remainingEligibleComponents: number;
    heldPaths: number;
    deferred: RootBatchDeferrals;
    /** Deterministic charge-model result, not allocated heap/RSS/reservation. */
    admittedMetadataBytes: number;
    /** Mandatory queued/edge preflight model, before actual ready overrides. */
    preflightMetadataBytes: number;
    /** Full graph peak model, including overrides and compaction workspace. */
    buildPeakMetadataBytes: number;
    /** Current compact records + workspace; no live union graph/edge arrays.
     * Experimental weights, not an actual memory reservation or heap reading. */
    retainedMetadataBytes: number;
    sharedReadyRows: number;
    overrideReadyRows: number;
    preflightRows: number;
    graphSteps: number;
    graphNodes: number;
    graphComponents: number;
    queuedRows: number;
    readyRows: number;
    dependencyRows: number;
    cooperationCalls: number;
    nextCalls: number;
    selectionComponentVisits: number;
    selectedPaths: number;
}
export interface RootBatchPlan {
    /** Null means no selectable component remains, NOT synced or ACKed. */
    next(): RootPlannedBatch | null;
    snapshot(): RootBatchPlanSnapshot;
    /** Drop unselected references. Does not consume, restore or ACK dirty work. */
    dispose(): void;
}
export class RootBatchPlanError extends Error {
    constructor(readonly code: "ROOT_BATCH_PLAN_ADMISSION" | "ROOT_BATCH_PLAN_CHANGED" | "ROOT_BATCH_PLAN_DISPOSED") {
        super(code === "ROOT_BATCH_PLAN_ADMISSION" ? "root batch plan metadata admission rejected" :
            code === "ROOT_BATCH_PLAN_CHANGED" ? "root batch plan immutable input changed" : "root batch plan is disposed");
        this.name = "RootBatchPlanError";
    }
}
const admissionFailure = (): never => { throw new RootBatchPlanError("ROOT_BATCH_PLAN_ADMISSION"); };
const changed = (): never => { throw new RootBatchPlanError("ROOT_BATCH_PLAN_CHANGED"); };
function ceiling(value: number | undefined, maximum: number): number {
    if (value === undefined) return maximum;
    if (!Number.isSafeInteger(value) || value < 1 || value > maximum) admissionFailure();
    return value;
}
function copyDeferrals(value: RootBatchDeferrals): RootBatchDeferrals {
    return { "missing-peer": { ...value["missing-peer"] }, "unresolved-dependency": { ...value["unresolved-dependency"] },
        "stale-capture": { ...value["stale-capture"] }, "component-limit": { ...value["component-limit"] },
        "transaction-limit": { ...value["transaction-limit"] } };
}
interface PlannedEntry {
    queued: DirtyFileChange;
    witness: DirtyFileChange;
    ready: FileChange;
    queuedOrder: number;
    readyOrder: number;
}
interface PlannedComponent {
    paths: number;
    cuts: number;
    bytes: number;
    metadataBytes: number;
    /** One membership array; graph ready/queued adjacency arrays are dropped. */
    members: PlannedEntry[];
}
function assertHint(node: PlannedEntry): void {
    const hint = node.queued!, witness = node.witness!;
    if ("data" in hint || hint.path !== witness.path || hint.action !== witness.action ||
        hint.journalId !== witness.journalId || hint.hash !== witness.hash ||
        !Object.is(hint.mtime, witness.mtime) || !Object.is(hint.size, witness.size)) changed();
}
function projectReady(entry: PlannedEntry): FileChange {
    const row = entry.ready;
    // A shared witness has a journalId. It must never leak into FileChange
    // outputs; hashes remain exactly the original captured ready hashes.
    return { action: row.action, path: entry.witness.path,
        ...(row.hash === undefined ? {} : { hash: row.hash }),
        ...(row.mtime === undefined ? {} : { mtime: row.mtime }),
        ...(row.size === undefined ? {} : { size: row.size }) };
}

class AdmittedRootBatchPlan implements RootBatchPlan {
    private cursor = 0;
    private disposed = false;
    constructor(private readonly components: Array<PlannedComponent | undefined>,
        private readonly limits: ResolvedRootBatchLimits,
        private readonly holds: RootBatchDeferrals,
        private readonly state: Omit<RootBatchPlanSnapshot, "disposed" | "deferred">,
        private readonly signal?: AbortSignal,
        private memoryLease?: SyncMemoryLease) {}

    next(): RootPlannedBatch | null {
        if (this.disposed) throw new RootBatchPlanError("ROOT_BATCH_PLAN_DISPOSED");
        throwIfWorkAborted(this.signal);
        this.state.nextCalls++;
        const chosen: PlannedComponent[] = [];
        let paths = 0, cuts = 0, bytes = 0;
        let next = this.cursor;
        // Prefix packing: at most <=256 included components plus one boundary
        // peek. A group which fits alone but not this tail starts the NEXT
        // batch; it is not consumed, skipped forever, or split to fill a hole.
        while (next < this.components.length) {
            const component = this.components[next]!;
            this.state.selectionComponentVisits++;
            if (paths + component.paths > this.limits.maxPaths || cuts + component.cuts > this.limits.maxCuts ||
                bytes + component.bytes + ROOT_BATCH_LIMITS.headerBytes > this.limits.maxMetadataBytes) break;
            chosen.push(component); paths += component.paths; cuts += component.cuts; bytes += component.bytes; next++;
        }
        if (!chosen.length) return null;
        const ready: PlannedEntry[] = [], queued: PlannedEntry[] = [];
        for (const component of chosen) {
            for (const node of component.members) { assertHint(node); queued.push(node); ready.push(node); }
        }
        // Validate the entire <=256 output before consuming a single group.
        throwIfWorkAborted(this.signal);
        ready.sort((left, right) => left.readyOrder - right.readyOrder);
        queued.sort((left, right) => left.queuedOrder - right.queuedOrder);
        const groups = chosen.filter(component => component.paths > 1)
            .map(component => Object.freeze(component.members.map(node => node.witness.path)));
        const result = { ready: ready.map(projectReady), queued: queued.map(node => node.queued),
            selectedGroups: Object.freeze(groups), estimatedMetadataBytes: bytes + ROOT_BATCH_LIMITS.headerBytes,
            cutCount: cuts, deferred: copyDeferrals(this.holds) };
        for (; this.cursor < next; this.cursor++) this.components[this.cursor] = undefined;
        this.state.remainingEligibleComponents -= chosen.length;
        this.state.remainingEligiblePaths -= paths;
        this.state.selectedPaths += paths;
        for (const component of chosen) this.state.retainedMetadataBytes -= component.metadataBytes;
        result.deferred["transaction-limit"] = { components: this.state.remainingEligibleComponents,
            paths: this.state.remainingEligiblePaths };
        return result;
    }
    snapshot(): RootBatchPlanSnapshot {
        const deferred = copyDeferrals(this.holds);
        deferred["transaction-limit"] = { components: this.state.remainingEligibleComponents, paths: this.state.remainingEligiblePaths };
        return { ...this.state, disposed: this.disposed, deferred };
    }
    dispose(): void {
        if (this.disposed) return;
        this.disposed = true; this.components.length = 0; this.state.retainedMetadataBytes = 0;
        this.memoryLease?.release(); this.memoryLease = undefined;
    }
}

/** Prerequisite planner only; NOT engine review-cache activation.
 *
 * ALL arrays, queued hints and dependency rows must describe ONE already fully
 * materialized/reviewed immutable epoch and remain immutable during creation
 * and use. Outputs consume scheduling positions only: caller still owns dirty
 * claims, current generations/dependencies, whole-queue cooldown/deletion
 * policy, applicability checks, source reads, retry/recovery and exact ACKs.
 * A new live event/rename/policy/base change requires caller invalidation; this
 * object never observes those mutations or grants stale review authority.
 *
 * Existing selectRootBatch remains greedy-skip. This plan skips intrinsic held
 * components ONCE while building, then packs strict complete-component prefixes
 * per next(). Small unused holes are deliberate bounded-work behavior.
 *
 * Preflight checks every input plus mandatory queued/edge charges before graph
 * copying, yielding by bounded work units. Ready rows intern exact matching
 * queued witnesses; only actual scalar overrides are copied/charged before
 * retention. All paths and equal hashes use existing captured canonical values.
 * The same weights and 16 MiB cap apply, including bounded projection/compaction
 * workspace. Finished union graph and parallel membership arrays are dropped;
 * the returned plan holds compact component membership, not the build graph.
 * A second charge pass detects model growth observed at each visit. It cannot bound arbitrary later mutations of
 * already-retained caller objects: the immutable-input contract is mandatory.
 * Selected hint scalar witnesses fail closed without inventing/restoring durable provenance.
 * No partial plan escapes errors/abort; caller inputs/WAL/dirty state untouched. */
export async function createRootBatchPlan(
    ready: readonly FileChange[], queued: readonly DirtyFileChange[], dependencies: readonly RootBatchDependency[],
    options: RootBatchPlanOptions,
): Promise<RootBatchPlan> {
    const { cooperate, signal } = options;
    throwIfWorkAborted(signal);
    if (typeof cooperate !== "function") throw new RootBatchError("root batch plan requires host cooperation");
    if (!Array.isArray(ready) || !Array.isArray(queued) || !Array.isArray(dependencies)) throw new RootBatchError("invalid root batch collections");
    const limits = resolveRootBatchLimits(options.limits);
    const admission = { maxPaths: ceiling(options.admission?.maxPaths, ROOT_BATCH_PLAN_ADMISSION.maxPaths),
        maxDependencies: ceiling(options.admission?.maxDependencies, ROOT_BATCH_PLAN_ADMISSION.maxDependencies),
        maxMetadataBytes: ceiling(options.admission?.maxMetadataBytes, ROOT_BATCH_PLAN_ADMISSION.maxMetadataBytes) };
    const lengths = { ready: ready.length, queued: queued.length, dependencies: dependencies.length };
    if (lengths.ready > admission.maxPaths || lengths.queued > admission.maxPaths || lengths.dependencies > admission.maxDependencies) admissionFailure();
    let memoryLease: SyncMemoryLease | undefined;
    try {
        if (options.memoryArbiter) {
            memoryLease = await options.memoryArbiter.reserve("root-plan", admission.maxMetadataBytes,
                { signal, wait: options.memoryWait });
            throwIfWorkAborted(signal);
        }
    const state = { queuedPaths: queued.length, remainingEligiblePaths: 0, remainingEligibleComponents: 0, heldPaths: 0,
        admittedMetadataBytes: 0, preflightMetadataBytes: 0, buildPeakMetadataBytes: 0,
        retainedMetadataBytes: ROOT_BATCH_PLAN_ADMISSION.workspaceBytes, sharedReadyRows: 0, overrideReadyRows: 0,
        preflightRows: 0, graphSteps: 0, graphNodes: 0, graphComponents: 0,
        queuedRows: 0, readyRows: 0, dependencyRows: 0, cooperationCalls: 0, nextCalls: 0, selectionComponentVisits: 0, selectedPaths: 0 };
    const assertLengths = () => {
        if (ready.length !== lengths.ready || queued.length !== lengths.queued || dependencies.length !== lengths.dependencies) changed();
    };
    let used = ROOT_BATCH_PLAN_ADMISSION.workspaceBytes, units = 0;
    if (used > admission.maxMetadataBytes) admissionFailure();
    const charge = (bytes: number) => {
        if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > admission.maxMetadataBytes - used) admissionFailure();
        used += bytes;
    };
    const inspectPath = (path: unknown): string => {
        // Enforce the existing vault path length limit before validation's
        // character arrays or any UTF8/JSON encoding could allocate a large
        // temporary. The scanner itself creates no whole-string byte buffer.
        if (typeof path !== "string") throw new RootBatchError("invalid root batch path");
        if (path.length * 2 > admission.maxMetadataBytes) admissionFailure();
        if (path.length > 4096) throw new RootBatchError("invalid root batch path");
        units += Math.max(1, Math.ceil(path.length / 256));
        return path;
    };
    const pathModelBytes = (value: unknown): number => {
        const path = inspectPath(value);
        const scan = rootBatchPathBytesSteps(path);
        let step = scan.next(); while (!step.done) step = scan.next();
        return path.length * 2 + step.value;
    };
    const inspectHash = (value: FileChange) => {
        if (value.hash !== undefined && (typeof value.hash !== "string" || value.hash.length !== 64)) {
            throw new RootBatchError("invalid root batch hash hint");
        }
    };
    const chargeRow = (kind: "queued" | "ready", value: FileChange) => {
        if (kind === "ready") {
            // No second ready row is retained until the shared builder proves
            // that it differs from its queued witness. Still inspect the full
            // input shape's bounded strings before graph copying.
            inspectPath(value?.path); inspectHash(value);
        } else {
            charge(ROOT_BATCH_PLAN_ADMISSION.queuedRowBytes);
            charge(pathModelBytes(value?.path)); inspectHash(value);
            if (value.hash !== undefined) charge(128);
        }
    };
    const chargeDependency = (edge: RootBatchDependency) => {
        charge(ROOT_BATCH_PLAN_ADMISSION.dependencyBytes); charge(pathModelBytes(edge?.left)); charge(pathModelBytes(edge?.right));
    };
    const checkpoint = (): Promise<void> | undefined => {
        throwIfWorkAborted(signal); assertLengths();
        if (units < ROOT_BATCH_PLAN_ADMISSION.cooperationUnits) return;
        units = 0; state.cooperationCalls++;
        return (async () => { await cooperate(); throwIfWorkAborted(signal); assertLengths(); })();
    };
    for (let index = 0; index < lengths.queued; index++) {
        const row = queued[index];
        throwIfWorkAborted(signal); chargeRow("queued", row); state.preflightRows++;
        const wait = checkpoint(); if (wait) await wait;
    }
    for (let index = 0; index < lengths.ready; index++) {
        const row = ready[index];
        throwIfWorkAborted(signal); chargeRow("ready", row); state.preflightRows++;
        const wait = checkpoint(); if (wait) await wait;
    }
    for (let index = 0; index < lengths.dependencies; index++) {
        const edge = dependencies[index];
        throwIfWorkAborted(signal); chargeDependency(edge); state.preflightRows++;
        const wait = checkpoint(); if (wait) await wait;
    }
    state.preflightMetadataBytes = used;
    state.admittedMetadataBytes = used;
    used = ROOT_BATCH_PLAN_ADMISSION.workspaceBytes; units = 0;
    const builder = buildRootBatchGraphSteps(ready, queued, dependencies, {
        beforeRow: chargeRow,
        beforeDependency: chargeDependency,
        beforeNode: count => { if (count > admission.maxPaths) admissionFailure(); },
        internReady: true,
        beforeReadyCapture: (change, witness, shared) => {
            if (shared) state.sharedReadyRows++;
            else {
                charge(ROOT_BATCH_PLAN_ADMISSION.readyRowBytes);
                if (change.hash !== undefined && change.hash !== witness.hash) charge(128);
                state.overrideReadyRows++;
            }
        },
    });
    let step;
    for (;;) {
        throwIfWorkAborted(signal); assertLengths();
        step = builder.next(); if (step.done) break;
        state.graphSteps++;
        if (step.value === "queued") state.queuedRows++;
        else if (step.value === "ready") state.readyRows++;
        else if (step.value === "dependency") state.dependencyRows++;
        units += step.value === "string" ? ROOT_BATCH_PLAN_ADMISSION.cooperationUnits : 1;
        const wait = checkpoint(); if (wait) await wait;
    }
    state.admittedMetadataBytes = Math.max(state.admittedMetadataBytes, used);
    state.buildPeakMetadataBytes = used;
    const graph = step.value;
    state.graphNodes = graph.nodes.length; state.graphComponents = graph.components.length;
    // Component lists are sufficient for compact projection. Drop the full
    // parallel node/order arrays before copying; each eligible <=256-member
    // component overlaps with only its own bounded projection workspace.
    graph.nodes = []; graph.ready = []; graph.queued = [];
    const buildingComponents = graph.components as Array<(typeof graph.components)[number] | undefined>;
    graph.components = [];
    const holds = emptyRootBatchDeferrals(), eligible: PlannedComponent[] = [];
    for (let index = 0; index < buildingComponents.length; index++) {
        const component = buildingComponents[index]!;
        throwIfWorkAborted(signal);
        const reason = rootBatchComponentHold(component, limits);
        if (component.paths > 0) {
            if (reason) { holds[reason].components++; holds[reason].paths += component.paths; state.heldPaths += component.paths; }
            else {
                const members: PlannedEntry[] = [];
                let metadataBytes = 0;
                for (const node of component.ready) {
                    const witness = node.witness!, ready = node.ready!;
                    metadataBytes += ROOT_BATCH_PLAN_ADMISSION.queuedRowBytes + pathModelBytes(witness.path) +
                        (witness.hash === undefined ? 0 : 128);
                    if (ready !== witness) metadataBytes += ROOT_BATCH_PLAN_ADMISSION.readyRowBytes +
                        (ready.hash !== undefined && ready.hash !== witness.hash ? 128 : 0);
                    members.push({ queued: node.queued!, witness, ready, queuedOrder: node.queuedOrder, readyOrder: node.readyOrder });
                    const wait = checkpoint(); if (wait) await wait;
                }
                eligible.push({ paths: component.paths, cuts: component.cuts, bytes: component.bytes, metadataBytes, members });
                state.retainedMetadataBytes += metadataBytes;
                state.remainingEligiblePaths += component.paths; state.remainingEligibleComponents++;
            }
        }
        // The compact plan never keeps union parents, node/component cycles,
        // or a second ready/queued membership array alive.
        component.ready = []; component.queued = [];
        buildingComponents[index] = undefined;
        units++; const wait = checkpoint(); if (wait) await wait;
    }
    buildingComponents.length = 0;
    throwIfWorkAborted(signal); assertLengths();
    const plan = new AdmittedRootBatchPlan(eligible, limits, holds, state, signal, memoryLease);
    memoryLease = undefined;
    return plan;
    } catch (error) {
        memoryLease?.release();
        throw error;
    }
}
