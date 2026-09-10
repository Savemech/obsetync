import type { TreeJobProgress } from "./tree-candidate-job";
import { throwIfWorkAborted, yieldWork } from "./work-scheduler";

export type TreeCandidateMutationKind = "update" | "delete";

/** Keep one native primitive below the ordinary renderer's 4ms target. The
 * host may amortize multiple fast primitives only while its clock proves the
 * same cooperative CPU slice is still current. */
export const TREE_CANDIDATE_MUTATION_STEP_UNITS = 64;

export interface CandidateMutationJobTree {
    has_candidate(): boolean;
    tree_version?: () => number;
    candidate_revision?: () => number;
    abort_candidate?: () => unknown;
    begin_candidate_update_job?: (payloadJson: string) => number;
    begin_candidate_delete_job?: (payloadJson: string) => number;
    step_tree_job?: (token: number, maxUnits: number) => unknown;
    finish_candidate_mutation_job?: (token: number) => void;
    cancel_tree_job?: (token: number) => void;
    step_candidate_mutation_output_memory_v1_job?: (token: number, maxUnits: number) => unknown;
    candidate_mutation_output_memory_plan_v1_job?: (token: number) => unknown;
    resume_candidate_mutation_output_memory_v1_job?: (token: number, nodePayload: number,
        endpointPeak: number, endpointResident: number) => void;
    candidate_mutation_output_memory_ready_v1_job?: (token: number) => unknown;
    finish_candidate_mutation_job_deferred?: (token: number) => void;
    cancel_candidate_mutation_job_deferred?: (token: number) => void;
    step_tree_retirement?: (token: number, maxUnits: number) => unknown;
}

export interface CandidateMutationOutputMemoryPlan {
    schema: 1;
    scope: "v1-candidate-mutation-output" | "v2-candidate-mutation-output";
    nodePayloadBytes: number;
    rangeEndpointPeakRequestedBytes: number;
    rangeEndpointResidentRequestedBytes: number;
    peakAdmissionBytes: number;
    residentAdmissionBytes: number;
}

export interface CandidateMutationOutputMemoryReady {
    schema: 1;
    scope: CandidateMutationOutputMemoryPlan["scope"];
    stagedNodePayloadBytes: number;
    rangeEndpointResidentRequestedBytes: number;
    residentAdmissionBytes: number;
}

/** Synchronous ownership transitions, not additional global reservations.
 * A throwing transition is retained for explicit cleanup retry and therefore
 * must be idempotent for this exact owner. No callback may return a Promise. */
export interface CandidateMutationOutputOwner {
    releaseBeforeOutput(): void;
    ready(plan: CandidateMutationOutputMemoryReady): void;
    published(): void;
    detached(): void;
    retired(): void;
}

export interface ApplyTreeCandidateMutationOptions {
    /** Must yield to the real host, including the caller's heavy-work gate. */
    cooperate(): Promise<void>;
    /** Mandatory cleanup turn: ignores operation cancellation, visibility and
     * normal work admission. Defaults to the shared scheduler without signal. */
    cooperateRetirement?: () => Promise<void>;
    signal?: AbortSignal;
    /** Revalidates the caller's captured tree/source/policy ownership. */
    assertCurrent?(): void;
    /** One synchronous native compatibility call, with the same kind/payload. */
    legacy(): void;
    /** Synchronous ownership handoff after native state changed and before any
     * post-mutation guard can reject. Must record the caller's new witness. */
    onCandidateMutated?(): void;
    /** Synchronous fail-fast policy before native output allocation. Absent
     * policy measures/validates the ABI but does not claim memory admission.
     * Older/absent output APIs may not bypass this policy. */
    onOutputMemoryPlan?(plan: CandidateMutationOutputMemoryPlan): CandidateMutationOutputOwner | undefined;
    /** Monotonic test seam. Production uses performance.now()/Date.now(). */
    now?: () => number;
    /** Refuse the synchronous candidate mutation compatibility path. */
    requireIncremental?: boolean;
}

const RETIREMENT_UNITS = 256;
/** A native step remains independently bounded to 64 units. Fast steps may
 * share one host turn, but the driver returns control after this CPU budget. */
export const TREE_CANDIDATE_MUTATION_CPU_SLICE_MS = 4;
export const TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN = 32;
const OUTPUT_SCOPES = ["v1-candidate-mutation-output", "v2-candidate-mutation-output"] as const;

function defaultMonotonicClock(): () => number {
    try {
        const perf = globalThis.performance;
        if (typeof perf?.now === "function") return perf.now.bind(perf);
    } catch { /* Fall back once; never mix incomparable clock epochs. */ }
    return Date.now;
}

function monotonicClock(now: () => number): () => number | undefined {
    let last: number | undefined;
    return () => {
        let value: number;
        try { value = now(); } catch { return undefined; }
        if (typeof value !== "number" || !Number.isFinite(value) || value < 0 ||
            (last !== undefined && value < last)) return undefined;
        last = value;
        return value;
    };
}

interface CandidateMutationCpuSlice {
    startedAt: number | undefined;
    lastObservedAt: number | undefined;
}

function startCpuSlice(now: () => number | undefined): CandidateMutationCpuSlice {
    const startedAt = now();
    return { startedAt, lastObservedAt: startedAt };
}

/** A repeated, invalid, throwing or backwards sample is not evidence that the
 * renderer still has time. Fail closed to one native step for that turn; a
 * valid sample after the next real host turn may recover batching. */
function cpuSliceExhausted(slice: CandidateMutationCpuSlice,
    now: () => number | undefined): boolean {
    const observedAt = now();
    if (slice.startedAt === undefined || slice.lastObservedAt === undefined ||
        observedAt === undefined || observedAt <= slice.lastObservedAt) return true;
    slice.lastObservedAt = observedAt;
    return observedAt - slice.startedAt >= TREE_CANDIDATE_MUTATION_CPU_SLICE_MS;
}

function exactRecord(value: unknown, fields: readonly string[], label: string): Record<string, unknown> {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        ![Object.prototype, null].includes(Object.getPrototypeOf(value))) {
        throw new TypeError(`invalid candidate mutation ${label}`);
    }
    const keys = Reflect.ownKeys(value);
    if (keys.length !== fields.length || !fields.every(field => keys.includes(field))) {
        throw new TypeError(`invalid candidate mutation ${label}`);
    }
    const row: Record<string, unknown> = {};
    for (const field of fields) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor)) throw new TypeError(`invalid candidate mutation ${label}`);
        row[field] = descriptor.value;
    }
    return row;
}

function count(value: unknown, label: string): number {
    if (!Number.isSafeInteger(value) || (value as number) < 0) {
        throw new TypeError(`invalid candidate mutation ${label}`);
    }
    return value as number;
}

function sum(a: number, b: number): number {
    return count(a + b, "memory sum");
}

export function parseCandidateMutationOutputMemoryPlan(value: unknown): CandidateMutationOutputMemoryPlan {
    const fields = ["schema", "scope", "nodePayloadBytes", "rangeEndpointPeakRequestedBytes",
        "rangeEndpointResidentRequestedBytes", "peakAdmissionBytes", "residentAdmissionBytes"] as const;
    const row = exactRecord(value, fields, "output plan");
    if (row.schema !== 1 || !OUTPUT_SCOPES.includes(row.scope as typeof OUTPUT_SCOPES[number])) {
        throw new TypeError("invalid candidate mutation output plan");
    }
    const node = count(row.nodePayloadBytes, "output plan");
    const peak = count(row.rangeEndpointPeakRequestedBytes, "output plan");
    const resident = count(row.rangeEndpointResidentRequestedBytes, "output plan");
    if (resident > peak || count(row.peakAdmissionBytes, "output plan") !== sum(node, peak) ||
        count(row.residentAdmissionBytes, "output plan") !== sum(node, resident) ||
        (row.residentAdmissionBytes as number) > (row.peakAdmissionBytes as number)) {
        throw new TypeError("inconsistent candidate mutation output plan");
    }
    return Object.freeze({ schema: 1, scope: row.scope as CandidateMutationOutputMemoryPlan["scope"], nodePayloadBytes: node,
        rangeEndpointPeakRequestedBytes: peak, rangeEndpointResidentRequestedBytes: resident,
        peakAdmissionBytes: row.peakAdmissionBytes as number, residentAdmissionBytes: row.residentAdmissionBytes as number });
}

export function parseCandidateMutationOutputMemoryReady(value: unknown,
    plan: CandidateMutationOutputMemoryPlan): CandidateMutationOutputMemoryReady {
    const row = exactRecord(value, ["schema", "scope", "stagedNodePayloadBytes",
        "rangeEndpointResidentRequestedBytes", "residentAdmissionBytes"], "output ready");
    const staged = count(row.stagedNodePayloadBytes, "output ready");
    const endpoint = count(row.rangeEndpointResidentRequestedBytes, "output ready");
    const resident = count(row.residentAdmissionBytes, "output ready");
    const endpointMatches = plan.scope === "v1-candidate-mutation-output"
        ? endpoint <= plan.rangeEndpointResidentRequestedBytes
        : endpoint === plan.rangeEndpointResidentRequestedBytes;
    if (row.schema !== 1 || row.scope !== plan.scope || staged > plan.nodePayloadBytes ||
        !endpointMatches || resident !== sum(staged, endpoint) ||
        resident > plan.residentAdmissionBytes) throw new TypeError("inconsistent candidate mutation output ready");
    return Object.freeze({ schema: 1, scope: plan.scope, stagedNodePayloadBytes: staged,
        rangeEndpointResidentRequestedBytes: endpoint, residentAdmissionBytes: resident });
}

interface OutputApi {
    version: number;
    step: NonNullable<CandidateMutationJobTree["step_candidate_mutation_output_memory_v1_job"]>;
    plan: NonNullable<CandidateMutationJobTree["candidate_mutation_output_memory_plan_v1_job"]>;
    resume: NonNullable<CandidateMutationJobTree["resume_candidate_mutation_output_memory_v1_job"]>;
    ready: NonNullable<CandidateMutationJobTree["candidate_mutation_output_memory_ready_v1_job"]>;
    finish: NonNullable<CandidateMutationJobTree["finish_candidate_mutation_job_deferred"]>;
    cancel: NonNullable<CandidateMutationJobTree["cancel_candidate_mutation_job_deferred"]>;
    retire: NonNullable<CandidateMutationJobTree["step_tree_retirement"]>;
    hasCandidate: CandidateMutationJobTree["has_candidate"];
    revision: NonNullable<CandidateMutationJobTree["candidate_revision"]>;
    abort: NonNullable<CandidateMutationJobTree["abort_candidate"]>;
}

function outputApi(tree: CandidateMutationJobTree): OutputApi | null {
    const step = tree.step_candidate_mutation_output_memory_v1_job;
    const plan = tree.candidate_mutation_output_memory_plan_v1_job;
    const resume = tree.resume_candidate_mutation_output_memory_v1_job;
    const ready = tree.candidate_mutation_output_memory_ready_v1_job;
    const finish = tree.finish_candidate_mutation_job_deferred;
    const cancel = tree.cancel_candidate_mutation_job_deferred;
    const specific = [step, plan, resume, ready, finish, cancel];
    // The retirement export also exists for replacement jobs; alone it does
    // not claim this all-or-nothing candidate family.
    if (!specific.some(member => member !== undefined)) return null;
    const retire = tree.step_tree_retirement, version = tree.tree_version;
    const hasCandidate = tree.has_candidate, revision = tree.candidate_revision, abort = tree.abort_candidate;
    if (![...specific, retire, version, hasCandidate, revision, abort].every(member => typeof member === "function")) {
        throw new TypeError("WASM tree exposes an incomplete candidate mutation output API");
    }
    const actualVersion = version!.call(tree);
    if (actualVersion !== 1 && actualVersion !== 2) throw new TypeError("invalid candidate mutation tree version");
    return { version: actualVersion, step: step!, plan: plan!, resume: resume!, ready: ready!,
        finish: finish!, cancel: cancel!, retire: retire!, hasCandidate, revision: revision!, abort: abort! };
}

export class TreeCandidateMutationRetirementError extends Error {
    readonly code = "TREE_CANDIDATE_MUTATION_RETIREMENT_FAILED";
    constructor(readonly cleanupCause: unknown) {
        super("candidate mutation retirement failed; exact native owner remains retained");
        this.name = "TreeCandidateMutationRetirementError";
    }
}

interface PendingMutationRetirement {
    token: number | undefined;
    active: boolean;
    needsCancel: boolean;
    done: boolean;
    completed: number;
    api: OutputApi;
    cooperate(): Promise<void>;
    now(): number | undefined;
    cooperating: boolean;
    flight?: Promise<void>;
    output?: CandidateMutationOutputOwner;
    transition?: "releaseBeforeOutput" | "published" | "detached";
    transitionDone: boolean;
    abortCandidateRevision?: number;
    abortCandidateOverride?: () => unknown;
}

// A failed cleanup is not permission to GC/free a wrapper. The next mutation
// or lifecycle caller must join/retry this exact slot. No mutation may resume
// while an active or retained retirement owns the same wrapper.
const pendingMutationRetirements = new Map<CandidateMutationJobTree, PendingMutationRetirement>();

export function hasPendingTreeCandidateMutationRetirement(tree: CandidateMutationJobTree): boolean {
    return pendingMutationRetirements.has(tree);
}

function candidatePresent(tree: CandidateMutationJobTree, read: CandidateMutationJobTree["has_candidate"]): boolean {
    const value = read.call(tree);
    if (typeof value !== "boolean") throw new TypeError("invalid candidate mutation candidate presence");
    return value;
}

/** Retain the outer push's exact abort obligation with its failed native
 * cleanup. A later candidate revision is never rolled back by this debt. */
export async function abortTreeCandidateAfterMutationRetirement(tree: CandidateMutationJobTree,
    expectedRevision: number, abortCandidateOverride?: () => unknown): Promise<boolean> {
    count(expectedRevision, "abort revision");
    const owner = pendingMutationRetirements.get(tree);
    if (owner) {
        if (owner.abortCandidateRevision !== undefined && owner.abortCandidateRevision !== expectedRevision) {
            throw new Error("conflicting candidate mutation abort ownership");
        }
        owner.abortCandidateRevision = expectedRevision;
        if (!owner.abortCandidateOverride && abortCandidateOverride) {
            owner.abortCandidateOverride = abortCandidateOverride;
        }
        // A debt can join a flight after its final native/abort check but
        // before its Promise finalizer. Keep joining until this debt, not
        // merely an earlier cleanup flight, has really been settled.
        do { await drainTreeCandidateMutationRetirement(tree); }
        while (pendingMutationRetirements.get(tree)?.abortCandidateRevision !== undefined);
        return !candidatePresent(tree, owner.api.hasCandidate);
    }
    const present = tree.has_candidate, revision = tree.candidate_revision;
    const abort = abortCandidateOverride ?? tree.abort_candidate;
    if (![present, revision, abort].every(fn => typeof fn === "function")) {
        throw new TypeError("candidate mutation abort requires exact candidate operations");
    }
    if (!candidatePresent(tree, present)) return true;
    if (count(revision!.call(tree), "abort revision") !== expectedRevision) return false;
    abort!.call(tree);
    if (candidatePresent(tree, present)) throw new Error("candidate mutation abort did not release the expected candidate");
    return true;
}

function transitionOutput(owner: PendingMutationRetirement): void {
    if (!owner.output || owner.transitionDone) return;
    if (!owner.transition) throw new Error("candidate mutation output ownership transition is missing");
    owner.output[owner.transition]();
    owner.transitionDone = true;
    if (owner.transition === "releaseBeforeOutput") owner.output = undefined;
}

export function drainTreeCandidateMutationRetirement(tree: CandidateMutationJobTree): Promise<void> {
    const owner = pendingMutationRetirements.get(tree);
    if (!owner) return Promise.resolve();
    if (owner.active || owner.cooperating) return Promise.reject(new TreeCandidateMutationRetirementError(
        new Error("candidate mutation cleanup cannot re-enter its active owner")));
    if (owner.flight) return owner.flight;
    const work = Promise.resolve().then(async () => {
        if (owner.token === undefined) throw new Error("candidate mutation cleanup lacks an exact token");
        if (owner.needsCancel) {
            owner.api.cancel.call(tree, owner.token);
            owner.needsCancel = false;
        }
        transitionOutput(owner);
        let slice = owner.done ? undefined : startCpuSlice(owner.now), sliceSteps = 0;
        while (!owner.done) {
            const row = exactRecord(owner.api.retire.call(tree, owner.token, RETIREMENT_UNITS),
                ["done", "units", "completed"], "retirement progress");
            const units = count(row.units, "retirement progress");
            const completed = count(row.completed, "retirement progress");
            if (typeof row.done !== "boolean" || units > RETIREMENT_UNITS || (!row.done && units === 0) ||
                completed !== sum(owner.completed, units)) throw new TypeError("inconsistent candidate mutation retirement progress");
            owner.completed = completed; owner.done = row.done;
            sliceSteps++;
            // The completed retirement still owns one mandatory final host
            // turn before its wrapper/admission owner can be released.
            if (owner.done || sliceSteps >= TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN ||
                cpuSliceExhausted(slice!, owner.now)) {
                let cooperation: Promise<void>;
                owner.cooperating = true;
                try { cooperation = owner.cooperate(); }
                finally { owner.cooperating = false; }
                await cooperation;
                if (!owner.done) slice = startCpuSlice(owner.now);
                sliceSteps = 0;
            }
        }
        if (owner.output) {
            // Native retirement is the proof which lets a detached private
            // output owner leave the admission ledger. Complete that handoff
            // before an owed outer-candidate abort: the atomic abort receipt
            // can acquire a settlement ticket only after no retiring owner
            // remains for this wrapper. A throwing callback keeps both the
            // output owner and abort debt in this strong retry slot.
            owner.output.retired();
            owner.output = undefined;
        }
        if (owner.abortCandidateRevision !== undefined) {
            if (candidatePresent(tree, owner.api.hasCandidate) &&
                count(owner.api.revision.call(tree), "abort revision") === owner.abortCandidateRevision) {
                (owner.abortCandidateOverride ?? owner.api.abort).call(tree);
                if (candidatePresent(tree, owner.api.hasCandidate)) {
                    throw new Error("candidate mutation abort did not release the expected candidate");
                }
            }
            owner.abortCandidateRevision = undefined;
            owner.abortCandidateOverride = undefined;
        }
    });
    const flight = work.finally(() => {
        owner.flight = undefined;
        if (owner.done && owner.output) {
            // A failed final host turn cannot undo the completed native drop.
            // Finish this infallible/idempotent handoff here as well; if a
            // hostile callback throws, the strong owner remains retryable.
            owner.output.retired();
            owner.output = undefined;
        }
        if (owner.done && !owner.output) {
            if (pendingMutationRetirements.get(tree) !== owner) throw new Error("candidate mutation owner identity changed");
            if (owner.abortCandidateRevision === undefined) pendingMutationRetirements.delete(tree);
        }
    }).catch(error => { throw new TreeCandidateMutationRetirementError(error); });
    owner.flight = flight;
    return flight;
}

function captureOutputOwner(value: CandidateMutationOutputOwner | undefined): CandidateMutationOutputOwner | undefined {
    if (value === undefined) return undefined;
    if (value instanceof Promise) {
        // Observe only a wrongly returned local Promise's rejection; never
        // await it or pretend an asynchronous policy acquired a valid owner.
        void value.catch(() => {});
        throw new TypeError("candidate mutation output policy must be synchronous");
    }
    if (!value || typeof value !== "object") throw new TypeError("invalid candidate mutation output policy owner");
    const names = ["releaseBeforeOutput", "ready", "published", "detached", "retired"] as const;
    const captured = {} as CandidateMutationOutputOwner;
    for (const name of names) {
        const fn = value[name];
        if (typeof fn !== "function") throw new TypeError("invalid candidate mutation output policy owner");
        Object.defineProperty(captured, name, { value: (...args: unknown[]) => {
            const result: unknown = Reflect.apply(fn, value, args);
            if (result !== undefined) {
                if (result instanceof Promise) void result.catch(() => {});
                throw new TypeError("candidate mutation output owner callbacks must be synchronous and return undefined");
            }
        } });
    }
    return captured;
}

function mutationJobApi(tree: CandidateMutationJobTree): boolean {
    const specific = [tree.begin_candidate_update_job, tree.begin_candidate_delete_job,
        tree.finish_candidate_mutation_job];
    const claimed = specific.some(method => method !== undefined);
    // Shared methods also exist on older begin/chunk jobs. Their presence
    // alone must not claim mutation support or disable the legacy operation.
    if (claimed && ![...specific, tree.step_tree_job, tree.cancel_tree_job]
        .every(method => typeof method === "function")) {
        throw new TypeError("WASM tree exposes an incomplete candidate mutation job API");
    }
    return claimed;
}

function validateProgress(value: unknown, completed: number, reachable: number): TreeJobProgress {
    if (!value || typeof value !== "object") throw new TypeError("WASM tree mutation job returned invalid progress");
    const row = value as Partial<TreeJobProgress>;
    for (const field of ["units", "completed", "remaining", "reachable"] as const) {
        if (!Number.isSafeInteger(row[field]) || row[field]! < 0) {
            throw new TypeError("WASM tree mutation job returned invalid progress");
        }
    }
    if (typeof row.done !== "boolean" || row.units! > TREE_CANDIDATE_MUTATION_STEP_UNITS ||
        (!row.done && row.units === 0) || (row.done && row.remaining !== 0)) {
        throw new TypeError("WASM tree mutation job returned invalid progress");
    }
    if (row.completed !== completed + row.units! || row.reachable! < reachable ||
        row.reachable! > row.completed!) {
        throw new TypeError("WASM tree mutation job progress is inconsistent");
    }
    return row as TreeJobProgress;
}

function outputProgress(value: unknown, completed: number, reachable: number): TreeJobProgress & { phase: string } {
    const row = exactRecord(value, ["done", "units", "completed", "remaining", "reachable", "phase"], "output progress");
    const units = count(row.units, "output progress"), next = count(row.completed, "output progress");
    const remaining = count(row.remaining, "output progress"), seen = count(row.reachable, "output progress");
    if (typeof row.done !== "boolean" || typeof row.phase !== "string" || row.phase.length === 0 ||
        row.phase.length > 64 || units > TREE_CANDIDATE_MUTATION_STEP_UNITS ||
        (!row.done && units === 0 && row.phase !== "plan ready") ||
        remaining !== (row.done ? 0 : 1) || next !== sum(completed, units) || seen < reachable || seen > next ||
        (row.phase === "plan ready" && row.done)) throw new TypeError("inconsistent candidate mutation output progress");
    return { done: row.done, units, completed: next, remaining, reachable: seen, phase: row.phase };
}

async function applyOutputMutation(tree: CandidateMutationJobTree,
    begin: (payload: string) => number, api: OutputApi, payload: string,
    options: { check(): void; cooperate(): Promise<void>; cooperateRetirement(): Promise<void>;
        onCandidateMutated(): void; onOutputMemoryPlan?: ApplyTreeCandidateMutationOptions["onOutputMemoryPlan"];
        now(): number | undefined }): Promise<void> {
    if (pendingMutationRetirements.has(tree)) throw new Error("candidate mutation owner is already active");
    // Allocate/register cleanup state before native begin, not after a possibly
    // irreversible output allocation or publication has already succeeded.
    const owner: PendingMutationRetirement = { token: undefined, active: true, needsCancel: true,
        done: false, completed: 0, api, cooperate: options.cooperateRetirement,
        now: options.now, cooperating: false, transitionDone: false };
    pendingMutationRetirements.set(tree, owner);
    let began = false, resumed = false, finished = false, failed = false, primary: unknown;
    try {
        const token = begin.call(tree, payload);
        began = true;
        // Keep a safely representable returned token for cleanup, even if a
        // corrupt port returned an out-of-contract zero. Without any exact
        // token, retain quarantine rather than inventing/freeing another job.
        if (Number.isSafeInteger(token)) owner.token = token;
        if (!Number.isSafeInteger(token) || token < 1 || token > 0xffff_ffff) {
            throw new TypeError("WASM tree mutation job returned an invalid token");
        }
        let completed = 0, reachable = 0;
        let plan: CandidateMutationOutputMemoryPlan | undefined;
        let slice = startCpuSlice(options.now);
        let sliceSteps = 0;
        for (;;) {
            options.check();
            // Unlike the legacy step, new ABI execution errors retain the
            // same-token failed/retirement owner for deferred cancellation.
            const current = outputProgress(api.step.call(tree, token, TREE_CANDIDATE_MUTATION_STEP_UNITS), completed, reachable);
            sliceSteps++;
            completed = current.completed; reachable = current.reachable;
            options.check();
            if (current.phase === "plan ready") {
                if (plan) throw new TypeError("candidate mutation repeated its output plan barrier");
                if (current.reachable !== 0) throw new TypeError("candidate mutation allocated output before its plan barrier");
                plan = parseCandidateMutationOutputMemoryPlan(api.plan.call(tree, token));
                if (plan.scope !== `${api.version === 1 ? "v1" : "v2"}-candidate-mutation-output`) {
                    throw new TypeError("candidate mutation output plan version changed");
                }
                options.check();
                const rawOutput = options.onOutputMemoryPlan?.(plan);
                if (rawOutput !== undefined) {
                    if (rawOutput instanceof Promise) {
                        // This internal hook is synchronous by contract. It
                        // cannot transfer an owner through a future value.
                        void rawOutput.catch(() => {});
                        throw new TypeError("candidate mutation output policy must be synchronous");
                    }
                    // Register the raw owner before any getter validation or
                    // wrapper allocation. If capture fails, cancellation can
                    // still release/quarantine the exact owner instead of
                    // forgetting a successfully reserved ledger attempt.
                    owner.output = rawOutput;
                    owner.output = captureOutputOwner(rawOutput);
                }
                options.check();
                resumed = true;
                api.resume.call(tree, token, plan.nodePayloadBytes,
                    plan.rangeEndpointPeakRequestedBytes, plan.rangeEndpointResidentRequestedBytes);
                options.check();
            } else if (!plan && current.reachable !== 0) {
                throw new TypeError("candidate mutation allocated output before its plan barrier");
            }
            if (current.done) {
                if (!plan && (current.completed !== 0 || current.reachable !== 0)) {
                    throw new TypeError("candidate mutation omitted its output plan barrier");
                }
                if (sliceSteps >= TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN ||
                    cpuSliceExhausted(slice, options.now)) {
                    await options.cooperate();
                    options.check();
                }
                if (plan) {
                    const ready = parseCandidateMutationOutputMemoryReady(api.ready.call(tree, token), plan);
                    options.check();
                    owner.output?.ready(ready);
                    options.check();
                }
                break;
            }
            if (sliceSteps >= TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN ||
                cpuSliceExhausted(slice, options.now)) {
                await options.cooperate();
                options.check();
                slice = startCpuSlice(options.now);
                sliceSteps = 0;
            }
        }
        options.check();
        api.finish.call(tree, token);
        finished = true; owner.needsCancel = false; owner.active = false; owner.transition = "published";
        // Publication is irreversible. Install the cleanup/transfer state
        // first, then make both synchronous caller handoffs before any guard.
        let handoffFailed = false, handoffError: unknown;
        try { transitionOutput(owner); }
        catch (error) { handoffFailed = true; handoffError = error; }
        try { options.onCandidateMutated(); }
        catch (error) {
            if (handoffFailed) reportCleanupFailure(handoffError, error);
            else { handoffFailed = true; handoffError = error; }
        }
        if (handoffFailed) throw handoffError;
        options.check();
    } catch (error) {
        failed = true; primary = error; throw error;
    } finally {
        if (!began) {
            // A rejected begin has not returned a host-owned native token.
            pendingMutationRetirements.delete(tree);
        } else {
            owner.active = false;
            if (!finished) owner.transition = resumed ? "detached" : "releaseBeforeOutput";
            try { await drainTreeCandidateMutationRetirement(tree); }
            catch (cleanup) {
                if (failed) reportCleanupFailure(primary, cleanup); else throw cleanup;
            }
        }
    }
    // Cleanup waits are uncancellable, but their success must not authorize a
    // stale caller to continue publication/index IO afterwards.
    options.check();
}

function reportCleanupFailure(primary: unknown, cleanup: unknown): void {
    if ((typeof primary === "object" && primary !== null) || typeof primary === "function") {
        try {
            const target = primary as { treeCandidateCleanupErrors?: unknown[] };
            const failures = target.treeCandidateCleanupErrors ?? [];
            failures.push(cleanup);
            Object.defineProperty(target, "treeCandidateCleanupErrors", { configurable: true, value: failures });
        } catch { /* A frozen/foreign error must retain its primary identity. */ }
    }
    try { console.error("[obsetync] candidate mutation cleanup failed:", cleanup); }
    catch { /* Diagnostic failures must not replace the primary operation error. */ }
}

/** Apply exactly one caller-owned candidate operation. This helper never
 * parses/serializes the payload, combines delete/update calls, publishes a
 * committed root, or aborts the outer candidate. Native job cancellation must
 * leave the candidate available for the caller's existing abort/commit owner.
 * No borrowed WASM view crosses a host wait. Legacy step errors drop the job;
 * the V2 output family retains failed ownership until deferred cleanup. This
 * driver does not itself impose a quota when no output policy is supplied. */
export async function applyTreeCandidateMutation(
    tree: CandidateMutationJobTree,
    kind: TreeCandidateMutationKind,
    payloadJson: string,
    options: ApplyTreeCandidateMutationOptions,
): Promise<void> {
    if ((kind !== "update" && kind !== "delete") || typeof payloadJson !== "string") {
        throw new TypeError("candidate mutation requires a kind and a serialized payload");
    }
    if (typeof options?.cooperate !== "function" || typeof options.legacy !== "function" ||
        (options.assertCurrent !== undefined && typeof options.assertCurrent !== "function") ||
        (options.onCandidateMutated !== undefined && typeof options.onCandidateMutated !== "function") ||
        (options.cooperateRetirement !== undefined && typeof options.cooperateRetirement !== "function") ||
        (options.onOutputMemoryPlan !== undefined && typeof options.onOutputMemoryPlan !== "function") ||
        (options.now !== undefined && typeof options.now !== "function") ||
        (options.requireIncremental !== undefined && typeof options.requireIncremental !== "boolean")) {
        throw new TypeError("candidate mutation requires host cooperation and a fallback");
    }
    const { cooperate, signal, assertCurrent: assertOwner, legacy,
        onCandidateMutated = () => {}, cooperateRetirement = () => yieldWork(), onOutputMemoryPlan,
        now: rawNow } = options;
    const now = monotonicClock(rawNow ?? defaultMonotonicClock());
    if (pendingMutationRetirements.has(tree)) await drainTreeCandidateMutationRetirement(tree);
    // Cleanup can use its pinned old wrapper after stop, but a NEW operation
    // must not even call a version getter on a freed/stale native wrapper.
    throwIfWorkAborted(signal);
    assertOwner?.();
    throwIfWorkAborted(signal);
    const api = outputApi(tree), version = tree.tree_version, hasCandidate = tree.has_candidate;
    const jobs = mutationJobApi(tree);
    if (onOutputMemoryPlan && !api) throw new TypeError("candidate mutation output policy requires the complete output API");
    if (api && !jobs) throw new TypeError("WASM tree exposes an incomplete candidate mutation output API");
    const begin = kind === "update" ? tree.begin_candidate_update_job! : tree.begin_candidate_delete_job!;
    const step = tree.step_tree_job!, finish = tree.finish_candidate_mutation_job!, cancel = tree.cancel_tree_job!;
    const assertCurrent = () => {
        throwIfWorkAborted(signal);
        assertOwner?.();
        if (hasCandidate.call(tree) !== true) throw new Error("candidate disappeared during mutation");
        if (api && version!.call(tree) !== api.version) throw new Error("candidate mutation tree version changed");
        // A host ownership hook may synchronously stop its own operation.
        throwIfWorkAborted(signal);
    };
    assertCurrent();
    await cooperate();
    assertCurrent();

    if (!jobs) {
        if (options.requireIncremental) {
            throw new TypeError("incremental candidate mutation API is required");
        }
        legacy();
        onCandidateMutated();
        assertCurrent();
        return;
    }
    if (api) {
        await applyOutputMutation(tree, begin, api, payloadJson, { check: assertCurrent, cooperate,
            cooperateRetirement, onCandidateMutated, onOutputMemoryPlan, now });
        return;
    }
    const token = begin.call(tree, payloadJson);
    if (!Number.isSafeInteger(token) || token < 1 || token > 0xffff_ffff) {
        const failure = new TypeError("WASM tree mutation job returned an invalid token");
        if (Number.isSafeInteger(token)) {
            try { cancel.call(tree, token); } catch (cleanup) { reportCleanupFailure(failure, cleanup); }
        }
        throw failure;
    }

    let jobOpen = true, failed = false, primary: unknown;
    let completed = 0, reachable = 0;
    try {
        let slice = startCpuSlice(now);
        let sliceSteps = 0;
        for (;;) {
            assertCurrent();
            let raw: unknown;
            try { raw = step.call(tree, token, TREE_CANDIDATE_MUTATION_STEP_UNITS); }
            catch (error) { jobOpen = false; throw error; }
            const current = validateProgress(raw, completed, reachable);
            sliceSteps++;
            completed = current.completed; reachable = current.reachable;
            assertCurrent();
            const mustYield = sliceSteps >= TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN ||
                cpuSliceExhausted(slice, now);
            if (mustYield) {
                await cooperate();
                assertCurrent();
            }
            if (current.done) break;
            if (mustYield) {
                slice = startCpuSlice(now);
                sliceSteps = 0;
            }
        }
        assertCurrent();
        finish.call(tree, token);
        jobOpen = false;
        onCandidateMutated();
        assertCurrent();
    } catch (error) {
        failed = true; primary = error; throw error;
    } finally {
        if (jobOpen) {
            try { cancel.call(tree, token); }
            catch (cleanup) {
                if (failed) reportCleanupFailure(primary, cleanup); else throw cleanup;
            }
        }
    }
}
