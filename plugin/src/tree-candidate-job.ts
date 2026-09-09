import { throwIfWorkAborted } from "./work-scheduler";

export const TREE_CANDIDATE_JOB_STEP_UNITS = 1;
export const TREE_REACHABILITY_RETIREMENT_STEP_UNITS = 256;
export const TREE_CANDIDATE_CHUNK_SORT_STEP_UNITS = 4096;
export const TREE_CANDIDATE_CHUNK_PAGE_HASHES = 256;

export interface TreeJobProgress {
    done: boolean;
    units: number;
    completed: number;
    remaining: number;
    reachable: number;
}

export interface CandidateJobTree {
    begin_candidate(): void;
    has_candidate(): boolean;
    abort_candidate(): unknown;
    tree_version?: () => number;
    begin_candidate_job?: () => number;
    step_tree_job?: (token: number, maxUnits: number) => unknown;
    step_reachability_job_deferred?: (token: number, maxUnits: number) => unknown;
    finish_candidate_job?: (token: number) => number;
    finish_candidate_job_deferred?: (token: number) => number;
    candidate_open_memory_plan_v1_job?: (token: number) => unknown;
    resume_candidate_open_memory_v1_job?: (token: number, identityBytes: number,
        endpointBytes: number, rootStringBytes: number) => void;
    begin_candidate_chunks_job?: () => number;
    finish_candidate_chunks_job?: (token: number) => unknown;
    finish_candidate_chunks_job_deferred?: (token: number) => unknown;
    candidate_chunks_sort_memory_plan_v1_job?: (token: number) => unknown;
    resume_candidate_chunks_sort_memory_v1_job?: (token: number,
        sourceHashesBytes: number, scratchHashesBytes: number) => void;
    step_candidate_chunks_sort_v1_job?: (token: number, maxUnits: number) => unknown;
    candidate_chunks_plan_info_v1_job?: (token: number) => unknown;
    read_candidate_chunks_page_v1_job?: (token: number,
        expectedOffset: number, maxHashes: number) => unknown;
    finish_candidate_chunks_plan_v1_job?: (token: number) => void;
    cancel_tree_job?: (token: number) => void;
    cancel_reachability_job_deferred?: (token: number) => void;
    step_tree_retirement?: (token: number, maxUnits: number) => unknown;
    step_reachability_retirement?: (token: number, expectedCompleted: number, maxUnits: number) => unknown;
    finish_reachability_retirement?: (token: number, expectedCompleted: number) => void;
    candidate_revision?: () => number;
}

export interface BeginTreeCandidateOptions {
    /** Must yield to the real host and may apply the current heavy-work gate. */
    cooperate(): Promise<void>;
    /** Real host turn used only for mandatory native cleanup. It must ignore
     * the operation signal, visibility/policy gates and normal admission. */
    cooperateRetirement?: () => Promise<void>;
    signal?: AbortSignal;
    /** Rechecks captured policy/source ownership before every native step. */
    assertCurrent?(): void;
    /** Fail-fast resident admission for the exact native root clone requests.
     * When supplied, an older/partial packaged ABI may not bypass it. */
    onOpenMemoryPlan?(plan: CandidateOpenMemoryPlan): CandidateOpenMemoryOwner;
    /** Exact terminal abort used only if publication succeeded but ownership
     * handoff failed. It must settle any attached resident admission too. */
    abortCandidateOpened?(expectedRevision: number): unknown;
    /** Synchronous ownership handoff. Once this returns, the caller must abort
     * the candidate on every pre-accept failure. */
    onCandidateOpened(reachable: number | null): void;
}

export interface CandidateOpenMemoryPlan {
    schema: 1;
    scope: "v1-candidate-open-root" | "v2-candidate-open-root";
    residentChunkCount: number;
    rootStringCount: number;
    rootIdentityRequestedBytes: number;
    rootEndpointRequestedBytes: number;
    rootStringRequestedBytes: number;
    baselineKeySnapshotRequestedBytes: 0;
    peakAdmissionBytes: number;
    baselineStrategy: "insertion-generation-v1";
}

/** Synchronous ownership transitions around the native prepared root. The
 * owner is registered before resume, and detached cleanup remains charged
 * until the same-token native retirement has really completed. */
export interface CandidateOpenMemoryOwner {
    releaseBeforeOutput(): void;
    ready(): void;
    published(): void;
    detached(): void;
    retired(): void;
}

export interface BeginTreeCandidateResult {
    cooperative: boolean;
    reachable: number | null;
}

export interface TreeCandidateChunkPlan {
    all: string[];
    fresh: string[];
}

export interface CollectTreeCandidateChunksOptions {
    cooperate(): Promise<void>;
    cooperateRetirement?: () => Promise<void>;
    signal?: AbortSignal;
    assertCurrent?(): void;
    /** Compatibility path for structural ports or an older packaged tree. */
    legacy(): unknown;
}

export interface CandidateChunkSortMemoryPlan {
    schema: 1;
    scope: "candidate-chunk-plan-sort-workspace";
    hashCount: number;
    hashSizeBytes: 32;
    sourceHashesRequestedBytes: number;
    scratchHashesRequestedBytes: number;
    peakAdmissionBytes: number;
    reachableSetUnmeasured: true;
    pageOutputUnmeasured: true;
    sortStrategy: "stable-lsd-radix-v1";
    pageMaxHashes: 256;
}

export interface CandidateChunkSortMemoryOwner {
    /** Releases only the native sort/page workspace admission. Page bridge
     * values are explicitly outside the native plan and die at each sink. */
    release(): void;
}

export interface TreeCandidateChunkPage {
    offset: number;
    nextOffset: number;
    done: boolean;
    all: string[];
    fresh: string[];
}

export interface TreeCandidateChunkPageSummary {
    allCount: number;
    freshCount: number;
}

export interface CollectTreeCandidateChunkPagesOptions extends CollectTreeCandidateChunksOptions {
    /** Required for the additive bounded native path. The owner is held until
     * same-token native retirement completes, including cancellation. */
    onSortMemoryPlan(plan: CandidateChunkSortMemoryPlan):
        CandidateChunkSortMemoryOwner | Promise<CandidateChunkSortMemoryOwner>;
    /** Receives at most 256 canonical hashes per list. The callback must
     * settle before the native page cursor advances. */
    onPage(page: TreeCandidateChunkPage, allCount: number): Promise<void>;
}

export class TreeReachabilityRetirementError extends Error {
    readonly code = "TREE_REACHABILITY_RETIREMENT_FAILED";
    constructor(readonly cleanupCause: unknown, readonly originalCause?: unknown) {
        super("tree reachability retirement failed; native owner remains quarantined");
        this.name = "TreeReachabilityRetirementError";
    }
}

const CANDIDATE_OPEN_MEMORY_SCOPES = Object.freeze({
    1: "v1-candidate-open-root",
    2: "v2-candidate-open-root",
} as const);

function exactRecord(value: unknown, fields: readonly string[], label: string): Record<string, unknown> {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        ![Object.prototype, null].includes(Object.getPrototypeOf(value))) {
        throw new TypeError(`invalid candidate open ${label}`);
    }
    const keys = Reflect.ownKeys(value);
    if (keys.length !== fields.length || !fields.every(field => keys.includes(field))) {
        throw new TypeError(`invalid candidate open ${label}`);
    }
    const row: Record<string, unknown> = Object.create(null);
    for (const field of fields) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor)) throw new TypeError(`invalid candidate open ${label}`);
        row[field] = descriptor.value;
    }
    return row;
}

function count(value: unknown, label: string): number {
    if (!Number.isSafeInteger(value) || (value as number) < 0) {
        throw new TypeError(`invalid candidate open ${label}`);
    }
    return value as number;
}

export function parseCandidateOpenMemoryPlan(value: unknown): CandidateOpenMemoryPlan {
    const fields = ["schema", "scope", "residentChunkCount", "rootStringCount",
        "rootIdentityRequestedBytes", "rootEndpointRequestedBytes", "rootStringRequestedBytes",
        "baselineKeySnapshotRequestedBytes", "peakAdmissionBytes", "baselineStrategy"] as const;
    const row = exactRecord(value, fields, "memory plan");
    const resident = count(row.residentChunkCount, "resident chunk count");
    const strings = count(row.rootStringCount, "root string count");
    const identity = count(row.rootIdentityRequestedBytes, "identity request");
    const endpoint = count(row.rootEndpointRequestedBytes, "endpoint request");
    const total = count(row.rootStringRequestedBytes, "root string request");
    const snapshot = count(row.baselineKeySnapshotRequestedBytes, "baseline snapshot request");
    const peak = count(row.peakAdmissionBytes, "peak admission");
    const scope = row.scope;
    const v1 = scope === CANDIDATE_OPEN_MEMORY_SCOPES[1];
    const v2 = scope === CANDIDATE_OPEN_MEMORY_SCOPES[2];
    if (row.schema !== 1 || (!v1 && !v2) ||
        row.baselineStrategy !== "insertion-generation-v1" ||
        (v1 ? strings < 2 : (strings !== 2 && strings !== 4)) || snapshot !== 0 ||
        !Number.isSafeInteger(identity + endpoint) || total !== identity + endpoint || peak !== total ||
        (strings === 2 && endpoint !== 0)) {
        throw new TypeError("inconsistent candidate open memory plan");
    }
    return Object.freeze({
        schema: 1, scope, residentChunkCount: resident,
        rootStringCount: strings, rootIdentityRequestedBytes: identity,
        rootEndpointRequestedBytes: endpoint, rootStringRequestedBytes: total,
        baselineKeySnapshotRequestedBytes: 0, peakAdmissionBytes: peak,
        baselineStrategy: "insertion-generation-v1",
    });
}

interface CandidateOpenMemoryApi {
    version: 1 | 2;
    plan: NonNullable<CandidateJobTree["candidate_open_memory_plan_v1_job"]>;
    resume: NonNullable<CandidateJobTree["resume_candidate_open_memory_v1_job"]>;
}

function candidateOpenMemoryApi(tree: CandidateJobTree): CandidateOpenMemoryApi | null {
    const plan = tree.candidate_open_memory_plan_v1_job;
    const resume = tree.resume_candidate_open_memory_v1_job;
    if (plan === undefined && resume === undefined) return null;
    if (typeof plan !== "function" || typeof resume !== "function") {
        throw new TypeError("WASM tree exposes an incomplete candidate open memory API");
    }
    const readVersion = tree.tree_version;
    if (typeof readVersion !== "function") {
        throw new TypeError("candidate open memory API requires an exact tree version witness");
    }
    const version = readVersion.call(tree);
    if (version !== 1 && version !== 2) {
        throw new TypeError("WASM tree returned an invalid tree version");
    }
    return { version, plan, resume };
}

function captureOpenMemoryOwner(value: CandidateOpenMemoryOwner | undefined): CandidateOpenMemoryOwner | undefined {
    if (value === undefined) return undefined;
    if (value instanceof Promise) {
        void value.catch(() => {});
        throw new TypeError("candidate open memory policy must be synchronous");
    }
    if (!value || typeof value !== "object") throw new TypeError("invalid candidate open memory owner");
    const names = ["releaseBeforeOutput", "ready", "published", "detached", "retired"] as const;
    const captured = {} as CandidateOpenMemoryOwner;
    for (const name of names) {
        const method = value[name];
        if (typeof method !== "function") throw new TypeError("invalid candidate open memory owner");
        Object.defineProperty(captured, name, { value: () => {
            const result: unknown = Reflect.apply(method, value, []);
            if (result !== undefined) {
                if (result instanceof Promise) void result.catch(() => {});
                throw new TypeError("candidate open memory owner callbacks must return undefined");
            }
        } });
    }
    return captured;
}

interface DeferredReachabilityApi {
    step(token: number, maxUnits: number): unknown;
    finishBegin(token: number): number;
    finishChunks(token: number): unknown;
    cancel(token: number): void;
    retire(token: number, expectedCompleted: number, maxUnits: number): unknown;
    finishRetirement(token: number, expectedCompleted: number): void;
    revision(): number;
    hasCandidate(): boolean;
    abortCandidate(): unknown;
}

interface PendingReachabilityRetirement {
    token: number;
    needsCancel: boolean;
    done: boolean;
    finalized: boolean;
    completed: number;
    abortCandidateRevision?: number;
    cancel: DeferredReachabilityApi["cancel"];
    step: DeferredReachabilityApi["retire"];
    finish: DeferredReachabilityApi["finishRetirement"];
    revision: DeferredReachabilityApi["revision"];
    hasCandidate: DeferredReachabilityApi["hasCandidate"];
    abortCandidate: DeferredReachabilityApi["abortCandidate"];
    abortCandidateOverride?: () => unknown;
    active: boolean;
    output?: CandidateOpenMemoryOwner;
    outputTransition?: "releaseBeforeOutput" | "published" | "detached";
    outputTransitionDone: boolean;
    cooperate(): Promise<void>;
    cooperating: boolean;
    flight?: Promise<void>;
}

// Strong ownership is deliberate: a failed cleanup may leave a native cursor
// alive. Lifecycle disposal and the next helper invocation must join/retry the
// exact wrapper/token instead of trusting JS GC or admitting overlapping work.
const pendingReachabilityRetirements = new Map<CandidateJobTree, PendingReachabilityRetirement>();

export function hasPendingTreeReachabilityRetirement(tree: CandidateJobTree): boolean {
    return pendingReachabilityRetirements.has(tree);
}

/** Transfer an outer push's exact candidate-abort obligation across a retained
 * reachability cleanup. A later candidate revision is never rolled back. */
export async function abortTreeCandidateAfterReachabilityRetirement(
    tree: CandidateJobTree,
    expectedRevision: number,
    abortCandidateOverride?: () => unknown,
): Promise<boolean> {
    if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 0) {
        throw new TypeError("candidate retirement abort requires an exact revision");
    }
    const owner = pendingReachabilityRetirements.get(tree);
    if (owner) {
        retainCandidateAbortDebt(owner, expectedRevision, abortCandidateOverride);
        do {
            await drainTreeReachabilityRetirement(tree);
        } while (pendingReachabilityRetirements.get(tree)?.abortCandidateRevision !== undefined);
        return checkedCandidatePresence(owner, tree) === false;
    }
    const hasCandidate = tree.has_candidate, revision = tree.candidate_revision;
    const abortCandidate = abortCandidateOverride ?? tree.abort_candidate;
    if (typeof hasCandidate !== "function" || typeof revision !== "function" ||
        typeof abortCandidate !== "function") {
        throw new TypeError("candidate retirement abort requires exact candidate operations");
    }
    const candidate = hasCandidate.call(tree);
    if (typeof candidate !== "boolean") throw new TypeError("WASM tree returned invalid candidate presence");
    if (!candidate) return true;
    const currentRevision = revision.call(tree);
    if (!Number.isSafeInteger(currentRevision) || currentRevision < 0) {
        throw new TypeError("WASM tree returned an invalid candidate revision");
    }
    if (currentRevision === expectedRevision) {
        abortCandidate.call(tree);
        if (hasCandidate.call(tree) !== false) {
            throw new Error("candidate abort did not release the expected candidate");
        }
        return true;
    }
    return false;
}

function retainCandidateAbortDebt(owner: PendingReachabilityRetirement, revision: number,
    abortCandidateOverride?: () => unknown): void {
    if (owner.abortCandidateRevision === undefined) {
        owner.abortCandidateRevision = revision;
        owner.abortCandidateOverride = abortCandidateOverride;
    } else if (owner.abortCandidateRevision !== revision) {
        throw new Error("conflicting candidate revision cannot replace retained abort ownership");
    } else if (!owner.abortCandidateOverride && abortCandidateOverride) {
        owner.abortCandidateOverride = abortCandidateOverride;
    }
}

function checkedCandidatePresence(owner: PendingReachabilityRetirement, tree: CandidateJobTree): boolean {
    const candidate = owner.hasCandidate.call(tree);
    if (typeof candidate !== "boolean") throw new TypeError("WASM tree returned invalid candidate presence");
    return candidate;
}

function checkedCandidateRevision(owner: PendingReachabilityRetirement, tree: CandidateJobTree): number {
    const revision = owner.revision.call(tree);
    if (!Number.isSafeInteger(revision) || revision < 0) {
        throw new TypeError("WASM tree returned an invalid candidate revision");
    }
    return revision;
}

function retirementProgress(value: unknown, previous: number): { done: boolean; completed: number } {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
        throw new TypeError("WASM tree retirement returned invalid progress");
    }
    const fields = ["done", "units", "completed"] as const;
    const keys = Reflect.ownKeys(value);
    if (keys.length !== fields.length || !fields.every(field => keys.includes(field))) {
        throw new TypeError("WASM tree retirement returned invalid progress");
    }
    const row: Record<string, unknown> = {};
    for (const field of fields) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor)) {
            throw new TypeError("WASM tree retirement returned invalid progress");
        }
        row[field] = descriptor.value;
    }
    if (typeof row.done !== "boolean" || !Number.isSafeInteger(row.units) ||
        (row.units as number) < 0 || (row.units as number) > TREE_REACHABILITY_RETIREMENT_STEP_UNITS ||
        (!row.done && row.units === 0) || !Number.isSafeInteger(row.completed) ||
        row.completed !== previous + (row.units as number)) {
        throw new TypeError("WASM tree retirement returned invalid progress");
    }
    return { done: row.done, completed: row.completed as number };
}

/** Join/retry mandatory cleanup. A rejection is not permission to free the
 * wrapper; the registry keeps exact ownership until retirement and any owed
 * candidate abort have both completed. */
export function drainTreeReachabilityRetirement(tree: CandidateJobTree): Promise<void> {
    const owner = pendingReachabilityRetirements.get(tree);
    if (!owner) return Promise.resolve();
    if (owner.active) return Promise.reject(new TreeReachabilityRetirementError(
        new Error("reachability cleanup cannot drain its active candidate owner")));
    if (owner.flight) {
        if (owner.cooperating) {
            return Promise.reject(new TreeReachabilityRetirementError(
                new Error("reachability retirement cooperation cannot re-enter its own drain"),
            ));
        }
        return owner.flight;
    }
    const work = Promise.resolve().then(async () => {
        if (owner.needsCancel) {
            owner.cancel.call(tree, owner.token);
            owner.needsCancel = false;
        }
        transitionCandidateOpenOutput(owner);
        while (!owner.done) {
            const current = retirementProgress(owner.step.call(tree, owner.token, owner.completed,
                TREE_REACHABILITY_RETIREMENT_STEP_UNITS), owner.completed);
            owner.completed = current.completed;
            owner.done = current.done;
            // Cross a real host boundary even after the final native step.
            // Keep the re-entry marker only around the synchronous callback
            // invocation. While its returned promise is pending, unrelated
            // lifecycle callers must still be able to join this flight.
            let cooperation: Promise<void>;
            owner.cooperating = true;
            try { cooperation = owner.cooperate(); }
            finally { owner.cooperating = false; }
            await cooperation;
        }
        if (!owner.finalized) {
            owner.finish.call(tree, owner.token, owner.completed);
            owner.finalized = true;
        }
        if (owner.output) {
            owner.output.retired();
            owner.output = undefined;
        }
        if (owner.abortCandidateRevision !== undefined) {
            if (checkedCandidatePresence(owner, tree) &&
                checkedCandidateRevision(owner, tree) === owner.abortCandidateRevision) {
                (owner.abortCandidateOverride ?? owner.abortCandidate).call(tree);
                if (checkedCandidatePresence(owner, tree)) {
                    throw new Error("candidate abort did not release the expected candidate");
                }
            }
            owner.abortCandidateRevision = undefined;
            owner.abortCandidateOverride = undefined;
        }
    });
    const flight = work.catch(error => {
        throw new TreeReachabilityRetirementError(error);
    }).finally(() => {
        owner.flight = undefined;
        if (owner.finalized && owner.output) {
            owner.output.retired();
            owner.output = undefined;
        }
        if (owner.finalized && !owner.output && owner.abortCandidateRevision === undefined) {
            pendingReachabilityRetirements.delete(tree);
        }
    });
    owner.flight = flight;
    return flight;
}

function transitionCandidateOpenOutput(owner: PendingReachabilityRetirement): void {
    if (!owner.output || owner.outputTransitionDone) return;
    const transition = owner.outputTransition;
    if (!transition) throw new Error("candidate open memory ownership transition is missing");
    owner.output[transition]();
    owner.outputTransitionDone = true;
    if (transition === "releaseBeforeOutput") owner.output = undefined;
}

function deferredApi(tree: CandidateJobTree): DeferredReachabilityApi | null {
    const step = tree.step_reachability_job_deferred;
    const finishBegin = tree.finish_candidate_job_deferred;
    const finishChunks = tree.finish_candidate_chunks_job_deferred;
    const cancel = tree.cancel_reachability_job_deferred;
    const retire = tree.step_reachability_retirement;
    const finishRetirement = tree.finish_reachability_retirement;
    const reachabilityMethods = [step, finishBegin, finishChunks, cancel, retire, finishRetirement];
    const claimed = reachabilityMethods.some(method => method !== undefined);
    if (!claimed) return null;
    const revision = tree.candidate_revision, hasCandidate = tree.has_candidate;
    const abortCandidate = tree.abort_candidate;
    if (![...reachabilityMethods, revision, hasCandidate, abortCandidate]
        .every(method => typeof method === "function")) {
        throw new TypeError("WASM tree exposes an incomplete deferred reachability API");
    }
    return {
        step: step!, finishBegin: finishBegin!, finishChunks: finishChunks!,
        cancel: cancel!, retire: retire!, finishRetirement: finishRetirement!, revision: revision!,
        hasCandidate, abortCandidate,
    };
}

function retainRetirement(
    tree: CandidateJobTree,
    token: number,
    api: DeferredReachabilityApi,
    cooperate: () => Promise<void>,
    needsCancel: boolean,
    output?: CandidateOpenMemoryOwner,
): PendingReachabilityRetirement {
    if (pendingReachabilityRetirements.has(tree)) {
        throw new TreeReachabilityRetirementError(new Error("tree already has retained reachability cleanup"));
    }
    const owner: PendingReachabilityRetirement = {
        token, needsCancel, done: false, finalized: false, completed: 0,
        cancel: api.cancel, step: api.retire, finish: api.finishRetirement,
        revision: api.revision, hasCandidate: api.hasCandidate, abortCandidate: api.abortCandidate,
        cooperate, cooperating: false, active: false, output, outputTransitionDone: false,
    };
    pendingReachabilityRetirements.set(tree, owner);
    return owner;
}

function requestCandidateAbortAfterRetirement(tree: CandidateJobTree, revision: number,
    abortCandidateOverride?: () => unknown,
    completedOwner?: PendingReachabilityRetirement): void {
    let owner = pendingReachabilityRetirements.get(tree);
    if (!owner && completedOwner) {
        // The mandatory cursor retirement normally finishes before the caller
        // handoff. Re-register its already-finalized owner so a failed exact
        // terminal settlement remains durable/retryable instead of leaking a
        // candidate and its attached root lease outside every cleanup registry.
        if (!completedOwner.finalized || completedOwner.active || completedOwner.output ||
            completedOwner.needsCancel || completedOwner.flight) {
            throw new TreeReachabilityRetirementError(
                new Error("completed reachability owner cannot retain candidate abort debt"));
        }
        pendingReachabilityRetirements.set(tree, completedOwner);
        owner = completedOwner;
    }
    if (owner) retainCandidateAbortDebt(owner, revision, abortCandidateOverride);
}

interface CandidateTraversalApi {
    begin(): number;
    step(token: number, maxUnits: number): unknown;
    finish(token: number): number;
    cancel(token: number): void;
}

function jobApi(tree: CandidateJobTree): CandidateTraversalApi | null {
    const begin = tree.begin_candidate_job, step = tree.step_tree_job;
    const finish = tree.finish_candidate_job, cancel = tree.cancel_tree_job;
    const methods = [begin, step, finish, cancel];
    const claimed = methods.some(method => method !== undefined);
    if (claimed && !methods.every(method => typeof method === "function")) {
        throw new TypeError("WASM tree exposes an incomplete candidate job API");
    }
    return claimed ? { begin: begin!, step: step!, finish: finish!, cancel: cancel! } : null;
}

function reportCleanupFailure(primary: unknown, cleanup: unknown): void {
    if ((typeof primary === "object" && primary !== null) || typeof primary === "function") {
        try {
            const target = primary as { treeCandidateCleanupErrors?: unknown[] };
            const failures = target.treeCandidateCleanupErrors ?? [];
            failures.push(cleanup);
            Object.defineProperty(target, "treeCandidateCleanupErrors", {
                configurable: true, value: failures,
            });
        } catch { /* retain the primary failure even when it is non-extensible */ }
    }
    try { console.error("[obsetync] candidate begin cleanup failed:", cleanup); }
    catch { /* diagnostics must never replace the primary sync failure */ }
}

function progress(value: unknown): TreeJobProgress {
    if (!value || typeof value !== "object") throw new TypeError("WASM tree job returned invalid progress");
    const row = value as Partial<TreeJobProgress>;
    for (const field of ["units", "completed", "remaining", "reachable"] as const) {
        if (!Number.isSafeInteger(row[field]) || row[field]! < 0) {
            throw new TypeError("WASM tree job returned invalid progress");
        }
    }
    if (typeof row.done !== "boolean" || row.units! > TREE_CANDIDATE_JOB_STEP_UNITS ||
        (!row.done && row.units === 0) || (row.done && row.remaining !== 0)) {
        throw new TypeError("WASM tree job returned invalid progress");
    }
    return row as TreeJobProgress;
}

interface CandidateChunkTraversalApi {
    begin(): number;
    step(token: number, maxUnits: number): unknown;
    finish(token: number): unknown;
    cancel(token: number): void;
}

function chunkJobApi(tree: CandidateJobTree): CandidateChunkTraversalApi | null {
    const begin = tree.begin_candidate_chunks_job, finish = tree.finish_candidate_chunks_job;
    const step = tree.step_tree_job, cancel = tree.cancel_tree_job;
    const specific = [begin, finish];
    const claimed = specific.some(method => method !== undefined);
    if (claimed && ![...specific, step, cancel]
        .every(method => typeof method === "function")) {
        throw new TypeError("WASM tree exposes an incomplete candidate chunk job API");
    }
    return claimed ? { begin: begin!, step: step!, finish: finish!, cancel: cancel! } : null;
}

function chunkHashes(value: unknown): string[] {
    if (!Array.isArray(value) || value.length > 1_000_000) {
        throw new TypeError("WASM tree chunk job returned an invalid plan");
    }
    const length = value.length;
    const output = new Array<string>(length);
    for (let index = 0; index < length; index++) {
        const hash = value[index];
        if (typeof hash !== "string" || !/^[0-9a-f]{64}$/.test(hash) ||
            (index > 0 && output[index - 1] >= hash)) {
            throw new TypeError("WASM tree chunk job returned an invalid plan");
        }
        output[index] = hash;
    }
    return output;
}

function chunkPlan(value: unknown): TreeCandidateChunkPlan {
    if (!value || typeof value !== "object") {
        throw new TypeError("WASM tree chunk job returned an invalid plan");
    }
    const row = value as Partial<TreeCandidateChunkPlan>;
    const all = chunkHashes(row.all), fresh = chunkHashes(row.fresh);
    let cursor = 0;
    for (const hash of fresh) {
        while (cursor < all.length && all[cursor] < hash) cursor++;
        if (all[cursor] !== hash) throw new TypeError("WASM tree chunk job returned an invalid plan");
        cursor++;
    }
    return { all, fresh };
}

interface CandidateChunkPagedApi {
    plan: NonNullable<CandidateJobTree["candidate_chunks_sort_memory_plan_v1_job"]>;
    resume: NonNullable<CandidateJobTree["resume_candidate_chunks_sort_memory_v1_job"]>;
    step: NonNullable<CandidateJobTree["step_candidate_chunks_sort_v1_job"]>;
    info: NonNullable<CandidateJobTree["candidate_chunks_plan_info_v1_job"]>;
    read: NonNullable<CandidateJobTree["read_candidate_chunks_page_v1_job"]>;
    finish: NonNullable<CandidateJobTree["finish_candidate_chunks_plan_v1_job"]>;
}

function candidateChunkPagedApi(tree: CandidateJobTree): CandidateChunkPagedApi | null {
    const plan = tree.candidate_chunks_sort_memory_plan_v1_job;
    const resume = tree.resume_candidate_chunks_sort_memory_v1_job;
    const step = tree.step_candidate_chunks_sort_v1_job;
    const info = tree.candidate_chunks_plan_info_v1_job;
    const read = tree.read_candidate_chunks_page_v1_job;
    const finish = tree.finish_candidate_chunks_plan_v1_job;
    const methods = [plan, resume, step, info, read, finish];
    if (methods.every(method => method === undefined)) return null;
    if (!methods.every(method => typeof method === "function")) {
        throw new TypeError("WASM tree exposes an incomplete candidate chunk page API");
    }
    return { plan: plan!, resume: resume!, step: step!, info: info!, read: read!, finish: finish! };
}

function candidateChunkRecord(value: unknown, fields: readonly string[], label: string): Record<string, unknown> {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        ![Object.prototype, null].includes(Object.getPrototypeOf(value))) {
        throw new TypeError(`WASM tree returned an invalid candidate chunk ${label}`);
    }
    const keys = Reflect.ownKeys(value);
    if (keys.length !== fields.length || !fields.every(field => keys.includes(field))) {
        throw new TypeError(`WASM tree returned an invalid candidate chunk ${label}`);
    }
    const row: Record<string, unknown> = Object.create(null);
    for (const field of fields) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor)) {
            throw new TypeError(`WASM tree returned an invalid candidate chunk ${label}`);
        }
        row[field] = descriptor.value;
    }
    return row;
}

function candidateChunkCount(value: unknown, label: string): number {
    if (!Number.isSafeInteger(value) || (value as number) < 0) {
        throw new TypeError(`WASM tree returned an invalid candidate chunk ${label}`);
    }
    return value as number;
}

function candidateChunkSortPlan(value: unknown, reachable: number): CandidateChunkSortMemoryPlan {
    const fields = ["schema", "scope", "hashCount", "hashSizeBytes",
        "sourceHashesRequestedBytes", "scratchHashesRequestedBytes", "peakAdmissionBytes",
        "reachableSetUnmeasured", "pageOutputUnmeasured", "sortStrategy", "pageMaxHashes"] as const;
    const row = candidateChunkRecord(value, fields, "sort memory plan");
    const hashCount = candidateChunkCount(row.hashCount, "hash count");
    const source = candidateChunkCount(row.sourceHashesRequestedBytes, "source hash request");
    const scratch = candidateChunkCount(row.scratchHashesRequestedBytes, "scratch hash request");
    const peak = candidateChunkCount(row.peakAdmissionBytes, "peak admission");
    if (row.schema !== 1 || row.scope !== "candidate-chunk-plan-sort-workspace" ||
        row.hashSizeBytes !== 32 || hashCount !== reachable ||
        !Number.isSafeInteger(hashCount * 32) || source !== hashCount * 32 || scratch !== source ||
        !Number.isSafeInteger(source + scratch) || peak !== source + scratch ||
        row.reachableSetUnmeasured !== true || row.pageOutputUnmeasured !== true ||
        row.sortStrategy !== "stable-lsd-radix-v1" || row.pageMaxHashes !== TREE_CANDIDATE_CHUNK_PAGE_HASHES) {
        throw new TypeError("WASM tree returned an inconsistent candidate chunk sort memory plan");
    }
    return Object.freeze({
        schema: 1, scope: "candidate-chunk-plan-sort-workspace", hashCount, hashSizeBytes: 32,
        sourceHashesRequestedBytes: source, scratchHashesRequestedBytes: scratch,
        peakAdmissionBytes: peak, reachableSetUnmeasured: true, pageOutputUnmeasured: true,
        sortStrategy: "stable-lsd-radix-v1", pageMaxHashes: TREE_CANDIDATE_CHUNK_PAGE_HASHES,
    });
}

interface CandidateChunkSortProgress {
    done: boolean;
    units: number;
    completed: number;
    remaining: number;
    allCount: number;
    phase: string;
}

function candidateChunkSortProgress(value: unknown, reachable: number): CandidateChunkSortProgress {
    const fields = ["schema", "scope", "done", "units", "completed", "remaining", "allCount", "phase"] as const;
    const row = candidateChunkRecord(value, fields, "sort progress");
    const units = candidateChunkCount(row.units, "sort units");
    const completed = candidateChunkCount(row.completed, "sort completed count");
    const remaining = candidateChunkCount(row.remaining, "sort remaining count");
    const allCount = candidateChunkCount(row.allCount, "sort all count");
    const phases = ["collect", "initialize-scratch", "count-byte", "scatter-byte", "ready"];
    const total = reachable * 66;
    if (row.schema !== 1 || row.scope !== "candidate-chunk-plan-sort" ||
        typeof row.done !== "boolean" || units > TREE_CANDIDATE_CHUNK_SORT_STEP_UNITS ||
        (!row.done && units === 0) || (row.done && remaining !== 0) || allCount !== reachable ||
        !Number.isSafeInteger(total) || !Number.isSafeInteger(completed + remaining) ||
        completed + remaining !== total ||
        typeof row.phase !== "string" || !phases.includes(row.phase) ||
        (row.done !== (row.phase === "ready"))) {
        throw new TypeError("WASM tree returned invalid candidate chunk sort progress");
    }
    return { done: row.done, units, completed, remaining, allCount, phase: row.phase };
}

function candidateChunkPlanInfo(value: unknown, reachable: number): number {
    const fields = ["schema", "scope", "allCount", "pageMaxHashes"] as const;
    const row = candidateChunkRecord(value, fields, "plan info");
    const allCount = candidateChunkCount(row.allCount, "plan all count");
    if (row.schema !== 1 || row.scope !== "candidate-chunk-plan-pages" || allCount !== reachable ||
        row.pageMaxHashes !== TREE_CANDIDATE_CHUNK_PAGE_HASHES) {
        throw new TypeError("WASM tree returned inconsistent candidate chunk plan info");
    }
    return allCount;
}

function candidateChunkPage(value: unknown, expectedOffset: number, allCount: number): TreeCandidateChunkPage {
    const fields = ["schema", "scope", "offset", "nextOffset", "done", "all", "fresh"] as const;
    const row = candidateChunkRecord(value, fields, "plan page");
    const offset = candidateChunkCount(row.offset, "page offset");
    const nextOffset = candidateChunkCount(row.nextOffset, "page next offset");
    const all = chunkHashes(row.all), fresh = chunkHashes(row.fresh);
    if (row.schema !== 1 || row.scope !== "candidate-chunk-plan-page" ||
        typeof row.done !== "boolean" || offset !== expectedOffset || nextOffset > allCount ||
        nextOffset - offset !== all.length || all.length > TREE_CANDIDATE_CHUNK_PAGE_HASHES ||
        (row.done !== (nextOffset === allCount)) || (!row.done && all.length === 0)) {
        throw new TypeError("WASM tree returned inconsistent candidate chunk plan page");
    }
    let cursor = 0;
    for (const hash of fresh) {
        while (cursor < all.length && all[cursor] < hash) cursor++;
        if (all[cursor] !== hash) throw new TypeError("WASM tree chunk page fresh hashes are not a subset");
        cursor++;
    }
    Object.freeze(all);
    Object.freeze(fresh);
    return Object.freeze({ offset, nextOffset, done: row.done, all, fresh });
}

/** Open a candidate only after bounded native validation steps. New V2 trees
 * preallocate an admitted root before finish and capture their all-resident
 * baseline as one insertion-generation scalar. No borrowed view crosses await. */
export async function beginTreeCandidate(
    tree: CandidateJobTree,
    options: BeginTreeCandidateOptions,
): Promise<BeginTreeCandidateResult> {
    if (typeof options?.cooperate !== "function" || typeof options.onCandidateOpened !== "function" ||
        (options.onOpenMemoryPlan !== undefined && typeof options.onOpenMemoryPlan !== "function") ||
        (options.abortCandidateOpened !== undefined && typeof options.abortCandidateOpened !== "function")) {
        throw new TypeError("candidate begin requires host cooperation and an ownership handoff");
    }
    const cooperate = options.cooperate;
    const cooperateRetirementOption = options.cooperateRetirement;
    const signal = options.signal;
    const assertOwner = options.assertCurrent;
    const onCandidateOpened = options.onCandidateOpened;
    const onOpenMemoryPlan = options.onOpenMemoryPlan;
    const abortCandidateOpened = options.abortCandidateOpened;
    if (hasPendingTreeReachabilityRetirement(tree)) {
        await drainTreeReachabilityRetirement(tree);
    }
    const traversal = jobApi(tree);
    const deferred = deferredApi(tree);
    const openMemory = candidateOpenMemoryApi(tree);
    if (openMemory && !deferred) {
        throw new TypeError("candidate open memory API requires deferred reachability ownership");
    }
    if (onOpenMemoryPlan && !openMemory) {
        throw new TypeError("candidate open memory policy requires the complete memory API");
    }
    const hasCandidate = deferred?.hasCandidate ?? tree.has_candidate;
    const abortCandidate = deferred?.abortCandidate ?? tree.abort_candidate;
    const candidatePresent = () => {
        const value = hasCandidate.call(tree);
        if (typeof value !== "boolean") throw new TypeError("WASM tree returned invalid candidate presence");
        return value;
    };
    if (deferred && typeof cooperateRetirementOption !== "function") {
        throw new TypeError("deferred candidate begin requires uncancellable retirement cooperation");
    }
    const assertCurrent = () => {
        throwIfWorkAborted(signal);
        assertOwner?.();
    };
    assertCurrent();
    await cooperate();
    assertCurrent();

    if (deferred && !traversal) {
        throw new TypeError("deferred reachability requires the candidate job API");
    }
    if (!traversal) {
        let opened = false, handedOff = false;
        let failed = false, primary: unknown;
        try {
            tree.begin_candidate(); opened = true;
            onCandidateOpened(null); handedOff = true;
            return { cooperative: false, reachable: null };
        } catch (error) {
            failed = true; primary = error; throw error;
        } finally {
            if (opened && !handedOff) {
                try { if (candidatePresent()) abortCandidate.call(tree); }
                catch (cleanup) {
                    if (failed) reportCleanupFailure(primary, cleanup); else throw cleanup;
                }
            }
        }
    }

    const begin = traversal.begin;
    const step = deferred?.step ?? traversal.step;
    const finish = deferred?.finishBegin ?? traversal.finish;
    const cancel = traversal.cancel;
    const cooperateRetirement = cooperateRetirementOption!;
    const token = begin.call(tree);
    if (!Number.isSafeInteger(token) || token < 1 || token > 0xffff_ffff) {
        const failure = new TypeError("WASM tree job returned an invalid token");
        if (Number.isSafeInteger(token)) {
            try {
                if (deferred) {
                    retainRetirement(tree, token, deferred, cooperateRetirement, true);
                    await drainTreeReachabilityRetirement(tree);
                } else {
                    cancel.call(tree, token);
                }
            }
            catch (cleanup) { reportCleanupFailure(failure, cleanup); }
        }
        throw failure;
    }
    let jobOpen = true, candidateOpened = false, handedOff = false;
    let retained: PendingReachabilityRetirement | undefined;
    let resumeStarted = false;
    let finishedRevision: number | undefined;
    let completed = 0, reachable = 0;
    let failed = false, primary: unknown;
    try {
        for (;;) {
            assertCurrent();
            let raw: unknown;
            try {
                raw = step.call(tree, token, TREE_CANDIDATE_JOB_STEP_UNITS);
            } catch (error) {
                // Only the legacy step atomically discards on execution error.
                // The deferred step preserves exact same-token retirement.
                if (!deferred) jobOpen = false;
                throw error;
            }
            const current = progress(raw);
            if (current.completed !== completed + current.units || current.reachable < reachable ||
                current.reachable > current.completed) {
                throw new TypeError("WASM tree job progress is inconsistent");
            }
            completed = current.completed; reachable = current.reachable;
            if (current.done) break;
            await cooperate();
            assertCurrent();
        }
        assertCurrent();
        if (deferred) {
            const beforeFinish = deferred.revision.call(tree);
            if (!Number.isSafeInteger(beforeFinish) || beforeFinish < 0 ||
                beforeFinish >= Number.MAX_SAFE_INTEGER) {
                throw new TypeError("WASM tree returned an invalid candidate revision");
            }
            finishedRevision = beforeFinish + 1;
        }
        if (openMemory && deferred) {
            const plan = parseCandidateOpenMemoryPlan(openMemory.plan.call(tree, token));
            if (plan.scope !== CANDIDATE_OPEN_MEMORY_SCOPES[openMemory.version]) {
                throw new TypeError("candidate open memory plan tree version mismatch");
            }
            assertCurrent();
            // Register the exact cleanup slot before policy reservation or
            // native root allocation. Later transitions only mutate this owner.
            retained = retainRetirement(tree, token, deferred, cooperateRetirement, true);
            retained.active = true;
            if (onOpenMemoryPlan) {
                const rawOutput = onOpenMemoryPlan(plan);
                if (rawOutput === undefined) {
                    throw new TypeError("candidate open memory policy returned no ownership");
                }
                // Preserve a successfully reserved raw owner even if method
                // capture rejects a hostile/partial callback surface.
                retained.output = rawOutput;
                retained.output = captureOpenMemoryOwner(rawOutput);
            }
            assertCurrent();
            resumeStarted = true;
            openMemory.resume.call(tree, token, plan.rootIdentityRequestedBytes,
                plan.rootEndpointRequestedBytes, plan.rootStringRequestedBytes);
            retained.output?.ready();
            assertCurrent();
        }
        const validated = finish.call(tree, token);
        jobOpen = false;
        if (deferred) {
            // Native finish guarantees publication before its infallible O(1)
            // transition to retirement. Register that owner before any host
            // getter or validation can throw.
            candidateOpened = true;
            if (retained) {
                retained.needsCancel = false;
                retained.outputTransition = "published";
                retained.active = false;
                transitionCandidateOpenOutput(retained);
            } else {
                retained = retainRetirement(tree, token, deferred, cooperateRetirement, false);
            }
        } else {
            candidateOpened = candidatePresent();
        }
        if ((deferred && !candidatePresent()) || (!deferred && !candidateOpened)) {
            throw new TypeError("WASM tree job finished without a candidate");
        }
        if (deferred && deferred.revision.call(tree) !== finishedRevision) {
            throw new TypeError("WASM tree returned an invalid candidate revision");
        }
        if (!Number.isSafeInteger(validated) || validated < 0 || validated !== reachable) {
            throw new TypeError("WASM tree job returned an invalid reachable count");
        }
        if (deferred) {
            await drainTreeReachabilityRetirement(tree);
            assertCurrent();
            if (!candidatePresent() || deferred.revision.call(tree) !== finishedRevision) {
                throw new Error("candidate changed during reachability retirement");
            }
        }
        onCandidateOpened(validated); handedOff = true;
        return { cooperative: true, reachable: validated };
    } catch (error) {
        failed = true; primary = error; throw error;
    } finally {
        const cleanups: unknown[] = [];
        if (jobOpen) {
            try {
                if (deferred) {
                    if (retained) {
                        retained.outputTransition = resumeStarted ? "detached" : "releaseBeforeOutput";
                        retained.active = false;
                    } else {
                        retained = retainRetirement(tree, token, deferred, cooperateRetirement, true);
                    }
                    jobOpen = false;
                } else {
                    cancel.call(tree, token);
                }
            } catch (error) { cleanups.push(error); }
        }
        if (candidateOpened && !handedOff && finishedRevision !== undefined) {
            const override = abortCandidateOpened
                ? () => abortCandidateOpened(finishedRevision!)
                : undefined;
            try { requestCandidateAbortAfterRetirement(tree, finishedRevision, override, retained); }
            catch (error) { cleanups.push(error); }
        }
        if (hasPendingTreeReachabilityRetirement(tree)) {
            try { await drainTreeReachabilityRetirement(tree); }
            catch (error) { cleanups.push(error); }
        }
        if (candidateOpened && !handedOff && !hasPendingTreeReachabilityRetirement(tree)) {
            try {
                const stillOwned = !deferred || (finishedRevision !== undefined &&
                    deferred.revision.call(tree) === finishedRevision);
                if (stillOwned && candidatePresent()) {
                    if (abortCandidateOpened && finishedRevision !== undefined) {
                        abortCandidateOpened(finishedRevision);
                    } else {
                        abortCandidate.call(tree);
                    }
                }
            }
            catch (error) { cleanups.push(error); }
        }
        if (cleanups.length > 0) {
            if (failed) for (const cleanup of cleanups) reportCleanupFailure(primary, cleanup);
            else throw cleanups[0];
        }
    }
}

/** Collect both complete and transaction-fresh candidate chunk hashes through
 * one resumable graph traversal. Candidate/root state is read-only throughout;
 * sorting, hex conversion and result serialization remain a synchronous finish
 * residual. Cursor ownership itself is retired cooperatively. */
export async function collectTreeCandidateChunks(
    tree: CandidateJobTree,
    options: CollectTreeCandidateChunksOptions,
): Promise<TreeCandidateChunkPlan> {
    if (typeof options?.cooperate !== "function" || typeof options.legacy !== "function") {
        throw new TypeError("candidate chunk collection requires host cooperation and a fallback");
    }
    const cooperate = options.cooperate;
    const cooperateRetirementOption = options.cooperateRetirement;
    const signal = options.signal;
    const assertOwner = options.assertCurrent;
    const legacy = options.legacy;
    if (hasPendingTreeReachabilityRetirement(tree)) {
        await drainTreeReachabilityRetirement(tree);
    }
    const traversal = chunkJobApi(tree);
    const deferred = deferredApi(tree);
    const hasCandidate = deferred?.hasCandidate ?? tree.has_candidate;
    const candidatePresent = () => {
        const value = hasCandidate.call(tree);
        if (typeof value !== "boolean") throw new TypeError("WASM tree returned invalid candidate presence");
        return value;
    };
    if (deferred && typeof cooperateRetirementOption !== "function") {
        throw new TypeError("deferred candidate chunks require uncancellable retirement cooperation");
    }
    const assertCurrent = () => {
        throwIfWorkAborted(signal);
        assertOwner?.();
        if (!candidatePresent()) throw new Error("candidate disappeared during chunk collection");
    };
    assertCurrent();
    await cooperate();
    assertCurrent();
    if (deferred && !traversal) {
        throw new TypeError("deferred reachability requires the candidate chunk job API");
    }
    if (!traversal) {
        const plan = chunkPlan(legacy());
        assertCurrent();
        return plan;
    }

    const begin = traversal.begin;
    const step = deferred?.step ?? traversal.step;
    const finish = deferred?.finishChunks ?? traversal.finish;
    const cancel = traversal.cancel;
    const cooperateRetirement = cooperateRetirementOption!;
    const expectedRevision = deferred?.revision.call(tree);
    if (deferred && (!Number.isSafeInteger(expectedRevision) || expectedRevision! < 0)) {
        throw new TypeError("WASM tree returned an invalid candidate revision");
    }
    const token = begin.call(tree);
    if (!Number.isSafeInteger(token) || token < 1 || token > 0xffff_ffff) {
        const failure = new TypeError("WASM tree chunk job returned an invalid token");
        if (Number.isSafeInteger(token)) {
            try {
                if (deferred) {
                    retainRetirement(tree, token, deferred, cooperateRetirement, true);
                    await drainTreeReachabilityRetirement(tree);
                } else {
                    cancel.call(tree, token);
                }
            }
            catch (cleanup) { reportCleanupFailure(failure, cleanup); }
        }
        throw failure;
    }
    let jobOpen = true, failed = false, primary: unknown;
    let completed = 0, reachable = 0;
    try {
        for (;;) {
            assertCurrent();
            let raw: unknown;
            try { raw = step.call(tree, token, TREE_CANDIDATE_JOB_STEP_UNITS); }
            catch (error) {
                if (!deferred) jobOpen = false;
                throw error;
            }
            const current = progress(raw);
            if (current.completed !== completed + current.units || current.reachable < reachable ||
                current.reachable > current.completed) {
                throw new TypeError("WASM tree chunk job progress is inconsistent");
            }
            completed = current.completed; reachable = current.reachable;
            assertCurrent();
            if (current.done) break;
            await cooperate();
            assertCurrent();
        }
        assertCurrent();
        const result = finish.call(tree, token);
        jobOpen = false;
        if (deferred) retainRetirement(tree, token, deferred, cooperateRetirement, false);
        const plan = chunkPlan(result);
        if (plan.all.length !== reachable) {
            throw new TypeError("WASM tree chunk job returned an invalid reachable count");
        }
        if (deferred) {
            await drainTreeReachabilityRetirement(tree);
            assertCurrent();
            if (!candidatePresent() || deferred.revision.call(tree) !== expectedRevision) {
                throw new Error("candidate changed during reachability retirement");
            }
        }
        assertCurrent();
        return plan;
    } catch (error) {
        failed = true; primary = error; throw error;
    } finally {
        const cleanups: unknown[] = [];
        if (jobOpen) {
            try {
                if (deferred) {
                    retainRetirement(tree, token, deferred, cooperateRetirement, true);
                    jobOpen = false;
                } else {
                    cancel.call(tree, token);
                }
            } catch (cleanup) { cleanups.push(cleanup); }
        }
        if (hasPendingTreeReachabilityRetirement(tree)) {
            try { await drainTreeReachabilityRetirement(tree); }
            catch (cleanup) { cleanups.push(cleanup); }
        }
        if (cleanups.length > 0) {
            if (failed) for (const cleanup of cleanups) reportCleanupFailure(primary, cleanup);
            else throw cleanups[0];
        }
    }
}

/** Cooperatively prepare and consume the candidate chunk plan without ever
 * materializing both complete native result arrays in JavaScript. Packaged
 * trees page one sorted plan; older structural ports retain the legacy result
 * only for compatibility and are sliced into the same bounded sink contract. */
export async function collectTreeCandidateChunkPages(
    tree: CandidateJobTree,
    options: CollectTreeCandidateChunkPagesOptions,
): Promise<TreeCandidateChunkPageSummary> {
    if (typeof options?.cooperate !== "function" || typeof options.legacy !== "function" ||
        typeof options.cooperateRetirement !== "function" ||
        typeof options.onSortMemoryPlan !== "function" || typeof options.onPage !== "function") {
        throw new TypeError("candidate chunk pages require cooperation, admission, a sink and a fallback");
    }
    if (hasPendingTreeReachabilityRetirement(tree)) await drainTreeReachabilityRetirement(tree);
    const paged = candidateChunkPagedApi(tree);
    if (!paged) {
        const plan = await collectTreeCandidateChunks(tree, options);
        let freshOffset = 0;
        for (let offset = 0; offset < plan.all.length;) {
            const nextOffset = Math.min(plan.all.length, offset + TREE_CANDIDATE_CHUNK_PAGE_HASHES);
            const all = plan.all.slice(offset, nextOffset);
            const fresh: string[] = [];
            while (freshOffset < plan.fresh.length && plan.fresh[freshOffset] < all[0]) freshOffset++;
            while (freshOffset < plan.fresh.length && plan.fresh[freshOffset] <= all[all.length - 1]) {
                fresh.push(plan.fresh[freshOffset++]);
            }
            Object.freeze(all); Object.freeze(fresh);
            const page = Object.freeze({
                offset, nextOffset, done: nextOffset === plan.all.length, all, fresh,
            });
            throwIfWorkAborted(options.signal);
            options.assertCurrent?.();
            await options.onPage(page, plan.all.length);
            throwIfWorkAborted(options.signal);
            options.assertCurrent?.();
            offset = nextOffset;
            if (!page.done) {
                await options.cooperate();
                throwIfWorkAborted(options.signal);
                options.assertCurrent?.();
            }
        }
        return { allCount: plan.all.length, freshCount: plan.fresh.length };
    }

    const traversal = chunkJobApi(tree);
    const deferred = deferredApi(tree);
    if (!traversal || !deferred) {
        throw new TypeError("candidate chunk page API requires deferred candidate traversal");
    }
    const signal = options.signal;
    const candidatePresent = () => {
        const value = deferred.hasCandidate.call(tree);
        if (typeof value !== "boolean") throw new TypeError("WASM tree returned invalid candidate presence");
        return value;
    };
    const assertCurrent = () => {
        throwIfWorkAborted(signal);
        options.assertCurrent?.();
        if (!candidatePresent()) throw new Error("candidate disappeared during chunk collection");
        throwIfWorkAborted(signal);
    };
    assertCurrent();
    await options.cooperate();
    assertCurrent();
    const expectedRevision = deferred.revision.call(tree);
    if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 0) {
        throw new TypeError("WASM tree returned an invalid candidate revision");
    }
    const token = traversal.begin.call(tree);
    if (!Number.isSafeInteger(token) || token < 1 || token > 0xffff_ffff) {
        const failure = new TypeError("WASM tree chunk job returned an invalid token");
        if (Number.isSafeInteger(token)) {
            try {
                retainRetirement(tree, token, deferred, options.cooperateRetirement, true);
                await drainTreeReachabilityRetirement(tree);
            } catch (cleanup) { reportCleanupFailure(failure, cleanup); }
        }
        throw failure;
    }

    let jobOpen = true, failed = false, primary: unknown;
    let completed = 0, reachable = 0, sortOwnerAttached = false, sortResumed = false;
    let sortOwner: CandidateOpenMemoryOwner | undefined;
    try {
        for (;;) {
            assertCurrent();
            const current = progress(deferred.step.call(tree, token, TREE_CANDIDATE_JOB_STEP_UNITS));
            if (current.completed !== completed + current.units || current.reachable < reachable ||
                current.reachable > current.completed) {
                throw new TypeError("WASM tree chunk job progress is inconsistent");
            }
            completed = current.completed; reachable = current.reachable;
            assertCurrent();
            if (current.done) break;
            await options.cooperate();
            assertCurrent();
        }

        assertCurrent();
        const memoryPlan = candidateChunkSortPlan(paged.plan.call(tree, token), reachable);
        await options.cooperate();
        assertCurrent();
        const rawOwner = await options.onSortMemoryPlan(memoryPlan);
        if (!rawOwner || typeof rawOwner !== "object") {
            throw new TypeError("invalid candidate chunk sort memory owner");
        }
        const release = rawOwner.release;
        if (typeof release !== "function") throw new TypeError("invalid candidate chunk sort memory owner");
        let released = false;
        const releaseOnce = () => {
            if (released) return;
            const result: unknown = Reflect.apply(release, rawOwner, []);
            if (result !== undefined) {
                if (result instanceof Promise) void result.catch(() => {});
                throw new TypeError("candidate chunk sort memory release must return undefined");
            }
            released = true;
        };
        sortOwner = {
            releaseBeforeOutput: releaseOnce,
            ready: () => {}, published: () => {}, detached: () => {}, retired: releaseOnce,
        };
        assertCurrent();
        paged.resume.call(tree, token, memoryPlan.sourceHashesRequestedBytes,
            memoryPlan.scratchHashesRequestedBytes);
        sortResumed = true;

        if (reachable > 0) {
            let sortCompleted = 0;
            for (;;) {
                assertCurrent();
                const current = candidateChunkSortProgress(
                    paged.step.call(tree, token, TREE_CANDIDATE_CHUNK_SORT_STEP_UNITS), reachable);
                if (current.completed !== sortCompleted + current.units) {
                    throw new TypeError("WASM tree candidate chunk sort progress is inconsistent");
                }
                sortCompleted = current.completed;
                assertCurrent();
                if (current.done) break;
                await options.cooperate();
                assertCurrent();
            }
        }

        const allCount = candidateChunkPlanInfo(paged.info.call(tree, token), reachable);
        let offset = 0, freshCount = 0;
        let previousAll: string | undefined, previousFresh: string | undefined;
        while (offset < allCount) {
            assertCurrent();
            const page = candidateChunkPage(paged.read.call(tree, token, offset,
                TREE_CANDIDATE_CHUNK_PAGE_HASHES), offset, allCount);
            if ((previousAll && previousAll >= page.all[0]) ||
                (previousFresh && page.fresh.length > 0 && previousFresh >= page.fresh[0])) {
                throw new TypeError("WASM tree candidate chunk pages are not globally canonical");
            }
            previousAll = page.all[page.all.length - 1];
            if (page.fresh.length > 0) previousFresh = page.fresh[page.fresh.length - 1];
            freshCount += page.fresh.length;
            assertCurrent();
            await options.onPage(page, allCount);
            assertCurrent();
            offset = page.nextOffset;
            if (!page.done) await options.cooperate();
        }
        assertCurrent();
        paged.finish.call(tree, token);
        jobOpen = false;
        const retained = retainRetirement(tree, token, deferred, options.cooperateRetirement,
            false, sortOwner);
        retained.outputTransition = "detached";
        sortOwnerAttached = true;
        await drainTreeReachabilityRetirement(tree);
        assertCurrent();
        if (!candidatePresent() || deferred.revision.call(tree) !== expectedRevision) {
            throw new Error("candidate changed during reachability retirement");
        }
        return { allCount, freshCount };
    } catch (error) {
        failed = true; primary = error; throw error;
    } finally {
        const cleanups: unknown[] = [];
        if (jobOpen) {
            try {
                const retained = retainRetirement(tree, token, deferred, options.cooperateRetirement,
                    true, sortResumed ? sortOwner : undefined);
                if (sortResumed && sortOwner) retained.outputTransition = "detached";
                sortOwnerAttached = sortResumed && !!sortOwner;
                jobOpen = false;
            } catch (cleanup) { cleanups.push(cleanup); }
        }
        if (hasPendingTreeReachabilityRetirement(tree)) {
            try { await drainTreeReachabilityRetirement(tree); }
            catch (cleanup) { cleanups.push(cleanup); }
        }
        if (sortOwner && !sortOwnerAttached) {
            try { sortOwner.retired(); }
            catch (cleanup) { cleanups.push(cleanup); }
        }
        if (cleanups.length > 0) {
            if (failed) for (const cleanup of cleanups) reportCleanupFailure(primary, cleanup);
            else throw cleanups[0];
        }
    }
}
