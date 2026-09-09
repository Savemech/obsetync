import { readFile } from "node:fs/promises";

export const NATIVE_ROOT_INSTRUMENTATION_LIMITS = Object.freeze({ plans: 256, batchesPerPlan: 4096 });

function replaceOnce(source, before, after) {
    if (source.split(before).length !== 2) throw new Error("native root instrumentation source seam changed");
    return source.replace(before, after);
}

// These observers are appended only to the in-memory benchmark bundle. The
// same production source on disk is never rewritten. A completion belongs to
// the collector captured at invocation, even if the global phase has ended.
const common = `
function __nativeRootMetricIncrement(work: any, key: string): void {
    work[key] = (work[key] ?? 0) + 1;
}
function __nativeRootMetricObserve(work: any, observation: () => void): void {
    try { observation(); }
    catch { try { __nativeRootMetricIncrement(work, "instrumentationErrors"); } catch {} }
}
`;

const plannerObserver = `${common}
export function createRootBatchPlan(...args: Parameters<typeof __nativeRootCreatePlan>): ReturnType<typeof __nativeRootCreatePlan> {
    const work = (globalThis as any).__nativeRootWork;
    if (!work) return __nativeRootCreatePlan(...args);
    const started = performance.now();
    const input = { ready: args[0]?.length, queued: args[1]?.length, dependencies: args[2]?.length ?? 0 };
    __nativeRootMetricObserve(work, () => __nativeRootMetricIncrement(work, "planCreatesRequested"));
    let pending: ReturnType<typeof __nativeRootCreatePlan>;
    try { pending = __nativeRootCreatePlan(...args); }
    catch (error) {
        __nativeRootMetricObserve(work, () => __nativeRootMetricIncrement(work, "planCreatesFailed"));
        throw error;
    }
    pending.then(plan => {
        __nativeRootMetricObserve(work, () => {
            __nativeRootMetricIncrement(work, "planCreatesCompleted");
            if (work.plans.length >= ${NATIVE_ROOT_INSTRUMENTATION_LIMITS.plans}) {
                __nativeRootMetricIncrement(work, "plansDropped"); return;
            }
            const sample = { ...input, creationMs: performance.now() - started, selectionMs: 0,
                selectionCalls: 0, selectionFailures: 0, disposalCalls: 0, disposalFailures: 0,
                initial: plan.snapshot(), latest: plan.snapshot(), batches: [] as number[], batchSamplesDropped: 0 };
            const next = plan.next, dispose = plan.dispose;
            plan.next = function(...selectedArgs: Parameters<typeof next>): ReturnType<typeof next> {
                const began = performance.now();
                let failed = true;
                let result: ReturnType<typeof next>;
                try { result = Reflect.apply(next, this, selectedArgs); failed = false; return result; }
                finally { __nativeRootMetricObserve(work, () => {
                    sample.selectionCalls++; sample.selectionMs += performance.now() - began;
                    if (failed) sample.selectionFailures++;
                    else if (sample.batches.length < ${NATIVE_ROOT_INSTRUMENTATION_LIMITS.batchesPerPlan}) sample.batches.push(result?.ready.length ?? 0);
                    else { sample.batchSamplesDropped++; __nativeRootMetricIncrement(work, "batchSamplesDropped"); }
                    sample.latest = plan.snapshot();
                }); }
            };
            plan.dispose = function(...disposeArgs: Parameters<typeof dispose>): ReturnType<typeof dispose> {
                let failed = true;
                try { const result = Reflect.apply(dispose, this, disposeArgs); failed = false; return result; }
                finally { __nativeRootMetricObserve(work, () => {
                    sample.disposalCalls++; if (failed) sample.disposalFailures++;
                    sample.latest = plan.snapshot();
                }); }
            };
            work.plans.push(sample);
        });
    }, () => { __nativeRootMetricObserve(work, () => __nativeRootMetricIncrement(work, "planCreatesFailed")); });
    return pending;
}
`;

const schedulerObserver = `${common}
export function yieldWork(...args: Parameters<typeof __nativeRootYieldWork>): ReturnType<typeof __nativeRootYieldWork> {
    const work = (globalThis as any).__nativeRootWork;
    if (!work) return __nativeRootYieldWork(...args);
    let candidateMutationLoop = false;
    __nativeRootMetricObserve(work, () => {
        __nativeRootMetricIncrement(work, "hostYieldsRequested");
        candidateMutationLoop = (work.candidateMutationActiveLoops ?? 0) > 0;
        if (candidateMutationLoop) __nativeRootMetricIncrement(work, "candidateMutationLoopYieldsRequested");
    });
    let pending: ReturnType<typeof __nativeRootYieldWork>;
    try { pending = __nativeRootYieldWork(...args); }
    catch (error) {
        __nativeRootMetricObserve(work, () => {
            __nativeRootMetricIncrement(work, "hostYieldsFailed");
            if (candidateMutationLoop) __nativeRootMetricIncrement(work, "candidateMutationLoopYieldsFailed");
        });
        throw error;
    }
    pending.then(() => {
        __nativeRootMetricObserve(work, () => {
            __nativeRootMetricIncrement(work, "hostYieldsCompleted");
            if (candidateMutationLoop) __nativeRootMetricIncrement(work, "candidateMutationLoopYieldsCompleted");
        });
    }, () => { __nativeRootMetricObserve(work, () => {
        __nativeRootMetricIncrement(work, "hostYieldsFailed");
        if (candidateMutationLoop) __nativeRootMetricIncrement(work, "candidateMutationLoopYieldsFailed");
    }); });
    return pending;
}
`;

const reviewedQueueObserver = `${common}
const __nativeRootRetainFailedIndependentSelection = RootReviewedQueue.prototype.retainFailedIndependentSelection;
RootReviewedQueue.prototype.retainFailedIndependentSelection = function(
    ...args: Parameters<typeof __nativeRootRetainFailedIndependentSelection>
): ReturnType<typeof __nativeRootRetainFailedIndependentSelection> {
    const work = (globalThis as any).__nativeRootWork;
    const retained = Reflect.apply(__nativeRootRetainFailedIndependentSelection, this, args);
    if (retained && work) {
        __nativeRootMetricObserve(work, () => __nativeRootMetricIncrement(work, "sourceDriftRetentionsAccepted"));
    }
    return retained;
};
`;

/** Transparent bundle-only seams for actual production planner/scheduler and
 * the reviewed-queue source-drift retention decision.
 *
 * The caller installs globalThis.__nativeRootWork ONLY for the measured phase:
 *   { plans: [], hostYieldsRequested: 0, hostYieldsCompleted: 0,
 *     candidateMutationActiveLoops: 0 }
 * Additive counters are initialized lazily: planCreatesRequested/Completed/
 * Failed, plansDropped, batchSamplesDropped, hostYieldsFailed,
 * candidateMutationLoopYieldsRequested/Completed/Failed,
 * sourceDriftRetentionsAccepted, instrumentationErrors.
 * A successful plan record contains only aggregate snapshots and <=4096 batch
 * lengths; <=256 plans are retained. Dropped observations never change actual
 * scheduling or publication. A report with drops/errors is not complete evidence.
 *
 * Original Promise and plan/selection identities are returned unchanged;
 * observers join actual completions, never Promise.race or fake timer yields.
 * As with any rejection observer, native rejection is marked observed; this is
 * not an unhandled-rejection detector or suite-completion guard. Instrumentation
 * and per-plan snapshots add overhead; no native performance baseline is implied.
 */
export function nativeRootInstrumentationPlugin() {
    return { name: "native-root-work-observers", setup(builder) {
        builder.onLoad({ filter: /[\\/]root-batch-plan\.ts$/ }, async ({ path }) => ({
            contents: replaceOnce(await readFile(path, "utf8"),
                "export async function createRootBatchPlan(", "async function __nativeRootCreatePlan(") + plannerObserver,
            loader: "ts",
        }));
        builder.onLoad({ filter: /[\\/]work-scheduler\.ts$/ }, async ({ path }) => ({
            contents: replaceOnce(await readFile(path, "utf8"),
                "export function yieldWork(", "function __nativeRootYieldWork(") + schedulerObserver,
            loader: "ts",
        }));
        builder.onLoad({ filter: /[\\/]root-reviewed-queue\.ts$/ }, async ({ path }) => {
            const source = await readFile(path, "utf8");
            const seam = "    retainFailedIndependentSelection(hints: readonly DirtyFileChange[]): boolean {";
            return { contents: replaceOnce(source, seam, seam) + reviewedQueueObserver, loader: "ts" };
        });
    } };
}
