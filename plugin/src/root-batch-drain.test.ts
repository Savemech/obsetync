import { strict as assert } from "node:assert";
import { runRootBatchDrain, type RootBatchDrainOptions } from "./root-batch-drain";
import { EngineWorkTracker } from "./engine-work";

function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const microtasks = async () => { await Promise.resolve(); await Promise.resolve(); };
let assertions = 0;
function same(actual: unknown, expected: unknown, message: string): void {
    assertions++; assert.deepEqual(actual, expected, message);
}
function check(actual: unknown, message: string): void {
    assertions++; assert.ok(actual, message);
}
async function exactRejection(promise: Promise<unknown>, failure: unknown): Promise<void> {
    assertions++; await assert.rejects(promise, error => error === failure);
}

async function unconditionalFirstAttemptAndNoProgress(): Promise<void> {
    for (const outcome of [undefined, "continue"] as const) {
        let attempts = 0, conditions = 0, yields = 0;
        const work = runRootBatchDrain({
            attempt: async () => { attempts++; return outcome; },
            shouldContinue: () => { conditions++; return false; },
            cooperate: async () => { yields++; },
        });
        same(attempts, 1, "first attempt was not synchronously admitted without an eligibility check");
        same(conditions, 0, "condition was consulted before first attempt actually settled");
        await work;
        same([attempts, conditions, yields], [1, Number(outcome === "continue"), 0],
            "no progress or closed eligibility admitted another operation");
    }
    let conditions = 0;
    await runRootBatchDrain({ attempt: async () => undefined,
        shouldContinue: () => { conditions++; return true; },
        cooperate: async () => { throw new Error("no-progress drain must not yield/retry"); } });
    same(conditions, 0, "no-progress consulted eligibility despite being terminal");
}

async function hundredsOfOrderedNonOverlappingBatches(): Promise<void> {
    const total = 257, events: string[] = [];
    let count = 0, active = 0;
    await runRootBatchDrain({
        attempt: async () => {
            check(active === 0, "attempts overlapped"); active++;
            const current = ++count; events.push(`attempt:${current}`);
            try { await microtasks(); return current < total ? "continue" : undefined; }
            finally { active--; events.push(`settled:${current}`); }
        },
        shouldContinue: () => { check(active === 0, "predicate ran before operation cleanup"); events.push("check"); return true; },
        cooperate: async () => {
            check(active === 0, "cooperation overlapped an actual attempt");
            events.push("yield"); await microtasks(); events.push("resumed");
        },
    });
    const expected: string[] = [];
    for (let i = 1; i <= total; i++) {
        expected.push(`attempt:${i}`, `settled:${i}`);
        if (i < total) expected.push("check", "yield", "resumed", "check");
    }
    same(events, expected, "drain did not preserve attempt/cleanup/check/yield/check order");
    same([count, active], [total, 0], "drain dropped or retained an actual attempt");
}

async function eligibilityChangeAcrossHeldCooperation(): Promise<void> {
    const host = deferred<void>(), entered = deferred<void>();
    let eligible = true, attempts = 0, conditions = 0, done = false;
    const work = runRootBatchDrain({
        attempt: async () => { attempts++; return "continue"; },
        shouldContinue: () => { conditions++; return eligible; },
        cooperate: () => { entered.resolve(); return host.promise; },
    });
    void work.then(() => { done = true; });
    await entered.promise;
    same([attempts, conditions, done], [1, 1, false], "host wait was not part of the owned tail");
    eligible = false; await microtasks();
    same([attempts, conditions, done], [1, 1, false], "stop virtually completed or retried held host cooperation");
    host.resolve(); await work;
    same([attempts, conditions, done], [1, 2, true], "condition was not rechecked after host resumed");
}

async function errorsNeverBecomeRetrySignals(): Promise<void> {
    for (const kind of ["attempt-async", "attempt-sync", "cooperation", "condition-before", "condition-after"] as const) {
        const failure = new Error(kind); failure.name = kind === "cooperation" ? "AbortError" : "Error";
        let attempts = 0, conditions = 0, yields = 0;
        const work = runRootBatchDrain({
            attempt: () => {
                attempts++;
                if (kind === "attempt-sync") throw failure;
                return kind === "attempt-async" ? Promise.reject(failure) : Promise.resolve("continue");
            },
            shouldContinue: () => {
                conditions++;
                if (kind === "condition-before" || (kind === "condition-after" && conditions === 2)) throw failure;
                return true;
            },
            cooperate: async () => { yields++; if (kind === "cooperation") throw failure; },
        });
        await exactRejection(work, failure); await microtasks();
        same(attempts, 1, "failed attempt/cooperation/predicate was retried");
        same([conditions, yields], kind.startsWith("attempt") ? [0, 0] :
            kind === "condition-before" ? [1, 0] : kind === "cooperation" ? [1, 1] : [2, 1],
        "drain performed work after the actual failure boundary");
    }
    const laterFailure = new Error("batch 130 failed");
    let attempts = 0, yields = 0;
    await exactRejection(runRootBatchDrain({
        attempt: async () => { if (++attempts === 130) throw laterFailure; return "continue"; },
        shouldContinue: () => true,
        cooperate: async () => { yields++; },
    }), laterFailure);
    same([attempts, yields], [130, 129], "later failure was swallowed or restarted the drain");
}

async function closeWaitsForActualAttemptAndCleanup(): Promise<void> {
    for (const outcome of ["continue", "void", "abort"] as const) {
        const tracker = new EngineWorkTracker(), abort = new AbortController();
        const native = deferred<void>(), cleanup = deferred<void>(), cleanupEntered = deferred<void>();
        const failure = new Error("actual operation aborted"); failure.name = "AbortError";
        let attempts = 0, yields = 0, drainDone = false, closeDone = false;
        const release = tracker.enter();
        const work = runRootBatchDrain({
            attempt: async () => {
                attempts++;
                try { await native.promise; return outcome === "continue" ? "continue" : undefined; }
                finally { cleanupEntered.resolve(); await cleanup.promise; }
            },
            shouldContinue: () => !abort.signal.aborted,
            cooperate: async () => { yields++; },
        }).finally(() => { release(); drainDone = true; });
        const observed = outcome === "abort" ? exactRejection(work, failure) : work;
        same(tracker.snapshot().active, 1, "complete drain was not synchronously tracked");
        abort.abort();
        const close = tracker.closeAndDrain();
        void close.then(() => { closeDone = true; });
        await microtasks();
        same([drainDone, closeDone, attempts, yields], [false, false, 1, 0],
            "abort/close virtually completed native operation or started another batch");
        if (outcome === "abort") native.reject(failure); else native.resolve();
        await cleanupEntered.promise; await microtasks();
        same([drainDone, closeDone, tracker.snapshot().active], [false, false, 1],
            "native settlement released drain before its real cleanup/accepted tail");
        cleanup.resolve(); await observed; await close;
        same([drainDone, closeDone, attempts, yields, tracker.snapshot().active], [true, true, 1, 0, 0],
            "actual drain completion lost close ownership or retried after abort");
    }
}

async function callbacksCapturedBeforeAwait(): Promise<void> {
    const first = deferred<"continue">();
    let attempts = 0, conditions = 0, yields = 0;
    const options: RootBatchDrainOptions = {
        attempt: () => ++attempts === 1 ? first.promise : Promise.resolve(),
        shouldContinue: () => { conditions++; return true; },
        cooperate: async () => { yields++; },
    };
    const work = runRootBatchDrain(options);
    options.attempt = async () => { throw new Error("replacement attempt must not be adopted"); };
    options.shouldContinue = () => { throw new Error("replacement predicate must not be adopted"); };
    options.cooperate = async () => { throw new Error("replacement cooperation must not be adopted"); };
    first.resolve("continue"); await work;
    same([attempts, conditions, yields], [2, 2, 1], "callback mutation changed an admitted drain's ownership");
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed) { console.error("root batch drain tests did not settle their actual owners"); process.exitCode = 1; }
});
void (async () => {
    await unconditionalFirstAttemptAndNoProgress();
    await hundredsOfOrderedNonOverlappingBatches();
    await eligibilityChangeAcrossHeldCooperation();
    await errorsNeverBecomeRetrySignals();
    await closeWaitsForActualAttemptAndCleanup();
    await callbacksCapturedBeforeAwait();
    completed = true;
    console.log(`root batch drain: ${assertions} assertions passed`);
})().catch(error => { console.error(error); process.exitCode = 1; });
