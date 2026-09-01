import {
    AdaptiveHashTuner,
    hashTuningForRuntime,
    planByteBoundedBatches,
} from "./hash-runtime";

const check = (condition: unknown, message: string) => {
    if (!condition) throw new Error(message);
};

function platformBudgetsAreBounded(): void {
    const desktop = hashTuningForRuntime("desktop");
    const mobile = hashTuningForRuntime("mobile");
    check(desktop.feedBytes === 512 * 1024, "desktop feed is not 512 KiB");
    check(desktop.maxFeedBytes === 1024 * 1024, "desktop feed cap is not 1 MiB");
    check(desktop.maxBatchFiles === 256, "desktop file budget is wrong");
    check(desktop.maxBatchBytes === 8 * 1024 * 1024, "desktop byte budget is wrong");
    check(mobile.feedBytes === 256 * 1024, "mobile feed is not 256 KiB");
    check(mobile.maxFeedBytes === 256 * 1024, "mobile feed cap is not bounded");
    check(mobile.maxBatchFiles === 64, "mobile file budget is wrong");
    check(mobile.maxBatchBytes === 2 * 1024 * 1024, "mobile byte budget is wrong");
    check(mobile.applyConcurrency === 8, "mobile apply concurrency is not bounded");
}

function bytePlannerHonorsEveryLimit(): void {
    const items = [4, 4, 4, 20, 1, 1, 1, 1];
    const batches = planByteBoundedBatches(items, (size) => size, {
        maxFiles: 3,
        maxBytes: 10,
        maxSingleBytes: 8,
    });
    check(JSON.stringify(batches) === "[[4,4],[4],[20],[1,1,1],[1]]", "batch plan differs");
    for (const batch of batches) {
        check(batch.length <= 3 || batch.length === 1, "file cap exceeded");
        const bytes = batch.reduce((sum, size) => sum + size, 0);
        check(bytes <= 10 || batch.length === 1, "byte cap exceeded outside singleton");
    }
    check(planByteBoundedBatches([], (size) => size, {
        maxFiles: 1,
        maxBytes: 1,
        maxSingleBytes: 1,
    }).length === 0, "empty input created a batch");
}

function lagAndPressureReduceFeedThenHealthyWorkRaisesIt(): void {
    const tuner = new AdaptiveHashTuner("desktop");
    check(tuner.current().feedBytes === 512 * 1024, "unexpected initial feed");
    tuner.observe({ bytes: 1024 * 1024, durationMs: 2, eventLoopLagMs: 50 });
    check(tuner.current().feedBytes === 256 * 1024, "lag did not reduce feed");
    tuner.observe({ bytes: 1024, durationMs: 1, memoryPressure: true });
    tuner.observe({ bytes: 1024, durationMs: 1, memoryPressure: true });
    check(tuner.current().feedBytes === 64 * 1024, "minimum feed clamp failed");
    for (let i = 0; i < 4; i++) {
        tuner.observe({ bytes: 1024 * 1024, durationMs: 2, eventLoopLagMs: 1 });
    }
    check(tuner.current().feedBytes === 128 * 1024, "healthy samples did not raise feed");
}

function bytePlannerFlushesAtHoldDeadline(): void {
    const observedAt = [0, 3, 11, 12];
    let clockIndex = 0;
    const batches = planByteBoundedBatches([1, 2, 3, 4], (size) => size, {
        maxFiles: 10,
        maxBytes: 100,
        maxSingleBytes: 100,
        maxHoldMs: 10,
    }, () => observedAt[clockIndex++]);
    check(JSON.stringify(batches) === "[[1,2],[3,4]]", "hold deadline did not flush");
    check(clockIndex === observedAt.length, "planner did not sample every arrival");

    let rejected = false;
    try {
        planByteBoundedBatches([1], (size) => size, {
            maxFiles: 1,
            maxBytes: 1,
            maxSingleBytes: 1,
            maxHoldMs: 0,
        });
    } catch (error) {
        rejected = error instanceof RangeError;
    }
    check(rejected, "non-positive hold deadline was accepted");
}

platformBudgetsAreBounded();
bytePlannerHonorsEveryLimit();
lagAndPressureReduceFeedThenHealthyWorkRaisesIt();
bytePlannerFlushesAtHoldDeadline();
console.log("hash-runtime.test: 27 assertions passed");
