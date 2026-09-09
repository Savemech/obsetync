import assert from "node:assert/strict";
import { MaxLatencyCoalescer, type CoalescerTimers } from "./max-latency-coalescer";

class FakeTimers implements CoalescerTimers {
    nowMs = 0;
    private next = 1;
    private callbacks = new Map<number, { at: number; callback: () => void }>();

    now(): number { return this.nowMs; }
    setTimeout(callback: () => void, delayMs: number): number {
        const id = this.next++;
        this.callbacks.set(id, { at: this.nowMs + delayMs, callback });
        return id;
    }
    clearTimeout(handle: unknown): void { this.callbacks.delete(handle as number); }
    advance(ms: number): void {
        const target = this.nowMs + ms;
        for (;;) {
            const due = [...this.callbacks.entries()]
                .filter(([, row]) => row.at <= target)
                .sort((a, b) => a[1].at - b[1].at || a[0] - b[0])[0];
            if (!due) break;
            this.callbacks.delete(due[0]);
            this.nowMs = due[1].at;
            due[1].callback();
        }
        this.nowMs = target;
    }
    size(): number { return this.callbacks.size; }
}

async function turn(): Promise<void> {
    await Promise.resolve();
    await Promise.resolve();
}

async function quietBurstRunsOnce(): Promise<void> {
    const timers = new FakeTimers();
    let runs = 0;
    const scheduler = new MaxLatencyCoalescer({
        quietMs: 100, maxLatencyMs: 500, timers, run: () => { runs++; },
    });
    scheduler.trigger();
    timers.advance(60);
    scheduler.trigger();
    timers.advance(99);
    assert.equal(runs, 0);
    timers.advance(1);
    await turn();
    assert.equal(runs, 1);
    assert.deepEqual(scheduler.snapshot(), {
        pending: false, running: false, closed: false, pendingForMs: 0,
    });
}

async function continuousBurstHasMaximumLatency(): Promise<void> {
    const timers = new FakeTimers();
    const calls: number[] = [];
    const scheduler = new MaxLatencyCoalescer({
        quietMs: 100, maxLatencyMs: 250, timers, run: () => { calls.push(timers.now()); },
    });
    scheduler.trigger();
    for (let elapsed = 0; elapsed < 240; elapsed += 40) {
        timers.advance(40);
        scheduler.trigger();
    }
    timers.advance(10);
    await turn();
    assert.deepEqual(calls, [250]);
}

async function runningCallbackProducesOneFollowup(): Promise<void> {
    const timers = new FakeTimers();
    let release!: () => void;
    let runs = 0;
    const scheduler = new MaxLatencyCoalescer({
        quietMs: 10,
        maxLatencyMs: 50,
        timers,
        run: () => {
            runs++;
            if (runs === 1) return new Promise<void>(resolve => { release = resolve; });
        },
    });
    scheduler.trigger();
    timers.advance(10);
    assert.equal(runs, 1);
    scheduler.trigger();
    scheduler.trigger();
    timers.advance(100);
    assert.equal(runs, 1, "a running callback was overlapped");
    release();
    await turn();
    timers.advance(10);
    await turn();
    assert.equal(runs, 2, "events during callback did not coalesce into one follow-up");
}

async function errorsAndCloseAreContained(): Promise<void> {
    const timers = new FakeTimers();
    const errors: unknown[] = [];
    const failure = new Error("expected");
    const scheduler = new MaxLatencyCoalescer({
        quietMs: 10, maxLatencyMs: 20, timers,
        run: () => { throw failure; },
        onError: error => errors.push(error),
    });
    scheduler.trigger();
    timers.advance(10);
    await turn();
    assert.deepEqual(errors, [failure]);
    scheduler.trigger();
    scheduler.close();
    timers.advance(100);
    assert.equal(errors.length, 1);
    assert.equal(timers.size(), 0);
    assert.equal(scheduler.snapshot().closed, true);
}

void (async () => {
    await quietBurstRunsOnce();
    await continuousBurstHasMaximumLatency();
    await runningCallbackProducesOneFollowup();
    await errorsAndCloseAreContained();
    console.log("max-latency-coalescer.test: passed");
})().catch(error => { setTimeout(() => { throw error; }, 0); });
