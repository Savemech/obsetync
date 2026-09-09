import { strict as assert } from "node:assert";
import test from "node:test";
import { createNativeRootMetrics, median, nearestRankPercentile, NATIVE_ROOT_METRIC_LIMITS } from "./lib/native-root-metrics.mjs";

function gate() {
    let resolve, reject;
    const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const turns = async () => { for (let index = 0; index < 4; index++) await Promise.resolve(); };

test("sync wrappers preserve actual this, result/error identity and inherited descriptors", () => {
    let time = 0;
    const result = {}, failure = new Error("native failure");
    class Native {
        #marker = result;
        update(value) { assert.equal(value, this.#marker); time += 0.75; return value; }
        commit() { time += 3; throw failure; }
    }
    const target = new Native(), sibling = new Native(), metrics = createNativeRootMetrics({ now: () => time });
    const original = target.update, restore = metrics.wrapSync(target, ["update", "commit"], "tree");
    assert.notEqual(target.update, original); assert.equal(sibling.update, original);
    assert.equal(target.update(result), result);
    assert.throws(() => target.commit(), error => error === failure);
    const observed = metrics.snapshot();
    assert.deepEqual({ ...observed.methods["tree.update"], histogram: undefined }, {
        calls: 1, active: 0, completed: 1, errors: 0, timedCompletions: 1, totalMs: 0.75, maxMs: 0.75,
        histogram: undefined, p95UpperBoundMs: 1, p95AboveHistogramRange: false,
    });
    assert.equal(observed.methods["tree.commit"].errors, 1);
    assert.equal(observed.methods["tree.commit"].maxMs, 3);
    restore(); restore();
    assert.equal(target.update, original); assert.equal(Object.hasOwn(target, "update"), false);
});

test("async wrapper returns the same native promise and records only its actual settlement", async () => {
    let time = 0;
    const first = gate(), second = gate(), marker = {}, failure = new Error("native rejected");
    const target = { marker, run(pending) { assert.equal(this.marker, marker); return pending; } };
    const metrics = createNativeRootMetrics({ now: () => time });
    metrics.wrapAsync(target, ["run"], "io");
    assert.equal(target.run(first.promise), first.promise);
    assert.equal(target.run(second.promise), second.promise);
    // An unrelated resolved race must not close the actual native owner.
    await Promise.race([first.promise, Promise.resolve()]);
    assert.equal(metrics.snapshot().methods["io.run"].active, 2);
    assert.equal(metrics.snapshot().methods["io.run"].completed, 0);
    time = 8; first.resolve(marker); assert.equal(await first.promise, marker);
    await turns();
    assert.equal(metrics.snapshot().methods["io.run"].active, 1);
    metrics.restore(); // Already-started completion remains observable.
    time = 16; second.reject(failure); await assert.rejects(second.promise, error => error === failure);
    await turns();
    const observed = metrics.snapshot().methods["io.run"];
    assert.equal(observed.active, 0); assert.equal(observed.completed, 2);
    assert.equal(observed.errors, 1); assert.equal(observed.totalMs, 24); assert.equal(observed.maxMs, 16);
});

test("async wrapper does not asyncify a synchronous native throw", () => {
    const failure = new Error("synchronous native entry failure"), target = { run() { throw failure; } };
    const metrics = createNativeRootMetrics({ now: () => 1 }); metrics.wrapAsync(target, ["run"]);
    assert.throws(() => target.run(), error => error === failure);
    assert.equal(metrics.snapshot().methods.run.completed, 1);
    assert.equal(metrics.snapshot().methods.run.errors, 1);
});

test("histogram stays fixed sized, upper-bound p95 is explicit, snapshots detached", () => {
    let time = 0;
    const target = { work(duration) { time += duration; } }, metrics = createNativeRootMetrics({ now: () => time });
    metrics.wrapSync(target, ["work"]);
    for (let index = 0; index < 10_000; index++) target.work(index < 9500 ? 0.75 : 3);
    const first = metrics.snapshot(), record = first.methods.work;
    assert.equal(record.calls, 10_000); assert.equal(record.histogram.length, first.durationBucketUpperBoundsMs.length);
    assert.equal(record.histogram.reduce((sum, value) => sum + value, 0), 10_000);
    assert.equal(record.p95UpperBoundMs, 1); assert.equal(record.maxMs, 3);
    assert.equal(Object.hasOwn(record, "samples"), false);
    record.histogram.fill(0); first.methods.work.calls = 0;
    assert.equal(metrics.snapshot().methods.work.calls, 10_000);
    assert.equal(metrics.snapshot().methods.work.histogram.reduce((sum, value) => sum + value, 0), 10_000);
    const overflow = createNativeRootMetrics({ now: () => time });
    overflow.wrapSync(target, ["work"], "slow"); target.work(100_000);
    assert.equal(overflow.snapshot().methods["slow.work"].p95UpperBoundMs, null);
    assert.equal(overflow.snapshot().methods["slow.work"].p95AboveHistogramRange, true);
    overflow.restore(); metrics.restore();
});

test("explicit wrapper limits and whole declaration validation precede mutation", () => {
    const metrics = createNativeRootMetrics(), target = { okay() {} }, original = target.okay;
    assert.throws(() => metrics.wrapSync(target, ["okay", "missing"]), /cannot be wrapped/);
    assert.equal(target.okay, original); assert.deepEqual(metrics.snapshot().methods, {});
    assert.throws(() => metrics.wrapSync(target, ["okay", "okay"]), /unique/);
    assert.throws(() => metrics.wrapSync(target, ["okay"], "private/path"), /invalid/);
    let reads = 0;
    Object.defineProperty(target, "getter", { get() { reads++; return original; }, configurable: true });
    assert.throws(() => metrics.wrapSync(target, ["getter"]), /cannot be wrapped/); assert.equal(reads, 0);
    assert.throws(() => metrics.wrapSync(Object.freeze({ okay() {} }), ["okay"]), /facade/);
    const many = Object.fromEntries(Array.from({ length: NATIVE_ROOT_METRIC_LIMITS.methods }, (_, index) => [`method_${index}`, () => {}]));
    metrics.wrapSync(many, Object.keys(many), "bounded");
    assert.throws(() => metrics.wrapSync(target, ["okay"]), /diagnostic limit/); assert.equal(target.okay, original);
    assert.equal(Object.keys(metrics.snapshot().methods).length, NATIVE_ROOT_METRIC_LIMITS.methods);
    metrics.restore();
    // Restored historical metric labels are still counted against admission.
    assert.throws(() => metrics.wrapSync(target, ["okay"]), /diagnostic limit/);
});

test("method lists are indexed bounded arrays and restoration never overwrites newer caller methods", () => {
    const target = { one() {}, two() {} }, one = target.one, two = target.two, metrics = createNativeRootMetrics();
    const names = ["one"];
    names[Symbol.iterator] = function* () { yield "one"; yield "two"; };
    const restore = metrics.wrapSync(target, names);
    assert.notEqual(target.one, one); assert.equal(target.two, two);
    assert.throws(() => metrics.wrapSync(target, ["one"]), /already wrapped/);
    const newer = () => "newer"; target.one = newer;
    restore(); metrics.restore(); assert.equal(target.one, newer);
});

test("observer failures do not replace native results or errors", () => {
    const marker = {}, failure = new Error("native"), target = { okay() { return marker; }, fail() { throw failure; } };
    const metrics = createNativeRootMetrics({ now() { throw new Error("observer"); } });
    metrics.wrapSync(target, ["okay", "fail"]);
    assert.equal(target.okay(), marker); assert.throws(() => target.fail(), error => error === failure);
    assert.equal(metrics.snapshot().observerErrors, 4);
    assert.equal(metrics.snapshot().methods.okay.timedCompletions, 0);
    assert.equal(metrics.snapshot().methods.fail.errors, 1);
    assert.equal(metrics.snapshot().methods.okay.p95UpperBoundMs, null);
});

test("memory samples separate process counters from current WASM linear capacity", () => {
    let usage = { rss: 1000, heapTotal: 500, heapUsed: 250, external: 200, arrayBuffers: 100 };
    const native = new WebAssembly.Memory({ initial: 1, maximum: 2 });
    const metrics = createNativeRootMetrics({ memoryUsage: () => usage });
    const first = metrics.sampleMemory(native); first.heapUsed = 0;
    const oldBuffer = native.buffer; native.grow(1);
    assert.equal(oldBuffer.byteLength, 0, "test host did not detach the previous memory buffer");
    usage = { rss: 900, heapTotal: 600, heapUsed: 200, external: 400, arrayBuffers: 300 };
    metrics.sampleMemory(native);
    const state = metrics.snapshot();
    assert.equal(state.memory.samples, 2); assert.equal(state.memory.processSamples, 2); assert.equal(state.memory.wasmSamples, 2);
    assert.deepEqual(state.memory.sampledMax, { rss: 1000, heapTotal: 600, heapUsed: 250, external: 400,
        arrayBuffers: 300, wasmLinearCapacityBytes: 131072 });
    assert.equal(state.memory.last.rss, 900); assert.equal(state.memory.last.wasmLinearCapacityBytes, 131072);
    state.memory.sampledMax.rss = 0; assert.equal(metrics.snapshot().memory.sampledMax.rss, 1000);
    assert(state.limitations.some(text => text.includes("must not be added")));
    assert(state.limitations.some(text => text.includes("not allocator")));
});

test("invalid memory observation is explicit and never mixed into valid samples", () => {
    const metrics = createNativeRootMetrics({ memoryUsage: () => ({ rss: 12, heapTotal: NaN }) });
    assert.deepEqual(metrics.sampleMemory({ get buffer() { throw new Error("retired runtime"); } }), {});
    const observed = metrics.snapshot();
    assert.equal(observed.observerErrors, 2); assert.equal(observed.memory.processSamples, 0);
    assert.equal(observed.memory.wasmSamples, 0); assert.deepEqual(observed.memory.sampledMax, {});
});

test("bounded exact sample helpers distinguish medians and nearest-rank percentiles", () => {
    const values = [8, 1, 3, 2];
    assert.equal(median(values), 2.5); assert.deepEqual(values, [8, 1, 3, 2]);
    assert.equal(median([3, 1, 2]), 2); assert.equal(median([]), null);
    assert.equal(nearestRankPercentile(values, 0.5), 2); assert.equal(nearestRankPercentile(values, 0.95), 8);
    assert.equal(nearestRankPercentile([], 0.95), null);
    for (const bad of [NaN, Infinity, -1]) assert.throws(() => median([bad]), /finite and non-negative/);
    assert.throws(() => nearestRankPercentile(values, 0), /percentile/);
    assert.throws(() => nearestRankPercentile(values, 1.1), /percentile/);
    const tooMany = new Array(NATIVE_ROOT_METRIC_LIMITS.sampleValues + 1);
    Object.defineProperty(tooMany, 0, { get() { throw new Error("read before admission"); } });
    assert.throws(() => median(tooMany), /diagnostic limit/);
});
