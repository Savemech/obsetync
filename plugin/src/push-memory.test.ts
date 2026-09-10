import { strict as assert } from "node:assert";
import { hashTuningForRuntime } from "./hash-runtime";
import { estimateManifestBytes, planPushMemory, pushGroupingByteLimit } from "./push-memory";
import { TRANSPORT_ERROR_PAYLOAD_ALLOWANCE_BYTES } from "./transient-memory";

const mib = 1024 * 1024;
const mobile = hashTuningForRuntime("mobile");
const desktop = hashTuningForRuntime("desktop");
const small = planPushMemory([{ size: 100, chunked: false, ranged: false }], mobile);
assert.ok(small.ownerBytes > 5 * 100);
assert.ok(small.totalBytes === small.ownerBytes + small.workBytes);
assert.ok(small.transportPayloadBytes >= TRANSPORT_ERROR_PAYLOAD_ALLOWANCE_BYTES,
    "push admission omitted the WS CHECK response floor");
const large = planPushMemory([{ size: 128 * mib, chunked: true, ranged: false }], mobile);
assert.ok(large.totalBytes > 32 * mib, "whole-file sources cannot hide behind singleton grouping");
const ranged = planPushMemory([{ size: 128 * mib, chunked: true, ranged: true }], mobile, 4 * mib);
assert.ok(ranged.totalBytes <= 32 * mib, "existing 4 MiB chunks must fit bounded native range admission");
assert.equal(ranged.transportPayloadBytes, 4 * mib);
const desktopRange = planPushMemory([{ size: 128 * mib, chunked: true, ranged: true }], desktop);
assert.ok(desktopRange.totalBytes < 128 * mib);
assert.equal(desktopRange.rangeQueueBytes, 8 * mib);
const fourWorkers = planPushMemory(Array.from({ length: 4 }, () => ({
    size: 1, chunked: false, ranged: false, worker: true,
})), desktop);
assert.ok(fourWorkers.ownerBytes >= 2 * desktop.maxFeedBytes * 4,
    "concurrent native worker feed buffers require per-job admission");
assert.ok(planPushMemory([{ size: 1, backingBytes: 128 * mib, chunked: false, ranged: false }], mobile).totalBytes > 32 * mib,
    "small views retain their complete parent allocation");
assert.ok(estimateManifestBytes(128 * mib) > estimateManifestBytes(mib));
assert.throws(() => planPushMemory([{ size: Number.NaN, chunked: false, ranged: false }], mobile));
assert.throws(() => planPushMemory([{ size: 2, backingBytes: 1, chunked: false, ranged: false }], mobile));
for (const tuning of [mobile, desktop]) {
    const capacity = tuning.runtime === "mobile" ? 32 * mib : 128 * mib;
    const bytes = pushGroupingByteLimit(tuning, capacity);
    assert.ok(bytes > 0 && bytes <= tuning.maxBatchBytes);
    const estimate = planPushMemory([{ size: bytes, chunked: false, ranged: false }], tuning);
    assert.ok(estimate.totalBytes <= capacity);
}
console.log("push-memory.test: source copies, ranges, immutable chunks and grouping admission passed");
