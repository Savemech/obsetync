import { strict as assert } from "node:assert";

export function outputPlanCount(value, label) {
    assert(Number.isSafeInteger(value) && value >= 0, `${label}: unsafe output-plan integer`);
    return value;
}

export function outputPlanSum(left, right, label) {
    return outputPlanCount(left + right, label);
}

export function replacementOutputMemoryPlan(value, label, expectedScope = "v2-replacement-output") {
    const fields = ["schema", "scope", "nodePayloadBytes", "rangeEndpointPeakRequestedBytes",
        "rangeEndpointResidentRequestedBytes", "peakAdmissionBytes", "residentAdmissionBytes"];
    assert(value !== null && typeof value === "object" && !Array.isArray(value), `${label}: invalid output plan`);
    assert.deepEqual(Reflect.ownKeys(value).sort(), fields.slice().sort(), `${label}: output-plan shape`);
    for (const field of fields) assert("value" in Object.getOwnPropertyDescriptor(value, field),
        `${label}: output-plan accessor`);
    assert.equal(value.schema, 1); assert.equal(value.scope, expectedScope);
    for (const field of fields.slice(2)) outputPlanCount(value[field], `${label}/${field}`);
    assert(value.rangeEndpointPeakRequestedBytes >= value.rangeEndpointResidentRequestedBytes);
    assert.equal(value.peakAdmissionBytes,
        outputPlanSum(value.nodePayloadBytes, value.rangeEndpointPeakRequestedBytes, label));
    assert.equal(value.residentAdmissionBytes,
        outputPlanSum(value.nodePayloadBytes, value.rangeEndpointResidentRequestedBytes, label));
    if (value.nodePayloadBytes === 0) {
        assert.equal(value.rangeEndpointPeakRequestedBytes, 0);
        assert.equal(value.rangeEndpointResidentRequestedBytes, 0);
    }
    return value;
}

// Independent wire oracle: one descriptor per canonical node, including the
// root child. D counts its UTF-8 endpoint requests once; R is the root pair.
// This does not use the native plan, capacities or component counters.
export function canonicalReplacementOutputMemoryPlan(chunks, rootHex, label) {
    const utf8 = new TextDecoder("utf-8", { fatal: true });
    const reader = bytes => {
        let offset = 0;
        const take = length => {
            assert(Number.isSafeInteger(length) && length >= 0 && offset + length <= bytes.length,
                `${label}: truncated canonical V2 bytes`);
            const part = bytes.subarray(offset, offset + length); offset += length; return part;
        };
        return { take, u16: () => take(2).readUInt16LE(), u32: () => take(4).readUInt32LE(),
            u64: () => outputPlanCount(Number(take(8).readBigUInt64LE()), label),
            finish: () => assert.equal(offset, bytes.length, `${label}: trailing canonical bytes`) };
    };
    const path = (r, length) => {
        assert(length > 0 && length <= 4096, `${label}: canonical endpoint length`);
        const bytes = r.take(length);
        utf8.decode(bytes);
        return bytes;
    };
    const descriptors = new Map();
    const range = (r, height) => {
        const minLength = r.u16(), maxLength = r.u16();
        const min = path(r, minLength), max = path(r, maxLength);
        const hash = r.take(32).toString("hex"), count = r.u64(), length = r.u32();
        assert(Buffer.compare(min, max) <= 0 && count > 0 && length > 0 && length <= 256 * 1024);
        assert(height <= 16 && !descriptors.has(hash), `${label}: duplicate/invalid canonical descriptor`);
        const result = { min, max, hash, count, length, height };
        descriptors.set(hash, result);
        return result;
    };
    const root = reader(Buffer.from(rootHex, "hex"));
    assert.equal(root.take(4).toString(), "OVR2"); assert.equal(root.u32(), 2);
    root.take(8);
    const totalFiles = root.u64(), vaultLength = root.u16(), deviceLength = root.u16();
    const flags = root.take(1)[0]; assert.equal(flags & ~3, 0);
    assert.deepEqual(root.take(3), Buffer.alloc(3)); root.take(32);
    if (flags & 1) root.take(32);
    const rootChild = flags & 2 ? range(root, root.u16()) : null;
    root.take(vaultLength + deviceLength); root.finish();
    assert.equal(rootChild?.count ?? 0, totalFiles);
    const R = rootChild ? rootChild.min.length + rootChild.max.length : 0;
    const decoded = new Map();
    let P = 0;
    for (const [hash, hex] of chunks) {
        assert(!decoded.has(hash), `${label}: duplicate canonical chunk`);
        const bytes = Buffer.from(hex, "hex"), r = reader(bytes), magic = r.take(4).toString();
        P = outputPlanSum(P, bytes.length, label);
        let min, max, count, height;
        if (magic === "OVL2") {
            count = r.u32(); assert(count > 0 && count <= 1024);
            height = 0;
            for (let index = 0; index < count; index++) {
                const current = path(r, r.u16());
                if (max) assert(Buffer.compare(max, current) < 0, `${label}: unordered leaf paths`);
                min ??= current; max = current; r.take(48);
            }
        } else {
            assert.equal(magic, "OVI2");
            const childHeight = r.u16(); assert(childHeight < 16); assert.equal(r.u16(), 0);
            const children = r.u32(); assert(children > 0 && children <= 256);
            height = childHeight + 1; count = 0;
            for (let index = 0; index < children; index++) {
                const child = range(r, childHeight);
                if (max) assert(Buffer.compare(max, child.min) < 0, `${label}: unordered child ranges`);
                min ??= child.min; max = child.max; count = outputPlanSum(count, child.count, label);
            }
        }
        r.finish(); decoded.set(hash, { min, max, count, height, length: bytes.length });
    }
    assert.equal(descriptors.size, chunks.length, `${label}: descriptors do not cover canonical nodes`);
    let D = 0;
    for (const [hash, descriptor] of descriptors) {
        const node = decoded.get(hash); assert(node, `${label}: missing canonical child`);
        for (const field of ["min", "max", "count", "height", "length"])
            assert.deepEqual(node[field], descriptor[field], `${label}: descriptor differs from child ${field}`);
        D = outputPlanSum(D, descriptor.min.length + descriptor.max.length, label);
    }
    return replacementOutputMemoryPlan({ schema: 1, scope: "v2-replacement-output", nodePayloadBytes: P,
        rangeEndpointPeakRequestedBytes: outputPlanSum(D, R, label),
        rangeEndpointResidentRequestedBytes: R, peakAdmissionBytes: outputPlanSum(P, outputPlanSum(D, R, label), label),
        residentAdmissionBytes: outputPlanSum(P, R, label) }, label);
}

export function outputMemoryWitnesses(plan) {
    return [plan.nodePayloadBytes, plan.rangeEndpointPeakRequestedBytes, plan.rangeEndpointResidentRequestedBytes];
}

export function assertReplacementOutputOwnersEmpty(tree, label) {
    const stores = tree.chunk_memory_snapshot();
    assert.deepEqual(stores.replacement, { chunks: 0, map_capacity: 0, payload_bytes: 0,
        buffer_capacity_bytes: 0, counters_valid: true }, `${label}: pre-resume chunk output allocated`);
    const ranges = tree.replacement_v2_ranges_memory_snapshot().replacement;
    assert(ranges !== null && ranges.counters_valid === true, `${label}: unmeasured range output`);
    for (const name of ["ranges", "next_ranges", "closure_pending", "closure_expanding"]) {
        assert.deepEqual(ranges[name], { owners: 0, length_slots: 0, capacity_slots: 0,
            slot_size_bytes: 0, backing_capacity_bytes: 0, counters_valid: true },
        `${label}: pre-resume ${name} owner allocated`);
    }
    assert.equal(ranges.descriptor_ranges, 0);
    for (const name of ["vector_paths", "descriptor_paths"]) assert.deepEqual(ranges[name],
        { strings: 0, length_bytes: 0, capacity_bytes: 0, counters_valid: true },
        `${label}: pre-resume endpoint output allocated`);
    // metadata.replacement may be null here (unmeasured), never proof of zero.
}

export function rejectReplacementOutputPlanPhase(tree, token, label, plan) {
    const witnesses = plan ? outputMemoryWitnesses(plan) : [0, 0, 0];
    assert.throws(() => tree.replacement_rebuild_output_memory_plan_v1_job(token),
        `${label}: output plan accepted wrong phase/kind/token`);
    assert.throws(() => tree.resume_replacement_rebuild_output_memory_v1_job(token, ...witnesses),
        `${label}: output resume accepted wrong phase/kind/token`);
}

export function qualifyReplacementOutputPlan(tree, token, oldPlan, expected, budget, completed, label, version = 2) {
    const scope = version === 1 ? "v1-replacement-output" : "v2-replacement-output";
    const plan = replacementOutputMemoryPlan(
        tree.replacement_rebuild_output_memory_plan_v1_job(token), label, scope);
    if (oldPlan !== null)
        assert.equal(plan.nodePayloadBytes, oldPlan.nodePayloadBytes, `${label}: old/new payload witnesses differ`);
    if (expected !== undefined) assert.deepEqual(plan, expected, `${label}: output plan differs from wire oracle`);
    const witnesses = outputMemoryWitnesses(plan);
    const paused = () => {
        assert.deepEqual(tree.step_replacement_rebuild_output_memory_v1_job(token, budget),
            { done: false, units: 0, completed, phase: "plan ready" }, `${label}: rejection consumed PlanReady`);
        if (oldPlan === null)
            assert.throws(() => tree.replacement_rebuild_plan_job(token), `${label}: V1 fabricated a node plan`);
        else assert.deepEqual(tree.replacement_rebuild_plan_job(token), oldPlan, `${label}: old plan changed`);
        assert.deepEqual(replacementOutputMemoryPlan(
            tree.replacement_rebuild_output_memory_plan_v1_job(token), label, scope), plan,
        `${label}: output plan changed`);
        assertReplacementOutputOwnersEmpty(tree, label);
    };
    paused();
    const wrong = token === 0xffff_ffff ? token - 1 : token + 1;
    assert.throws(() => tree.replacement_rebuild_output_memory_plan_v1_job(wrong));
    assert.throws(() => tree.resume_replacement_rebuild_output_memory_v1_job(wrong, ...witnesses));
    paused();
    for (let field = 0; field < witnesses.length; field++) {
        for (const bad of new Set([witnesses[field] - 1, witnesses[field] + 1,
            NaN, Infinity, -Infinity, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])) {
            const invalid = witnesses.slice(); invalid[field] = bad;
            assert.throws(() => tree.resume_replacement_rebuild_output_memory_v1_job(token, ...invalid),
                `${label}: output witness field ${field} accepted ${bad}`);
            paused();
        }
    }
    return plan;
}
