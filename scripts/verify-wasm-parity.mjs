#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { strict as assert } from "node:assert";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const outputDir = resolve(process.argv[2] ?? "plugin/wasm");
const scalarModuleUrl = pathToFileURL(resolve(outputDir, "sync_core.js")).href;
const simdModuleUrl = pathToFileURL(resolve(outputDir, "sync_core_simd.js")).href;
const scalar = await import(scalarModuleUrl);
const simd = await import(simdModuleUrl);
const scalarBytes = await readFile(resolve(outputDir, "sync_core_bg.wasm"));
const simdBytes = await readFile(resolve(outputDir, "sync_core_simd_bg.wasm"));

if (!WebAssembly.validate(scalarBytes)) {
    throw new Error("scalar module did not validate");
}
if (!WebAssembly.validate(simdBytes)) {
    throw new Error("SIMD module did not validate on this test host");
}
const scalarNative = await scalar.default({ module_or_path: scalarBytes });
const simdNative = await simd.default({ module_or_path: simdBytes });

function deterministicPayload(length) {
    const bytes = new Uint8Array(length);
    let state = 0x6d2b79f5;
    for (let index = 0; index < length; index++) {
        state = Math.imul(state ^ (state >>> 15), 1 | state);
        state ^= state + Math.imul(state ^ (state >>> 7), 61 | state);
        bytes[index] = (state ^ (state >>> 14)) & 0xff;
    }
    return bytes;
}

function streamingHash(module, bytes, feedBytes) {
    const hasher = new module.Hasher();
    try {
        for (let offset = 0; offset < bytes.length; offset += feedBytes) {
            hasher.update(bytes.subarray(offset, offset + feedBytes));
        }
        return hasher.finalize();
    } finally {
        hasher.free();
    }
}

function chunkManifest(module, bytes, feedBytes) {
    const chunker = new module.WasmChunker();
    try {
        for (let offset = 0; offset < bytes.length; offset += feedBytes) {
            chunker.update(bytes.subarray(offset, offset + feedBytes));
        }
        return chunker.finish();
    } finally {
        chunker.free();
    }
}

const feedSizes = [64, 128, 256, 512, 1024].map((kib) => kib * 1024);
for (const length of [0, 1, 63, 64, 65, 1024, 65_535, 65_536, 65_537, 1_048_593]) {
    const bytes = deterministicPayload(length);
    const expected = scalar.wasm_hash(bytes);
    if (simd.wasm_hash(bytes) !== expected) {
        throw new Error(`scalar/SIMD hash mismatch at ${length} bytes`);
    }
    for (const feedBytes of feedSizes) {
        if (streamingHash(scalar, bytes, feedBytes) !== expected) {
            throw new Error(`scalar streaming mismatch at ${length}/${feedBytes}`);
        }
        if (streamingHash(simd, bytes, feedBytes) !== expected) {
            throw new Error(`SIMD streaming mismatch at ${length}/${feedBytes}`);
        }
    }
}

const large = deterministicPayload(10 * 1024 * 1024 + 123);
const baselineManifest = JSON.stringify(chunkManifest(scalar, large, feedSizes[0]));
const chunkFeedSizes = [...feedSizes, 4 * 1024 * 1024 - 1, 4 * 1024 * 1024,
    4 * 1024 * 1024 + 1, large.length];
for (const feedBytes of chunkFeedSizes) {
    if (JSON.stringify(chunkManifest(scalar, large, feedBytes)) !== baselineManifest) {
        throw new Error(`scalar FastCDC boundary mismatch at feed ${feedBytes}`);
    }
    if (JSON.stringify(chunkManifest(simd, large, feedBytes)) !== baselineManifest) {
        throw new Error(`SIMD FastCDC boundary mismatch at feed ${feedBytes}`);
    }
}

const batchData = deterministicPayload(3000);
const offsets = new Uint32Array([0, 1000, 2000]);
const sizes = new Uint32Array([1000, 1000, 1000]);
if (
    JSON.stringify(scalar.wasm_hash_batch(batchData, offsets, sizes)) !==
    JSON.stringify(simd.wasm_hash_batch(batchData, offsets, sizes))
) {
    throw new Error("scalar/SIMD batch hash mismatch");
}

function deterministicTreeEntries(count) {
    const rows = [];
    for (let index = 0; index < count; index++) {
        const path = `notes/${String(index % 17).padStart(2, "0")}/${String(index).padStart(6, "0")}.md`;
        rows.push({
            path,
            hash: scalar.wasm_hash(deterministicPayload(128 + (index % 257))),
            mtime_ms: 1_700_000_000_000 + index,
            size: 128 + (index % 257),
        });
    }
    return rows;
}

function treeV2State(module, entries) {
    const tree = new module.WasmTree("parity-vault", "parity-device");
    tree.set_tree_version(2);
    if (tree.tree_version() !== 2) {
        tree.free();
        throw new Error("WasmTree did not select Tree v2");
    }
    tree.build_from_entries(JSON.stringify(entries));
    const rootBytes = tree.root_bytes();
    if (!rootBytes || module.wasm_root_version_from_bytes(rootBytes) !== 2) {
        tree.free();
        throw new Error("WasmTree emitted a non-v2 persisted root");
    }
    return tree;
}

function assertTreeParity(left, right, phase) {
    if (left.root_hash_hex() !== right.root_hash_hex()) {
        throw new Error(`scalar/SIMD Tree v2 root mismatch after ${phase}`);
    }
    if (left.total_files() !== right.total_files()) {
        throw new Error(`scalar/SIMD Tree v2 count mismatch after ${phase}`);
    }
    const leftChunks = JSON.stringify(scalar.wasm_tree_committed_chunk_hashes(left));
    const rightChunks = JSON.stringify(simd.wasm_tree_committed_chunk_hashes(right));
    if (leftChunks !== rightChunks) {
        throw new Error(`scalar/SIMD Tree v2 graph mismatch after ${phase}`);
    }
    assertChunkLengths(scalar, left, phase);
    assertChunkLengths(simd, right, phase);
}

function assertChunkLengths(module, tree, phase) {
    for (const hash of module.wasm_tree_chunk_hashes(tree)) {
        // Query before exporting bytes, as upload admission does. The query
        // must also address new candidate nodes, not just the committed root.
        const length = module.wasm_tree_chunk_byte_length(tree, hash);
        const bytes = module.wasm_tree_get_chunk(tree, hash);
        if (!Number.isSafeInteger(length) || length < 0 || !bytes ||
            bytes.byteLength !== length || module.wasm_hash(bytes) !== hash) {
            throw new Error(`tree chunk length/payload mismatch after ${phase}`);
        }
        if (module.wasm_tree_chunk_byte_length(tree, hash.toUpperCase()) !== length) {
            throw new Error(`tree chunk length rejected uppercase hash after ${phase}`);
        }
    }
    for (const missing of ["invalid", "00".repeat(32)]) {
        if (module.wasm_tree_chunk_byte_length(tree, missing) !== undefined) {
            throw new Error(`tree chunk length did not distinguish missing hash after ${phase}`);
        }
    }
}

function bytesEqual(left, right) {
    if (!(left instanceof Uint8Array) || !(right instanceof Uint8Array) ||
        left.byteLength !== right.byteLength) return false;
    for (let index = 0; index < left.byteLength; index++) {
        if (left[index] !== right[index]) return false;
    }
    return true;
}

function expectThrow(operation, message) {
    let threw = false;
    try { operation(); } catch { threw = true; }
    if (!threw) throw new Error(message);
}

function validateTreeJobProgress(value, previous, maxUnits, phase) {
    if (!value || typeof value !== "object" || typeof value.done !== "boolean") {
        throw new Error(`invalid tree job progress after ${phase}`);
    }
    for (const field of ["units", "completed", "remaining", "reachable"]) {
        if (!Number.isSafeInteger(value[field]) || value[field] < 0) {
            throw new Error(`invalid tree job ${field} after ${phase}`);
        }
    }
    if (value.units > maxUnits || (!value.done && value.units === 0) ||
        (value.done && value.remaining !== 0) ||
        value.completed !== previous.completed + value.units ||
        value.reachable < previous.reachable || value.reachable > value.completed) {
        throw new Error(`inconsistent tree job progress after ${phase}`);
    }
}

function validateExactTreeJobProgress(value, previous, maxUnits, phase) {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).sort().join(",") !== "completed,done,reachable,remaining,units") {
        throw new Error(`invalid exact tree job progress shape after ${phase}`);
    }
    validateTreeJobProgress(value, previous, maxUnits, phase);
}

function validateReachabilityRetirementProgress(value, previousCompleted, maxUnits, phase) {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).sort().join(",") !== "completed,done,units" ||
        typeof value.done !== "boolean" || !Number.isSafeInteger(value.units) ||
        value.units < 0 || value.units > maxUnits ||
        (!value.done && value.units === 0) ||
        !Number.isSafeInteger(value.completed) || value.completed < 0 ||
        value.completed !== previousCompleted + value.units) {
        throw new Error(`invalid reachability retirement progress during ${phase}`);
    }
}

function differentTreeJobToken(token) {
    return token === 0xffff_ffff ? token - 1 : token + 1;
}

function requireTreeJobToken(token, phase) {
    if (!Number.isSafeInteger(token) || token < 1 || token > 0xffff_ffff) {
        throw new Error(`invalid WASM tree job token during ${phase}`);
    }
    return token;
}

function reachabilityVisibleState(tree, phase) {
    const committedBytes = tree.root_bytes();
    const candidate = tree.has_candidate();
    const candidateBytes = candidate ? tree.candidate_root_bytes() : undefined;
    if (!(committedBytes instanceof Uint8Array) ||
        (candidate && !(candidateBytes instanceof Uint8Array))) {
        throw new Error(`missing visible tree bytes during ${phase}`);
    }
    const state = {
        version: tree.tree_version(),
        committedRevision: tree.committed_revision(),
        candidateRevision: readCandidateRevision(tree, phase),
        candidate,
        committedHash: tree.root_hash_hex(),
        committedCount: tree.total_files(),
        committedBytes: Buffer.from(committedBytes).toString("hex"),
        candidateHash: candidate ? tree.candidate_root_hash_hex() : null,
        candidateCount: candidate ? tree.candidate_total_files() : null,
        candidateBytes: candidate ? Buffer.from(candidateBytes).toString("hex") : null,
    };
    for (const field of ["committedRevision", "candidateRevision", "committedCount",
        ...(candidate ? ["candidateCount"] : [])]) {
        if (!Number.isSafeInteger(state[field]) || state[field] < 0) {
            throw new Error(`invalid ${field} during ${phase}`);
        }
    }
    return state;
}

function assertReachabilityVisibleState(tree, expected, phase) {
    const actual = reachabilityVisibleState(tree, phase);
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(`reachability lifecycle changed visible tree state during ${phase}`);
    }
}

function stepDeferredReachabilityToReady(tree, token, maxUnits, expected, phase) {
    let previous = { completed: 0, reachable: 0 }, turns = 0;
    for (;;) {
        const current = tree.step_reachability_job_deferred(token, maxUnits);
        validateExactTreeJobProgress(current, previous, maxUnits, phase);
        assertReachabilityVisibleState(tree, expected, `${phase} step`);
        previous = current;
        turns++;
        if (turns > 100_000) throw new Error(`deferred reachability did not converge during ${phase}`);
        if (current.done) {
            const replayed = tree.step_reachability_job_deferred(token, maxUnits);
            validateExactTreeJobProgress(replayed, previous, maxUnits, `${phase} repeated READY`);
            if (!replayed.done || replayed.units !== 0 ||
                replayed.completed !== previous.completed || replayed.reachable !== previous.reachable) {
                throw new Error(`deferred reachability READY did more work during ${phase}`);
            }
            assertReachabilityVisibleState(tree, expected, `${phase} repeated READY`);
            return { turns, completed: current.completed, reachable: current.reachable };
        }
    }
}

function drainReachabilityRetirement(tree, token, maxUnits, expected, phase) {
    const wrongToken = differentTreeJobToken(token);
    let completed = 0, turns = 0;
    expectThrow(() => tree.step_reachability_retirement(wrongToken, completed, maxUnits),
        `wrong token entered reachability retirement during ${phase}`);
    expectThrow(() => tree.finish_reachability_retirement(token, completed),
        `incomplete reachability retirement acknowledged during ${phase}`);
    for (;;) {
        expectThrow(() => tree.step_tree_retirement(token, maxUnits),
            `generic retirement consumed reachability owner during ${phase}`);
        const progress = tree.step_reachability_retirement(token, completed, maxUnits);
        validateReachabilityRetirementProgress(progress, completed, maxUnits, phase);
        assertReachabilityVisibleState(tree, expected, `${phase} step`);
        const replayed = tree.step_reachability_retirement(token, completed, maxUnits);
        validateReachabilityRetirementProgress(replayed, completed, maxUnits, `${phase} replay`);
        if (JSON.stringify(replayed) !== JSON.stringify(progress)) {
            throw new Error(`reachability retirement replay changed exact result during ${phase}`);
        }
        assertReachabilityVisibleState(tree, expected, `${phase} replay`);
        expectThrow(() => tree.step_reachability_retirement(token, progress.completed + 1, maxUnits),
            `wrong expected retirement count advanced owner during ${phase}`);
        completed = progress.completed;
        turns++;
        if (turns > 100_000) throw new Error(`reachability retirement did not converge during ${phase}`);
        if (progress.done) break;
    }
    if (tree.cancel_reachability_job_deferred(token) !== undefined ||
        tree.cancel_reachability_job_deferred(token) !== undefined) {
        throw new Error(`terminal reachability cancel returned a value during ${phase}`);
    }
    expectThrow(() => tree.finish_reachability_retirement(token, completed + 1),
        `wrong final retirement count acknowledged during ${phase}`);
    if (tree.finish_reachability_retirement(token, completed) !== undefined ||
        tree.finish_reachability_retirement(token, completed) !== undefined) {
        throw new Error(`reachability retirement acknowledgement returned a value during ${phase}`);
    }
    expectThrow(() => tree.step_reachability_retirement(token, completed, maxUnits),
        `acknowledged reachability retirement remained step-able during ${phase}`);
    expectThrow(() => tree.cancel_reachability_job_deferred(token),
        `acknowledged reachability retirement remained cancellable during ${phase}`);
    expectThrow(() => tree.finish_reachability_retirement(token, completed + 1),
        `reachability retirement tombstone accepted a different count during ${phase}`);
    assertReachabilityVisibleState(tree, expected, `${phase} acknowledged`);
    return { turns, completed };
}

function readCandidateRevision(tree, phase) {
    if (typeof tree.candidate_revision !== "function") {
        throw new Error(`packaged WASM lacks candidate_revision during ${phase}`);
    }
    const revision = tree.candidate_revision();
    if (!Number.isSafeInteger(revision) || revision < 0) {
        throw new Error(`invalid candidate revision during ${phase}`);
    }
    return revision;
}

function assertCandidateRevision(tree, expected, phase) {
    const actual = readCandidateRevision(tree, phase);
    if (actual !== expected) {
        throw new Error(`candidate revision ${actual} differed from ${expected} during ${phase}`);
    }
}

function captureCandidateState(tree, phase) {
    if (!tree.has_candidate()) throw new Error(`candidate chunk fixture has no candidate during ${phase}`);
    return {
        version: tree.tree_version(),
        revision: tree.committed_revision(),
        candidateRevision: tree.candidate_revision(),
        committedBytes: tree.root_bytes(),
        committedHash: tree.root_hash_hex(),
        committedCount: tree.total_files(),
        candidateBytes: tree.candidate_root_bytes(),
        candidateHash: tree.candidate_root_hash_hex(),
        candidateCount: tree.candidate_total_files(),
    };
}

function assertCandidateState(tree, before, phase) {
    if (!tree.has_candidate() || tree.tree_version() !== before.version ||
        tree.committed_revision() !== before.revision ||
        tree.candidate_revision() !== before.candidateRevision ||
        tree.root_hash_hex() !== before.committedHash || tree.total_files() !== before.committedCount ||
        !bytesEqual(tree.root_bytes(), before.committedBytes) ||
        tree.candidate_root_hash_hex() !== before.candidateHash ||
        tree.candidate_total_files() !== before.candidateCount ||
        !bytesEqual(tree.candidate_root_bytes(), before.candidateBytes)) {
        throw new Error(`candidate chunk job changed candidate/committed state during ${phase}`);
    }
}

function validateCandidateChunkPlan(value, phase) {
    if (!value || typeof value !== "object" || Object.keys(value).sort().join(",") !== "all,fresh") {
        throw new Error(`invalid candidate chunk plan shape during ${phase}`);
    }
    for (const field of ["all", "fresh"]) {
        const hashes = value[field];
        if (!Array.isArray(hashes)) throw new Error(`invalid candidate chunk ${field} during ${phase}`);
        for (let index = 0; index < hashes.length; index++) {
            if (typeof hashes[index] !== "string" || !/^[0-9a-f]{64}$/.test(hashes[index]) ||
                (index > 0 && hashes[index - 1] >= hashes[index])) {
                throw new Error(`candidate chunk ${field} is not canonical, unique and sorted during ${phase}`);
            }
        }
    }
    const all = new Set(value.all);
    if (value.fresh.some(hash => !all.has(hash))) {
        throw new Error(`candidate fresh chunks are not a subset during ${phase}`);
    }
}

const CANDIDATE_CHUNK_PAGE_MAX_HASHES = 256;
const CANDIDATE_CHUNK_SORT_MAX_UNITS = 4096;
function validateCandidateChunkSortMemoryPlan(value, expectedCount, phase) {
    const keys = ["hashCount", "hashSizeBytes", "pageMaxHashes", "pageOutputUnmeasured",
        "peakAdmissionBytes", "reachableSetUnmeasured", "schema", "scope",
        "scratchHashesRequestedBytes", "sortStrategy", "sourceHashesRequestedBytes"];
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).sort().join(",") !== keys.join(",") || value.schema !== 1 ||
        value.scope !== "candidate-chunk-plan-sort-workspace" ||
        value.sortStrategy !== "stable-lsd-radix-v1" || value.hashCount !== expectedCount ||
        value.hashSizeBytes !== 32 || value.pageMaxHashes !== CANDIDATE_CHUNK_PAGE_MAX_HASHES ||
        value.sourceHashesRequestedBytes !== expectedCount * 32 ||
        value.scratchHashesRequestedBytes !== expectedCount * 32 ||
        value.peakAdmissionBytes !== expectedCount * 64 ||
        value.reachableSetUnmeasured !== true || value.pageOutputUnmeasured !== true) {
        throw new Error(`invalid candidate chunk sort memory plan during ${phase}`);
    }
    return value;
}

function validateCandidateChunkSortProgress(value, previous, maxUnits, expectedCount, phase) {
    const completed = value?.completed;
    const expectedPhase = completed === expectedCount * 66 ? "ready"
        : completed < expectedCount ? "collect"
        : completed < expectedCount * 2 ? "initialize-scratch"
        : (completed - expectedCount * 2) % (expectedCount * 2) < expectedCount
            ? "count-byte" : "scatter-byte";
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).sort().join(",") !==
            "allCount,completed,done,phase,remaining,schema,scope,units" ||
        value.schema !== 1 || value.scope !== "candidate-chunk-plan-sort" ||
        value.allCount !== expectedCount || !Number.isSafeInteger(value.units) ||
        value.units < 0 || value.units > maxUnits ||
        !Number.isSafeInteger(value.completed) || value.completed !== previous.completed + value.units ||
        !Number.isSafeInteger(value.remaining) || value.remaining < 0 ||
        value.completed + value.remaining !== expectedCount * 66 || value.phase !== expectedPhase ||
        typeof value.done !== "boolean" || value.done !== (value.phase === "ready" && value.remaining === 0) ||
        (!value.done && value.units === 0)) {
        throw new Error(`invalid candidate chunk sort progress during ${phase}`);
    }
}

function validateCandidateChunkPlanInfo(value, expectedCount, phase) {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).sort().join(",") !== "allCount,pageMaxHashes,schema,scope" ||
        value.schema !== 1 || value.scope !== "candidate-chunk-plan-pages" ||
        value.allCount !== expectedCount || value.pageMaxHashes !== CANDIDATE_CHUNK_PAGE_MAX_HASHES) {
        throw new Error(`invalid candidate chunk plan info during ${phase}`);
    }
}

function validateCandidateChunkPage(value, expectedOffset, maxHashes, total, phase) {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).sort().join(",") !== "all,done,fresh,nextOffset,offset,schema,scope" ||
        value.schema !== 1 || value.scope !== "candidate-chunk-plan-page" ||
        value.offset !== expectedOffset || !Number.isSafeInteger(value.nextOffset) ||
        value.nextOffset < value.offset || value.nextOffset > total ||
        value.nextOffset - value.offset > maxHashes ||
        value.done !== (value.nextOffset === total)) {
        throw new Error(`invalid candidate chunk page during ${phase}`);
    }
    validateCandidateChunkPlan({ all: value.all, fresh: value.fresh }, `${phase} payload`);
    if (value.all.length !== value.nextOffset - value.offset) {
        throw new Error(`candidate chunk page length disagreed with offsets during ${phase}`);
    }
}

function candidateChunksWithPagedJobs(module, tree, phase) {
    const methods = ["begin_candidate_chunks_job", "step_reachability_job_deferred",
        "candidate_chunks_sort_memory_plan_v1_job", "resume_candidate_chunks_sort_memory_v1_job",
        "step_candidate_chunks_sort_v1_job", "candidate_chunks_plan_info_v1_job",
        "read_candidate_chunks_page_v1_job", "finish_candidate_chunks_plan_v1_job",
        "cancel_reachability_job_deferred", "step_reachability_retirement",
        "finish_reachability_retirement"];
    for (const method of methods) {
        if (typeof tree[method] !== "function") {
            throw new Error(`packaged WASM lacks ${method} during ${phase}`);
        }
    }
    const before = reachabilityVisibleState(tree, `${phase} initial`);
    const expected = {
        all: module.wasm_tree_candidate_chunk_hashes(tree),
        fresh: module.wasm_tree_new_candidate_chunk_hashes(tree),
    };
    validateCandidateChunkPlan(expected, `${phase} oracle`);

    // Cancellation after sort admission must retire both the reachability
    // cursor and the admitted sort workspace under the original token.
    const cancelledToken = requireTreeJobToken(tree.begin_candidate_chunks_job(), `${phase} cancel begin`);
    stepDeferredReachabilityToReady(tree, cancelledToken, 256, before, `${phase} cancel traversal`);
    const cancelledPlan = validateCandidateChunkSortMemoryPlan(
        tree.candidate_chunks_sort_memory_plan_v1_job(cancelledToken), expected.all.length,
        `${phase} cancel plan`);
    tree.resume_candidate_chunks_sort_memory_v1_job(cancelledToken,
        cancelledPlan.sourceHashesRequestedBytes, cancelledPlan.scratchHashesRequestedBytes);
    if (expected.all.length > 0) {
        const partial = tree.step_candidate_chunks_sort_v1_job(cancelledToken, 1);
        validateCandidateChunkSortProgress(partial, { completed: 0, phase: "collect" }, 1,
            expected.all.length, `${phase} cancel sort`);
    }
    tree.cancel_reachability_job_deferred(cancelledToken);
    tree.cancel_reachability_job_deferred(cancelledToken);
    drainReachabilityRetirement(tree, cancelledToken, 1, before, `${phase} cancelled sort cleanup`);

    const results = [];
    for (const [sortUnits, pageHashes] of [[1, 1], [CANDIDATE_CHUNK_SORT_MAX_UNITS, 256]]) {
        const token = requireTreeJobToken(tree.begin_candidate_chunks_job(), `${phase} begin/${sortUnits}`);
        const wrongToken = differentTreeJobToken(token);
        const ready = stepDeferredReachabilityToReady(tree, token, 256, before,
            `${phase} traversal/${sortUnits}`);
        if (ready.reachable !== expected.all.length) {
            throw new Error(`candidate chunk traversal count mismatch during ${phase}/${sortUnits}`);
        }
        for (const operation of [
            () => tree.candidate_chunks_sort_memory_plan_v1_job(wrongToken),
            () => tree.resume_candidate_chunks_sort_memory_v1_job(wrongToken, 0, 0),
            () => tree.step_candidate_chunks_sort_v1_job(token, sortUnits),
            () => tree.candidate_chunks_plan_info_v1_job(token),
            () => tree.read_candidate_chunks_page_v1_job(token, 0, pageHashes),
            () => tree.finish_candidate_chunks_plan_v1_job(token),
        ]) {
            expectThrow(operation, `invalid pre-resume operation changed chunk plan during ${phase}/${sortUnits}`);
            assertReachabilityVisibleState(tree, before, `${phase} rejected pre-resume operation`);
        }
        const memory = validateCandidateChunkSortMemoryPlan(
            tree.candidate_chunks_sort_memory_plan_v1_job(token), expected.all.length,
            `${phase} memory/${sortUnits}`);
        expectThrow(() => tree.resume_candidate_chunks_sort_memory_v1_job(token,
            memory.sourceHashesRequestedBytes + 32, memory.scratchHashesRequestedBytes),
        `candidate chunk sort accepted wrong memory witness during ${phase}/${sortUnits}`);
        tree.resume_candidate_chunks_sort_memory_v1_job(token,
            memory.sourceHashesRequestedBytes, memory.scratchHashesRequestedBytes);
        expectThrow(() => tree.candidate_chunks_sort_memory_plan_v1_job(token),
            `resumed chunk sort still exposed its preallocation plan during ${phase}/${sortUnits}`);
        for (const budget of [0, CANDIDATE_CHUNK_SORT_MAX_UNITS + 1]) expectThrow(
            () => tree.step_candidate_chunks_sort_v1_job(token, budget),
            `candidate chunk sort accepted budget ${budget} during ${phase}/${sortUnits}`);
        expectThrow(() => tree.step_candidate_chunks_sort_v1_job(wrongToken, sortUnits),
            `wrong token stepped candidate chunk sort during ${phase}/${sortUnits}`);

        let progress = { completed: 0, phase: "collect" }, turns = 0;
        for (;;) {
            const current = tree.step_candidate_chunks_sort_v1_job(token, sortUnits);
            validateCandidateChunkSortProgress(current, progress, sortUnits, expected.all.length,
                `${phase} sort/${sortUnits}`);
            assertReachabilityVisibleState(tree, before, `${phase} sort step/${sortUnits}`);
            progress = current; turns++;
            if (turns > Math.max(1, expected.all.length * 66 + 1)) {
                throw new Error(`candidate chunk sort did not converge during ${phase}/${sortUnits}`);
            }
            if (current.done) break;
        }
        const replayed = tree.step_candidate_chunks_sort_v1_job(token, sortUnits);
        validateCandidateChunkSortProgress(replayed, progress, sortUnits, expected.all.length,
            `${phase} repeated ready/${sortUnits}`);
        if (replayed.units !== 0 || replayed.done !== progress.done ||
            replayed.completed !== progress.completed || replayed.remaining !== progress.remaining ||
            replayed.allCount !== progress.allCount || replayed.phase !== progress.phase) {
            throw new Error(`candidate chunk sort READY replay changed progress during ${phase}/${sortUnits}`);
        }
        validateCandidateChunkPlanInfo(tree.candidate_chunks_plan_info_v1_job(token),
            expected.all.length, `${phase} page info/${sortUnits}`);
        if (expected.all.length > 0) expectThrow(() => tree.finish_candidate_chunks_plan_v1_job(token),
            `candidate chunk plan finished before page EOF during ${phase}/${sortUnits}`);

        const all = [], fresh = [], pages = [];
        let offset = 0;
        do {
            expectThrow(() => tree.read_candidate_chunks_page_v1_job(wrongToken, offset, pageHashes),
                `wrong token read candidate chunk page during ${phase}/${sortUnits}`);
            expectThrow(() => tree.read_candidate_chunks_page_v1_job(token, offset + 1, pageHashes),
                `wrong offset read candidate chunk page during ${phase}/${sortUnits}`);
            for (const size of [0, 257]) expectThrow(
                () => tree.read_candidate_chunks_page_v1_job(token, offset, size),
                `candidate chunk page accepted size ${size} during ${phase}/${sortUnits}`);
            const page = tree.read_candidate_chunks_page_v1_job(token, offset, pageHashes);
            validateCandidateChunkPage(page, offset, pageHashes, expected.all.length,
                `${phase} page/${sortUnits}`);
            all.push(...page.all); fresh.push(...page.fresh);
            pages.push(page.all.length);
            const oldOffset = offset; offset = page.nextOffset;
            assertReachabilityVisibleState(tree, before, `${phase} page read/${sortUnits}`);
            if (!page.done) {
                expectThrow(() => tree.read_candidate_chunks_page_v1_job(token, oldOffset, pageHashes),
                    `stale offset replayed candidate chunk page during ${phase}/${sortUnits}`);
                expectThrow(() => tree.finish_candidate_chunks_plan_v1_job(token),
                    `candidate chunk plan finished before EOF during ${phase}/${sortUnits}`);
            }
            if (page.done) break;
        } while (pages.length <= expected.all.length + 1);
        const output = { all, fresh };
        validateCandidateChunkPlan(output, `${phase} paged output/${sortUnits}`);
        if (JSON.stringify(output) !== JSON.stringify(expected)) {
            throw new Error(`paged candidate chunk plan differs from oracle during ${phase}/${sortUnits}`);
        }
        if (tree.finish_candidate_chunks_plan_v1_job(token) !== undefined) {
            throw new Error(`candidate chunk page finish returned a value during ${phase}/${sortUnits}`);
        }
        assertReachabilityVisibleState(tree, before, `${phase} page finish/${sortUnits}`);
        const cleanup = drainReachabilityRetirement(tree, token, 256, before,
            `${phase} page cleanup/${sortUnits}`);
        for (const operation of [
            () => tree.candidate_chunks_sort_memory_plan_v1_job(token),
            () => tree.resume_candidate_chunks_sort_memory_v1_job(token,
                memory.sourceHashesRequestedBytes, memory.scratchHashesRequestedBytes),
            () => tree.step_candidate_chunks_sort_v1_job(token, sortUnits),
            () => tree.candidate_chunks_plan_info_v1_job(token),
            () => tree.read_candidate_chunks_page_v1_job(token, offset, pageHashes),
            () => tree.finish_candidate_chunks_plan_v1_job(token),
        ]) expectThrow(operation, `stale chunk plan token remained live during ${phase}/${sortUnits}`);
        results.push({ sortUnits, pageHashes, turns, pages, cleanup, plan: output });
    }
    return results;
}

function candidateChunksWithJobs(module, tree, phase) {
    for (const method of ["begin_candidate_chunks_job", "step_tree_job",
        "finish_candidate_chunks_job", "finish_candidate_job", "cancel_tree_job"]) {
        if (typeof tree[method] !== "function") throw new Error(`packaged WASM lacks ${method} during ${phase}`);
    }
    const before = captureCandidateState(tree, phase);
    // These synchronous calls are TEST ORACLES only. Each production-style
    // job below must obtain both lists through one candidate traversal.
    const expected = {
        all: module.wasm_tree_candidate_chunk_hashes(tree),
        fresh: module.wasm_tree_new_candidate_chunk_hashes(tree),
    };
    validateCandidateChunkPlan(expected, `${phase} legacy oracle`);
    assertCandidateState(tree, before, `${phase} legacy oracle`);

    const cancelled = tree.begin_candidate_chunks_job();
    const first = tree.step_tree_job(cancelled, 1);
    validateTreeJobProgress(first, { completed: 0, reachable: 0 }, 1, `${phase} cancel step`);
    assertCandidateState(tree, before, `${phase} cancel step`);
    expectThrow(() => tree.finish_candidate_job(cancelled), `wrong finish kind accepted before cancel during ${phase}`);
    assertCandidateState(tree, before, `${phase} rejected finish before cancel`);
    tree.cancel_tree_job(cancelled);
    assertCandidateState(tree, before, `${phase} cancelled`);

    const results = [];
    for (const stepUnits of [1, 256]) {
        const token = tree.begin_candidate_chunks_job();
        expectThrow(() => tree.cancel_tree_job(cancelled), `stale chunk token cancelled a new job during ${phase}`);
        assertCandidateState(tree, before, `${phase} begin/${stepUnits}`);
        let previous = { completed: 0, reachable: 0 }, steps = 0;
        // This fixture is an ordinary tree (no shared-node DAG): every object
        // has one incoming root/internal edge. The independent graph shape
        // therefore requires exactly one edge + one node unit per object.
        const expectedUnits = expected.all.length * 2;
        const maxSteps = Math.max(1, expectedUnits + 1);
        for (; steps < maxSteps; steps++) {
            const current = tree.step_tree_job(token, stepUnits);
            validateTreeJobProgress(current, previous, stepUnits, `${phase}/${stepUnits}`);
            assertCandidateState(tree, before, `${phase} step/${stepUnits}`);
            previous = current;
            if (current.done) { steps++; break; }
        }
        if (!previous.done || previous.completed !== expectedUnits || previous.reachable !== expected.all.length) {
            throw new Error(`candidate chunk job did not perform one complete traversal during ${phase}/${stepUnits}`);
        }
        // Check the kind at a READY job: an "incomplete" rejection cannot
        // accidentally satisfy this guard. The correct finish must still own it.
        expectThrow(() => tree.finish_candidate_job(token), `ready chunk job accepted the wrong finish during ${phase}`);
        assertCandidateState(tree, before, `${phase} ready wrong finish/${stepUnits}`);
        const plan = tree.finish_candidate_chunks_job(token);
        validateCandidateChunkPlan(plan, `${phase}/${stepUnits}`);
        if (JSON.stringify(plan.all) !== JSON.stringify(expected.all) ||
            JSON.stringify(plan.fresh) !== JSON.stringify(expected.fresh)) {
            throw new Error(`candidate chunk job differs from legacy oracle during ${phase}/${stepUnits}`);
        }
        assertCandidateState(tree, before, `${phase} finished/${stepUnits}`);
        expectThrow(() => tree.finish_candidate_chunks_job(token), `finished chunk job remained active during ${phase}`);
        assertCandidateState(tree, before, `${phase} repeated finish/${stepUnits}`);
        results.push({ stepUnits, steps, completed: previous.completed, reachable: previous.reachable, plan });
    }
    return results;
}

function assertTreeJobGuards(module, tree, phase) {
    const rootBytes = tree.root_bytes();
    const rootHash = tree.root_hash_hex();
    const committedChunks = JSON.stringify(module.wasm_tree_committed_chunk_hashes(tree));
    const candidateRevision = readCandidateRevision(tree, phase);
    const token = tree.begin_candidate_job();
    assertCandidateRevision(tree, candidateRevision, `${phase} begin`);
    const wrongToken = token === 0xffff_ffff ? token - 1 : token + 1;
    if (tree.has_candidate()) throw new Error(`tree job exposed a candidate during ${phase}`);
    expectThrow(() => tree.begin_candidate_job(), `nested tree job accepted during ${phase}`);
    assertCandidateRevision(tree, candidateRevision, `${phase} rejected nested begin`);
    expectThrow(() => tree.finish_candidate_job(token), `incomplete tree job finished during ${phase}`);
    assertCandidateRevision(tree, candidateRevision, `${phase} rejected incomplete finish`);
    expectThrow(() => tree.step_tree_job(token, 0), `zero tree job budget accepted during ${phase}`);
    expectThrow(() => tree.step_tree_job(token, 257), `oversized tree job budget accepted during ${phase}`);
    expectThrow(() => tree.step_tree_job(wrongToken, 1), `wrong tree job token accepted during ${phase}`);
    assertCandidateRevision(tree, candidateRevision, `${phase} rejected steps`);
    expectThrow(() => tree.begin_candidate(), `legacy candidate begin overlapped a tree job during ${phase}`);
    expectThrow(() => tree.load_root(rootBytes), `root load overlapped a tree job during ${phase}`);
    expectThrow(() => tree.rebuild_from_entries_in_version(tree.tree_version(), "[]"),
        `tree rebuild overlapped a tree job during ${phase}`);
    expectThrow(() => tree.update_batch("[]"), `committed update overlapped a tree job during ${phase}`);
    expectThrow(() => tree.delete_batch("[]"), `committed delete overlapped a tree job during ${phase}`);
    expectThrow(() => tree.candidate_update_batch("[]"), `candidate update overlapped a tree job during ${phase}`);
    expectThrow(() => tree.commit_candidate(), `candidate commit overlapped a tree job during ${phase}`);
    expectThrow(() => tree.abort_candidate(), `candidate abort overlapped a tree job during ${phase}`);
    assertCandidateRevision(tree, candidateRevision, `${phase} rejected mutator fences`);
    const first = tree.step_tree_job(token, 1);
    validateTreeJobProgress(first, { completed: 0, reachable: 0 }, 1, phase);
    assertCandidateRevision(tree, candidateRevision, `${phase} step`);
    expectThrow(() => tree.cancel_tree_job(wrongToken), `wrong cancel token accepted during ${phase}`);
    assertCandidateRevision(tree, candidateRevision, `${phase} rejected cancel`);
    tree.cancel_tree_job(token);
    assertCandidateRevision(tree, candidateRevision, `${phase} cancel`);
    if (tree.has_candidate() || tree.root_hash_hex() !== rootHash ||
        !bytesEqual(tree.root_bytes(), rootBytes) ||
        JSON.stringify(module.wasm_tree_committed_chunk_hashes(tree)) !== committedChunks) {
        throw new Error(`cancelled tree job changed committed state during ${phase}`);
    }
    expectThrow(() => tree.cancel_tree_job(token), `cancelled tree job remained active during ${phase}`);
    assertCandidateRevision(tree, candidateRevision, `${phase} stale cancelled token`);
    const replacement = tree.begin_candidate_job();
    assertCandidateRevision(tree, candidateRevision, `${phase} replacement begin`);
    expectThrow(() => tree.cancel_tree_job(token), `stale token cancelled a replacement job during ${phase}`);
    assertCandidateRevision(tree, candidateRevision, `${phase} stale token against replacement`);
    tree.cancel_tree_job(replacement);
    assertCandidateRevision(tree, candidateRevision, `${phase} replacement cancel`);
}

function beginCandidateWithJob(module, tree, phase, stepUnits = 1) {
    for (const method of ["begin_candidate_job", "step_tree_job",
        "finish_candidate_job", "finish_candidate_chunks_job", "cancel_tree_job"]) {
        if (typeof tree[method] !== "function") {
            throw new Error(`packaged WASM lacks ${method} during ${phase}`);
        }
    }
    const rootBytes = tree.root_bytes();
    const rootHash = tree.root_hash_hex();
    const totalFiles = tree.total_files();
    const expectedReachable = module.wasm_tree_committed_chunk_hashes(tree).length;
    const candidateRevision = readCandidateRevision(tree, phase);
    const token = tree.begin_candidate_job();
    assertCandidateRevision(tree, candidateRevision, `${phase} begin`);
    const progress = [];
    let previous = { completed: 0, reachable: 0 };
    const maxSteps = 100_000;
    for (let step = 0; step < maxSteps; step++) {
        if (tree.has_candidate()) throw new Error(`tree job exposed a candidate during ${phase}`);
        const current = tree.step_tree_job(token, stepUnits);
        validateTreeJobProgress(current, previous, stepUnits, phase);
        assertCandidateRevision(tree, candidateRevision, `${phase} step ${step}`);
        if (tree.has_candidate() || tree.root_hash_hex() !== rootHash ||
            tree.total_files() !== totalFiles || !bytesEqual(tree.root_bytes(), rootBytes)) {
            throw new Error(`tree job mutated committed/candidate state during ${phase}`);
        }
        progress.push(current);
        previous = current;
        if (current.done) break;
        if (step === maxSteps - 1) throw new Error(`tree job did not finish during ${phase}`);
    }
    expectThrow(() => tree.finish_candidate_chunks_job(token), `ready begin job accepted chunk finish during ${phase}`);
    assertCandidateRevision(tree, candidateRevision, `${phase} rejected chunk finish`);
    expectThrow(() => tree.finish_candidate_mutation_job(token), `ready begin job accepted mutation finish during ${phase}`);
    assertCandidateRevision(tree, candidateRevision, `${phase} rejected mutation finish`);
    if (tree.has_candidate() || tree.root_hash_hex() !== rootHash || tree.total_files() !== totalFiles ||
        !bytesEqual(tree.root_bytes(), rootBytes)) {
        throw new Error(`wrong-kind finish changed begin job state during ${phase}`);
    }
    const reachable = tree.finish_candidate_job(token);
    assertCandidateRevision(tree, candidateRevision + 1, `${phase} finish`);
    if (!tree.has_candidate() || reachable !== previous.reachable ||
        reachable !== expectedReachable ||
        tree.root_hash_hex() !== rootHash || tree.total_files() !== totalFiles ||
        !bytesEqual(tree.root_bytes(), rootBytes) ||
        tree.candidate_root_hash_hex() !== rootHash ||
        tree.candidate_total_files() !== totalFiles ||
        !bytesEqual(tree.candidate_root_bytes(), rootBytes)) {
        throw new Error(`tree job changed candidate begin semantics during ${phase}`);
    }
    return { progress, reachable };
}

function assertEmptyTreeJobs(module, label) {
    for (const version of [1, 2]) {
        const tree = new module.WasmTree("parity-vault", `empty-${version}`);
        try {
            tree.set_tree_version(version);
            tree.build_from_entries("[]");
            const opened = beginCandidateWithJob(module, tree, `${label} empty v${version}`);
            if (opened.reachable !== 0 || opened.progress.length !== 1 ||
                opened.progress[0].completed !== 0 || opened.progress[0].units !== 0) {
                throw new Error(`empty v${version} tree job did unexpected work in ${label}`);
            }
            const plans = candidateChunksWithJobs(module, tree, `${label} empty v${version} chunks`);
            if (plans.some(result => result.steps !== 1 || result.completed !== 0 ||
                result.plan.all.length !== 0 || result.plan.fresh.length !== 0)) {
                throw new Error(`empty v${version} candidate chunk job did unexpected work in ${label}`);
            }
            tree.abort_candidate();
        } finally { tree.free(); }
    }
}

function assertTreeJobStepFailureRecovery(module, rootBytes, phase) {
    const tree = new module.WasmTree("parity-vault", "missing-chunks");
    try {
        tree.load_root(rootBytes);
        const rootHash = tree.root_hash_hex();
        const token = tree.begin_candidate_job();
        let failed = false;
        for (let step = 0; step < 10_000; step++) {
            let current;
            try {
                current = tree.step_tree_job(token, 1);
            } catch {
                failed = true;
                break;
            }
            if (current.done) throw new Error(`incomplete graph validated during ${phase}`);
        }
        if (!failed || tree.has_candidate() || tree.root_hash_hex() !== rootHash) {
            throw new Error(`failed tree job changed state during ${phase}`);
        }
        const retry = tree.begin_candidate_job();
        tree.cancel_tree_job(retry);
    } finally {
        tree.free();
    }
}

function drainGenericRetirementWithReachabilityGuards(tree, token, expected, phase) {
    let completed = 0, turns = 0;
    for (;;) {
        for (const operation of [
            () => tree.step_reachability_retirement(token, completed, 1),
            () => tree.finish_reachability_retirement(token, completed),
            () => tree.cancel_reachability_job_deferred(token),
        ]) {
            expectThrow(operation, `reachability family accepted generic retirement during ${phase}`);
            assertReachabilityVisibleState(tree, expected, `${phase} rejected reachability operation`);
        }
        const progress = tree.step_tree_retirement(token, 1);
        if (!progress || typeof progress !== "object" || Array.isArray(progress) ||
            Object.keys(progress).sort().join(",") !== "completed,done,units" ||
            typeof progress.done !== "boolean" || !Number.isSafeInteger(progress.units) ||
            progress.units < 0 || progress.units > 1 || (!progress.done && progress.units === 0) ||
            !Number.isSafeInteger(progress.completed) || progress.completed !== completed + progress.units) {
            throw new Error(`invalid generic retirement progress during ${phase}`);
        }
        completed = progress.completed;
        turns++;
        assertReachabilityVisibleState(tree, expected, `${phase} generic step`);
        if (turns > 100_000) throw new Error(`generic retirement did not converge during ${phase}`);
        if (progress.done) break;
    }
    expectThrow(() => tree.step_tree_retirement(token, 1),
        `completed generic retirement remained live during ${phase}`);
    expectThrow(() => tree.finish_reachability_retirement(token, completed),
        `generic retirement fabricated a reachability tombstone during ${phase}`);
    return { turns, completed };
}

function assertPackagedDeferredReachability() {
    // This stays deliberately compact: the older matrix above exercises the
    // large/multi-node traversal itself. Here every scalar/SIMD + v1/v2 case
    // focuses on the additive ownership, replay and acknowledgement protocol.
    const entries = deterministicTreeEntries(96), reports = [], references = new Map();
    const now = Date.now;
    Date.now = () => 1_905_000_000_000;
    try {
        for (const version of [1, 2]) for (const [variant, module] of [["scalar", scalar], ["SIMD", simd]]) {
            const phase = `${variant} v${version} deferred reachability`;
            const tree = new module.WasmTree("deferred-parity-vault", "deferred-parity-device");
            let failed;
            try {
                const required = ["step_reachability_job_deferred", "finish_candidate_job_deferred",
                    "finish_candidate_chunks_job_deferred", "cancel_reachability_job_deferred",
                    "step_reachability_retirement", "finish_reachability_retirement",
                    "step_tree_retirement", "cancel_replacement_rebuild_job_deferred"];
                for (const method of required) {
                    if (typeof tree[method] !== "function") {
                        throw new Error(`packaged ${variant} WASM lacks ${method}; rebuild both variants before parity`);
                    }
                }
                tree.set_tree_version(version);
                tree.build_from_entries(JSON.stringify(entries));
                const initial = reachabilityVisibleState(tree, `${phase} initial`);

                // Partial cancellation must retain the exact job token, reject
                // the legacy generic retirement family, and never expose a
                // candidate or advance either revision.
                const cancelledToken = requireTreeJobToken(tree.begin_candidate_job(), `${phase} cancel begin`);
                const wrongCancelled = differentTreeJobToken(cancelledToken);
                expectThrow(() => tree.step_reachability_job_deferred(cancelledToken, 0),
                    `zero deferred reachability budget accepted during ${phase}`);
                expectThrow(() => tree.step_reachability_job_deferred(cancelledToken, 257),
                    `oversized deferred reachability budget accepted during ${phase}`);
                expectThrow(() => tree.step_reachability_job_deferred(wrongCancelled, 1),
                    `wrong token stepped deferred reachability during ${phase}`);
                const partial = tree.step_reachability_job_deferred(cancelledToken, 1);
                validateExactTreeJobProgress(partial, { completed: 0, reachable: 0 }, 1, `${phase} partial cancel`);
                if (partial.done) throw new Error(`partial cancellation fixture was already READY during ${phase}`);
                assertReachabilityVisibleState(tree, initial, `${phase} partial cancel`);
                expectThrow(() => tree.finish_candidate_job_deferred(cancelledToken),
                    `incomplete deferred candidate job published during ${phase}`);
                expectThrow(() => tree.cancel_reachability_job_deferred(wrongCancelled),
                    `wrong token cancelled deferred reachability during ${phase}`);
                if (tree.cancel_reachability_job_deferred(cancelledToken) !== undefined ||
                    tree.cancel_reachability_job_deferred(cancelledToken) !== undefined) {
                    throw new Error(`deferred reachability cancellation returned a value during ${phase}`);
                }
                expectThrow(() => tree.step_reachability_job_deferred(cancelledToken, 1),
                    `cancelled traversal remained executable during ${phase}`);
                const cancelled = drainReachabilityRetirement(tree, cancelledToken, 1, initial,
                    `${phase} cancelled cleanup`);

                // A successful finish publishes exactly once, then the cursor
                // remains exclusively owned by retirement under that token.
                const beforePublish = reachabilityVisibleState(tree, `${phase} before publish`);
                const publishToken = requireTreeJobToken(tree.begin_candidate_job(), `${phase} publish begin`);
                if (publishToken === cancelledToken) throw new Error(`tree job token reused after cleanup during ${phase}`);
                for (const operation of [
                    () => tree.cancel_reachability_job_deferred(cancelledToken),
                    () => tree.finish_reachability_retirement(cancelledToken, cancelled.completed),
                ]) expectThrow(operation, `stale token affected replacement traversal during ${phase}`);
                const ready = stepDeferredReachabilityToReady(tree, publishToken, 256, beforePublish,
                    `${phase} publish traversal`);
                expectThrow(() => tree.finish_candidate_chunks_job_deferred(publishToken),
                    `candidate-open traversal accepted chunk finish during ${phase}`);
                assertReachabilityVisibleState(tree, beforePublish, `${phase} wrong publish finish`);
                const reachable = tree.finish_candidate_job_deferred(publishToken);
                if (!Number.isSafeInteger(reachable) || reachable !== ready.reachable) {
                    throw new Error(`deferred candidate finish returned an invalid reachable count during ${phase}`);
                }
                const published = reachabilityVisibleState(tree, `${phase} published`);
                if (!published.candidate || published.committedRevision !== beforePublish.committedRevision ||
                    published.candidateRevision !== beforePublish.candidateRevision + 1 ||
                    published.committedHash !== beforePublish.committedHash ||
                    published.committedBytes !== beforePublish.committedBytes ||
                    published.candidateHash !== published.committedHash ||
                    published.candidateCount !== published.committedCount ||
                    published.candidateBytes !== published.committedBytes) {
                    throw new Error(`deferred candidate publication changed legacy semantics during ${phase}`);
                }
                const publishedCleanup = drainReachabilityRetirement(tree, publishToken, 256, published,
                    `${phase} published cleanup`);

                // Prove the acknowledged owner hands the slot to a real native
                // mutation job. Its finish advances only the candidate witness.
                const mutationBefore = reachabilityVisibleState(tree, `${phase} mutation before`);
                const updated = [{ ...entries[7], hash: module.wasm_hash(deterministicPayload(4_097)),
                    mtime_ms: 1_905_000_000_777, size: 4_097 }];
                const mutationToken = requireTreeJobToken(
                    tree.begin_candidate_update_job(JSON.stringify(updated)), `${phase} mutation begin`);
                let mutationPrevious = { completed: 0, reachable: 0 }, mutationTurns = 0;
                for (;;) {
                    const progress = tree.step_tree_job(mutationToken, 256);
                    validateExactTreeJobProgress(progress, mutationPrevious, 256, `${phase} mutation step`);
                    assertReachabilityVisibleState(tree, mutationBefore, `${phase} mutation private`);
                    mutationPrevious = progress;
                    mutationTurns++;
                    if (mutationTurns > 10_000) throw new Error(`mutation handoff did not converge during ${phase}`);
                    if (progress.done) break;
                }
                if (tree.finish_candidate_mutation_job(mutationToken) !== undefined) {
                    throw new Error(`candidate mutation finish returned a value during ${phase}`);
                }
                const mutationAfter = reachabilityVisibleState(tree, `${phase} mutation after`);
                if (mutationAfter.committedRevision !== mutationBefore.committedRevision ||
                    mutationAfter.committedHash !== mutationBefore.committedHash ||
                    mutationAfter.committedBytes !== mutationBefore.committedBytes ||
                    mutationAfter.candidateRevision !== mutationBefore.candidateRevision + 1 ||
                    mutationAfter.candidateHash === mutationBefore.candidateHash) {
                    throw new Error(`native mutation handoff changed the wrong visible owner during ${phase}`);
                }

                // Candidate-chunk deferred finish is read-only and must return
                // the exact legacy sorted plan before retiring the descriptor
                // cursor. This also covers a non-empty fresh set.
                const expectedPlan = {
                    all: module.wasm_tree_candidate_chunk_hashes(tree),
                    fresh: module.wasm_tree_new_candidate_chunk_hashes(tree),
                };
                validateCandidateChunkPlan(expectedPlan, `${phase} chunk oracle`);
                if (expectedPlan.fresh.length === 0) throw new Error(`chunk fixture has no fresh objects during ${phase}`);
                const beforeChunks = reachabilityVisibleState(tree, `${phase} chunks before`);
                const chunksToken = requireTreeJobToken(tree.begin_candidate_chunks_job(), `${phase} chunks begin`);
                const chunksReady = stepDeferredReachabilityToReady(tree, chunksToken, 256, beforeChunks,
                    `${phase} chunks traversal`);
                expectThrow(() => tree.finish_candidate_job_deferred(chunksToken),
                    `candidate-chunk traversal accepted candidate-open finish during ${phase}`);
                const plan = tree.finish_candidate_chunks_job_deferred(chunksToken);
                validateCandidateChunkPlan(plan, `${phase} deferred chunk finish`);
                if (JSON.stringify(plan) !== JSON.stringify(expectedPlan)) {
                    throw new Error(`deferred candidate chunk plan differs from legacy oracle during ${phase}`);
                }
                assertReachabilityVisibleState(tree, beforeChunks, `${phase} chunk finish`);
                const chunksCleanup = drainReachabilityRetirement(tree, chunksToken, 1, beforeChunks,
                    `${phase} chunks cleanup`);

                tree.abort_candidate();
                const beforeGeneric = reachabilityVisibleState(tree, `${phase} generic before`);
                if (beforeGeneric.candidate ||
                    beforeGeneric.candidateRevision !== beforeChunks.candidateRevision + 1) {
                    throw new Error(`candidate abort witness differed before generic retirement during ${phase}`);
                }
                const genericToken = requireTreeJobToken(
                    tree.begin_replacement_rebuild_job(version, 0, 2), `${phase} generic begin`);
                if (tree.cancel_replacement_rebuild_job_deferred(genericToken) !== undefined) {
                    throw new Error(`generic deferred cancellation returned a value during ${phase}`);
                }
                const generic = drainGenericRetirementWithReachabilityGuards(
                    tree, genericToken, beforeGeneric, `${phase} generic isolation`);

                // Removing every resident child from a root-only wrapper forces
                // the deferred stepping error path. The failed cursor must be
                // retired using the same token instead of disappearing.
                failed = new module.WasmTree("deferred-parity-vault", "deferred-missing-device");
                failed.load_root(tree.root_bytes());
                const failedBefore = reachabilityVisibleState(failed, `${phase} missing before`);
                const failedToken = requireTreeJobToken(failed.begin_candidate_job(), `${phase} missing begin`);
                let failedPrevious = { completed: 0, reachable: 0 }, failedTurns = 0, sawFailure = false;
                for (; failedTurns < 10_000; failedTurns++) {
                    let progress;
                    try {
                        progress = failed.step_reachability_job_deferred(failedToken, 1);
                    } catch {
                        sawFailure = true;
                        break;
                    }
                    validateExactTreeJobProgress(progress, failedPrevious, 1, `${phase} missing step`);
                    if (progress.done) throw new Error(`missing graph was accepted during ${phase}`);
                    assertReachabilityVisibleState(failed, failedBefore, `${phase} missing private step`);
                    failedPrevious = progress;
                }
                if (!sawFailure) throw new Error(`missing graph never entered failed retirement during ${phase}`);
                assertReachabilityVisibleState(failed, failedBefore, `${phase} missing failure`);
                if (failed.cancel_reachability_job_deferred(failedToken) !== undefined ||
                    failed.cancel_reachability_job_deferred(failedToken) !== undefined) {
                    throw new Error(`failed reachability cancel returned a value during ${phase}`);
                }
                expectThrow(() => failed.step_reachability_job_deferred(failedToken, 1),
                    `failed reachability cursor resumed during ${phase}`);
                const failedCleanup = drainReachabilityRetirement(failed, failedToken, 256, failedBefore,
                    `${phase} failed cleanup`);

                const summary = { version, cancelled: { partial, cleanup: cancelled },
                    published: { ready, reachable, cleanup: publishedCleanup },
                    mutation: { turns: mutationTurns, completed: mutationPrevious.completed,
                        candidateHash: mutationAfter.candidateHash },
                    chunks: { ready: chunksReady, plan, cleanup: chunksCleanup },
                    generic, failed: { turns: failedTurns, cleanup: failedCleanup } };
                const comparable = JSON.stringify(summary);
                if (references.has(version) && references.get(version) !== comparable) {
                    throw new Error(`scalar/SIMD deferred reachability parity mismatch at v${version}`);
                }
                references.set(version, comparable);
                reports.push({ variant, ...summary });
                if (process.env.OBSETYNC_PARITY_PROGRESS === "1") {
                    console.error(`deferred reachability parity: ${phase} passed`);
                }
            } finally {
                failed?.free();
                tree.free();
            }
        }
        return reports;
    } finally { Date.now = now; }
}

const CANDIDATE_OPEN_MEMORY_FIELDS = ["schema", "scope", "residentChunkCount",
    "rootStringCount", "rootIdentityRequestedBytes", "rootEndpointRequestedBytes",
    "rootStringRequestedBytes", "baselineKeySnapshotRequestedBytes", "peakAdmissionBytes",
    "baselineStrategy"];

function candidateOpenMemoryPlan(value, expected, phase, expectedScope = "v2-candidate-open-root") {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        JSON.stringify(Reflect.ownKeys(value).slice().sort()) !==
            JSON.stringify(CANDIDATE_OPEN_MEMORY_FIELDS.slice().sort())) {
        throw new Error(`invalid candidate-open memory plan shape during ${phase}`);
    }
    for (const field of CANDIDATE_OPEN_MEMORY_FIELDS) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor)) {
            throw new Error(`candidate-open memory plan exposed an accessor during ${phase}`);
        }
    }
    if (value.schema !== 1 || value.scope !== expectedScope ||
        value.baselineStrategy !== "insertion-generation-v1") {
        throw new Error(`invalid candidate-open memory plan identity during ${phase}`);
    }
    for (const field of ["residentChunkCount", "rootStringCount", "rootIdentityRequestedBytes",
        "rootEndpointRequestedBytes", "rootStringRequestedBytes",
        "baselineKeySnapshotRequestedBytes", "peakAdmissionBytes"]) {
        if (!Number.isSafeInteger(value[field]) || value[field] < 0) {
            throw new Error(`invalid candidate-open ${field} during ${phase}`);
        }
    }
    if (value.rootStringRequestedBytes !==
            value.rootIdentityRequestedBytes + value.rootEndpointRequestedBytes ||
        value.peakAdmissionBytes !== value.rootStringRequestedBytes ||
        value.baselineKeySnapshotRequestedBytes !== 0) {
        throw new Error(`inconsistent candidate-open memory plan during ${phase}`);
    }
    for (const [field, expectedValue] of Object.entries(expected)) {
        if (field === "scope") continue;
        if (value[field] !== expectedValue) {
            throw new Error(`candidate-open ${field} ${value[field]} differed from independent ` +
                `${expectedValue} during ${phase}`);
        }
    }
    return value;
}

function candidateOpenRows(count) {
    return Array.from({ length: count }, (_, index) => ({
        path: `candidate-open/${String(index).padStart(8, "0")}.md`,
        hash: (index + 1).toString(16).padStart(64, "0"),
        mtime_ms: 1_906_000_000_000 + index,
        size: 64 + index % 4_096,
    }));
}

function candidateOpenExpected(module, tree, rows, vault, device) {
    const paths = rows.map(row => Buffer.from(row.path, "utf8"))
        .sort((left, right) => Buffer.compare(left, right));
    const rootIdentityRequestedBytes = Buffer.byteLength(vault) + Buffer.byteLength(device);
    const rootEndpointRequestedBytes = paths.length === 0 ? 0 :
        paths[0].byteLength + paths[paths.length - 1].byteLength;
    const rootStringRequestedBytes = rootIdentityRequestedBytes + rootEndpointRequestedBytes;
    return {
        residentChunkCount: module.wasm_tree_committed_chunk_hashes(tree).length,
        rootStringCount: paths.length === 0 ? 2 : 4,
        rootIdentityRequestedBytes,
        rootEndpointRequestedBytes,
        rootStringRequestedBytes,
        baselineKeySnapshotRequestedBytes: 0,
        peakAdmissionBytes: rootStringRequestedBytes,
    };
}

function assertCandidateOpenPlanRefusals(tree, token, witnesses, expectedState, phase) {
    const wrongToken = differentTreeJobToken(token);
    expectThrow(() => tree.candidate_open_memory_plan_v1_job(wrongToken),
        `wrong token read candidate-open plan during ${phase}`);
    expectThrow(() => tree.resume_candidate_open_memory_v1_job(wrongToken, ...witnesses),
        `wrong token resumed candidate-open during ${phase}`);
    assertReachabilityVisibleState(tree, expectedState, `${phase} wrong token`);
    for (let field = 0; field < witnesses.length; field++) {
        for (const bad of new Set([witnesses[field] + 1, witnesses[field] - 1,
            -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])) {
            const invalid = witnesses.slice();
            invalid[field] = bad;
            expectThrow(() => tree.resume_candidate_open_memory_v1_job(token, ...invalid),
                `candidate-open witness ${field} accepted ${bad} during ${phase}`);
            assertReachabilityVisibleState(tree, expectedState,
                `${phase} rejected witness ${field}/${String(bad)}`);
        }
    }
}

function assertPackagedCandidateOpenMemory() {
    const reports = [], references = new Map(), now = Date.now;
    Date.now = () => 1_906_000_000_000;
    try {
        for (const [variant, module] of [["scalar", scalar], ["SIMD", simd]]) {
            for (const count of [0, 1, 25_000]) {
                const phase = `${variant} v2 candidate-open memory/${count}`;
                const vault = `candidate-open-vault-${count}`;
                const device = `candidate-open-device-${count}`;
                const rows = candidateOpenRows(count);
                const tree = new module.WasmTree(vault, device);
                try {
                    for (const method of ["candidate_open_memory_plan_v1_job",
                        "resume_candidate_open_memory_v1_job", "step_reachability_job_deferred",
                        "finish_candidate_job_deferred", "cancel_reachability_job_deferred",
                        "step_reachability_retirement", "finish_reachability_retirement"]) {
                        if (typeof tree[method] !== "function") {
                            throw new Error(`packaged ${variant} WASM lacks ${method}; rebuild both variants before parity`);
                        }
                    }
                    tree.set_tree_version(2);
                    tree.build_from_entries(JSON.stringify(rows));
                    const initial = reachabilityVisibleState(tree, `${phase} initial`);
                    const baselineHashes = module.wasm_tree_committed_chunk_hashes(tree);
                    const baseline = new Set(baselineHashes);
                    if (baseline.size !== baselineHashes.length) {
                        throw new Error(`committed baseline contains duplicate keys during ${phase}`);
                    }
                    const expected = candidateOpenExpected(module, tree, rows, vault, device);

                    // A completed traversal reaches the new admission barrier without
                    // cloning the root or materializing the old all-resident key set.
                    const cancelledToken = requireTreeJobToken(tree.begin_candidate_job(), `${phase} cancel begin`);
                    expectThrow(() => tree.candidate_open_memory_plan_v1_job(cancelledToken),
                        `candidate-open plan was visible before READY during ${phase}`);
                    expectThrow(() => tree.resume_candidate_open_memory_v1_job(cancelledToken,
                        expected.rootIdentityRequestedBytes, expected.rootEndpointRequestedBytes,
                        expected.rootStringRequestedBytes),
                    `candidate-open resumed before READY during ${phase}`);
                    assertReachabilityVisibleState(tree, initial, `${phase} pre-barrier refusal`);
                    const cancelledReady = stepDeferredReachabilityToReady(tree, cancelledToken, 256,
                        initial, `${phase} cancel traversal`);
                    const cancelledPlan = candidateOpenMemoryPlan(
                        tree.candidate_open_memory_plan_v1_job(cancelledToken), expected, `${phase} cancel plan`);
                    const repeatedPlan = candidateOpenMemoryPlan(
                        tree.candidate_open_memory_plan_v1_job(cancelledToken), expected, `${phase} repeated plan`);
                    if (JSON.stringify(cancelledPlan) !== JSON.stringify(repeatedPlan)) {
                        throw new Error(`candidate-open plan was not stable during ${phase}`);
                    }
                    const witnesses = [cancelledPlan.rootIdentityRequestedBytes,
                        cancelledPlan.rootEndpointRequestedBytes, cancelledPlan.rootStringRequestedBytes];
                    assertCandidateOpenPlanRefusals(tree, cancelledToken, witnesses, initial,
                        `${phase} cancellation`);
                    if (tree.resume_candidate_open_memory_v1_job(cancelledToken, ...witnesses) !== undefined) {
                        throw new Error(`candidate-open resume returned a value during ${phase}`);
                    }
                    assertReachabilityVisibleState(tree, initial, `${phase} prepared private root`);
                    expectThrow(() => tree.candidate_open_memory_plan_v1_job(cancelledToken),
                        `prepared candidate-open still exposed its plan during ${phase}`);
                    expectThrow(() => tree.resume_candidate_open_memory_v1_job(cancelledToken, ...witnesses),
                        `prepared candidate-open resumed twice during ${phase}`);
                    expectThrow(() => tree.step_reachability_job_deferred(cancelledToken, 1),
                        `prepared candidate-open resumed validation during ${phase}`);
                    expectThrow(() => tree.cancel_tree_job(cancelledToken),
                        `generic cancel consumed prepared candidate-open during ${phase}`);
                    assertReachabilityVisibleState(tree, initial, `${phase} prepared refusal`);
                    tree.cancel_reachability_job_deferred(cancelledToken);
                    tree.cancel_reachability_job_deferred(cancelledToken);
                    const cancelledCleanup = drainReachabilityRetirement(tree, cancelledToken, 256,
                        initial, `${phase} prepared cancellation`);

                    // The accepted path publishes exactly the pre-existing semantic root.
                    const publishToken = requireTreeJobToken(tree.begin_candidate_job(), `${phase} publish begin`);
                    const publishReady = stepDeferredReachabilityToReady(tree, publishToken, 256,
                        initial, `${phase} publish traversal`);
                    const publishPlan = candidateOpenMemoryPlan(
                        tree.candidate_open_memory_plan_v1_job(publishToken), expected, `${phase} publish plan`);
                    const publishWitnesses = [publishPlan.rootIdentityRequestedBytes,
                        publishPlan.rootEndpointRequestedBytes, publishPlan.rootStringRequestedBytes];
                    tree.resume_candidate_open_memory_v1_job(publishToken, ...publishWitnesses);
                    const reachable = tree.finish_candidate_job_deferred(publishToken);
                    if (!Number.isSafeInteger(reachable) || reachable !== expected.residentChunkCount ||
                        reachable !== publishReady.reachable) {
                        throw new Error(`candidate-open finish returned the wrong reachable count during ${phase}`);
                    }
                    const published = reachabilityVisibleState(tree, `${phase} published`);
                    if (!published.candidate || published.committedRevision !== initial.committedRevision ||
                        published.candidateRevision !== initial.candidateRevision + 1 ||
                        published.committedHash !== initial.committedHash ||
                        published.committedBytes !== initial.committedBytes ||
                        published.candidateHash !== initial.committedHash ||
                        published.candidateBytes !== initial.committedBytes ||
                        published.candidateCount !== initial.committedCount) {
                        throw new Error(`candidate-open publication changed root semantics during ${phase}`);
                    }
                    const publishCleanup = drainReachabilityRetirement(tree, publishToken, 256,
                        published, `${phase} published cleanup`);

                    // Reconstruct the old all-resident-at-open behavior without
                    // consulting the native baseline representation.
                    const changed = rows.length === 0 ? [{ path: "candidate-open/new.md",
                        hash: "fe".repeat(32), mtime_ms: 1_906_000_100_000, size: 999 }]
                        : [{ ...rows[0], hash: "fe".repeat(32),
                            mtime_ms: rows[0].mtime_ms + 100_000, size: rows[0].size + 1 }];
                    tree.candidate_update_batch(JSON.stringify(changed));
                    const mutation = reachabilityVisibleState(tree, `${phase} mutated`);
                    const chunksToken = requireTreeJobToken(tree.begin_candidate_chunks_job(),
                        `${phase} chunk begin`);
                    expectThrow(() => tree.candidate_open_memory_plan_v1_job(chunksToken),
                        `candidate-chunk job exposed candidate-open plan during ${phase}`);
                    expectThrow(() => tree.resume_candidate_open_memory_v1_job(chunksToken,
                        ...publishWitnesses), `candidate-chunk job accepted candidate-open resume during ${phase}`);
                    const chunksReady = stepDeferredReachabilityToReady(tree, chunksToken, 256,
                        mutation, `${phase} chunk traversal`);
                    expectThrow(() => tree.candidate_open_memory_plan_v1_job(chunksToken),
                        `ready candidate-chunk job exposed candidate-open plan during ${phase}`);
                    expectThrow(() => tree.resume_candidate_open_memory_v1_job(chunksToken,
                        ...publishWitnesses), `ready candidate-chunk job accepted candidate-open resume during ${phase}`);
                    const chunkPlan = tree.finish_candidate_chunks_job_deferred(chunksToken);
                    validateCandidateChunkPlan(chunkPlan, `${phase} independent baseline result`);
                    const expectedFresh = chunkPlan.all.filter(hash => !baseline.has(hash));
                    if (JSON.stringify(chunkPlan.fresh) !== JSON.stringify(expectedFresh) ||
                        expectedFresh.length === 0) {
                        throw new Error(`generation baseline differs from all-resident-at-open oracle during ${phase}`);
                    }
                    const chunksCleanup = drainReachabilityRetirement(tree, chunksToken, 256,
                        mutation, `${phase} chunk cleanup`);
                    const freshBytes = new Map(chunkPlan.fresh.map(hash => {
                        const bytes = module.wasm_tree_get_chunk(tree, hash);
                        if (!(bytes instanceof Uint8Array) || module.wasm_hash(bytes) !== hash) {
                            throw new Error(`fresh candidate chunk unavailable before abort during ${phase}`);
                        }
                        return [hash, Buffer.from(bytes).toString("hex")];
                    }));
                    const beforeAbortRevision = tree.candidate_revision();
                    tree.abort_candidate();
                    const aborted = reachabilityVisibleState(tree, `${phase} aborted`);
                    if (aborted.candidate || aborted.committedHash !== initial.committedHash ||
                        aborted.committedBytes !== initial.committedBytes ||
                        aborted.committedRevision !== initial.committedRevision ||
                        aborted.candidateRevision !== beforeAbortRevision + 1) {
                        throw new Error(`candidate-open abort changed committed state during ${phase}`);
                    }
                    for (const hash of freshBytes.keys()) {
                        if (module.wasm_tree_chunk_byte_length(tree, hash) !== undefined ||
                            module.wasm_tree_get_chunk(tree, hash) !== undefined) {
                            throw new Error(`aborted generation-fresh chunk remained resident during ${phase}`);
                        }
                    }
                    for (const hash of baseline) {
                        if (module.wasm_tree_chunk_byte_length(tree, hash) === undefined) {
                            throw new Error(`abort swept an all-resident-at-open chunk during ${phase}`);
                        }
                    }

                    const summary = { count, rootHash: initial.committedHash, plan: publishPlan,
                        cancelled: { ready: cancelledReady, cleanup: cancelledCleanup },
                        published: { ready: publishReady, reachable, cleanup: publishCleanup },
                        baseline: { resident: baseline.size, all: chunkPlan.all.length,
                            fresh: chunkPlan.fresh.length, ready: chunksReady, cleanup: chunksCleanup } };
                    const comparable = JSON.stringify(summary);
                    if (references.has(count) && references.get(count) !== comparable) {
                        throw new Error(`scalar/SIMD candidate-open parity mismatch for ${count} entries`);
                    }
                    references.set(count, comparable);
                    reports.push({ variant, ...summary });
                } finally { tree.free(); }
            }

            // V1 now shares the additive root-output admission family. The
            // legacy finish path remains compatible by internally preparing
            // the same root when an older caller does not opt into the plan.
            const phase = `${variant} v1 candidate-open compatibility`;
            const tree = new module.WasmTree("candidate-open-v1-vault", "candidate-open-v1-device");
            try {
                tree.set_tree_version(1);
                tree.build_from_entries(JSON.stringify(candidateOpenRows(1)));
                const before = reachabilityVisibleState(tree, `${phase} initial`);
                const expected = { scope: "v1-candidate-open-root",
                    residentChunkCount: module.wasm_tree_committed_chunk_hashes(tree).length,
                    rootIdentityRequestedBytes: Buffer.byteLength("candidate-open-v1-vault")
                        + Buffer.byteLength("candidate-open-v1-device") };

                const cancelToken = requireTreeJobToken(tree.begin_candidate_job(), `${phase} cancel begin`);
                const cancelReady = stepDeferredReachabilityToReady(tree, cancelToken, 256, before,
                    `${phase} cancel traversal`);
                const cancelPlan = candidateOpenMemoryPlan(
                    tree.candidate_open_memory_plan_v1_job(cancelToken), expected,
                    `${phase} cancel plan`, "v1-candidate-open-root");
                if (cancelPlan.rootStringCount < 2 || cancelPlan.rootEndpointRequestedBytes === 0 ||
                    cancelReady.reachable !== cancelPlan.residentChunkCount) {
                    throw new Error(`V1 candidate-open plan omitted resident root ownership during ${phase}`);
                }
                const cancelWitnesses = [cancelPlan.rootIdentityRequestedBytes,
                    cancelPlan.rootEndpointRequestedBytes, cancelPlan.rootStringRequestedBytes];
                assertCandidateOpenPlanRefusals(tree, cancelToken, cancelWitnesses, before,
                    `${phase} cancellation`);
                tree.resume_candidate_open_memory_v1_job(cancelToken, ...cancelWitnesses);
                assertReachabilityVisibleState(tree, before, `${phase} admitted private root`);
                tree.cancel_reachability_job_deferred(cancelToken);
                const cancelled = drainReachabilityRetirement(tree, cancelToken, 1, before,
                    `${phase} cancel cleanup`);

                const token = requireTreeJobToken(tree.begin_candidate_job(), `${phase} publish begin`);
                const ready = stepDeferredReachabilityToReady(tree, token, 256, before, `${phase} traversal`);
                const plan = candidateOpenMemoryPlan(tree.candidate_open_memory_plan_v1_job(token), expected,
                    `${phase} publish plan`, "v1-candidate-open-root");
                const witnesses = [plan.rootIdentityRequestedBytes,
                    plan.rootEndpointRequestedBytes, plan.rootStringRequestedBytes];
                tree.resume_candidate_open_memory_v1_job(token, ...witnesses);
                assertReachabilityVisibleState(tree, before, `${phase} publish prepared`);
                const reachable = tree.finish_candidate_job_deferred(token);
                if (reachable !== ready.reachable) {
                    throw new Error(`V1 legacy candidate finish changed during ${phase}`);
                }
                const published = reachabilityVisibleState(tree, `${phase} published`);
                if (!published.candidate || published.candidateRevision !== before.candidateRevision + 1 ||
                    published.candidateBytes !== before.committedBytes) {
                    throw new Error(`V1 legacy candidate publication changed during ${phase}`);
                }
                const cleanup = drainReachabilityRetirement(tree, token, 256, published, `${phase} cleanup`);
                tree.abort_candidate();

                const legacyBefore = reachabilityVisibleState(tree, `${phase} legacy initial`);
                const legacyToken = requireTreeJobToken(tree.begin_candidate_job(), `${phase} legacy begin`);
                const legacyReady = stepDeferredReachabilityToReady(tree, legacyToken, 256, legacyBefore,
                    `${phase} legacy traversal`);
                const legacyReachable = tree.finish_candidate_job_deferred(legacyToken);
                const legacyPublished = reachabilityVisibleState(tree, `${phase} legacy published`);
                const legacyCleanup = drainReachabilityRetirement(tree, legacyToken, 256, legacyPublished,
                    `${phase} legacy cleanup`);
                tree.abort_candidate();
                reports.push({ variant, version: 1, compatibility: { plan, cancelled,
                    ready, reachable, cleanup, legacy: {
                        ready: legacyReady, reachable: legacyReachable, cleanup: legacyCleanup } } });
            } finally { tree.free(); }
        }
        const scalarV1 = reports.find(row => row.variant === "scalar" && row.version === 1);
        const simdV1 = reports.find(row => row.variant === "SIMD" && row.version === 1);
        if (JSON.stringify({ ...scalarV1, variant: undefined }) !==
            JSON.stringify({ ...simdV1, variant: undefined })) {
            throw new Error("scalar/SIMD V1 candidate-open compatibility mismatch");
        }
        return reports;
    } finally { Date.now = now; }
}

function assertPackagedDeferredInternalExpansion() {
    // Just beyond the native V2 leaf ceiling: enough for a real Internal root,
    // without repeating the large mutation/rebuild corpus. Inspect the legacy
    // immutable bytes independently of the deferred cursor's progress report.
    const entries = deterministicTreeEntries(1_025), reports = [];
    for (const [variant, module] of [["scalar", scalar], ["SIMD", simd]]) {
        const phase = `${variant} v2 deferred internal expansion`;
        const tree = treeV2State(module, entries);
        try {
            const before = reachabilityVisibleState(tree, `${phase} initial`);
            const hashes = module.wasm_tree_committed_chunk_hashes(tree);
            validateCandidateChunkPlan({ all: hashes, fresh: [] }, `${phase} legacy oracle`);
            const chunks = hashes.map(hash => {
                const bytes = module.wasm_tree_get_chunk(tree, hash);
                if (!(bytes instanceof Uint8Array) || bytes.length < 8 || module.wasm_hash(bytes) !== hash) {
                    throw new Error(`invalid immutable oracle chunk during ${phase}`);
                }
                return { hash, bytes, magic: Buffer.from(bytes.subarray(0, 4)).toString("ascii") };
            });
            const internals = chunks.filter(chunk => chunk.magic === "OVI2");
            const leaves = chunks.filter(chunk => chunk.magic === "OVL2");
            if (internals.length !== 1 || leaves.length < 2 || leaves.length + 1 !== chunks.length ||
                internals[0].bytes.length < 12) {
                throw new Error(`fixture lacks a single Internal root over multiple leaves during ${phase}`);
            }
            const header = new DataView(internals[0].bytes.buffer,
                internals[0].bytes.byteOffset, internals[0].bytes.byteLength);
            const childCount = header.getUint32(8, true);
            if (header.getUint16(4, true) !== 0 || header.getUint16(6, true) !== 0 ||
                childCount !== leaves.length) {
                throw new Error(`Internal-root child oracle disagrees with the reachable graph during ${phase}`);
            }
            const token = requireTreeJobToken(tree.begin_candidate_job(), `${phase} begin`);
            let previous = { completed: 0, reachable: 0 };
            const progress = [];
            for (const label of ["seed", "decode Internal", "move first child"]) {
                const current = tree.step_reachability_job_deferred(token, 1);
                validateExactTreeJobProgress(current, previous, 1, `${phase} ${label}`);
                assertReachabilityVisibleState(tree, before, `${phase} ${label}`);
                progress.push(current); previous = current;
            }
            const [seed, decoded, moved] = progress;
            if (seed.done || seed.completed !== 1 || seed.reachable !== 0 || seed.remaining !== 1 ||
                decoded.done || decoded.completed !== 2 || decoded.reachable !== 1 || decoded.remaining !== childCount ||
                moved.done || moved.completed !== 3 || moved.reachable !== 1 || moved.remaining !== childCount) {
                throw new Error(`fixture did not reach one moved child plus unfinished expansion during ${phase}`);
            }
            // The third unit cannot be another node read: reachable remains
            // one, and all child descriptors remain pending/expanding. Both
            // populated owners must now survive the O(1) cancel handoff.
            expectThrow(() => tree.finish_candidate_job_deferred(token),
                `partially expanded Internal root published a candidate during ${phase}`);
            tree.cancel_reachability_job_deferred(token);
            tree.cancel_reachability_job_deferred(token);
            expectThrow(() => tree.begin_candidate_job(), `cancelled cursor released its slot early during ${phase}`);
            expectThrow(() => tree.build_from_entries("[]"), `committed rebuild bypassed retirement during ${phase}`);
            assertReachabilityVisibleState(tree, before, `${phase} cancelled`);
            const cleanup = drainReachabilityRetirement(tree, token, 1, before, `${phase} cleanup`);
            // k child descriptors split across expanding/pending, one decoded
            // Internal descriptor, one reachable hash, and four empty backing
            // owners. The terminal zero-unit observation is not another unit.
            if (cleanup.completed !== childCount + 6 || cleanup.turns !== cleanup.completed + 1) {
                throw new Error(`partial Internal cleanup skipped or combined populated owners during ${phase}`);
            }
            const afterHashes = module.wasm_tree_committed_chunk_hashes(tree);
            if (JSON.stringify(afterHashes) !== JSON.stringify(hashes) || chunks.some(chunk =>
                !bytesEqual(module.wasm_tree_get_chunk(tree, chunk.hash), chunk.bytes))) {
                throw new Error(`partial Internal cleanup changed immutable resident chunks during ${phase}`);
            }
            const summary = { version: 2, entries: entries.length, root_hash: before.committedHash,
                internal_root: { hash: internals[0].hash, children: childCount, child_height: 0 },
                resident_chunks: hashes.length, seed, decoded, moved, cleanup };
            if (reports.length && JSON.stringify(reports[0].result) !== JSON.stringify(summary)) {
                throw new Error("scalar/SIMD deferred Internal expansion parity mismatch");
            }
            reports.push({ variant, result: summary });
            if (process.env.OBSETYNC_PARITY_PROGRESS === "1") console.error(`${phase} passed`);
        } finally { tree.free(); }
    }
    return reports;
}

const CHUNK_EXPORT_PAGE_BYTES = 64 * 1024;

function exportTreeState(module, tree, phase) {
    const state = { version: tree.tree_version(), candidate: tree.has_candidate(),
        committedRevision: tree.committed_revision(),
        committedHash: tree.root_hash_hex(), committedCount: tree.total_files(),
        committedBytes: tree.root_bytes(), candidateHash: tree.candidate_root_hash_hex(),
        candidateCount: tree.candidate_total_files(), candidateBytes: tree.candidate_root_bytes() };
    for (const [bytes, hash] of [[state.committedBytes, state.committedHash],
        [state.candidateBytes, state.candidateHash]]) {
        if (bytes && (module.wasm_root_hash_from_bytes(bytes) !== hash ||
            module.wasm_root_version_from_bytes(bytes) !== state.version)) {
            throw new Error(`chunk export fixture has invalid semantic root identity during ${phase}`);
        }
    }
    return JSON.stringify(state);
}

function assertExportState(module, tree, expected, phase) {
    if (exportTreeState(module, tree, phase) !== expected) {
        throw new Error(`chunk export changed committed/candidate root bytes or identity during ${phase}`);
    }
}

function exportDescriptor(tree, hash, expectedLength, phase) {
    const value = tree.begin_tree_chunk_export_job(hash);
    if (!value || typeof value !== "object" || Object.keys(value).sort().join(",") !== "length,token" ||
        !Number.isSafeInteger(value.token) || value.token < 1 || value.token > 0xffff_ffff ||
        !Number.isSafeInteger(value.length) || value.length < 0 || value.length > 0xffff_ffff ||
        value.length !== expectedLength) throw new Error(`invalid chunk export descriptor during ${phase}`);
    return value;
}

function assertExportPages(pages, phase) {
    for (const { page, expected } of pages) {
        if (!bytesEqual(page, expected) || page.byteOffset !== 0 || page.buffer.byteLength !== expected.length) {
            throw new Error(`detached exported page lifetime failed during ${phase}`);
        }
    }
}

function exportPage(tree, native, info, offset, cap, source, retained, phase) {
    const page = tree.read_tree_chunk_export_job(info.token, offset, cap);
    const length = Math.min(cap, source.length - offset);
    if (!(page instanceof Uint8Array) || page.length !== length || page.byteOffset !== 0 ||
        page.buffer.byteLength !== length || page.buffer === native?.memory?.buffer ||
        !bytesEqual(page, source.subarray(offset, offset + length))) {
        throw new Error(`chunk export page bytes/length/backing/order mismatch during ${phase}`);
    }
    assertExportPages(retained, `${phase} next native read`);
    retained.push({ page, expected: page.slice() });
    return page;
}

function chunkExportFences(module, tree, token, hash, state, phase) {
    const wrong = token === 0xffff_ffff ? token - 1 : token + 1;
    const root = tree.root_bytes(), version = tree.tree_version();
    const calls = [
        () => tree.begin_tree_chunk_export_job(hash),
        () => tree.begin_candidate_job(), () => tree.begin_candidate_chunks_job(),
        () => tree.begin_candidate_update_job("[]"), () => tree.begin_candidate_delete_job("[]"),
        () => tree.begin_candidate(), () => tree.commit_candidate(), () => tree.abort_candidate(),
        () => tree.candidate_update_batch("[]"), () => tree.candidate_delete_batch("[]"),
        () => tree.update_batch("[]"), () => tree.delete_batch("[]"),
        () => tree.update_entry("export-fence.md", "01".repeat(32), 1, 1),
        () => tree.delete_entry("export-fence.md"), () => tree.load_root(root),
        () => tree.build_from_entries("[]"), () => tree.rebuild_from_entries_in_version(version, "[]"),
        () => tree.set_tree_version(version === 1 ? 2 : 1),
        () => tree.step_tree_job(token, 1),
        () => tree.finish_candidate_job(token), () => tree.finish_candidate_chunks_job(token),
        () => tree.finish_candidate_mutation_job(token),
        () => tree.finish_tree_chunk_export_job(wrong), () => tree.cancel_tree_job(wrong),
        () => tree.read_tree_chunk_export_job(wrong, 0, 1),
    ];
    for (const [index, call] of calls.entries()) {
        expectThrow(call, `chunk export fence ${index} did not reject during ${phase}`);
        assertExportState(module, tree, state, `${phase} fence ${index}`);
    }
}

function chunkExportWrongPurpose(tree, candidate, phase) {
    // A READY different-purpose job must survive both export misuse calls.
    // Testing only an incomplete job could hide the wrong rejection reason.
    const kinds = candidate ? ["chunks", "mutation"] : ["begin"];
    for (const kind of kinds) {
        const token = kind === "chunks" ? tree.begin_candidate_chunks_job() :
            kind === "mutation" ? tree.begin_candidate_update_job("[]") : tree.begin_candidate_job();
        let ready = false, previous = { completed: 0, reachable: 0 };
        for (let step = 0; step < 100; step++) {
            const progress = tree.step_tree_job(token, 256);
            validateTreeJobProgress(progress, previous, 256, `${phase} ready ${kind}`);
            previous = progress;
            if (progress.done) { ready = true; break; }
        }
        if (!ready) throw new Error(`wrong-purpose fixture never became ready during ${phase}`);
        expectThrow(() => tree.read_tree_chunk_export_job(token, 0, 1), `export read accepted READY ${kind} job`);
        expectThrow(() => tree.finish_tree_chunk_export_job(token), `export finish accepted READY ${kind} job`);
        if (kind === "chunks") tree.finish_candidate_chunks_job(token);
        else if (kind === "mutation") tree.finish_candidate_mutation_job(token);
        else tree.cancel_tree_job(token); // Preserve the original committed-only fixture.
    }
}

function assertPackagedChunkExports() {
    // Long, bounded paths make a multi-page immutable leaf with only 128 rows.
    // No repeated large mutation corpus or 100,000-call one-byte drain here.
    const rows = deterministicTreeEntries(128).map((entry, index) => ({ ...entry,
        path: `export/${String(index).padStart(3, "0")}-${"x".repeat(1_080)}.md` }));
    const reports = [], now = Date.now;
    Date.now = () => 1_910_000_000_000;
    try {
        for (const version of [1, 2]) for (const candidate of [false, true]) {
            let oracle;
            for (const [label, module, native] of [["scalar", scalar, scalarNative], ["SIMD", simd, simdNative]]) {
                const phase = `${label} v${version} ${candidate ? "candidate" : "committed"} chunk export`;
                const tree = new module.WasmTree("export-parity-vault", "export-parity-device"), retained = [];
                let summary;
                try {
                    for (const method of ["begin_tree_chunk_export_job", "read_tree_chunk_export_job",
                        "finish_tree_chunk_export_job", "cancel_tree_job"]) {
                        if (typeof tree[method] !== "function") throw new Error(`packaged ${label} WASM lacks ${method}`);
                    }
                    expectThrow(() => tree.begin_tree_chunk_export_job("00".repeat(32)), "export accepted an uninitialized tree");
                    tree.set_tree_version(version); tree.build_from_entries(JSON.stringify(rows));
                    if (candidate) {
                        tree.begin_candidate();
                        tree.candidate_update_batch(JSON.stringify([{ ...rows[42], hash: "f1".repeat(32), mtime_ms: 77 }]));
                    }
                    const hashes = module.wasm_tree_chunk_hashes(tree).sort();
                    const hash = hashes.reduce((largest, value) =>
                        module.wasm_tree_chunk_byte_length(tree, value) > module.wasm_tree_chunk_byte_length(tree, largest)
                            ? value : largest);
                    // Independent byte oracle: the pre-existing synchronous getter,
                    // never an export job drained using another page schedule.
                    const expected = module.wasm_tree_get_chunk(tree, hash);
                    if (!expected || expected.length <= 2 * CHUNK_EXPORT_PAGE_BYTES || module.wasm_hash(expected) !== hash) {
                        throw new Error(`fixture did not produce a valid three-page index object during ${phase}`);
                    }
                    const state = exportTreeState(module, tree, phase), chunks = reachableChunkMap(module, tree, phase);
                    chunkExportWrongPurpose(tree, candidate, phase);
                    assertExportState(module, tree, state, `${phase} wrong-purpose recovery`);
                    for (const invalid of ["invalid", "00".repeat(32), "gg".repeat(32), "ab".repeat(33)]) {
                        expectThrow(() => tree.begin_tree_chunk_export_job(invalid), `invalid/missing export source accepted during ${phase}`);
                    }
                    const cancelled = exportDescriptor(tree, hash.toUpperCase(), expected.length, phase);
                    exportPage(tree, native, cancelled, 0, 17, expected, retained, `${phase} cancel after read`);
                    expectThrow(() => tree.finish_tree_chunk_export_job(cancelled.token), `partial export finished during ${phase}`);
                    tree.cancel_tree_job(cancelled.token);
                    assertExportPages(retained, `${phase} cancellation`);
                    assertExportState(module, tree, state, `${phase} cancellation`);

                    const schedules = [[CHUNK_EXPORT_PAGE_BYTES], [1, 17, CHUNK_EXPORT_PAGE_BYTES - 1, CHUNK_EXPORT_PAGE_BYTES]];
                    const reads = []; let growth = false;
                    for (const [run, caps] of schedules.entries()) {
                        const info = exportDescriptor(tree, hash, expected.length, phase), assembled = new Uint8Array(info.length);
                        if (info.token === cancelled.token) throw new Error(`cancelled export token was reused during ${phase}`);
                        for (const action of [() => tree.read_tree_chunk_export_job(cancelled.token, 0, 1),
                            () => tree.finish_tree_chunk_export_job(cancelled.token), () => tree.cancel_tree_job(cancelled.token)]) {
                            expectThrow(action, `stale export token consumed replacement during ${phase}`);
                        }
                        chunkExportFences(module, tree, info.token, hash, state, `${phase} pending`);
                        for (const [offset, cap] of [[1, 1], [0xffff_ffff, 1], [0, 0], [0, CHUNK_EXPORT_PAGE_BYTES + 1], [0, 0xffff_ffff]]) {
                            expectThrow(() => tree.read_tree_chunk_export_job(info.token, offset, cap), `invalid page request accepted during ${phase}`);
                        }
                        let offset = 0, pages = 0;
                        while (offset < info.length) {
                            const cap = caps[Math.min(pages, caps.length - 1)];
                            const page = exportPage(tree, native, info, offset, cap, expected, retained, phase);
                            assembled.set(page, offset); offset += page.length; pages++;
                            expectThrow(() => tree.read_tree_chunk_export_job(info.token, offset - page.length, cap),
                                `replayed offset accepted during ${phase}`);
                            if (!growth && native?.memory instanceof WebAssembly.Memory) {
                                native.memory.grow(1); growth = true;
                                assertExportPages(retained, `${phase} memory.grow(1)`);
                            }
                            assertExportState(module, tree, state, `${phase} read`);
                            if (pages > 8) throw new Error(`bounded page fixture did too many reads during ${phase}`);
                        }
                        if (!bytesEqual(assembled, expected)) throw new Error(`concatenated export differs from legacy bytes during ${phase}`);
                        for (let eof = 0; eof < 2; eof++) exportPage(tree, native, info, offset, CHUNK_EXPORT_PAGE_BYTES,
                            expected, retained, `${phase} exact EOF`);
                        chunkExportFences(module, tree, info.token, hash, state, `${phase} READY`);
                        if (run === 0) tree.finish_tree_chunk_export_job(info.token);
                        else tree.cancel_tree_job(info.token);
                        for (const action of [() => tree.read_tree_chunk_export_job(info.token, offset, 1),
                            () => tree.finish_tree_chunk_export_job(info.token), () => tree.cancel_tree_job(info.token)]) {
                            expectThrow(action, `released export token remained usable during ${phase}`);
                        }
                        assertExportPages(retained, `${phase} ${run === 0 ? "finish" : "ready cancel"}`);
                        assertExportState(module, tree, state, `${phase} released`);
                        reads.push(pages);
                    }
                    // Returned pages belong to JS: even a write through the return
                    // must not modify the immutable store or any future export.
                    retained[0].page[0] ^= 255; retained[0].expected[0] ^= 255;
                    if (!bytesEqual(module.wasm_tree_get_chunk(tree, hash), expected) ||
                        JSON.stringify(reachableChunkMap(module, tree, phase)) !== JSON.stringify(chunks)) {
                        throw new Error(`JS page mutation changed the tree store during ${phase}`);
                    }
                    const comparison = JSON.stringify({ hash, bytes: expected.length, state, chunks, reads });
                    if (oracle !== undefined && comparison !== oracle) throw new Error(`scalar/SIMD chunk export parity mismatch during ${phase}`);
                    oracle = comparison;
                    summary = { variant: label, version, candidate, bytes: expected.length, reads,
                        memory_growth: growth ? "tested" : "unavailable", detached_after_free: true };
                } finally { tree.free(); }
                assertExportPages(retained, `${phase} tree.free()`);
                reports.push(summary);
                if (process.env.OBSETYNC_PARITY_PROGRESS === "1") console.error(`chunk export parity: ${phase} passed`);
            }
        }
        return reports;
    } finally { Date.now = now; }
}

const ROOT_EXPORT_MAX_ARENA = 0xffff_ffff;
const ROOT_EXPORT_MAX_STEP_UNITS = 4096;
const ROOT_EXPORT_MAX_STEP_BYTES = 16 * 1024 * 1024;
const ROOT_EXPORT_MAX_ATOMIC_BYTES = 64 * 1024 + 128;

function rootExportToken(tree, mode, phase, maxArenaBytes = ROOT_EXPORT_MAX_ARENA) {
    const token = mode === "candidate" ? tree.begin_candidate_root_export_job(maxArenaBytes)
        : tree.begin_committed_root_export_job(maxArenaBytes);
    if (!Number.isSafeInteger(token) || token < 1 || token > 0xffff_ffff) throw new Error(`invalid root export token during ${phase}`);
    return token;
}

function rootExportIdentity(tree) {
    return JSON.stringify({ version: tree.tree_version(), candidate: tree.has_candidate(),
        committed: tree.root_hash_hex(), committedCount: tree.total_files(),
        proposed: tree.candidate_root_hash_hex(), proposedCount: tree.candidate_total_files() });
}

function validateRootExportProgress(current, previous, maxUnits, maxBytes, phase) {
    if (!current || Object.keys(current).sort().join(",") !== "bytes,completed,done,processed,units" ||
        typeof current.done !== "boolean" || !Number.isSafeInteger(current.units) || current.units < 0 ||
        !Number.isSafeInteger(current.bytes) || current.bytes < 0 ||
        !Number.isSafeInteger(current.completed) || current.completed < 0 ||
        !Number.isSafeInteger(current.processed) || current.processed < 0 || current.units > maxUnits ||
        ((!current.done || current.units > 0) && current.units === 0) ||
        ((current.units === 0) !== (current.bytes === 0)) ||
        (current.bytes > maxBytes && (current.units !== 1 || current.bytes > ROOT_EXPORT_MAX_ATOMIC_BYTES)) ||
        current.completed !== previous.completed + current.units ||
        current.processed !== previous.processed + current.bytes) {
        throw new Error(`invalid byte-budgeted root export progress during ${phase}`);
    }
}

function drainRootExportPhase(tree, token, maxUnits, maxBytes, identity, phase) {
    // Fresh counters for BOTH phases: Build must not inherit Plan's completed.
    let previous = { completed: 0, processed: 0 }, steps = 0;
    for (;;) {
        const current = tree.step_root_export_job(token, maxUnits, maxBytes);
        validateRootExportProgress(current, previous, maxUnits, maxBytes, phase);
        if (rootExportIdentity(tree) !== identity) throw new Error(`root export progress/owner changed during ${phase}`);
        previous = current; steps++;
        if (steps > 4096) throw new Error(`bounded root export fixture exceeded step limit during ${phase}`);
        if (current.done) break;
    }
    const repeated = tree.step_root_export_job(token, maxUnits, maxBytes);
    validateRootExportProgress(repeated, previous, maxUnits, maxBytes, `${phase} repeated READY`);
    if (!repeated.done || repeated.units !== 0 || repeated.bytes !== 0 ||
        repeated.completed !== previous.completed || repeated.processed !== previous.processed) {
        throw new Error(`root export READY changed progress during ${phase}`);
    }
    return { steps, units: previous.completed, processed: previous.processed };
}

function rootExportWorkset(tree, token, expectedBytes, version, children, phase) {
    const value = tree.root_export_workset(token);
    if (!value || Object.keys(value).sort().join(",") !== "max_length,offset_bytes" ||
        !Number.isSafeInteger(value.max_length) || value.max_length < expectedBytes || value.max_length > 0xffff_ffff ||
        !Number.isSafeInteger(value.offset_bytes) || value.offset_bytes < 0 || value.offset_bytes > 0xffff_ffff ||
        value.offset_bytes !== (version === 1 ? children * 4 : 0) ||
        (version === 2 && (value.max_length !== expectedBytes || value.max_length > 16 * 1024))) {
        throw new Error(`invalid root export workset during ${phase}`);
    }
    if (JSON.stringify(tree.root_export_workset(token)) !== JSON.stringify(value)) {
        throw new Error(`root export workset was not stable during ${phase}`);
    }
    return value;
}

function rootExportInfo(tree, token, expected, hash, version, phase) {
    const value = tree.root_export_info(token);
    if (!value || Object.keys(value).sort().join(",") !== "hash,length,version" ||
        value.length !== expected.length || value.hash !== hash || value.version !== version) {
        throw new Error(`root export info differs from legacy oracle during ${phase}`);
    }
    if (JSON.stringify(tree.root_export_info(token)) !== JSON.stringify(value)) {
        throw new Error(`root export seal descriptor was not repeatable during ${phase}`);
    }
    return value;
}

function rootExportPage(tree, native, token, offset, cap, expected, retained, phase) {
    const page = tree.read_root_export_job(token, offset, cap), length = Math.min(cap, expected.length - offset);
    if (!(page instanceof Uint8Array) || page.length !== length || page.byteOffset !== 0 ||
        page.buffer.byteLength !== length || page.buffer === native?.memory?.buffer ||
        !bytesEqual(page, expected.subarray(offset, offset + length))) {
        throw new Error(`root export page bytes/offset/backing differs during ${phase}`);
    }
    assertExportPages(retained, `${phase} next root read`);
    retained.push({ page, expected: page.slice() });
    return page;
}

function rootExportStaleToken(tree, token, phase) {
    for (const action of [() => tree.step_root_export_job(token, 1, 1), () => tree.cancel_tree_job(token),
        () => tree.root_export_workset(token), () => tree.start_root_export_build_job(token),
        () => tree.root_export_info(token), () => tree.read_root_export_job(token, 0, 1),
        () => tree.finish_root_export_job(token)]) expectThrow(action, `stale root export token accepted during ${phase}`);
}

function rootExportFences(tree, token, chunkHash, before, phase) {
    const wrong = token === 0xffff_ffff ? token - 1 : token + 1;
    const calls = [
        () => tree.begin_candidate_root_export_job(ROOT_EXPORT_MAX_ARENA),
        () => tree.begin_committed_root_export_job(ROOT_EXPORT_MAX_ARENA),
        () => tree.begin_tree_chunk_export_job(chunkHash),
        () => tree.begin_candidate_job(), () => tree.begin_candidate_chunks_job(),
        () => tree.begin_candidate_update_job("[]"), () => tree.begin_candidate_delete_job("[]"),
        () => tree.begin_candidate(), () => tree.commit_candidate(), () => tree.abort_candidate(),
        () => tree.candidate_update_batch("[]"), () => tree.candidate_delete_batch("[]"),
        () => tree.update_batch("[]"), () => tree.delete_batch("[]"),
        () => tree.update_entry("root-export-fence.md", "01".repeat(32), 1, 1),
        () => tree.delete_entry("root-export-fence.md"), () => tree.load_root(before.committedBytes),
        () => tree.build_from_entries("[]"), () => tree.rebuild_from_entries_in_version(before.version, "[]"),
        () => tree.set_tree_version(before.version === 1 ? 2 : 1),
        () => tree.finish_candidate_job(token), () => tree.finish_candidate_chunks_job(token),
        () => tree.finish_candidate_mutation_job(token), () => tree.finish_tree_chunk_export_job(token),
        () => tree.read_tree_chunk_export_job(token, 0, 1),
        () => tree.step_tree_job(token, 1),
        () => tree.step_root_export_job(token, 0, 1),
        () => tree.step_root_export_job(token, ROOT_EXPORT_MAX_STEP_UNITS + 1, 1),
        () => tree.step_root_export_job(token, 1, 0),
        () => tree.step_root_export_job(token, 1, ROOT_EXPORT_MAX_STEP_BYTES + 1),
    ];
    rootExportStaleToken(tree, wrong, `${phase} wrong token`);
    for (const [index, call] of calls.entries()) {
        expectThrow(call, `root export fence ${index} accepted during ${phase}`);
        assertCandidateState(tree, before, `${phase} fence ${index}`);
    }
}

function rootExportRejectsOtherReadyKinds(tree, chunkHash, chunkLength, before, phase) {
    for (const kind of ["chunks", "mutation", "index-export"]) {
        const token = kind === "chunks" ? tree.begin_candidate_chunks_job() : kind === "mutation"
            ? tree.begin_candidate_update_job("[]") : tree.begin_tree_chunk_export_job(chunkHash).token;
        if (kind === "index-export") {
            for (let offset = 0; offset < chunkLength;) {
                const page = tree.read_tree_chunk_export_job(token, offset, CHUNK_EXPORT_PAGE_BYTES);
                if (page.length !== Math.min(CHUNK_EXPORT_PAGE_BYTES, chunkLength - offset)) throw new Error("invalid wrong-kind fixture page");
                offset += page.length;
            }
        } else {
            let previous = { completed: 0, reachable: 0 }, done = false;
            for (let step = 0; step < 64; step++) {
                const current = tree.step_tree_job(token, 256);
                validateTreeJobProgress(current, previous, 256, `${phase} wrong-kind ${kind}`); previous = current;
                if (current.done) { done = true; break; }
            }
            if (!done) throw new Error(`wrong-kind fixture exceeded step bound during ${phase}`);
        }
        for (const call of [() => tree.root_export_workset(token), () => tree.start_root_export_build_job(token),
            () => tree.root_export_info(token), () => tree.read_root_export_job(token, 0, 1),
            () => tree.finish_root_export_job(token), () => tree.step_root_export_job(token, 1, 1)]) {
            expectThrow(call, `root export API accepted READY ${kind} job during ${phase}`);
        }
        if (kind === "index-export") tree.finish_tree_chunk_export_job(token);
        else if (kind === "chunks") tree.finish_candidate_chunks_job(token);
        else tree.finish_candidate_mutation_job(token);
        assertCandidateState(tree, before, `${phase} surviving correct ${kind} finish`);
    }
}

function assertPackagedRootExports() {
    // Distinct, long top-level prefixes produce a multi-page v1 root with only
    // 192 files. V2 retains its real <=16 KiB descriptor; no oversized fake v2
    // root and no duplicate large candidate-mutation corpus are introduced.
    const rows = deterministicTreeEntries(192).map((entry, index) => ({ ...entry,
        path: `root-export-${String(index).padStart(3, "0")}-${"x".repeat(384)}/note.md` }));
    const reports = [], now = Date.now;
    Date.now = () => 1_920_000_000_000;
    try {
        for (const version of [1, 2]) for (const mode of ["candidate", "committed"]) {
            let crossVariant;
            for (const [variant, module, native] of [["scalar", scalar, scalarNative], ["SIMD", simd, simdNative]]) {
                const phase = `${variant} v${version} ${mode} root export`, retained = [];
                const tree = new module.WasmTree("root-export-parity-vault", "root-export-parity-device");
                let summary;
                try {
                    for (const name of ["begin_candidate_root_export_job", "begin_committed_root_export_job", "step_root_export_job", "cancel_tree_job",
                        "root_export_workset", "start_root_export_build_job", "root_export_info", "read_root_export_job", "finish_root_export_job"]) {
                        if (typeof tree[name] !== "function") throw new Error(`packaged ${variant} WASM lacks ${name}`);
                    }
                    expectThrow(() => tree.begin_candidate_root_export_job(ROOT_EXPORT_MAX_ARENA), "uninitialized candidate root export accepted");
                    expectThrow(() => tree.begin_committed_root_export_job(ROOT_EXPORT_MAX_ARENA), "uninitialized committed root export accepted");
                    tree.set_tree_version(version); tree.build_from_entries(JSON.stringify(rows));
                    expectThrow(() => tree.begin_candidate_root_export_job(ROOT_EXPORT_MAX_ARENA), "absent candidate silently selected committed root");
                    tree.begin_candidate();
                    tree.candidate_delete_batch(JSON.stringify([rows[3].path]));
                    tree.candidate_update_batch(JSON.stringify([{ ...rows[17], hash: "f2".repeat(32), mtime_ms: 101 }]));
                    // Captured BEFORE any export begin. Exact legacy serialization
                    // is the independent oracle, including parent/device/time.
                    const before = captureCandidateState(tree, phase), identity = rootExportIdentity(tree);
                    const expected = mode === "candidate" ? before.candidateBytes : before.committedBytes;
                    const hash = mode === "candidate" ? before.candidateHash : before.committedHash;
                    if (!expected || bytesEqual(before.candidateBytes, before.committedBytes) ||
                        before.candidateCount !== rows.length - 1 || before.committedCount !== rows.length ||
                        module.wasm_root_hash_from_bytes(expected) !== hash || module.wasm_root_version_from_bytes(expected) !== version ||
                        (version === 1 && expected.length <= CHUNK_EXPORT_PAGE_BYTES) || (version === 2 && expected.length > 16 * 1024)) {
                        throw new Error(`root export fixture lacks distinct, valid legacy roots during ${phase}`);
                    }
                    const chunkHash = module.wasm_tree_chunk_hashes(tree).sort()[0];
                    const chunkLength = module.wasm_tree_chunk_byte_length(tree, chunkHash);
                    rootExportRejectsOtherReadyKinds(tree, chunkHash, chunkLength, before, phase);
                    expectThrow(() => mode === "candidate" ? tree.begin_candidate_root_export_job(0)
                        : tree.begin_committed_root_export_job(0), `zero arena ceiling accepted during ${phase}`);
                    const capped = rootExportToken(tree, mode, `${phase} capped`, 512);
                    expectThrow(() => tree.step_root_export_job(capped, ROOT_EXPORT_MAX_STEP_UNITS, 256 * 1024),
                        `impossible arena ceiling survived Plan during ${phase}`);
                    rootExportStaleToken(tree, capped, `${phase} capped failure`);
                    assertCandidateState(tree, before, `${phase} capped failure`);
                    const children = mode === "candidate" ? rows.length - 1 : rows.length;
                    const runs = []; let growth = false, lastToken;
                    for (const budget of [
                        { units: 1, bytes: 1 },
                        { units: ROOT_EXPORT_MAX_STEP_UNITS, bytes: 256 * 1024 },
                        { units: ROOT_EXPORT_MAX_STEP_UNITS, bytes: ROOT_EXPORT_MAX_STEP_BYTES },
                    ]) {
                        const token = rootExportToken(tree, mode, phase);
                        if (lastToken !== undefined) {
                            if (token <= lastToken) throw new Error(`root export reused a released token during ${phase}`);
                            rootExportStaleToken(tree, lastToken, `${phase} replacement`);
                        }
                        for (const call of [() => tree.root_export_workset(token), () => tree.start_root_export_build_job(token),
                            () => tree.root_export_info(token), () => tree.read_root_export_job(token, 0, 1),
                            () => tree.finish_root_export_job(token)]) expectThrow(call, `unplanned root operation accepted during ${phase}`);
                        if (budget.units === 1) rootExportFences(tree, token, chunkHash, before, `${phase} Plan`);
                        const plan = drainRootExportPhase(tree, token, budget.units, budget.bytes, identity,
                            `${phase} Plan/${budget.units}/${budget.bytes}`);
                        const workset = rootExportWorkset(tree, token, expected.length, version, children, phase);
                        if (plan.units !== (version === 1 ? 2 * children + 3 : 1) ||
                            (budget.units === 1 && plan.steps !== plan.units) ||
                            (budget.units > 1 && version === 1 && plan.steps >= plan.units)) {
                            throw new Error(`root export Plan work-count oracle differs during ${phase}`);
                        }
                        assertCandidateState(tree, before, `${phase} planned`);
                        expectThrow(() => tree.root_export_info(token), "Plan READY was accepted as Build READY");
                        tree.start_root_export_build_job(token);
                        expectThrow(() => tree.start_root_export_build_job(token), "root export Build restarted");
                        expectThrow(() => tree.root_export_workset(token), "root export workset re-entered during Build");
                        expectThrow(() => tree.root_export_info(token), "unfinished root Build sealed");
                        expectThrow(() => tree.read_root_export_job(token, 0, 1), "unfinished root Build read");
                        if (budget.units === 1) rootExportFences(tree, token, chunkHash, before, `${phase} Build`);
                        const build = drainRootExportPhase(tree, token, budget.units, budget.bytes, identity,
                            `${phase} Build/${budget.units}/${budget.bytes}`);
                        const buildUnits = version === 1 ? Math.ceil(workset.max_length / CHUNK_EXPORT_PAGE_BYTES) + 3 * children + 6 : 1;
                        if (build.units !== buildUnits || (budget.units === 1 && build.steps !== build.units) ||
                            (budget.units > 1 && version === 1 && build.steps >= build.units)) {
                            throw new Error(`root export Build/reset work-count oracle differs during ${phase}`);
                        }
                        expectThrow(() => tree.read_root_export_job(token, 0, 1), "unsealed root bytes were exposed");
                        const info = rootExportInfo(tree, token, expected, hash, version, phase);
                        expectThrow(() => tree.step_root_export_job(token, 1, 1), "sealed root returned to execution");
                        expectThrow(() => tree.finish_root_export_job(token), "root export finished before EOF");
                        if (budget.units === 1) rootExportFences(tree, token, chunkHash, before, `${phase} sealed`);
                        for (const [offset, cap] of [[1, 1], [0xffff_ffff, 1], [0, 0], [0, CHUNK_EXPORT_PAGE_BYTES + 1]]) {
                            expectThrow(() => tree.read_root_export_job(token, offset, cap), `invalid root page accepted during ${phase}`);
                        }
                        const assembled = new Uint8Array(info.length); let offset = 0, pages = 0;
                        while (offset < info.length) {
                            const page = rootExportPage(tree, native, token, offset, CHUNK_EXPORT_PAGE_BYTES, expected, retained, phase);
                            assembled.set(page, offset); const oldOffset = offset; offset += page.length; pages++;
                            expectThrow(() => tree.read_root_export_job(token, oldOffset, CHUNK_EXPORT_PAGE_BYTES), "root export replayed an old offset");
                            rootExportInfo(tree, token, expected, hash, version, phase); // Must not reset offset.
                            if (!growth && native?.memory instanceof WebAssembly.Memory) {
                                native.memory.grow(1); growth = true; assertExportPages(retained, `${phase} memory.grow(1)`);
                            }
                            assertCandidateState(tree, before, `${phase} page ${pages}`);
                            if (pages > 4) throw new Error(`bounded root fixture used too many pages during ${phase}`);
                        }
                        if (pages !== Math.ceil(expected.length / CHUNK_EXPORT_PAGE_BYTES) || !bytesEqual(assembled, expected) ||
                            module.wasm_root_hash_from_bytes(assembled) !== hash || module.wasm_root_version_from_bytes(assembled) !== version) {
                            throw new Error(`root export bytes/semantic identity differ from legacy during ${phase}`);
                        }
                        rootExportPage(tree, native, token, offset, CHUNK_EXPORT_PAGE_BYTES, expected, retained, `${phase} EOF`);
                        tree.finish_root_export_job(token); rootExportStaleToken(tree, token, `${phase} finished`);
                        assertExportPages(retained, `${phase} finish`); assertCandidateState(tree, before, `${phase} finish`);
                        runs.push({ budget, plan, build, bytes: info.length, max_length: workset.max_length,
                            offset_bytes: workset.offset_bytes, pages }); lastToken = token;
                    }
                    for (const cut of ["Plan", "Build", "partial-read"]) {
                        const token = rootExportToken(tree, mode, phase);
                        rootExportStaleToken(tree, lastToken, `${phase} cancel replacement`);
                        if (cut === "Plan") {
                            const first = tree.step_root_export_job(token, 1, 1);
                            validateRootExportProgress(first, { completed: 0, processed: 0 }, 1, 1, `${phase} cancel Plan`);
                        } else {
                            drainRootExportPhase(tree, token, ROOT_EXPORT_MAX_STEP_UNITS, 256 * 1024,
                                identity, `${phase} cancel Plan`);
                            rootExportWorkset(tree, token, expected.length, version, children, phase);
                            tree.start_root_export_build_job(token);
                            if (cut === "Build") {
                                const first = tree.step_root_export_job(token, 1, 1);
                                validateRootExportProgress(first, { completed: 0, processed: 0 }, 1, 1, `${phase} cancel Build`);
                            } else {
                                drainRootExportPhase(tree, token, ROOT_EXPORT_MAX_STEP_UNITS, 256 * 1024,
                                    identity, `${phase} cancel Build`);
                                rootExportInfo(tree, token, expected, hash, version, phase);
                                rootExportPage(tree, native, token, 0, Math.min(CHUNK_EXPORT_PAGE_BYTES, expected.length - 1),
                                    expected, retained, `${phase} partial cancel`);
                                expectThrow(() => tree.finish_root_export_job(token), "partial root read was accepted as EOF");
                            }
                        }
                        tree.cancel_tree_job(token); rootExportStaleToken(tree, token, `${phase} cancelled ${cut}`);
                        assertExportPages(retained, `${phase} cancelled ${cut}`); assertCandidateState(tree, before, `${phase} cancelled ${cut}`);
                        lastToken = token;
                    }
                    // JS page writes cannot modify either native root/history.
                    retained[0].page[0] ^= 255; retained[0].expected[0] ^= 255;
                    assertCandidateState(tree, before, `${phase} JS-owned page mutation`);
                    const comparison = JSON.stringify({ committed: Buffer.from(before.committedBytes).toString("hex"),
                        candidate: Buffer.from(before.candidateBytes).toString("hex"), runs });
                    if (crossVariant !== undefined && comparison !== crossVariant) throw new Error(`scalar/SIMD root export parity differs during ${phase}`);
                    crossVariant = comparison;
                    summary = { variant, version, mode, candidate_also_present: true, runs, cancellation: ["Plan", "Build", "partial-read"],
                        memory_growth: growth ? "tested" : "unavailable", detached_after_free: true };
                } finally { tree.free(); }
                assertExportPages(retained, `${phase} tree.free()`); reports.push(summary);
                if (process.env.OBSETYNC_PARITY_PROGRESS === "1") console.error(`root export parity: ${phase} passed`);
            }
        }
        return reports;
    } finally { Date.now = now; }
}

function reachableChunkMap(module, tree, phase) {
    const hashes = module.wasm_tree_chunk_hashes(tree).sort();
    validateCandidateChunkPlan({ all: hashes, fresh: [] }, phase);
    return hashes.map(hash => {
        const bytes = module.wasm_tree_get_chunk(tree, hash);
        if (!bytes || module.wasm_hash(bytes) !== hash ||
            module.wasm_tree_chunk_byte_length(tree, hash) !== bytes.length) {
            throw new Error(`invalid reachable mutation chunk during ${phase}`);
        }
        return [hash, Buffer.from(bytes).toString("hex")];
    });
}

function captureMutationState(module, tree, phase) {
    const plan = {
        all: module.wasm_tree_candidate_chunk_hashes(tree),
        fresh: module.wasm_tree_new_candidate_chunk_hashes(tree),
    };
    validateCandidateChunkPlan(plan, phase);
    return {
        state: captureCandidateState(tree, phase),
        chunks: reachableChunkMap(module, tree, phase),
        committed: module.wasm_tree_committed_chunk_hashes(tree).sort(),
        plan,
    };
}

function assertMutationState(module, tree, before, phase) {
    assertCandidateState(tree, before.state, phase);
    const after = captureMutationState(module, tree, phase);
    if (JSON.stringify(after.chunks) !== JSON.stringify(before.chunks) ||
        JSON.stringify(after.committed) !== JSON.stringify(before.committed) ||
        JSON.stringify(after.plan) !== JSON.stringify(before.plan)) {
        throw new Error(`candidate mutation changed reachable/fresh chunk maps during ${phase}`);
    }
}

function chunkWitnesses(module, tree, hashes, phase) {
    return hashes.map(hash => {
        const length = module.wasm_tree_chunk_byte_length(tree, hash);
        const bytes = module.wasm_tree_get_chunk(tree, hash);
        if ((length == null) !== (bytes == null) ||
            (bytes != null && (length !== bytes.length || module.wasm_hash(bytes) !== hash))) {
            throw new Error(`invalid future-chunk witness during ${phase}`);
        }
        return [hash, length ?? null, bytes == null ? null : Buffer.from(bytes).toString("hex")];
    });
}

function assertChunkWitnesses(module, tree, expected, phase) {
    const actual = chunkWitnesses(module, tree, expected.map(([hash]) => hash), phase);
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(`candidate mutation exposed a future reachable chunk before finish during ${phase}`);
    }
}

function assertMutationFences(tree, token, before, phase) {
    const wrong = token === 0xffff_ffff ? token - 1 : token + 1;
    const operations = [
        () => tree.begin_candidate_update_job("[]"),
        () => tree.begin_candidate_delete_job("[]"),
        () => tree.begin_candidate_job(),
        () => tree.begin_candidate_chunks_job(),
        () => tree.begin_candidate(),
        () => tree.candidate_update_batch("[]"),
        () => tree.candidate_delete_batch("[]"),
        () => tree.update_batch("[]"),
        () => tree.delete_batch("[]"),
        () => tree.update_entry("fence.md", "01".repeat(32), 1, 1),
        () => tree.delete_entry("fence.md"),
        () => tree.build_from_entries("[]"),
        () => tree.rebuild_from_entries_in_version(before.version, "[]"),
        () => tree.set_tree_version(before.version === 1 ? 2 : 1),
        () => tree.load_root(before.committedBytes),
        () => tree.commit_candidate(),
        () => tree.abort_candidate(),
        () => tree.step_tree_job(token, 0),
        () => tree.step_tree_job(token, 257),
        () => tree.step_tree_job(wrong, 1),
        () => tree.finish_candidate_mutation_job(wrong),
        () => tree.cancel_tree_job(wrong),
        () => tree.finish_candidate_job(token),
        () => tree.finish_candidate_chunks_job(token),
    ];
    for (let index = 0; index < operations.length; index++) {
        expectThrow(operations[index], `mutation fence ${index} did not reject during ${phase}`);
        assertCandidateState(tree, before, `${phase} fence ${index}`);
    }
}

function mutationSteps(module, tree, token, before, futureWitnesses, stepUnits, phase, stopAfterStaging = false) {
    let previous = { completed: 0, reachable: 0 }, staged = false;
    const reachableHashes = JSON.stringify(before.chunks.map(([hash]) => hash));
    // A test termination bound, not a production work/latency promise. Counters
    // describe native units; codecs, allocation, finish and drop remain native.
    for (let steps = 1; steps <= 250_000; steps++) {
        const current = tree.step_tree_job(token, stepUnits);
        validateTreeJobProgress(current, previous, stepUnits, phase);
        if (current.remaining !== (current.done ? 0 : 1)) {
            throw new Error(`mutation remaining must be a pending flag during ${phase}`);
        }
        assertCandidateState(tree, before.state, `${phase} step`);
        if (JSON.stringify(module.wasm_tree_chunk_hashes(tree).sort()) !== reachableHashes) {
            throw new Error(`mutation step changed the visible reachable graph during ${phase}`);
        }
        assertChunkWitnesses(module, tree, futureWitnesses, `${phase} future chunks`);
        if (!staged && current.reachable > 0) {
            staged = true;
            assertMutationState(module, tree, before, `${phase} first staged chunk`);
            if (stopAfterStaging) {
                if (current.done) throw new Error(`cancellation fixture reached readiness before staged gate during ${phase}`);
                return { ...current, steps };
            }
        }
        previous = current;
        if (current.done) return { ...current, steps };
    }
    throw new Error(`mutation exceeded test termination bound during ${phase}`);
}

function checkMutationJob(module, tree, oracle, operation, stepUnits, phase, cancelAfterStaging) {
    const payload = JSON.stringify(operation.rows);
    const begin = operation.kind === "update" ? "begin_candidate_update_job" : "begin_candidate_delete_job";
    const legacy = operation.kind === "update" ? "candidate_update_batch" : "candidate_delete_batch";
    const before = captureMutationState(module, tree, phase);
    assertMutationState(module, oracle, before, `${phase} identical legacy starting tree`);
    // Compute the exact legacy result on the independent tree before stepping
    // the job. Its final reachable hashes let the packaged test prove that
    // those future objects remain absent/unchanged in the job's resident store
    // until finish. Native all_chunks tests additionally cover intermediate
    // staged objects that are not named by the final reachable graph.
    oracle[legacy](payload);
    const expected = captureMutationState(module, oracle, `${phase} legacy result`);
    const futureWitnesses = chunkWitnesses(module, tree, expected.plan.all, `${phase} before job`);

    const cancelled = tree[begin](payload);
    if (cancelAfterStaging) {
        const progress = mutationSteps(module, tree, cancelled, before, futureWitnesses, 1,
            `${phase} staged cancel`, true);
        if (progress.reachable === 0) throw new Error(`cancellation did not exercise private staging during ${phase}`);
        expectThrow(() => tree.finish_candidate_mutation_job(cancelled), `incomplete mutation finished during ${phase}`);
    }
    tree.cancel_tree_job(cancelled);
    assertMutationState(module, tree, before, `${phase} cancel preserves candidate/reachable bytes`);
    assertChunkWitnesses(module, tree, futureWitnesses, `${phase} cancel future chunks`);

    const token = tree[begin](payload);
    if (before.state.version === 1 && operation.rows.length !== 0) {
        const paused = mutationOutputProgress(
            tree.step_candidate_mutation_output_memory_v1_job(token, stepUnits),
            { completed: 0, reachable: 0 }, stepUnits, `${phase}: V1 PlanReady`);
        assert.deepEqual(paused, { done: false, units: 0, completed: 0,
            remaining: 1, reachable: 0, phase: "plan ready" });
        const outputPlan = mutationOutputPlan(tree.candidate_mutation_output_memory_plan_v1_job(token),
            `${phase}: V1 plan`, "v1-candidate-mutation-output");
        assert(outputPlan.rangeEndpointResidentRequestedBytes > 0,
            `${phase}: V1 plan omitted its replacement root`);
        for (let field = 0; field < 3; field++) {
            const invalid = outputMemoryWitnesses(outputPlan); invalid[field]++;
            expectThrow(() => tree.resume_candidate_mutation_output_memory_v1_job(token, ...invalid),
                `${phase}: V1 accepted changed output witness ${field}`);
            assert.deepEqual(tree.step_candidate_mutation_output_memory_v1_job(token, stepUnits), paused,
                `${phase}: V1 rejected witness changed its pending input owner`);
        }
        tree.resume_candidate_mutation_output_memory_v1_job(token, ...outputMemoryWitnesses(outputPlan));
        let current = paused, turns = 0;
        while (!current.done) {
            current = mutationOutputProgress(
                tree.step_candidate_mutation_output_memory_v1_job(token, stepUnits),
                current, stepUnits, `${phase}: V1 build`);
            assertMutationState(module, tree, before, `${phase}: V1 output remained private`);
            assertChunkWitnesses(module, tree, futureWitnesses, `${phase}: V1 future chunks`);
            if (++turns > 250_000) throw new Error(`${phase}: V1 output did not converge`);
        }
        const outputReady = mutationOutputReady(
            tree.candidate_mutation_output_memory_ready_v1_job(token), outputPlan, `${phase}: V1 Ready`);
        assert(outputReady.rangeEndpointResidentRequestedBytes > 0,
            `${phase}: V1 Ready omitted its resident root`);
        tree.finish_candidate_mutation_job_deferred(token);
        assertMutationState(module, tree, expected, `${phase}: V1 deferred finish`);
        const retired = drainMutationOutput(tree, token, stepUnits, expected.state,
            `${phase}: V1 retirement`);
        assert(retired.turns > 0, `${phase}: V1 retirement lifecycle was not observed`);
        return { steps: turns, completed: current.completed, staged: current.reachable };
    }
    for (const action of [() => tree.step_tree_job(cancelled, 1),
        () => tree.finish_candidate_mutation_job(cancelled), () => tree.cancel_tree_job(cancelled)]) {
        expectThrow(action, `stale mutation token consumed replacement ownership during ${phase}`);
    }
    assertMutationFences(tree, token, before.state, `${phase} pending`);
    if (operation.rows.length !== 0) {
        expectThrow(() => tree.finish_candidate_mutation_job(token), `unstepped mutation finished during ${phase}`);
    }
    const progress = mutationSteps(module, tree, token, before, futureWitnesses, stepUnits, phase);
    assertMutationState(module, tree, before, `${phase} ready before finish`);
    assertMutationFences(tree, token, before.state, `${phase} READY wrong-kind/mutator fences`);
    const ready = tree.step_tree_job(token, stepUnits);
    validateTreeJobProgress(ready, progress, stepUnits, `${phase} repeated ready step`);
    if (!ready.done || ready.units !== 0 || ready.completed !== progress.completed ||
        ready.reachable !== progress.reachable) throw new Error(`ready mutation did more work during ${phase}`);

    // The old synchronous API is deliberately executed on another identically
    // seeded tree. It is not the new job drained with an unbounded step budget.
    tree.finish_candidate_mutation_job(token);
    assertMutationState(module, tree, expected, `${phase} exact legacy result`);
    if (!bytesEqual(tree.root_bytes(), before.state.committedBytes) ||
        tree.root_hash_hex() !== before.state.committedHash || tree.total_files() !== before.state.committedCount) {
        throw new Error(`mutation finish changed committed root during ${phase}`);
    }
    if (operation.rows.length === 0) {
        assertMutationState(module, tree, before, `${phase} empty operation is exact no-op`);
        if (progress.steps !== 1 || progress.completed !== 0 || progress.reachable !== 0) {
            throw new Error(`empty mutation did work during ${phase}`);
        }
    }
    for (const action of [() => tree.step_tree_job(token, 1),
        () => tree.finish_candidate_mutation_job(token), () => tree.cancel_tree_job(token)]) {
        expectThrow(action, `finished mutation token remained usable during ${phase}`);
    }
    assertMutationState(module, tree, expected, `${phase} after stale finished token`);
    const chunkToken = tree.begin_candidate_chunks_job();
    let chunkProgress = { completed: 0, reachable: 0 };
    for (let step = 0; step < 10_000; step++) {
        const current = tree.step_tree_job(chunkToken, 256);
        validateTreeJobProgress(current, chunkProgress, 256, `${phase} reverse-kind chunks`);
        chunkProgress = current;
        if (current.done) break;
    }
    if (!chunkProgress.done) throw new Error(`reverse-kind chunk fixture never became ready during ${phase}`);
    expectThrow(() => tree.finish_candidate_mutation_job(chunkToken), `READY chunk job accepted mutation finish during ${phase}`);
    const plan = tree.finish_candidate_chunks_job(chunkToken);
    if (JSON.stringify(plan) !== JSON.stringify(expected.plan)) {
        throw new Error(`wrong mutation finish consumed chunk job ownership during ${phase}`);
    }
    assertMutationState(module, tree, expected, `${phase} reverse-kind finish preserved owner`);
    return { steps: progress.steps, completed: progress.completed, staged: progress.reachable };
}

function mutationScenarios(entries) {
    // Keep a multi-leaf v1 graph (>1,000 entries) for the main mutation/cancel
    // fixture. The reverse lineage/order fixture needs no second large graph.
    const multipleLeaves = entries.slice(0, 1_005);
    const replacement = { ...entries[7], hash: scalar.wasm_hash(deterministicPayload(8_193)),
        mtime_ms: 1_800_000_000_000, size: 8_193 };
    const first = { ...entries[0], path: "unicode/\ue000.md" };
    const last = { ...replacement, path: "unicode/\u{10000}.md" };
    const changed = [{ kind: "delete", rows: [entries[7].path, entries[1_004].path] },
        { kind: "update", rows: [replacement, first, last] }];
    const duplicate = { ...replacement, path: entries[1].path };
    return [
        { name: "changed-delete-update", seed: multipleLeaves, operations: changed, cancel: true },
        { name: "changed-update-delete", seed: entries.slice(0, 32), operations: [
            changed[1], { kind: "delete", rows: [entries[7].path, entries[31].path] },
        ] },
        { name: "empty-tree", seed: [], operations: [
            { kind: "delete", rows: [] }, { kind: "update", rows: [] },
            { kind: "delete", rows: ["missing.md"] }, { kind: "update", rows: [first, last] },
        ] },
        { name: "empty-identical-missing-duplicates", seed: entries.slice(0, 32), operations: [
            { kind: "update", rows: [] }, { kind: "delete", rows: [] },
            { kind: "update", rows: [entries[0]] },
            { kind: "delete", rows: ["missing.md", "missing.md"] },
            { kind: "update", rows: [entries[1], entries[2], duplicate] },
            { kind: "update", rows: [duplicate, entries[1]] },
            { kind: "delete", rows: [entries[1].path, entries[1].path, "missing.md"] },
            { kind: "update", rows: [duplicate] },
        ] },
    ];
}

function assertPackagedMutationJobs(entries) {
    const now = Date.now;
    let clock = 1_900_000_000_000;
    const reports = [];
    Date.now = () => clock;
    try {
        for (const version of [1, 2]) {
            for (const scenario of mutationScenarios(entries)) {
                let reference;
                for (const [label, module] of [["scalar", scalar], ["SIMD", simd]]) {
                    for (const budget of [1, 256]) {
                        const tree = new module.WasmTree("mutation-parity-vault", "mutation-parity-device");
                        let oracle;
                        try {
                            for (const method of ["begin_candidate_update_job", "begin_candidate_delete_job",
                                "finish_candidate_mutation_job", "step_tree_job", "cancel_tree_job"]) {
                                if (typeof tree[method] !== "function") {
                                    throw new Error(`packaged ${label} WASM lacks ${method}; rebuild both variants before parity`);
                                }
                            }
                            oracle = new module.WasmTree("mutation-parity-vault", "mutation-parity-device");
                            clock = 1_900_000_000_000;
                            for (const target of [tree, oracle]) {
                                target.set_tree_version(version);
                                target.build_from_entries(JSON.stringify(scenario.seed));
                                expectThrow(() => target.begin_candidate_update_job("[]"), "mutation accepted absent candidate");
                                expectThrow(() => target.begin_candidate_delete_job("[]"), "delete accepted absent candidate");
                                target.begin_candidate();
                            }
                            const operations = [];
                            const expectedEntries = new Map(scenario.seed.map(entry => [entry.path, entry]));
                            for (const [index, operation] of scenario.operations.entries()) {
                                clock = 1_900_000_001_000 + index;
                                const phase = `${label} v${version}/${scenario.name}/${operation.kind}/${index}/budget${budget}`;
                                const progress = checkMutationJob(module, tree, oracle, operation, budget, phase,
                                    scenario.cancel === true && budget === 1);
                                // An independent plain-map expectation makes duplicate last-wins
                                // and delete dedup observable even if both mutation implementations
                                // accidentally shared the same normalization bug.
                                for (const row of operation.rows) {
                                    if (operation.kind === "update") expectedEntries.set(row.path, row);
                                    else expectedEntries.delete(row);
                                }
                                const semantic = new module.WasmTree("mutation-parity-vault", "mutation-parity-device");
                                try {
                                    semantic.set_tree_version(version);
                                    semantic.build_from_entries(JSON.stringify([...expectedEntries.values()]));
                                    if (tree.candidate_total_files() !== expectedEntries.size ||
                                        tree.candidate_root_hash_hex() !== semantic.root_hash_hex()) {
                                        throw new Error(`mutation final file state differs from independent map during ${phase}`);
                                    }
                                } finally { semantic.free(); }
                                const exact = captureMutationState(module, tree, phase);
                                // Compact reporting only; full maps and bytes were compared above.
                                operations.push({ kind: operation.kind, ...progress,
                                    exact_state_hash: module.wasm_hash(Buffer.from(JSON.stringify(exact))) });
                            }
                            const comparable = JSON.stringify(operations.map(({ steps, ...rest }) => rest));
                            if (reference !== undefined && reference !== comparable) {
                                throw new Error(`scalar/SIMD/budget mutation result or total-work mismatch at v${version}/${scenario.name}`);
                            }
                            reference = comparable;
                            const final = captureMutationState(module, tree, `${scenario.name} final`);
                            tree.commit_candidate();
                            oracle.commit_candidate();
                            if (tree.has_candidate() || tree.root_hash_hex() !== final.state.candidateHash ||
                                tree.total_files() !== final.state.candidateCount ||
                                !bytesEqual(tree.root_bytes(), final.state.candidateBytes) ||
                                !bytesEqual(tree.root_bytes(), oracle.root_bytes()) ||
                                JSON.stringify(reachableChunkMap(module, tree, scenario.name)) !==
                                    JSON.stringify(reachableChunkMap(module, oracle, scenario.name))) {
                                throw new Error(`mutation commit diverged from oracle at ${scenario.name}`);
                            }
                            reports.push({ variant: label, version, scenario: scenario.name, budget, operations });
                            if (process.env.OBSETYNC_PARITY_PROGRESS === "1") {
                                console.error(`mutation parity: ${label} v${version} ${scenario.name} budget=${budget} passed`);
                            }
                        } finally { oracle?.free(); tree.free(); }
                    }
                }
            }
        }
        return reports;
    } finally { Date.now = now; }
}

const MUTATION_OUTPUT_METHODS = ["begin_candidate_update_job", "begin_candidate_delete_job",
    "step_candidate_mutation_output_memory_v1_job", "candidate_mutation_output_memory_plan_v1_job",
    "resume_candidate_mutation_output_memory_v1_job", "candidate_mutation_output_memory_ready_v1_job",
    "finish_candidate_mutation_job_deferred", "cancel_candidate_mutation_job_deferred", "step_tree_retirement",
    "candidate_revision", "committed_revision", "abort_candidate", "chunk_memory_snapshot",
    "commit_candidate_output_settlement_v1", "abort_candidate_output_settlement_v1"];

const V2_OUTPUT_SETTLEMENT_FIELDS = ["schema", "scope", "outcome", "treeVersion", "before",
    "reachable", "removed", "after", "bytesRemoved", "committedRevision", "candidateRevision",
    "countersValid", "nodePayloadBytes", "rangeEndpointResidentRequestedBytes",
    "residentAdmissionBytes"];

function mutationOutputFields(value, fields, label) {
    assert(value !== null && typeof value === "object" && !Array.isArray(value), `${label}: object required`);
    assert.deepEqual(Reflect.ownKeys(value).sort(), fields.slice().sort(), `${label}: exact shape`);
    for (const field of fields) assert("value" in Object.getOwnPropertyDescriptor(value, field), `${label}: accessor`);
}

function mutationOutputPlan(value, label, expectedScope = "v2-candidate-mutation-output") {
    const fields = ["schema", "scope", "nodePayloadBytes", "rangeEndpointPeakRequestedBytes",
        "rangeEndpointResidentRequestedBytes", "peakAdmissionBytes", "residentAdmissionBytes"];
    mutationOutputFields(value, fields, label);
    assert.equal(value.schema, 1); assert.equal(value.scope, expectedScope);
    for (const field of fields.slice(2)) outputPlanCount(value[field], `${label}/${field}`);
    assert(value.rangeEndpointResidentRequestedBytes <= value.rangeEndpointPeakRequestedBytes);
    assert.equal(value.peakAdmissionBytes, outputPlanSum(value.nodePayloadBytes, value.rangeEndpointPeakRequestedBytes, label));
    assert.equal(value.residentAdmissionBytes, outputPlanSum(value.nodePayloadBytes, value.rangeEndpointResidentRequestedBytes, label));
    return value;
}

function mutationOutputReady(value, plan, label) {
    mutationOutputFields(value, ["schema", "scope", "stagedNodePayloadBytes",
        "rangeEndpointResidentRequestedBytes", "residentAdmissionBytes"], label);
    assert.equal(value.schema, 1); assert.equal(value.scope, plan.scope);
    for (const field of ["stagedNodePayloadBytes", "rangeEndpointResidentRequestedBytes", "residentAdmissionBytes"])
        outputPlanCount(value[field], `${label}/${field}`);
    assert(value.stagedNodePayloadBytes <= plan.nodePayloadBytes);
    if (plan.scope === "v1-candidate-mutation-output")
        assert(value.rangeEndpointResidentRequestedBytes <= plan.rangeEndpointResidentRequestedBytes);
    else assert.equal(value.rangeEndpointResidentRequestedBytes, plan.rangeEndpointResidentRequestedBytes);
    assert.equal(value.residentAdmissionBytes,
        outputPlanSum(value.stagedNodePayloadBytes, value.rangeEndpointResidentRequestedBytes, label));
    assert(value.residentAdmissionBytes <= plan.residentAdmissionBytes);
    return value;
}

function v2OutputSettlementTarget(module, tree, outcome, label) {
    assert.equal(tree.tree_version(), 2, `${label}: settlement target is not V2`);
    assert.equal(tree.has_candidate(), true, `${label}: settlement target lacks candidate`);
    const commit = outcome === "commit";
    const rootBytes = commit ? tree.candidate_root_bytes() : tree.root_bytes();
    const rootHash = commit ? tree.candidate_root_hash_hex() : tree.root_hash_hex();
    const totalFiles = commit ? tree.candidate_total_files() : tree.total_files();
    assert(rootBytes instanceof Uint8Array && typeof rootHash === "string",
        `${label}: missing target root`);
    const hashes = (commit ? module.wasm_tree_candidate_chunk_hashes(tree)
        : module.wasm_tree_committed_chunk_hashes(tree)).slice().sort();
    assert.equal(new Set(hashes).size, hashes.length, `${label}: duplicate target hash`);
    const chunks = hashes.map(hash => {
        const bytes = module.wasm_tree_get_chunk(tree, hash);
        assert(bytes instanceof Uint8Array && module.wasm_hash(bytes) === hash &&
            module.wasm_tree_chunk_byte_length(tree, hash) === bytes.length,
        `${label}: invalid target chunk`);
        return [hash, Buffer.from(bytes).toString("hex")];
    });
    const memory = canonicalReplacementOutputMemoryPlan(
        chunks, Buffer.from(rootBytes).toString("hex"), label);
    return { rootBytes: Buffer.from(rootBytes).toString("hex"), rootHash, totalFiles, hashes,
        chunks, nodePayloadBytes: memory.nodePayloadBytes,
        rangeEndpointResidentRequestedBytes: memory.rangeEndpointResidentRequestedBytes,
        residentAdmissionBytes: memory.residentAdmissionBytes };
}

function v2OutputSettlementReport(value, expected, label) {
    mutationOutputFields(value, V2_OUTPUT_SETTLEMENT_FIELDS, label);
    for (const field of ["treeVersion", "before", "reachable", "removed", "after", "bytesRemoved",
        "committedRevision", "candidateRevision", "nodePayloadBytes",
        "rangeEndpointResidentRequestedBytes", "residentAdmissionBytes"])
        outputPlanCount(value[field], `${label}/${field}`);
    assert.equal(value.schema, 1); assert.equal(value.scope, "v2-stable-tree-output");
    assert.equal(value.outcome, expected.outcome); assert.equal(value.treeVersion, 2);
    assert.equal(value.countersValid, true);
    assert.equal(value.after, value.reachable);
    assert.equal(value.removed, value.before - value.after);
    assert.equal(value.residentAdmissionBytes,
        outputPlanSum(value.nodePayloadBytes, value.rangeEndpointResidentRequestedBytes, label));
    assert.deepEqual(value, expected, `${label}: report differs from independent physical oracle`);
    return value;
}

function settleV2Output(module, tree, outcome, label) {
    const target = v2OutputSettlementTarget(module, tree, outcome, `${label}/target`);
    const beforeStore = tree.chunk_memory_snapshot().resident;
    mutationOutputFields(beforeStore,
        ["chunks", "map_capacity", "payload_bytes", "buffer_capacity_bytes", "counters_valid"],
        `${label}/before-store`);
    for (const field of ["chunks", "map_capacity", "payload_bytes", "buffer_capacity_bytes"])
        outputPlanCount(beforeStore[field], `${label}/before-store/${field}`);
    assert.equal(beforeStore.counters_valid, true);
    const beforeCommitted = outputPlanCount(tree.committed_revision(), `${label}/committed-revision`);
    const beforeCandidate = outputPlanCount(tree.candidate_revision(), `${label}/candidate-revision`);
    const expected = {
        schema: 1, scope: "v2-stable-tree-output", outcome, treeVersion: 2,
        before: beforeStore.chunks, reachable: target.hashes.length,
        removed: beforeStore.chunks - target.hashes.length, after: target.hashes.length,
        bytesRemoved: beforeStore.payload_bytes - target.nodePayloadBytes,
        committedRevision: beforeCommitted + (outcome === "commit" ? 1 : 0),
        candidateRevision: beforeCandidate + 1, countersValid: true,
        nodePayloadBytes: target.nodePayloadBytes,
        rangeEndpointResidentRequestedBytes: target.rangeEndpointResidentRequestedBytes,
        residentAdmissionBytes: target.residentAdmissionBytes,
    };
    for (const field of ["removed", "bytesRemoved", "committedRevision", "candidateRevision"])
        outputPlanCount(expected[field], `${label}/expected/${field}`);
    const value = tree[outcome === "commit" ? "commit_candidate_output_settlement_v1"
        : "abort_candidate_output_settlement_v1"]();
    const report = v2OutputSettlementReport(value, expected, `${label}/report`);
    assert.equal(tree.has_candidate(), false, `${label}: settlement retained candidate`);
    assert.equal(tree.committed_revision(), report.committedRevision);
    assert.equal(tree.candidate_revision(), report.candidateRevision);
    assert.equal(tree.root_hash_hex(), target.rootHash);
    assert.equal(tree.total_files(), target.totalFiles);
    assert.equal(Buffer.from(tree.root_bytes()).toString("hex"), target.rootBytes);
    assert.deepEqual(module.wasm_tree_committed_chunk_hashes(tree).slice().sort(), target.hashes,
        `${label}: committed reachability differs from target`);
    const afterStore = tree.chunk_memory_snapshot().resident;
    assert.equal(afterStore.counters_valid, true);
    assert.equal(afterStore.chunks, target.hashes.length);
    assert.equal(afterStore.payload_bytes, target.nodePayloadBytes);
    return { report, target_root_hash: target.rootHash,
        target_graph_hash: module.wasm_hash(Buffer.from(JSON.stringify(target.chunks))) };
}

function rejectV1OutputSettlement(module, entries, label) {
    const tree = new module.WasmTree("settlement-v1-vault", "settlement-v1-device");
    try {
        tree.set_tree_version(1);
        tree.build_from_entries(JSON.stringify(entries.slice(0, 32)));
        tree.begin_candidate();
        tree.candidate_update_batch(JSON.stringify([{ ...entries[0], hash: "ef".repeat(32) }]));
        const before = captureMutationState(module, tree, `${label}/before`);
        const beforeStore = tree.chunk_memory_snapshot().resident;
        for (const method of ["commit_candidate_output_settlement_v1",
            "abort_candidate_output_settlement_v1"]) {
            assert.throws(() => tree[method](), `${label}: ${method} accepted V1`);
            assertMutationState(module, tree, before, `${label}/${method}`);
            assert.deepEqual(tree.chunk_memory_snapshot().resident, beforeStore,
                `${label}: ${method} changed the V1 physical store`);
        }
        tree.abort_candidate();
        return true;
    } finally { tree.free(); }
}

function mutationOutputProgress(value, previous, budget, label) {
    mutationOutputFields(value, ["done", "units", "completed", "remaining", "reachable", "phase"], label);
    assert.equal(typeof value.done, "boolean"); assert.equal(typeof value.phase, "string");
    for (const field of ["units", "completed", "remaining", "reachable"]) outputPlanCount(value[field], `${label}/${field}`);
    assert(value.units <= budget);
    assert.equal(value.completed, outputPlanSum(previous.completed, value.units, label));
    assert(value.reachable >= previous.reachable && value.reachable <= value.completed);
    assert.equal(value.remaining, value.done ? 0 : 1);
    assert(value.done || value.units > 0 || value.phase === "plan ready", `${label}: pending without progress`);
    return value;
}

function rejectMutationOutputPhase(tree, token, plan, label, readyAllowed = false) {
    assert.throws(() => tree.candidate_mutation_output_memory_plan_v1_job(token), `${label}: plan wrong phase`);
    assert.throws(() => tree.resume_candidate_mutation_output_memory_v1_job(token, ...outputMemoryWitnesses(plan)),
        `${label}: resume wrong phase`);
    if (!readyAllowed) assert.throws(() => tree.candidate_mutation_output_memory_ready_v1_job(token), `${label}: ready wrong phase`);
}

function drainMutationOutput(tree, token, budget, before, label) {
    let completed = 0, turns = 0;
    const wrong = differentTreeJobToken(token);
    for (const action of [() => tree.step_tree_retirement(wrong, budget),
        () => tree.step_tree_retirement(token, 0), () => tree.step_tree_retirement(token, 257),
        () => tree.finish_candidate_mutation_job_deferred(token), () => tree.cancel_tree_job(token),
        () => tree.abort_candidate(), () => tree.begin_candidate_update_job("[]")]) assert.throws(action, label);
    for (;;) {
        // Idempotent cancel is not permission to replay an already-consumed
        // generic step. Completion must remain cumulative under the same token.
        assert.equal(tree.cancel_candidate_mutation_job_deferred(token), undefined);
        const progress = tree.step_tree_retirement(token, budget);
        mutationOutputFields(progress, ["done", "units", "completed"], label);
        validateReachabilityRetirementProgress(progress, completed, budget, label);
        completed = progress.completed; turns++;
        assertCandidateState(tree, before, `${label}: retirement preserves visible roots`);
        assert(turns <= 20_000, `${label}: retirement runaway`);
        if (progress.done) break;
    }
    for (const action of [() => tree.step_tree_retirement(token, budget),
        () => tree.cancel_candidate_mutation_job_deferred(token),
        () => tree.finish_candidate_mutation_job_deferred(token)]) assert.throws(action, `${label}: stale retirement token`);
    return { turns, completed };
}

function checkMutationOutputJob(module, tree, oracle, operation, budget, label, cancellationStages = []) {
    const before = captureMutationState(module, tree, label), payload = JSON.stringify(operation.rows);
    assertMutationState(module, oracle, before, `${label}: independent starting oracle`);
    oracle[operation.kind === "update" ? "candidate_update_batch" : "candidate_delete_batch"](payload);
    const expected = captureMutationState(module, oracle, `${label}: synchronous oracle`);
    const noop = operation.rows.length === 0;
    // Small one-leaf fixtures make a fully independent byte oracle possible:
    // one replacement leaf + its assembly descriptor allocate exactly 2R.
    // This does not infer a general mutation plan from final reachable nodes.
    assert.equal(expected.chunks.length, 1, `${label}: one-leaf output fixture changed shape`);
    assert.equal(Buffer.from(expected.chunks[0][1], "hex").subarray(0, 4).toString(), "OVL2");
    const independent = canonicalReplacementOutputMemoryPlan(expected.chunks,
        Buffer.from(expected.state.candidateBytes).toString("hex"), label);
    const expectedPlan = mutationOutputPlan({ ...independent, scope: "v2-candidate-mutation-output" }, label);
    assert.equal(expectedPlan.rangeEndpointPeakRequestedBytes, 2 * expectedPlan.rangeEndpointResidentRequestedBytes);
    const future = chunkWitnesses(module, tree, expected.plan.all, label);
    const actualBytes = expected.chunks.reduce((sum, [hash, hex]) => sum +
        (future.find(([key]) => key === hash)[1] === null ? hex.length / 2 : 0), 0);
    const cancelled = [];
    let lastToken;
    for (const cancellation of [...cancellationStages, null]) {
        // A tiny leaf can complete emit→Ready inside one budget-256 call.
        // Unit stepping makes this a distinct pre-Ready interruption probe;
        // normal/refusal/Ready paths and retirement still use both budgets.
        const buildBudget = cancellation === "staged" ? 1 : budget;
        const token = requireTreeJobToken(tree[operation.kind === "update"
            ? "begin_candidate_update_job" : "begin_candidate_delete_job"](payload), label);
        if (lastToken !== undefined) assert.throws(() => tree.step_tree_retirement(lastToken, budget));
        lastToken = token;
        const wrong = differentTreeJobToken(token);
        for (const action of [() => tree.step_candidate_mutation_output_memory_v1_job(wrong, budget),
            () => tree.step_candidate_mutation_output_memory_v1_job(token, 0),
            () => tree.step_candidate_mutation_output_memory_v1_job(token, 257),
            () => tree.candidate_mutation_output_memory_plan_v1_job(wrong),
            () => tree.candidate_mutation_output_memory_ready_v1_job(wrong),
            () => tree.resume_candidate_mutation_output_memory_v1_job(wrong, ...outputMemoryWitnesses(expectedPlan)),
            () => tree.finish_candidate_mutation_job_deferred(wrong),
            () => tree.cancel_candidate_mutation_job_deferred(wrong)]) assert.throws(action, `${label}: wrong token/budget`);
        rejectMutationOutputPhase(tree, token, expectedPlan, `${label}: before Plan`);
        let previous = { completed: 0, reachable: 0 }, turns = 0, plan, ready, stop = false;
        for (;;) {
            const current = mutationOutputProgress(tree.step_candidate_mutation_output_memory_v1_job(token, buildBudget),
                previous, buildBudget, label);
            previous = current; turns++; assert(turns <= 20_000, `${label}: mutation runaway`);
            assertMutationState(module, tree, before, `${label}: private build`);
            assertChunkWitnesses(module, tree, future, `${label}: no pre-publication overlay`);
            if (current.phase === "plan ready") {
                assert.equal(plan, undefined, `${label}: repeated PlanReady barrier`);
                assert.equal(current.done, false); assert.equal(current.reachable, 0);
                plan = mutationOutputPlan(tree.candidate_mutation_output_memory_plan_v1_job(token), label);
                assert.deepEqual(plan, expectedPlan, `${label}: native plan differs from canonical leaf/root`);
                const paused = () => {
                    assert.deepEqual(tree.step_candidate_mutation_output_memory_v1_job(token, budget),
                        { done: false, units: 0, completed: current.completed, remaining: 1, reachable: 0, phase: "plan ready" });
                    assert.deepEqual(tree.candidate_mutation_output_memory_plan_v1_job(token), plan);
                    assertMutationState(module, tree, before, `${label}: rejected witness preserves owner`);
                    assertChunkWitnesses(module, tree, future, `${label}: rejected witness allocated resident output`);
                };
                paused();
                assert.throws(() => tree.candidate_mutation_output_memory_ready_v1_job(token));
                assert.throws(() => tree.finish_candidate_mutation_job_deferred(token));
                for (let field = 0; field < 3; field++) for (const bad of
                    [outputMemoryWitnesses(plan)[field] - 1, outputMemoryWitnesses(plan)[field] + 1,
                        NaN, Infinity, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
                    const witnesses = outputMemoryWitnesses(plan); witnesses[field] = bad;
                    assert.throws(() => tree.resume_candidate_mutation_output_memory_v1_job(token, ...witnesses)); paused();
                }
                if (cancellation === "plan-refusal") { stop = true; break; }
                assert.equal(tree.resume_candidate_mutation_output_memory_v1_job(token, ...outputMemoryWitnesses(plan)), undefined);
                rejectMutationOutputPhase(tree, token, plan, `${label}: resumed`);
            } else if (!plan) assert.equal(current.reachable, 0);
            if (cancellation === "staged" && current.reachable > 0) {
                assert.equal(current.done, false, `${label}: staged cancel accidentally duplicated Ready cancel`);
                stop = true; break;
            }
            if (current.done) {
                if (noop) {
                    assert.equal(plan, undefined); assert.deepEqual(current,
                        { done: true, units: 0, completed: 0, remaining: 0, reachable: 0, phase: "ready" });
                    rejectMutationOutputPhase(tree, token, expectedPlan, `${label}: noop`);
                } else {
                    assert(plan, `${label}: missing PlanReady`);
                    ready = mutationOutputReady(tree.candidate_mutation_output_memory_ready_v1_job(token), plan, label);
                    assert.equal(ready.stagedNodePayloadBytes, actualBytes, `${label}: actual overlay differs from preexisting resident hashes`);
                    rejectMutationOutputPhase(tree, token, plan, `${label}: Ready`, true);
                }
                assert.deepEqual(tree.step_candidate_mutation_output_memory_v1_job(token, budget), { ...current, units: 0 });
                if (cancellation === "ready") stop = true;
                break;
            }
        }
        if (stop) {
            assert.equal(tree.cancel_candidate_mutation_job_deferred(token), undefined);
            const retired = drainMutationOutput(tree, token, budget, before.state, `${label}/${cancellation}`);
            assertMutationState(module, tree, before, `${label}: cancellation preserves full graph`);
            assertChunkWitnesses(module, tree, future, `${label}: cancelled overlay absent`);
            cancelled.push({ stage: cancellation, build_step_budget: buildBudget, build_units: previous.completed, ...retired });
            continue;
        }
        assert.equal(tree.finish_candidate_mutation_job_deferred(token), undefined);
        assertMutationState(module, tree, expected, `${label}: deferred finish exactly equals synchronous oracle`);
        const retired = drainMutationOutput(tree, token, budget, expected.state, `${label}/published`);
        assertMutationState(module, tree, expected, `${label}: published graph survives retirement`);
        return { kind: operation.kind, noop, turns, completed: previous.completed,
            plan: plan ?? null, ready: ready ?? null, retirement: retired, cancelled,
            exact_state_hash: module.wasm_hash(Buffer.from(JSON.stringify(expected))) };
    }
    assert.fail(`${label}: no publication run`);
}

function assertPackagedMutationOutputJobs(entries) {
    const reports = [], references = new Map(), settlementReferences = new Map(), now = Date.now;
    Date.now = () => 1_940_000_000_000;
    try {
        for (const [variant, module] of [["scalar", scalar], ["SIMD", simd]]) for (const budget of [1, 256]) {
            const tree = new module.WasmTree("output-parity-vault", "output-parity-device");
            const oracle = new module.WasmTree("output-parity-vault", "output-parity-device");
            try {
                for (const method of MUTATION_OUTPUT_METHODS) assert.equal(typeof tree[method], "function",
                    `packaged ${variant} lacks ${method}; rebuild both variants`);
                const seed = entries.slice(0, 32), changed = { ...seed[7], hash: "ab".repeat(32), size: 7 };
                for (const target of [tree, oracle]) {
                    target.set_tree_version(2); target.build_from_entries(JSON.stringify(seed)); target.begin_candidate();
                }
                const operations = [];
                const cases = [{ kind: "update", rows: [changed] }, { kind: "update", rows: [seed[7]] },
                    { kind: "delete", rows: [seed[3].path] }, { kind: "update", rows: [] }, { kind: "delete", rows: [] }];
                for (const [index, operation] of cases.entries()) {
                    const result = checkMutationOutputJob(module, tree, oracle, operation, budget,
                        `${variant}/output/${index}/budget${budget}`, index === 0 ? ["plan-refusal", "staged", "ready"] : []);
                    if (index === 1) {
                        assert(result.plan.nodePayloadBytes > 0);
                        assert.equal(result.ready.stagedNodePayloadBytes, 0, "restore-to-committed fixture failed to exercise resident dedup");
                    }
                    const comparable = { kind: result.kind, noop: result.noop, completed: result.completed,
                        plan: result.plan, ready: result.ready, exact_state_hash: result.exact_state_hash };
                    if (references.has(index)) assert.deepEqual(comparable, references.get(index), "scalar/SIMD/budget output parity");
                    else references.set(index, comparable);
                    operations.push(result);
                }
                const wrongKind = tree.begin_candidate_chunks_job();
                rejectMutationOutputPhase(tree, wrongKind, operations[0].plan, "candidate chunk wrong-kind");
                assert.throws(() => tree.step_candidate_mutation_output_memory_v1_job(wrongKind, budget));
                assert.throws(() => tree.finish_candidate_mutation_job_deferred(wrongKind));
                assert.throws(() => tree.cancel_candidate_mutation_job_deferred(wrongKind));
                tree.cancel_tree_job(wrongKind);
                const failedBefore = captureMutationState(module, tree, "invalid-path before");
                const failedToken = tree.begin_candidate_update_job(JSON.stringify([{ ...changed, path: "../invalid" }]));
                assert.throws(() => tree.step_candidate_mutation_output_memory_v1_job(failedToken, budget),
                    "invalid path did not cause actual native execution failure");
                assert.throws(() => tree.begin_candidate_update_job("[]"), "execution error dropped native ownership");
                tree.cancel_candidate_mutation_job_deferred(failedToken);
                const failedRetirement = drainMutationOutput(tree, failedToken, budget, failedBefore.state, "execution failure");
                assertMutationState(module, tree, failedBefore, "execution failure retirement");
                const outcome = budget === 1 ? "abort" : "commit";
                const settlement = settleV2Output(module, tree, outcome,
                    `${variant}/settlement/${outcome}`);
                assert(settlement.report.removed > 0,
                    `${variant}/${outcome}: fixture did not exercise physical sweep`);
                if (settlementReferences.has(outcome))
                    assert.deepEqual(settlement, settlementReferences.get(outcome),
                        `${outcome}: scalar/SIMD settlement mismatch`);
                else settlementReferences.set(outcome, settlement);
                if (outcome === "commit") oracle.commit_candidate();
                else oracle.abort_candidate();
                assert.equal(tree.root_hash_hex(), oracle.root_hash_hex(),
                    `${variant}/${outcome}: settlement diverged from legacy oracle`);
                assert.deepEqual(module.wasm_tree_committed_chunk_hashes(tree).slice().sort(),
                    module.wasm_tree_committed_chunk_hashes(oracle).slice().sort(),
                    `${variant}/${outcome}: settlement graph diverged from legacy oracle`);
                const v1_atomic_rejection = budget === 1
                    ? rejectV1OutputSettlement(module, entries, `${variant}/settlement-v1`) : null;
                reports.push({ variant, version: 2, budget,
                    independent_oracle: "legacy synchronous separate tree; canonical one-leaf bytes",
                    operations, execution_failure_retirement: failedRetirement,
                    settlement, v1_atomic_rejection });
            } finally { oracle.free(); tree.free(); }
        }
        return reports;
    } finally { Date.now = now; }
}

const REPLACEMENT_REBUILD_MAX_PAGE_ENTRIES = 256;
const REPLACEMENT_REBUILD_MAX_PAGE_BYTES = 256 * 1024;
const REPLACEMENT_REBUILD_MAX_ENTRIES = 65_536;
const REPLACEMENT_REBUILD_MAX_JSON_BYTES = 8 * 1024 * 1024;
const REPLACEMENT_SORT_PLAN_FIELDS = ["schema", "scope", "entryCount", "indexSizeBytes",
    "sourceIndexRequestedBytes", "targetIndexRequestedBytes", "peakAdmissionBytes"];

function replacementSortMemoryPlan(value, expectedEntries, label) {
    if (value === null || typeof value !== "object" || Array.isArray(value) ||
        Reflect.ownKeys(value).slice().sort().join(",") !== REPLACEMENT_SORT_PLAN_FIELDS.slice().sort().join(",")) {
        throw new Error(`${label}: invalid V2 replacement sort-memory plan shape`);
    }
    for (const field of REPLACEMENT_SORT_PLAN_FIELDS) {
        const descriptor = Object.getOwnPropertyDescriptor(value, field);
        if (!descriptor || !("value" in descriptor) || descriptor.get !== undefined || descriptor.set !== undefined) {
            throw new Error(`${label}: V2 replacement sort-memory plan has a non-data ${field}`);
        }
    }
    for (const field of ["schema", "entryCount", "indexSizeBytes", "sourceIndexRequestedBytes",
        "targetIndexRequestedBytes", "peakAdmissionBytes"]) {
        if (typeof value[field] !== "number" || !Number.isSafeInteger(value[field]) || value[field] < 0) {
            throw new Error(`${label}: unsafe V2 replacement sort-memory ${field}`);
        }
    }
    const expectedIndexBytes = expectedEntries * 4;
    if (value.schema !== 1 || value.scope !== "v2-replacement-sort-indices" ||
        value.entryCount !== expectedEntries || value.indexSizeBytes !== 4 ||
        value.sourceIndexRequestedBytes !== expectedIndexBytes ||
        value.targetIndexRequestedBytes !== expectedIndexBytes ||
        value.peakAdmissionBytes !== expectedIndexBytes * 2) {
        throw new Error(`${label}: V2 replacement sort-memory plan differs from the independent wasm32 oracle`);
    }
    return value;
}

function replacementSortInput(tree) {
    return tree.replacement_input_memory_snapshot();
}

function rejectReplacementSortPlanPhase(module, tree, token, before, label) {
    const input = replacementSortInput(tree);
    rejectReplacementOperation(module, tree, before,
        () => tree.replacement_rebuild_sort_memory_plan_v1_job(token),
        `${label}: sort plan accepted wrong phase/version`, `${label}: sort plan rejection`);
    assert.deepEqual(replacementSortInput(tree), input, `${label}: rejected sort plan changed input owner`);
    rejectReplacementOperation(module, tree, before,
        () => tree.start_replacement_rebuild_sort_memory_v1_job(token, 0, 0),
        `${label}: admitted sort start accepted wrong phase/version`, `${label}: admitted start rejection`);
    assert.deepEqual(replacementSortInput(tree), input, `${label}: rejected admitted start changed input owner`);
}

function startReplacementRebuildWithSortPlan(module, tree, token, version, entries, before, label) {
    const input = replacementSortInput(tree);
    if (version === 1) {
        rejectReplacementSortPlanPhase(module, tree, token, before, `${label}/v1-atomic-rejection`);
        assert.deepEqual(replacementSortInput(tree), input,
            `${label}: V1 sort admission rejection changed the complete input owner`);
        tree.start_replacement_rebuild_job(token);
        return null;
    }
    const plan = replacementSortMemoryPlan(
        tree.replacement_rebuild_sort_memory_plan_v1_job(token), entries, `${label}/sort-plan`);
    assert.deepEqual(replacementSortMemoryPlan(
        tree.replacement_rebuild_sort_memory_plan_v1_job(token), entries, `${label}/sort-plan-repeat`), plan,
    `${label}: repeated V2 sort plan changed`);
    const wrong = token === 0xffff_ffff ? token - 1 : token + 1;
    expectThrow(() => tree.replacement_rebuild_sort_memory_plan_v1_job(wrong),
        `${label}: wrong token exposed V2 sort plan`);
    expectThrow(() => tree.start_replacement_rebuild_sort_memory_v1_job(
        wrong, plan.sourceIndexRequestedBytes, plan.targetIndexRequestedBytes),
    `${label}: wrong token started V2 sort`);
    assert.deepEqual(replacementSortInput(tree), input, `${label}: wrong token changed input owner`);
    for (let field = 0; field < 2; field++) {
        const witnesses = [plan.sourceIndexRequestedBytes, plan.targetIndexRequestedBytes];
        for (const bad of new Set([witnesses[field] - 1, witnesses[field] + 1,
            NaN, Infinity, -Infinity, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])) {
            const invalid = witnesses.slice(); invalid[field] = bad;
            expectThrow(() => tree.start_replacement_rebuild_sort_memory_v1_job(token, ...invalid),
                `${label}: invalid V2 sort witness ${field}/${bad} started the builder`);
            assert.deepEqual(replacementSortInput(tree), input,
                `${label}: invalid V2 sort witness changed the complete input owner`);
            assertReplacementVisibleState(module, tree, before,
                `${label}: invalid V2 sort witness changed visible state`);
        }
    }
    tree.start_replacement_rebuild_sort_memory_v1_job(
        token, plan.sourceIndexRequestedBytes, plan.targetIndexRequestedBytes);
    expectThrow(() => tree.replacement_rebuild_sort_memory_plan_v1_job(token),
        `${label}: consumed V2 sort plan remained readable`);
    expectThrow(() => tree.start_replacement_rebuild_sort_memory_v1_job(
        token, plan.sourceIndexRequestedBytes, plan.targetIndexRequestedBytes),
    `${label}: admitted V2 sort start replay succeeded`);
    assertReplacementVisibleState(module, tree, before, `${label}: admitted V2 sort start changed visible state`);
    return plan;
}

function replacementRebuildCorpus() {
    const rows = deterministicTreeEntries(1_033).map((entry, index) => ({
        ...entry,
        // One wide v1 prefix crosses its legacy leaf boundary; the complete
        // corpus also crosses Tree v2 MAX_LEAF_ENTRIES.
        path: `wide/${String(index).padStart(6, "0")}.md`,
    }));
    const bmp = "\ue000-bmp/note.md", astral = "\u{10000}-astral/note.md";
    rows[0] = { ...rows[0], path: bmp };
    rows[1] = { ...rows[1], path: astral };
    rows[2] = { ...rows[2], path: "root-\ue000-bmp.md" };
    rows[3] = { ...rows[3], path: "root-\u{10000}-astral.md" };
    rows[4] = { ...rows[4], path: "alpha/note.md" };
    rows[5] = { ...rows[5], path: "zeta/note.md" };
    rows[6] = { ...rows[6], path: "mixed/deep/note.md" };
    // JS compares UTF-16 code units (astral surrogate first), while Rust String
    // ordering follows UTF-8/Unicode scalar order (BMP first). Never host-sort
    // the pages: the replacement must retain legacy native sorting/grouping.
    const jsFirst = [bmp, astral].sort()[0];
    const utf8First = [bmp, astral].sort((left, right) =>
        Buffer.compare(Buffer.from(left), Buffer.from(right)))[0];
    if (jsFirst !== astral || utf8First !== bmp) {
        throw new Error("replacement rebuild Unicode ordering fixture is ineffective");
    }
    // Deliberately non-sorted input also makes page boundaries unrelated to
    // native prefix groups. Stable duplicate behavior is exercised in v1 by
    // appending a second state for one earlier path below.
    return rows.filter((_, index) => index % 2 === 0).reverse()
        .concat(rows.filter((_, index) => index % 2 === 1));
}

function replacementRowsForVersion(corpus, version) {
    if (version !== 1) return corpus;
    return corpus.concat([{ ...corpus[17], hash: "d7".repeat(32),
        mtime_ms: corpus[17].mtime_ms + 10_000, size: corpus[17].size + 1 }]);
}

function oversizedReplacementPage(row) {
    const rows = Array.from({ length: 65 }, (_, index) => ({ ...row,
        path: `oversize/${String(index).padStart(2, "0")}/${"x".repeat(4_030)}.md` }));
    const json = JSON.stringify(rows);
    if (rows.length > REPLACEMENT_REBUILD_MAX_PAGE_ENTRIES ||
        Buffer.byteLength(json) <= REPLACEMENT_REBUILD_MAX_PAGE_BYTES) {
        throw new Error("replacement rebuild byte-limit fixture is ineffective");
    }
    return json;
}

function replacementVisibleState(module, tree, phase) {
    return captureMutationState(module, tree, phase);
}

function assertReplacementVisibleState(module, tree, before, phase) {
    assertMutationState(module, tree, before, phase);
}

function committedReplacementState(module, tree, phase) {
    if (tree.has_candidate()) throw new Error(`replacement retained candidate during ${phase}`);
    const root = tree.root_bytes();
    if (!(root instanceof Uint8Array)) throw new Error(`replacement lacks committed root during ${phase}`);
    return {
        version: tree.tree_version(),
        hash: tree.root_hash_hex(),
        count: tree.total_files(),
        bytes: Buffer.from(root).toString("hex"),
        chunks: reachableChunkMap(module, tree, phase),
    };
}

function replacementFixture(module, targetVersion) {
    const tree = new module.WasmTree("replacement-parity-vault", "replacement-parity-device");
    const sourceVersion = targetVersion === 1 ? 2 : 1;
    tree.set_tree_version(sourceVersion);
    const seed = deterministicTreeEntries(12).map((entry, index) => ({ ...entry,
        path: `old/${String(index).padStart(3, "0")}.md` }));
    tree.build_from_entries(JSON.stringify(seed));
    tree.begin_candidate();
    tree.candidate_update_batch(JSON.stringify([{ ...seed[3], hash: "c6".repeat(32),
        mtime_ms: seed[3].mtime_ms + 1 }]));
    if (!tree.has_candidate() || tree.candidate_root_hash_hex() === tree.root_hash_hex()) {
        tree.free();
        throw new Error("replacement rebuild fixture lacks a distinct active candidate");
    }
    return tree;
}

function appendReplacementPages(module, tree, token, rows, pageEntries, before, phase) {
    let offset = 0, pages = 0;
    while (offset < rows.length) {
        const page = rows.slice(offset, offset + pageEntries), json = JSON.stringify(page);
        const bytes = Buffer.byteLength(json);
        if (page.length < 1 || page.length > REPLACEMENT_REBUILD_MAX_PAGE_ENTRIES ||
            bytes > REPLACEMENT_REBUILD_MAX_PAGE_BYTES) {
            throw new Error(`replacement rebuild test emitted an oversized page during ${phase}`);
        }
        const cumulative = tree.append_replacement_rebuild_job(token, offset, json);
        offset += page.length; pages++;
        if (cumulative !== offset || !Number.isSafeInteger(cumulative)) {
            throw new Error(`replacement rebuild cumulative count differed during ${phase}`);
        }
        assertReplacementVisibleState(module, tree, before, `${phase} page ${pages}`);
    }
    return pages;
}

function rejectReplacementOperation(module, tree, before, operation, message, phase) {
    expectThrow(operation, message);
    assertReplacementVisibleState(module, tree, before, phase);
}

function assertReplacementRebuildFailure(module, label, corpus) {
    const rows = corpus.concat([{ ...corpus[17], hash: "e8".repeat(32),
        mtime_ms: corpus[17].mtime_ms + 20_000, size: corpus[17].size + 2 }]);
    const jsonBytes = Buffer.byteLength(JSON.stringify(rows));
    const tree = replacementFixture(module, 2);
    try {
        const before = replacementVisibleState(module, tree, `${label} v2 duplicate before`);
        const token = tree.begin_replacement_rebuild_job(2, rows.length, jsonBytes);
        appendReplacementPages(module, tree, token, rows, 256, before, `${label} v2 duplicate feed`);
        rejectReplacementOperation(module, tree, before,
            () => tree.finish_replacement_rebuild_job(token),
            `${label} v2 duplicate replacement build succeeded`, `${label} v2 duplicate terminal failure`);
        // A correct ready finish owns the terminal private-build failure: the
        // failed token cannot be cancelled or retried, and the job slot is free.
        for (const action of [
            () => tree.append_replacement_rebuild_job(token, rows.length, "[]"),
            () => tree.finish_replacement_rebuild_job(token),
            () => tree.cancel_tree_job(token),
        ]) expectThrow(action, `${label} v2 failed replacement token remained live`);
        assertReplacementVisibleState(module, tree, before, `${label} v2 duplicate stale token`);
        const retry = tree.begin_replacement_rebuild_job(2, 0, 2);
        tree.cancel_tree_job(retry);
        assertReplacementVisibleState(module, tree, before, `${label} v2 duplicate retry slot`);
        return { variant: label, version: 2, terminal_duplicate_failure: true };
    } finally { tree.free(); }
}

function assertPackagedReplacementRebuildJobs() {
    const now = Date.now, corpus = replacementRebuildCorpus(), reports = [], references = new Map();
    const outputPlanReferences = new Map();
    const oversizedPage = oversizedReplacementPage(corpus[0]);
    Date.now = () => 1_930_000_000_000;
    try {
        for (const version of [1, 2]) {
            const rows = replacementRowsForVersion(corpus, version);
            const fullJson = JSON.stringify(rows), jsonBytes = Buffer.byteLength(fullJson);
            if (rows.length <= 1_024 || rows.length > REPLACEMENT_REBUILD_MAX_ENTRIES ||
                jsonBytes > REPLACEMENT_REBUILD_MAX_JSON_BYTES) {
                throw new Error(`replacement rebuild corpus violates total limits at v${version}`);
            }
            for (const [label, module] of [["scalar", scalar], ["SIMD", simd]]) {
                for (const [pageEntries, buildBudget] of [[1, null], [256, null], [1, 1], [256, 256]]) {
                    const tree = replacementFixture(module, version);
                    let oracle;
                    const phase = `${label} v${version}/page${pageEntries}/build${buildBudget ?? "legacy"}`;
                    try {
                        for (const method of ["begin_replacement_rebuild_job", "append_replacement_rebuild_job",
                            "finish_replacement_rebuild_job", "start_replacement_rebuild_job", "step_replacement_rebuild_job",
                            "replacement_rebuild_sort_memory_plan_v1_job", "start_replacement_rebuild_sort_memory_v1_job",
                            "replacement_rebuild_plan_job", "resume_replacement_rebuild_job",
                            "step_replacement_rebuild_output_memory_v1_job",
                            "replacement_rebuild_output_memory_plan_v1_job", "resume_replacement_rebuild_output_memory_v1_job",
                            "chunk_memory_snapshot", "replacement_v2_ranges_memory_snapshot",
                            "replacement_input_memory_snapshot",
                            "cancel_tree_job", "committed_revision", "candidate_revision"]) {
                            if (typeof tree[method] !== "function") {
                                throw new Error(`packaged ${label} WASM lacks ${method}; rebuild both variants before parity`);
                            }
                        }
                        oracle = new module.WasmTree("replacement-parity-vault", "replacement-parity-device");
                        oracle.set_tree_version(version);
                        oracle.build_from_entries(fullJson);
                        const expected = committedReplacementState(module, oracle, `${phase} legacy oracle`);
                        const before = replacementVisibleState(module, tree, `${phase} before`);

                        const cancelled = tree.begin_replacement_rebuild_job(version, rows.length, jsonBytes);
                        rejectReplacementOutputPlanPhase(tree, cancelled, `${phase}/input-phase`);
                        rejectReplacementSortPlanPhase(module, tree, cancelled, before, `${phase}/incomplete-sort-plan`);
                        assertReplacementVisibleState(module, tree, before, `${phase} cancelled begin`);
                        const first = rows.slice(0, pageEntries), firstJson = JSON.stringify(first);
                        const accepted = tree.append_replacement_rebuild_job(cancelled, 0, firstJson);
                        if (accepted !== first.length) throw new Error(`replacement first-page count differed during ${phase}`);
                        assertReplacementVisibleState(module, tree, before, `${phase} cancelled first page`);
                        const wrong = cancelled === 0xffff_ffff ? cancelled - 1 : cancelled + 1;
                        const nextJson = JSON.stringify(rows.slice(accepted, accepted + Math.min(pageEntries, rows.length - accepted)));
                        for (const [name, action] of [
                            ["wrong token append", () => tree.append_replacement_rebuild_job(wrong, accepted, nextJson)],
                            ["wrong offset append", () => tree.append_replacement_rebuild_job(cancelled, accepted + 1, nextJson)],
                            ["invalid JSON append", () => tree.append_replacement_rebuild_job(cancelled, accepted, "{")],
                            ["entry-oversized page", () => tree.append_replacement_rebuild_job(cancelled, accepted,
                                JSON.stringify(rows.slice(0, REPLACEMENT_REBUILD_MAX_PAGE_ENTRIES + 1)))],
                            ["byte-oversized page", () => tree.append_replacement_rebuild_job(cancelled, accepted, oversizedPage)],
                            ["wrong token finish", () => tree.finish_replacement_rebuild_job(wrong)],
                            ["incomplete finish", () => tree.finish_replacement_rebuild_job(cancelled)],
                            ["wrong-kind finish", () => tree.finish_candidate_job(cancelled)],
                        ]) rejectReplacementOperation(module, tree, before, action,
                            `${name} succeeded during ${phase}`, `${phase} rejected ${name}`);
                        tree.cancel_tree_job(cancelled);
                        rejectReplacementOutputPlanPhase(tree, cancelled, `${phase}/cancelled-token`);
                        assertReplacementVisibleState(module, tree, before, `${phase} cancel`);

                        const token = tree.begin_replacement_rebuild_job(version, rows.length, jsonBytes);
                        assertReplacementVisibleState(module, tree, before, `${phase} replacement begin`);
                        for (const [name, action] of [
                            ["stale append", () => tree.append_replacement_rebuild_job(cancelled, 0, firstJson)],
                            ["stale finish", () => tree.finish_replacement_rebuild_job(cancelled)],
                            ["stale cancel", () => tree.cancel_tree_job(cancelled)],
                        ]) rejectReplacementOperation(module, tree, before, action,
                            `${name} consumed replacement during ${phase}`, `${phase} ${name}`);
                        const pages = appendReplacementPages(module, tree, token, rows, pageEntries, before, phase);
                        rejectReplacementOperation(module, tree, before,
                            () => tree.finish_candidate_job(token),
                            `replacement accepted candidate finish during ${phase}`, `${phase} ready wrong-kind finish`);

                        let build;
                        if (buildBudget !== null) {
                            const sortPlan = startReplacementRebuildWithSortPlan(
                                module, tree, token, version, rows.length, before, phase);
                            for (const action of [
                                () => tree.start_replacement_rebuild_job(token),
                                () => tree.replacement_rebuild_sort_memory_plan_v1_job(token),
                                () => tree.start_replacement_rebuild_sort_memory_v1_job(
                                    token, sortPlan?.sourceIndexRequestedBytes ?? 0,
                                    sortPlan?.targetIndexRequestedBytes ?? 0),
                                () => tree.append_replacement_rebuild_job(token, rows.length, "[]"),
                                () => tree.finish_replacement_rebuild_job(token),
                                () => tree.step_tree_job(token, 1),
                                () => tree.step_replacement_rebuild_job(token + 1, 1),
                                () => tree.step_replacement_rebuild_job(token, 0),
                                () => tree.step_replacement_rebuild_job(token, 257),
                                () => tree.abort_candidate(),
                            ]) rejectReplacementOperation(module, tree, before, action,
                                `started rebuild accepted misuse during ${phase}`, phase);
                            build = advanceReplacementBuild(module, tree, token, buildBudget, before, phase, version, expected);
                            if (outputPlanReferences.has(version)) assert.deepEqual(build.output_memory_plan,
                                outputPlanReferences.get(version), `${phase}: scalar/SIMD/budget output plan differs`);
                            else outputPlanReferences.set(version, build.output_memory_plan);
                        }
                        tree.finish_replacement_rebuild_job(token);
                        rejectReplacementOutputPlanPhase(tree, token, `${phase}/finished-token`,
                            build?.output_memory_plan);
                        if (tree.has_candidate() || tree.committed_revision() !== before.state.revision + 1 ||
                            tree.candidate_revision() !== before.state.candidateRevision + 1) {
                            throw new Error(`replacement did not atomically advance revisions/clear candidate during ${phase}`);
                        }
                        const actual = committedReplacementState(module, tree, `${phase} result`);
                        if (JSON.stringify(actual) !== JSON.stringify(expected)) {
                            throw new Error(`replacement rebuild differs from exact legacy oracle during ${phase}`);
                        }
                        for (const action of [
                            () => tree.append_replacement_rebuild_job(token, rows.length, "[]"),
                            () => tree.finish_replacement_rebuild_job(token),
                            () => tree.cancel_tree_job(token),
                        ]) expectThrow(action, `successful replacement token remained live during ${phase}`);
                        const comparable = JSON.stringify(actual);
                        if (references.has(version) && references.get(version) !== comparable) {
                            throw new Error(`scalar/SIMD/page replacement parity differs at v${version}`);
                        }
                        references.set(version, comparable);
                        reports.push({ variant: label, version, page_entries: pageEntries, pages, build_budget: buildBudget, build,
                            entries: rows.length, json_bytes: jsonBytes, exact_state_hash: module.wasm_hash(Buffer.from(comparable)) });
                        if (process.env.OBSETYNC_PARITY_PROGRESS === "1") {
                            console.error(`replacement parity: ${phase} passed`);
                        }
                    } finally { oracle?.free(); tree.free(); }
                }
            }
        }
        for (const [label, module] of [["scalar", scalar], ["SIMD", simd]]) {
            reports.push(assertReplacementRebuildFailure(module, label, corpus));
            reports.push(assertSteppedReplacementLifecycle(module, label, corpus));
            reports.push(assertDeferredReplacementRetirement(module, label, corpus));
        }
        return reports;
    } finally { Date.now = now; }
}

function advanceReplacementBuild(module, tree, token, budget, before, phase, version, expected) {
    let completed = 0, steps = 0, done = false, nodePlan = null, outputPlan = null;
    const expectedNodePlan = expected === undefined
        ? undefined : canonicalReplacementNodePayloadPlan(expected, phase);
    const expectedOutputPlan = version === 2 && expected !== undefined
        ? canonicalReplacementOutputMemoryPlan(expected.chunks, expected.bytes, phase) : undefined;
    const phases = new Set();
    if (version === 2) rejectReplacementOutputPlanPhase(tree, token, `${phase}/before-plan`);
    assertReplacementOutputOwnersEmpty(tree, `${phase}/before-plan`);
    while (!done) {
        const result = tree.step_replacement_rebuild_output_memory_v1_job(token, budget);
        if (!result || typeof result !== "object" || Object.keys(result).sort().join(",") !== "completed,done,phase,units" ||
            typeof result.done !== "boolean" || !Number.isSafeInteger(result.units) || result.units < 0 || result.units > budget ||
            (!result.done && result.units === 0 && result.phase !== "plan ready") || !Number.isSafeInteger(result.completed) ||
            result.completed !== completed + result.units || typeof result.phase !== "string" ||
            !result.phase.length || result.phase.length > 64) {
            throw new Error(`invalid replacement build progress during ${phase}`);
        }
        completed = result.completed; done = result.done; steps++;
        if (!phases.has(result.phase) || done || steps % 128 === 0) {
            assertReplacementVisibleState(module, tree, before, `${phase} step ${steps}`);
        }
        phases.add(result.phase);
        if (result.phase === "plan ready") {
            if (outputPlan !== null) {
                throw new Error(`replacement exposed an unexpected or repeated node plan during ${phase}`);
            }
            if (version === 2)
                nodePlan = replacementNodePayloadPlan(tree.replacement_rebuild_plan_job(token), phase);
            else expectThrow(() => tree.replacement_rebuild_plan_job(token),
                `V1 replacement fabricated a node-payload plan during ${phase}`);
            outputPlan = qualifyReplacementOutputPlan(tree, token, nodePlan, expectedOutputPlan,
                budget, completed, `${phase}/output-plan`, version);
            if (version === 1 && (outputPlan.nodePayloadBytes === 0 ||
                outputPlan.rangeEndpointResidentRequestedBytes === 0)) {
                throw new Error(`V1 replacement output plan omitted native output during ${phase}`);
            }
            if (expectedNodePlan !== undefined && expectedNodePlan !== null &&
                ["nodeCount", "leafCount", "internalCount", "nodePayloadBytes", "maxNodeBytes", "storedRootBytes"]
                .some(field => nodePlan[field] !== expectedNodePlan[field])) {
                throw new Error(`replacement node plan differs from canonical chunks/root during ${phase}: ` +
                    `planned=${JSON.stringify(nodePlan)} canonical=${JSON.stringify(expectedNodePlan)}`);
            }
            if (version === 2) {
                const wrong = token === 0xffff_ffff ? token - 1 : token + 1;
                expectThrow(() => tree.replacement_rebuild_plan_job(wrong), `wrong token exposed replacement plan during ${phase}`);
                for (const witness of [NaN, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, nodePlan.nodePayloadBytes + 1]) {
                    expectThrow(() => tree.resume_replacement_rebuild_job(token, witness),
                        `invalid payload witness ${witness} resumed replacement during ${phase}`);
                }
            }
            assertReplacementVisibleState(module, tree, before, `${phase} rejected plan witnesses`);
            tree.resume_replacement_rebuild_output_memory_v1_job(token, ...outputMemoryWitnesses(outputPlan));
            rejectReplacementOutputPlanPhase(tree, token, `${phase}/resumed`, outputPlan);
            assertReplacementVisibleState(module, tree, before, `${phase} resumed plan`);
        }
        if (steps > 2_000_000) throw new Error(`replacement build made excessive work during ${phase}`);
    }
    const repeated = tree.step_replacement_rebuild_output_memory_v1_job(token, budget);
    if (!repeated.done || repeated.units !== 0 || repeated.completed !== completed) {
        throw new Error(`ready replacement build advanced during ${phase}`);
    }
    if ((version === 2) !== (nodePlan !== null) || outputPlan === null) {
        throw new Error(`replacement node plan barrier count differs from v${version} contract during ${phase}`);
    }
    rejectReplacementOutputPlanPhase(tree, token, `${phase}/ready`, outputPlan);
    assertReplacementVisibleState(module, tree, before, `${phase} ready`);
    return { steps, completed, phases: [...phases], node_plan: nodePlan, output_memory_plan: outputPlan };
}

function canonicalReplacementNodePayloadPlan(expected, phase) {
    if (expected.version !== 2) return null;
    let leafCount = 0, internalCount = 0, nodePayloadBytes = 0, maxNodeBytes = 0;
    for (const [, hex] of expected.chunks) {
        const bytes = Buffer.from(hex, "hex");
        if (bytes.subarray(0, 4).equals(Buffer.from("OVL2"))) leafCount++;
        else if (bytes.subarray(0, 4).equals(Buffer.from("OVI2"))) internalCount++;
        else throw new Error(`canonical replacement contains an unknown V2 node during ${phase}`);
        nodePayloadBytes += bytes.length;
        maxNodeBytes = Math.max(maxNodeBytes, bytes.length);
    }
    const plan = { nodeCount: expected.chunks.length, leafCount, internalCount,
        nodePayloadBytes, maxNodeBytes, storedRootBytes: expected.bytes.length / 2 };
    replacementNodePayloadPlan(plan, `${phase} canonical`);
    return plan;
}

function replacementNodePayloadPlan(value, phase) {
    const fields = ["nodeCount", "leafCount", "internalCount", "nodePayloadBytes", "maxNodeBytes", "storedRootBytes"];
    if (!value || typeof value !== "object" || Object.keys(value).sort().join(",") !== fields.slice().sort().join(",")) {
        throw new Error(`invalid replacement node plan shape during ${phase}`);
    }
    for (const field of fields) {
        if (!Number.isSafeInteger(value[field]) || value[field] < 0) {
            throw new Error(`invalid replacement node plan ${field} during ${phase}`);
        }
    }
    if (value.nodeCount !== value.leafCount + value.internalCount || value.storedRootBytes < 64 ||
        value.storedRootBytes > 16 * 1024 || value.maxNodeBytes > 256 * 1024 ||
        (value.nodeCount === 0
            ? value.nodePayloadBytes !== 0 || value.maxNodeBytes !== 0
            : value.leafCount < 1 || value.maxNodeBytes < 8 || value.nodePayloadBytes < value.maxNodeBytes)) {
        throw new Error(`inconsistent replacement node plan during ${phase}`);
    }
    return value;
}

function outputPlanCount(value, label) {
    assert(Number.isSafeInteger(value) && value >= 0, `${label}: unsafe output-plan integer`);
    return value;
}

function outputPlanSum(left, right, label) {
    return outputPlanCount(left + right, label);
}

function replacementOutputMemoryPlan(value, label, expectedScope = "v2-replacement-output") {
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
function canonicalReplacementOutputMemoryPlan(chunks, rootHex, label) {
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

function outputMemoryWitnesses(plan) {
    return [plan.nodePayloadBytes, plan.rangeEndpointPeakRequestedBytes, plan.rangeEndpointResidentRequestedBytes];
}

function assertReplacementOutputOwnersEmpty(tree, label) {
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

function rejectReplacementOutputPlanPhase(tree, token, label, plan) {
    const witnesses = plan ? outputMemoryWitnesses(plan) : [0, 0, 0];
    assert.throws(() => tree.replacement_rebuild_output_memory_plan_v1_job(token),
        `${label}: output plan accepted wrong phase/kind/token`);
    assert.throws(() => tree.resume_replacement_rebuild_output_memory_v1_job(token, ...witnesses),
        `${label}: output resume accepted wrong phase/kind/token`);
}

function qualifyReplacementOutputPlan(tree, token, oldPlan, expected, budget, completed, label, version = 2) {
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

function assertSteppedReplacementLifecycle(module, label, corpus) {
    for (const version of [1, 2]) {
        const rows = replacementRowsForVersion(corpus, version), tree = replacementFixture(module, version);
        try {
            const before = replacementVisibleState(module, tree, `${label} lifecycle`);
            // Cancel both early construction and a fully validated private
            // graph: neither is a visible committed/candidate replacement.
            for (const ready of [false, true]) {
                const token = tree.begin_replacement_rebuild_job(version, rows.length, Buffer.byteLength(JSON.stringify(rows)));
                appendReplacementPages(module, tree, token, rows, 256, before, label);
                startReplacementRebuildWithSortPlan(module, tree, token, version, rows.length, before,
                    `${label}/stepped-${ready ? "ready" : "partial"}`);
                if (ready) advanceReplacementBuild(module, tree, token, 256, before, label, version);
                else tree.step_replacement_rebuild_job(token, 1);
                tree.cancel_tree_job(token);
                assertReplacementVisibleState(module, tree, before, `${label} cancel ready=${ready}`);
                expectThrow(() => tree.step_replacement_rebuild_job(token, 1), "cancelled build token remained live");
            }
            if (version === 2) {
                const duplicates = rows.concat([{ ...rows[0], hash: "ab".repeat(32) }]);
                const token = tree.begin_replacement_rebuild_job(version, duplicates.length, Buffer.byteLength(JSON.stringify(duplicates)));
                appendReplacementPages(module, tree, token, duplicates, 256, before, label);
                startReplacementRebuildWithSortPlan(module, tree, token, version, duplicates.length, before,
                    `${label}/duplicate`);
                expectThrow(() => advanceReplacementBuild(module, tree, token, 256, before, label, version),
                    "stepped duplicate rebuild succeeded");
                expectThrow(() => tree.finish_replacement_rebuild_job(token), "failed stepped build became ready");
                expectThrow(() => tree.step_replacement_rebuild_job(token, 1), "failed stepped build resumed");
                // Step failure retains the owner, unlike legacy ready finish's
                // terminal failure: the host explicitly cancels it.
                tree.cancel_tree_job(token);
                assertReplacementVisibleState(module, tree, before, `${label} failed build cancel`);
            }
            const empty = tree.begin_replacement_rebuild_job(version, 0, 2);
            startReplacementRebuildWithSortPlan(module, tree, empty, version, 0, before,
                `${label}/empty`);
            advanceReplacementBuild(module, tree, empty, 1, before, `${label} empty`, version);
            tree.finish_replacement_rebuild_job(empty);
            if (tree.total_files() !== 0 || tree.has_candidate() || tree.tree_version() !== version) {
                throw new Error(`empty stepped replacement differs during ${label}`);
            }
            if (version === 2) {
                // The empty replacement above intentionally cleared its old
                // candidate. Use a fresh candidate-bearing fixture so the
                // shared visibility oracle can still prove non-adoption.
                const singleTree = replacementFixture(module, 2);
                try {
                    const singleRows = [rows[0]], singleJson = JSON.stringify(singleRows);
                    const singleBefore = replacementVisibleState(module, singleTree, `${label} single before`);
                    const single = singleTree.begin_replacement_rebuild_job(2, 1, Buffer.byteLength(singleJson));
                    appendReplacementPages(module, singleTree, single, singleRows, 1, singleBefore, `${label} single`);
                    startReplacementRebuildWithSortPlan(module, singleTree, single, 2, 1, singleBefore,
                        `${label}/single`);
                    advanceReplacementBuild(module, singleTree, single, 1, singleBefore, `${label} single`, 2);
                    singleTree.finish_replacement_rebuild_job(single);
                    if (singleTree.total_files() !== 1 || singleTree.has_candidate() || singleTree.tree_version() !== 2) {
                        throw new Error(`single-row admitted replacement differs during ${label}`);
                    }
                } finally { singleTree.free(); }
            }
        } finally { tree.free(); }
    }
    return { variant: label, stepped_lifecycle: true };
}

function assertDeferredReplacementRetirement(module, label, corpus) {
    const reports = [];
    for (const version of [1, 2]) for (const budget of [1, 256]) {
        const rows = replacementRowsForVersion(corpus, version), tree = replacementFixture(module, version);
        let oracle;
        const phase = `${label} v${version} retirement budget=${budget}`;
        const drain = token => {
            const candidateActive = tree.has_candidate();
            const capture = () => candidateActive ? replacementVisibleState(module, tree, phase) : {
                committed: committedReplacementState(module, tree, phase),
                committed_revision: tree.committed_revision(), candidate_revision: tree.candidate_revision(),
            };
            const before = JSON.stringify(capture());
            let completed = 0, turns = 0;
            for (;;) {
                for (const action of [
                    () => tree.step_tree_retirement(token + 1, budget),
                    () => tree.step_tree_retirement(token, 0),
                    () => tree.step_tree_retirement(token, 257),
                    () => tree.begin_replacement_rebuild_job(version, 0, 2),
                    () => tree.begin_candidate(),
                    () => tree.cancel_tree_job(token),
                    () => tree.step_tree_job(token, 1),
                    () => tree.finish_replacement_rebuild_job(token),
                    () => tree.finish_replacement_rebuild_job_deferred(token),
                    () => tree.cancel_replacement_rebuild_job_deferred(token),
                ]) expectThrow(action, `retirement ownership lost during ${phase}`);
                const result = tree.step_tree_retirement(token, budget);
                if (!result || Object.keys(result).sort().join(",") !== "completed,done,units" ||
                    typeof result.done !== "boolean" || !Number.isSafeInteger(result.units) ||
                    result.units < 0 || result.units > budget || (!result.done && result.units === 0) ||
                    !Number.isSafeInteger(result.completed) || result.completed !== completed + result.units) {
                    throw new Error(`invalid retirement progress during ${phase}`);
                }
                completed = result.completed; turns++;
                if ((turns % 256 === 0 || result.done) && JSON.stringify(capture()) !== before) {
                    throw new Error(`retirement changed the published/candidate graph during ${phase}`);
                }
                if (turns > 2_000_000) throw new Error(`retirement exceeded work bound during ${phase}`);
                if (result.done) break;
            }
            expectThrow(() => tree.step_tree_retirement(token, 1), `done retirement token remains live during ${phase}`);
            return { turns, completed };
        };
        const begin = (entries = rows) => {
            const before = replacementVisibleState(module, tree, phase);
            const token = tree.begin_replacement_rebuild_job(version, entries.length, Buffer.byteLength(JSON.stringify(entries)));
            appendReplacementPages(module, tree, token, entries, 256, before, phase);
            return { token, before };
        };
        try {
            for (const method of ["cancel_replacement_rebuild_job_deferred", "finish_replacement_rebuild_job_deferred", "step_tree_retirement"]) {
                if (typeof tree[method] !== "function") throw new Error(`missing ${method} during ${phase}`);
            }
            const cancelled = [];
            for (const stage of ["input", "partial", "ready", ...(version === 2 ? ["failed"] : [])]) {
                const entries = stage === "failed" ? rows.concat([rows[0]]) : rows;
                const { token, before } = begin(entries);
                if (stage !== "input") {
                    startReplacementRebuildWithSortPlan(module, tree, token, version, entries.length, before,
                        `${phase}/${stage}`);
                    if (stage === "ready") advanceReplacementBuild(module, tree, token, 256, before, phase, version);
                    else if (stage === "failed") expectThrow(
                        () => advanceReplacementBuild(module, tree, token, 256, before, phase, version),
                        "duplicate build must fail");
                    else tree.step_replacement_rebuild_job(token, 256);
                }
                tree.cancel_replacement_rebuild_job_deferred(token);
                assertReplacementVisibleState(module, tree, before, phase);
                cancelled.push({ stage, ...drain(token) });
            }
            oracle = new module.WasmTree("replacement-parity-vault", "replacement-parity-device");
            oracle.rebuild_from_entries_in_version(version, JSON.stringify(rows));
            const expected = committedReplacementState(module, oracle, phase);
            const { token, before } = begin();
            expectThrow(() => tree.finish_replacement_rebuild_job_deferred(token), "deferred finish must not perform atomic build");
            startReplacementRebuildWithSortPlan(module, tree, token, version, rows.length, before,
                `${phase}/publish`);
            advanceReplacementBuild(module, tree, token, 256, before, phase, version, expected);
            const committed = tree.committed_revision(), candidate = tree.candidate_revision();
            tree.finish_replacement_rebuild_job_deferred(token);
            if (tree.committed_revision() !== committed + 1 || tree.candidate_revision() !== candidate + 1) {
                throw new Error(`deferred publication revisions differ during ${phase}`);
            }
            if (JSON.stringify(committedReplacementState(module, tree, phase)) !== JSON.stringify(expected)) {
                throw new Error(`deferred publication differs from legacy canonical graph during ${phase}`);
            }
            const published = drain(token);
            const next = tree.begin_replacement_rebuild_job(version, 0, 2);
            expectThrow(() => tree.step_tree_retirement(token, 1), "stale retirement must not consume new input");
            tree.cancel_replacement_rebuild_job_deferred(next);
            drain(next);
            reports.push({ version, budget, cancelled, published });
        } finally { oracle?.free(); tree.free(); }
    }
    return { variant: label, deferred_retirement: reports };
}

function assertPackagedCommittedRevisions() {
    const rows = deterministicTreeEntries(3), reports = [];
    for (const [variant, module] of [["scalar", scalar], ["SIMD", simd]]) for (const version of [1, 2]) {
        const tree = new module.WasmTree("revision-parity-vault", "revision-parity-device");
        try {
            if (typeof tree.committed_revision !== "function" || tree.committed_revision() !== 0 ||
                typeof tree.candidate_revision !== "function" || tree.candidate_revision() !== 0) {
                throw new Error(`packaged ${variant} WASM lacks initial tree revision witnesses`);
            }
            let revision = 0, candidateRevision = 0;
            const advance = (action, phase, candidateDelta = 0) => {
                action(); revision++; candidateRevision += candidateDelta;
                if (tree.committed_revision() !== revision) throw new Error(`committed revision did not advance after ${variant} v${version} ${phase}`);
                if (tree.candidate_revision() !== candidateRevision) throw new Error(`candidate revision differed after ${variant} v${version} ${phase}`);
            };
            const stable = (action, phase, candidateDelta = 0) => {
                action(); candidateRevision += candidateDelta;
                if (tree.committed_revision() !== revision) throw new Error(`committed revision changed after ${variant} v${version} ${phase}`);
                if (tree.candidate_revision() !== candidateRevision) throw new Error(`candidate revision differed after ${variant} v${version} ${phase}`);
            };
            advance(() => tree.set_tree_version(version), "format selection");
            expectThrow(() => tree.set_tree_version(99), `invalid ${variant} tree version accepted`);
            if (tree.committed_revision() !== revision) throw new Error(`invalid ${variant} version changed committed revision`);
            advance(() => tree.build_from_entries(JSON.stringify(rows.slice(0, 2))), "build");
            const rootA = tree.root_bytes(), hashA = tree.root_hash_hex();
            for (const [phase, action] of [
                ["invalid root load", () => tree.load_root(new Uint8Array([1, 2, 3]))],
                ["invalid update JSON", () => tree.update_batch("{")],
                ["missing candidate commit", () => tree.commit_candidate()],
                ["populated format switch", () => tree.set_tree_version(version === 1 ? 2 : 1)],
            ]) {
                expectThrow(action, `${variant} v${version} ${phase} accepted`);
                if (tree.committed_revision() !== revision || tree.candidate_revision() !== candidateRevision ||
                    tree.root_hash_hex() !== hashA || !bytesEqual(tree.root_bytes(), rootA)) {
                    throw new Error(`${variant} v${version} ${phase} changed committed state or revision`);
                }
            }
            stable(() => tree.begin_candidate(), "candidate abort begin", 1);
            stable(() => tree.candidate_update_batch(JSON.stringify([
                { ...rows[0], hash: "a1".repeat(32), mtime_ms: 99 },
            ])), "candidate abort update", 1);
            stable(() => tree.abort_candidate(), "candidate abort", 1);
            stable(() => tree.begin_candidate(), "candidate commit begin", 1);
            stable(() => tree.candidate_update_batch(JSON.stringify([
                { ...rows[0], hash: "b2".repeat(32), mtime_ms: 100 },
            ])), "candidate commit update", 1);
            advance(() => tree.commit_candidate(), "candidate commit", 1);
            advance(() => tree.update_entry(rows[0].path, "c3".repeat(32), 101, rows[0].size), "single update");
            advance(() => tree.update_batch(JSON.stringify([rows[2]])), "batch update");
            advance(() => tree.delete_entry(rows[2].path), "single delete");
            advance(() => tree.delete_batch(JSON.stringify(["absent.md"])), "batch delete");
            stable(() => { tree.update_batch("[]"); tree.delete_batch("[]"); }, "empty batches");
            for (const [phase, replace] of [
                ["active-candidate build", () => tree.build_from_entries(JSON.stringify(rows.slice(0, 2)))],
                ["active-candidate rebuild", () => tree.rebuild_from_entries_in_version(version, JSON.stringify(rows.slice(0, 2)))],
                ["active-candidate root load", () => tree.load_root(rootA)],
            ]) {
                stable(() => tree.begin_candidate(), `${phase} begin`, 1);
                if (!tree.has_candidate()) throw new Error(`${variant} v${version} ${phase} did not open a candidate`);
                advance(replace, phase, 1);
                if (tree.has_candidate()) throw new Error(`${variant} v${version} ${phase} retained the replaced candidate`);
            }
            // The active-candidate load intentionally produces a root-only
            // graph. Rebuild before any traversal, then finish with reload-only
            // ABA checks that do not claim resident child availability.
            advance(() => tree.rebuild_from_entries_in_version(version, JSON.stringify(rows.slice(0, 2))), "replacement rebuild");
            advance(() => tree.load_root(rootA), "ABA load");
            if (tree.root_hash_hex() !== hashA) throw new Error(`ABA load did not restore ${variant} v${version} semantic root`);
            advance(() => tree.load_root(rootA), "same-root reload");
            reports.push({ variant, version, revision, candidateRevision });
        } finally { tree.free(); }
    }
    const scalarReports = reports.filter(row => row.variant === "scalar").map(({ variant: _, ...row }) => row);
    const simdReports = reports.filter(row => row.variant === "SIMD").map(({ variant: _, ...row }) => row);
    if (JSON.stringify(scalarReports) !== JSON.stringify(simdReports)) throw new Error("scalar/SIMD committed revision parity mismatch");
    return reports;
}

const treeEntries = deterministicTreeEntries(2_048);
const chunkExportResults = assertPackagedChunkExports();
const rootExportResults = assertPackagedRootExports();
const committedRevisionResults = assertPackagedCommittedRevisions();
const candidateMutationResults = assertPackagedMutationJobs(treeEntries);
const candidateMutationOutputResults = assertPackagedMutationOutputJobs(treeEntries);
const replacementRebuildResults = assertPackagedReplacementRebuildJobs();
const deferredReachabilityResults = assertPackagedDeferredReachability();
const candidateOpenMemoryResults = assertPackagedCandidateOpenMemory();
const deferredInternalExpansionResults = assertPackagedDeferredInternalExpansion();
const scalarTree = treeV2State(scalar, treeEntries);
const simdTree = treeV2State(simd, treeEntries);
let treeV1ValidationSteps = 0;
let treeV2ValidationSteps = 0;
let treeV1CandidateChunkSteps = [];
let treeV2CandidateChunkSteps = [];
let treeV1CandidateChunkPages = [];
let treeV2CandidateChunkPages = [];
try {
    assertEmptyTreeJobs(scalar, "scalar");
    assertEmptyTreeJobs(simd, "SIMD");
    assertTreeParity(scalarTree, simdTree, "rebuild");
    assertTreeJobGuards(scalar, scalarTree, "scalar v2 guard/cancel");
    assertTreeJobGuards(simd, simdTree, "SIMD v2 guard/cancel");
    assertTreeJobStepFailureRecovery(scalar, scalarTree.root_bytes(), "scalar v2 missing graph");
    assertTreeJobStepFailureRecovery(simd, simdTree.root_bytes(), "SIMD v2 missing graph");
    const scalarV2Wide = beginCandidateWithJob(scalar, scalarTree, "scalar v2 budget 256", 256);
    const simdV2Wide = beginCandidateWithJob(simd, simdTree, "SIMD v2 budget 256", 256);
    if (JSON.stringify(scalarV2Wide.progress) !== JSON.stringify(simdV2Wide.progress)) {
        throw new Error("scalar/SIMD Tree v2 budget-256 progress mismatch");
    }
    scalarTree.abort_candidate();
    simdTree.abort_candidate();

    const replacement = [{
        ...treeEntries[1_024],
        hash: scalar.wasm_hash(deterministicPayload(8_193)),
        mtime_ms: 1_800_000_000_000,
        size: 8_193,
    }];
    const scalarV2Begin = beginCandidateWithJob(scalar, scalarTree, "scalar v2 candidate update");
    const simdV2Begin = beginCandidateWithJob(simd, simdTree, "SIMD v2 candidate update");
    if (JSON.stringify(scalarV2Begin.progress) !== JSON.stringify(simdV2Begin.progress)) {
        throw new Error("scalar/SIMD Tree v2 validation progress mismatch");
    }
    treeV2ValidationSteps = scalarV2Begin.progress.length;
    scalarTree.candidate_update_batch(JSON.stringify(replacement));
    scalarTree.candidate_delete_batch(JSON.stringify([treeEntries[7].path, treeEntries[1_777].path]));
    simdTree.candidate_update_batch(JSON.stringify(replacement));
    simdTree.candidate_delete_batch(JSON.stringify([treeEntries[7].path, treeEntries[1_777].path]));
    if (scalarTree.candidate_root_hash_hex() !== simdTree.candidate_root_hash_hex()) {
        throw new Error("scalar/SIMD Tree v2 candidate mismatch");
    }
    assertChunkLengths(scalar, scalarTree, "candidate before commit");
    assertChunkLengths(simd, simdTree, "candidate before commit");
    const scalarV2Chunks = candidateChunksWithJobs(scalar, scalarTree, "scalar v2 candidate chunks");
    const simdV2Chunks = candidateChunksWithJobs(simd, simdTree, "SIMD v2 candidate chunks");
    if (JSON.stringify(scalarV2Chunks) !== JSON.stringify(simdV2Chunks) ||
        scalarV2Chunks.some(result => result.plan.fresh.length === 0)) {
        throw new Error("scalar/SIMD changed Tree v2 candidate chunk job mismatch");
    }
    treeV2CandidateChunkSteps = scalarV2Chunks.map(({ stepUnits, steps }) => ({ stepUnits, steps }));
    const scalarV2Pages = candidateChunksWithPagedJobs(scalar, scalarTree,
        "scalar v2 candidate chunk pages");
    const simdV2Pages = candidateChunksWithPagedJobs(simd, simdTree,
        "SIMD v2 candidate chunk pages");
    if (JSON.stringify(scalarV2Pages) !== JSON.stringify(simdV2Pages) ||
        scalarV2Pages.some(result => result.plan.fresh.length === 0)) {
        throw new Error("scalar/SIMD changed Tree v2 candidate chunk paging mismatch");
    }
    treeV2CandidateChunkPages = scalarV2Pages.map(({ sortUnits, pageHashes, turns, pages }) =>
        ({ sortUnits, pageHashes, turns, pages }));
    scalarTree.commit_candidate();
    simdTree.commit_candidate();
    assertTreeParity(scalarTree, simdTree, "candidate commit");

    const committed = scalarTree.root_hash_hex();
    for (const [module, tree] of [[scalar, scalarTree], [simd, simdTree]]) {
        beginCandidateWithJob(module, tree, `${module === scalar ? "scalar" : "SIMD"} v2 candidate abort`);
        tree.candidate_delete_batch(JSON.stringify([treeEntries[42].path]));
        const discarded = module.wasm_tree_new_candidate_chunk_hashes(tree);
        assertChunkLengths(module, tree, "candidate before abort");
        tree.abort_candidate();
        for (const hash of discarded) {
            if (module.wasm_tree_chunk_byte_length(tree, hash) !== undefined) {
                throw new Error("aborted candidate chunk retained a byte length after sweep");
            }
        }
    }
    assertTreeParity(scalarTree, simdTree, "candidate abort");
    if (scalarTree.root_hash_hex() !== committed) {
        throw new Error("Tree v2 abort changed the committed root");
    }

    const transitionedEntries = treeEntries
        .filter((entry) => entry.path !== treeEntries[7].path && entry.path !== treeEntries[1_777].path)
        .map((entry) => entry.path === replacement[0].path ? replacement[0] : entry);
    for (const tree of [scalarTree, simdTree]) {
        const revision = tree.committed_revision();
        tree.rebuild_from_entries_in_version(1, JSON.stringify(transitionedEntries));
        if (tree.tree_version() !== 1 || tree.committed_revision() !== revision + 1) {
            throw new Error("live Tree v2→v1 rebuild/revision failed");
        }
    }
    assertTreeParity(scalarTree, simdTree, "live v2→v1 rebuild");
    assertTreeJobGuards(scalar, scalarTree, "scalar v1 guard/cancel");
    assertTreeJobGuards(simd, simdTree, "SIMD v1 guard/cancel");
    assertTreeJobStepFailureRecovery(scalar, scalarTree.root_bytes(), "scalar v1 missing graph");
    assertTreeJobStepFailureRecovery(simd, simdTree.root_bytes(), "SIMD v1 missing graph");
    const scalarV1Begin = beginCandidateWithJob(scalar, scalarTree, "scalar v1 candidate begin");
    const simdV1Begin = beginCandidateWithJob(simd, simdTree, "SIMD v1 candidate begin");
    if (JSON.stringify(scalarV1Begin.progress) !== JSON.stringify(simdV1Begin.progress)) {
        throw new Error("scalar/SIMD Tree v1 validation progress mismatch");
    }
    if (scalarV1Begin.reachable !== 4 ||
        scalarV1Begin.progress.at(-1)?.completed !== 8) {
        throw new Error("Tree v1 validation disagreed with the deterministic graph oracle");
    }
    treeV1ValidationSteps = scalarV1Begin.progress.length;
    const scalarV1Chunks = candidateChunksWithJobs(scalar, scalarTree, "scalar v1 candidate chunks");
    const simdV1Chunks = candidateChunksWithJobs(simd, simdTree, "SIMD v1 candidate chunks");
    if (JSON.stringify(scalarV1Chunks) !== JSON.stringify(simdV1Chunks) ||
        scalarV1Chunks.some(result => result.plan.all.length !== 4 || result.plan.fresh.length !== 0)) {
        throw new Error("scalar/SIMD unchanged Tree v1 candidate chunk job mismatch");
    }
    treeV1CandidateChunkSteps = scalarV1Chunks.map(({ stepUnits, steps }) => ({ stepUnits, steps }));
    const scalarV1Pages = candidateChunksWithPagedJobs(scalar, scalarTree,
        "scalar v1 candidate chunk pages");
    const simdV1Pages = candidateChunksWithPagedJobs(simd, simdTree,
        "SIMD v1 candidate chunk pages");
    if (JSON.stringify(scalarV1Pages) !== JSON.stringify(simdV1Pages) ||
        scalarV1Pages.some(result => result.plan.all.length !== 4 || result.plan.fresh.length !== 0)) {
        throw new Error("scalar/SIMD unchanged Tree v1 candidate chunk paging mismatch");
    }
    treeV1CandidateChunkPages = scalarV1Pages.map(({ sortUnits, pageHashes, turns, pages }) =>
        ({ sortUnits, pageHashes, turns, pages }));
    scalarTree.abort_candidate();
    simdTree.abort_candidate();
    const scalarV1Wide = beginCandidateWithJob(scalar, scalarTree, "scalar v1 budget 256", 256);
    const simdV1Wide = beginCandidateWithJob(simd, simdTree, "SIMD v1 budget 256", 256);
    if (JSON.stringify(scalarV1Wide.progress) !== JSON.stringify(simdV1Wide.progress) ||
        scalarV1Wide.reachable !== 4) {
        throw new Error("scalar/SIMD Tree v1 budget-256 validation mismatch");
    }
    scalarTree.abort_candidate();
    simdTree.abort_candidate();
    for (const tree of [scalarTree, simdTree]) {
        const revision = tree.committed_revision();
        tree.rebuild_from_entries_in_version(2, JSON.stringify(transitionedEntries));
        if (tree.tree_version() !== 2 || tree.committed_revision() !== revision + 1) {
            throw new Error("live Tree v1→v2 rebuild/revision failed");
        }
    }
    assertTreeParity(scalarTree, simdTree, "live v1→v2 rebuild");
    if (scalarTree.root_hash_hex() !== committed) {
        throw new Error("live Tree format roundtrip changed semantic v2 root");
    }
} finally {
    scalarTree.free();
    simdTree.free();
}

console.log(JSON.stringify({
    scalar_bytes: scalarBytes.length,
    simd_bytes: simdBytes.length,
    payload_bytes: large.length,
    feed_sizes_tested: feedSizes.length,
    chunk_feed_sizes_tested: chunkFeedSizes.length,
    tree_v2_entries: treeEntries.length,
    tree_v1_validation_steps: treeV1ValidationSteps,
    tree_v2_validation_steps: treeV2ValidationSteps,
    tree_v1_candidate_chunk_steps: treeV1CandidateChunkSteps,
    tree_v2_candidate_chunk_steps: treeV2CandidateChunkSteps,
    tree_v1_candidate_chunk_pages: treeV1CandidateChunkPages,
    tree_v2_candidate_chunk_pages: treeV2CandidateChunkPages,
    candidate_mutation_jobs: candidateMutationResults,
    candidate_mutation_output_jobs: candidateMutationOutputResults,
    tree_chunk_exports: chunkExportResults,
    tree_root_exports: rootExportResults,
    committed_revisions: committedRevisionResults,
    v2_output_settlements: candidateMutationOutputResults.map(
        ({ variant, budget, settlement, v1_atomic_rejection }) =>
            ({ variant, budget, settlement, v1_atomic_rejection })),
    replacement_rebuild_jobs: replacementRebuildResults,
    deferred_reachability_jobs: deferredReachabilityResults,
    candidate_open_memory_jobs: candidateOpenMemoryResults,
    deferred_internal_expansion_jobs: deferredInternalExpansionResults,
    parity: true,
}));
