#!/usr/bin/env node

// Real packaged WASM allocation/lifecycle qualification, not a memory limit,
// RSS/device benchmark, or shared-admission proof. Reads build artifacts only;
// every tree/input is synthetic and every native handle has explicit cleanup.
import { strict as assert } from "node:assert";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { performance } from "node:perf_hooks";

const outputDir = resolve(process.argv[2] ?? "plugin/wasm");
assert(process.argv.length <= 3, "Usage: verify-wasm-memory.mjs [packaged-output-directory]");
const COUNT = 25_000, PAGE_ENTRIES = 256, PAGE_BYTES = 256 * 1024, INPUT_BYTES = 8 * 1024 * 1024;
const VAULT = "memory-qualification-vault", DEVICE = "memory-qualification-device";
const NUMBERS = ["live_requested_bytes", "peak_requested_bytes", "live_allocations",
    "successful_allocations", "successful_reallocations", "allocation_failures",
    "reallocation_failures", "linear_memory_bytes"];
const MONOTONIC = ["peak_requested_bytes", "successful_allocations", "successful_reallocations",
    "allocation_failures", "reallocation_failures", "linear_memory_bytes"];
const FIELDS = ["schema", "scope", "enabled", "counters_valid", "precision_lost", ...NUMBERS].sort();
const STORES = ["resident", "replacement", "retiring"];
const STORE_NUMBERS = ["chunks", "map_capacity", "payload_bytes", "buffer_capacity_bytes"];
const STORE_FIELDS = [...STORE_NUMBERS, "counters_valid"];
const EMPTY_STORE = Object.freeze({ chunks: 0, map_capacity: 0, payload_bytes: 0,
    buffer_capacity_bytes: 0, counters_valid: true });
const METADATA_FIELDS = ["schema", "scope", "wrapper_ids", "resident", "replacement", "retiring",
    "other_private_jobs_unmeasured"];
const STRING_MEMORY_FIELDS = ["strings", "length_bytes", "capacity_bytes", "counters_valid"];
const ROOT_MEMORY_FIELDS = ["roots", "strings", "v1_children_length", "v1_children_capacity",
    "v1_children_backing_capacity_bytes", "counters_valid"];
const BASELINE_MEMORY_FIELDS = ["present", "hashes", "capacity_slots", "logical_hash_bytes",
    "counters_valid"];
const TREE_METADATA_FIELDS = ["committed", "candidate", "tree_ids", "candidate_baseline",
    "counters_valid"];
const INPUT_FIELDS = ["schema", "scope", "replacement", "retiring", "other_input_owners_unmeasured"];
const INPUT_MEMORY_FIELDS = ["entries", "paths", "counters_valid"];
const VEC_MEMORY_FIELDS = ["owners", "length_slots", "capacity_slots", "slot_size_bytes",
    "backing_capacity_bytes", "counters_valid"];
const V1_ENTRIES_FIELDS = ["schema", "scope", "replacement", "retiring",
    "other_entry_owners_unmeasured"];
const V1_ENTRIES_MEMORY_FIELDS = ["input", "groups", "sorting_rows", "rows", "leaf",
    "retiring_rows", "paths", "counters_valid"];
const V1_GRAPH_FIELDS = ["schema", "scope", "replacement", "retiring", "other_graph_owners_unmeasured"];
const V1_GRAPH_VECTORS = ["leaf_hashes", "hashes", "children", "retiring_children", "root_children"];
const V1_GRAPH_STRINGS = ["group_keys", "prefix", "child_labels", "root_child_labels", "identities"];
const V1_GRAPH_MEMORY_FIELDS = [...V1_GRAPH_VECTORS, ...V1_GRAPH_STRINGS,
    "sorting_children", "counters_valid"];
const SORT_FIELDS = ["schema", "scope", "replacement", "retiring", "other_sort_owners_unmeasured"];
const SORT_OWNER_FIELDS = ["entries", "children", "counters_valid"];
const SORT_MEMORY_FIELDS = ["sorters", "values", "source_indices", "target_indices", "counters_valid"];
const SORT_PLAN_FIELDS = ["schema", "scope", "entryCount", "indexSizeBytes",
    "sourceIndexRequestedBytes", "targetIndexRequestedBytes", "peakAdmissionBytes"];
const CANDIDATE_OPEN_MEMORY_FIELDS = ["schema", "scope", "residentChunkCount",
    "rootStringCount", "rootIdentityRequestedBytes", "rootEndpointRequestedBytes",
    "rootStringRequestedBytes", "baselineKeySnapshotRequestedBytes", "peakAdmissionBytes",
    "baselineStrategy"];
const CANDIDATE_CHUNK_SORT_PLAN_FIELDS = ["schema", "scope", "hashCount", "hashSizeBytes",
    "sourceHashesRequestedBytes", "scratchHashesRequestedBytes", "peakAdmissionBytes",
    "reachableSetUnmeasured", "pageOutputUnmeasured", "sortStrategy", "pageMaxHashes"];
const CANDIDATE_CHUNK_PAGE_HASHES = 256, CANDIDATE_CHUNK_SORT_UNITS = 4096;
const PLANNING_FIELDS = ["schema", "scope", "replacement", "retiring",
    "other_planning_owners_unmeasured"];
const PLANNING_VECTORS = ["spans", "leaf_spans", "planned_ranges", "next_planned_ranges",
    "planned_spans", "planned_internal_spans"];
const PLANNING_MEMORY_FIELDS = [...PLANNING_VECTORS, "counters_valid"];
const POST_SORT_FIELDS = ["schema", "scope", "replacement", "retiring",
    "other_post_sort_owners_unmeasured"];
const RANGES_FIELDS = ["schema", "scope", "replacement", "retiring", "other_range_owners_unmeasured"];
const RANGE_VECTORS = ["ranges", "next_ranges", "closure_pending", "closure_expanding"];
const RANGES_MEMORY_FIELDS = [...RANGE_VECTORS, "vector_paths", "descriptor_ranges",
    "descriptor_paths", "counters_valid"];
const MUTATION_OUTPUT_METHODS = ["begin_candidate_update_job", "begin_candidate_delete_job",
    "step_candidate_mutation_output_memory_v1_job", "candidate_mutation_output_memory_plan_v1_job",
    "resume_candidate_mutation_output_memory_v1_job", "candidate_mutation_output_memory_ready_v1_job",
    "finish_candidate_mutation_job_deferred", "cancel_candidate_mutation_job_deferred", "step_tree_retirement",
    "commit_candidate_output_settlement_v1", "abort_candidate_output_settlement_v1"];
const V2_OUTPUT_SETTLEMENT_FIELDS = ["schema", "scope", "outcome", "treeVersion", "before",
    "reachable", "removed", "after", "bytesRemoved", "committedRevision", "candidateRevision",
    "countersValid", "nodePayloadBytes", "rangeEndpointResidentRequestedBytes",
    "residentAdmissionBytes"];
const tick = () => new Promise(resolveTask => setImmediate(resolveTask));
let importSequence = 0;

function exactFields(value, fields, label) {
    assert(value !== null && typeof value === "object" && !Array.isArray(value), `${label}: not an object`);
    assert.deepEqual(Reflect.ownKeys(value).sort(), fields.slice().sort(), `${label}: unexpected schema fields`);
    for (const key of fields) assert("value" in Object.getOwnPropertyDescriptor(value, key), `${label}: accessor field`);
}

function chunkSnapshot(tree, label, expectedUnmeasured = false) {
    const value = tree.chunk_memory_snapshot();
    exactFields(value, ["schema", "scope", ...STORES, "other_private_jobs_unmeasured"], label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "tree-chunk-stores");
    assert.equal(value.other_private_jobs_unmeasured, expectedUnmeasured, `${label}: incomplete private-job coverage flag`);
    for (const name of STORES) {
        const row = value[name];
        exactFields(row, STORE_FIELDS, `${label}/${name}`);
        assert.equal(row.counters_valid, true, `${label}/${name}: invalid component counters`);
        for (const field of STORE_NUMBERS) assert(typeof row[field] === "number" &&
            Number.isSafeInteger(row[field]) && row[field] >= 0, `${label}/${name}: unsafe ${field}`);
        assert(row.map_capacity >= row.chunks, `${label}/${name}: logical capacity smaller than live entries`);
        assert(row.buffer_capacity_bytes >= row.payload_bytes, `${label}/${name}: capacity below payload`);
        if (row.chunks === 0) {
            assert.equal(row.payload_bytes, 0); assert.equal(row.buffer_capacity_bytes, 0);
            // An empty container can still own native map backing. Only the
            // completed retirement assertion below requires all-default state.
        }
    }
    return value;
}

function safeCount(value, label) {
    assert(typeof value === "number" && Number.isSafeInteger(value) && value >= 0,
        `${label}: unsafe nonnegative integer`);
}

function candidateOpenMemoryPlan(value, expected, label) {
    exactFields(value, CANDIDATE_OPEN_MEMORY_FIELDS, label);
    assert.equal(value.schema, 1, `${label}: schema`);
    assert.equal(value.scope, expected.scope ?? "v2-candidate-open-root", `${label}: scope`);
    assert.equal(value.baselineStrategy, "insertion-generation-v1", `${label}: baseline strategy`);
    for (const field of ["residentChunkCount", "rootStringCount", "rootIdentityRequestedBytes",
        "rootEndpointRequestedBytes", "rootStringRequestedBytes",
        "baselineKeySnapshotRequestedBytes", "peakAdmissionBytes"])
        safeCount(value[field], `${label}/${field}`);
    assert.equal(value.rootStringRequestedBytes,
        value.rootIdentityRequestedBytes + value.rootEndpointRequestedBytes,
        `${label}: root String request sum`);
    assert.equal(value.peakAdmissionBytes, value.rootStringRequestedBytes,
        `${label}: peak admission differs from root String request`);
    assert.equal(value.baselineKeySnapshotRequestedBytes, 0,
        `${label}: insertion-generation baseline allocated a key snapshot`);
    for (const [field, expectedValue] of Object.entries(expected)) {
        if (field === "scope") continue;
        assert.equal(value[field], expectedValue, `${label}: independent ${field} oracle`);
    }
    return value;
}

function candidateChunkSortMemoryPlan(value, expectedCount, label) {
    exactFields(value, CANDIDATE_CHUNK_SORT_PLAN_FIELDS, label);
    assert.equal(value.schema, 1, `${label}: schema`);
    assert.equal(value.scope, "candidate-chunk-plan-sort-workspace", `${label}: scope`);
    assert.equal(value.hashCount, expectedCount, `${label}: hash count`);
    assert.equal(value.hashSizeBytes, 32, `${label}: hash width`);
    assert.equal(value.sourceHashesRequestedBytes, expectedCount * 32, `${label}: source request`);
    assert.equal(value.scratchHashesRequestedBytes, expectedCount * 32, `${label}: scratch request`);
    assert.equal(value.peakAdmissionBytes, expectedCount * 64, `${label}: peak request`);
    assert.equal(value.reachableSetUnmeasured, true, `${label}: reachability scope`);
    assert.equal(value.pageOutputUnmeasured, true, `${label}: page scope`);
    assert.equal(value.sortStrategy, "stable-lsd-radix-v1", `${label}: sort strategy`);
    assert.equal(value.pageMaxHashes, CANDIDATE_CHUNK_PAGE_HASHES, `${label}: page maximum`);
    return value;
}

function candidateChunkSortProgress(value, previous, count, label) {
    exactFields(value, ["schema", "scope", "done", "units", "completed", "remaining",
        "allCount", "phase"], label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "candidate-chunk-plan-sort");
    safeCount(value.units, `${label}/units`); safeCount(value.completed, `${label}/completed`);
    safeCount(value.remaining, `${label}/remaining`); safeCount(value.allCount, `${label}/allCount`);
    assert(value.units <= CANDIDATE_CHUNK_SORT_UNITS, `${label}: units exceed requested budget`);
    assert.equal(value.completed, previous + value.units, `${label}: non-exact completion`);
    assert.equal(value.completed + value.remaining, count * 66, `${label}: total work`);
    assert.equal(value.allCount, count, `${label}: all count`);
    assert.equal(typeof value.done, "boolean");
    assert.equal(value.done, value.phase === "ready" && value.remaining === 0, `${label}: ready state`);
    assert(value.done || value.units > 0, `${label}: unfinished zero-work step`);
    return value;
}

function candidateChunkPage(value, expectedOffset, count, label) {
    exactFields(value, ["schema", "scope", "offset", "nextOffset", "done", "all", "fresh"], label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "candidate-chunk-plan-page");
    safeCount(value.offset, `${label}/offset`); safeCount(value.nextOffset, `${label}/nextOffset`);
    assert.equal(value.offset, expectedOffset); assert(value.nextOffset <= count);
    assert(Array.isArray(value.all) && Array.isArray(value.fresh));
    assert.equal(value.nextOffset - value.offset, value.all.length);
    assert(value.all.length <= CANDIDATE_CHUNK_PAGE_HASHES);
    assert.equal(value.done, value.nextOffset === count);
    const pageAll = new Set(value.all);
    let previous;
    for (const hash of value.all) {
        assert.match(hash, /^[0-9a-f]{64}$/); assert(previous === undefined || previous < hash);
        previous = hash;
    }
    previous = undefined;
    for (const hash of value.fresh) {
        assert.match(hash, /^[0-9a-f]{64}$/); assert(previous === undefined || previous < hash);
        assert(pageAll.has(hash), `${label}: fresh hash absent from page all`); previous = hash;
    }
    return value;
}

function stringMemory(value, label) {
    exactFields(value, STRING_MEMORY_FIELDS, label);
    for (const field of ["strings", "length_bytes", "capacity_bytes"])
        safeCount(value[field], `${label}/${field}`);
    assert.equal(value.counters_valid, true, `${label}: invalid string counters`);
    assert(value.capacity_bytes >= value.length_bytes, `${label}: capacity below length`);
    if (value.strings === 0) {
        assert.equal(value.length_bytes, 0, `${label}: zero strings retain logical bytes`);
        assert.equal(value.capacity_bytes, 0, `${label}: zero strings retain capacity bytes`);
    }
    return value;
}

function rootMemory(value, label) {
    exactFields(value, ROOT_MEMORY_FIELDS, label);
    for (const field of ["roots", "v1_children_length", "v1_children_capacity",
        "v1_children_backing_capacity_bytes"])
        safeCount(value[field], `${label}/${field}`);
    assert.equal(value.counters_valid, true, `${label}: invalid root counters`);
    stringMemory(value.strings, `${label}/strings`);
    assert(value.v1_children_capacity >= value.v1_children_length,
        `${label}: v1 Vec capacity below length`);
    assert.equal(value.v1_children_capacity === 0, value.v1_children_backing_capacity_bytes === 0,
        `${label}: v1 Vec backing/capacity disagreement`);
    if (value.roots === 0) {
        assert.equal(value.strings.strings, 0, `${label}: absent root retains strings`);
        assert.equal(value.v1_children_length, 0, `${label}: absent root retains children`);
        assert.equal(value.v1_children_capacity, 0, `${label}: absent root retains Vec capacity`);
    }
    return value;
}

function baselineMemory(value, label) {
    exactFields(value, BASELINE_MEMORY_FIELDS, label);
    assert.equal(typeof value.present, "boolean", `${label}: malformed presence`);
    assert.equal(value.counters_valid, true, `${label}: invalid baseline counters`);
    for (const field of ["hashes", "capacity_slots", "logical_hash_bytes"])
        safeCount(value[field], `${label}/${field}`);
    assert(value.capacity_slots >= value.hashes, `${label}: logical capacity below hashes`);
    assert(value.hashes <= Math.floor(Number.MAX_SAFE_INTEGER / 32), `${label}: hash bytes overflow`);
    assert.equal(value.logical_hash_bytes, value.hashes * 32, `${label}: logical hash bytes differ`);
    if (!value.present) {
        assert.equal(value.hashes, 0); assert.equal(value.capacity_slots, 0);
        assert.equal(value.logical_hash_bytes, 0);
    }
    return value;
}

function treeMetadata(value, label) {
    exactFields(value, TREE_METADATA_FIELDS, label);
    assert.equal(value.counters_valid, true, `${label}: invalid tree metadata counters`);
    rootMemory(value.committed, `${label}/committed`);
    rootMemory(value.candidate, `${label}/candidate`);
    stringMemory(value.tree_ids, `${label}/tree_ids`);
    baselineMemory(value.candidate_baseline, `${label}/candidate_baseline`);
    return value;
}

function metadataSnapshot(tree, label) {
    const value = tree.metadata_memory_snapshot();
    exactFields(value, METADATA_FIELDS, label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "tree-metadata");
    assert.equal(typeof value.other_private_jobs_unmeasured, "boolean",
        `${label}: malformed private coverage flag`);
    stringMemory(value.wrapper_ids, `${label}/wrapper_ids`);
    treeMetadata(value.resident, `${label}/resident`);
    for (const name of ["replacement", "retiring"]) {
        assert(value[name] === null || typeof value[name] === "object",
            `${label}/${name}: expected exact owner or null`);
        if (value[name] !== null) treeMetadata(value[name], `${label}/${name}`);
    }
    assert(value.replacement !== null && value.retiring !== null
        || value.other_private_jobs_unmeasured,
    `${label}: nullable private owner reported as completely measured`);
    return value;
}

function vecBackingMemory(value, label) {
    exactFields(value, VEC_MEMORY_FIELDS, label);
    for (const field of ["owners", "length_slots", "capacity_slots", "slot_size_bytes",
        "backing_capacity_bytes"]) safeCount(value[field], `${label}/${field}`);
    assert.equal(value.counters_valid, true, `${label}: invalid Vec counters`);
    assert(value.owners <= 1, `${label}: unexpected Vec owner cardinality`);
    assert(value.length_slots <= value.capacity_slots, `${label}: Vec length exceeds capacity`);
    if (value.owners === 0) {
        for (const field of ["length_slots", "capacity_slots", "slot_size_bytes", "backing_capacity_bytes"])
            assert.equal(value[field], 0, `${label}: absent Vec retained ${field}`);
    } else {
        assert(value.slot_size_bytes > 0, `${label}: present Vec has no slot size`);
        assert(value.capacity_slots <= Math.floor(Number.MAX_SAFE_INTEGER / value.slot_size_bytes),
            `${label}: Vec backing multiplication overflows`);
        assert.equal(value.backing_capacity_bytes, value.capacity_slots * value.slot_size_bytes,
            `${label}: Vec backing differs from capacity × slot size`);
    }
    return value;
}

function vecBackingAggregateMemory(value, label) {
    exactFields(value, VEC_MEMORY_FIELDS, label);
    for (const field of ["owners", "length_slots", "capacity_slots", "slot_size_bytes",
        "backing_capacity_bytes"]) safeCount(value[field], `${label}/${field}`);
    assert.equal(value.counters_valid, true, `${label}: invalid aggregate Vec counters`);
    assert(value.length_slots <= value.capacity_slots, `${label}: aggregate Vec length exceeds capacity`);
    if (value.owners === 0) {
        for (const field of ["length_slots", "capacity_slots", "slot_size_bytes", "backing_capacity_bytes"])
            assert.equal(value[field], 0, `${label}: absent aggregate retained ${field}`);
    } else {
        assert(value.slot_size_bytes > 0, `${label}: present aggregate has no slot size`);
        assert(value.capacity_slots <= Math.floor(Number.MAX_SAFE_INTEGER / value.slot_size_bytes),
            `${label}: aggregate Vec backing multiplication overflows`);
        assert.equal(value.backing_capacity_bytes, value.capacity_slots * value.slot_size_bytes,
            `${label}: aggregate Vec backing differs from capacity × slot size`);
    }
    return value;
}

function replacementInputMemory(value, label) {
    exactFields(value, INPUT_MEMORY_FIELDS, label);
    assert.equal(value.counters_valid, true, `${label}: invalid input counters`);
    vecBackingMemory(value.entries, `${label}/entries`);
    stringMemory(value.paths, `${label}/paths`);
    assert.equal(value.entries.length_slots, value.paths.strings,
        `${label}: path owner count differs from entries`);
    return value;
}

function replacementInputSnapshot(tree, label) {
    const value = tree.replacement_input_memory_snapshot();
    exactFields(value, INPUT_FIELDS, label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "replacement-input");
    assert.equal(typeof value.other_input_owners_unmeasured, "boolean",
        `${label}: malformed input coverage flag`);
    for (const name of ["replacement", "retiring"]) {
        assert(value[name] === null || typeof value[name] === "object",
            `${label}/${name}: expected exact input owner or null`);
        if (value[name] !== null) replacementInputMemory(value[name], `${label}/${name}`);
    }
    assert(value.replacement !== null && value.retiring !== null
        || value.other_input_owners_unmeasured,
    `${label}: nullable input owner reported as completely measured`);
    return value;
}

function inputCoverage(value, expected, label) {
    const actual = {
        replacement: value.replacement === null ? "unmeasured" : "exact",
        retiring: value.retiring === null ? "unmeasured" : "exact",
        other: value.other_input_owners_unmeasured,
    };
    const states = {
        exact: { replacement: "exact", retiring: "exact", other: false },
        "replacement-unmeasured": { replacement: "unmeasured", retiring: "exact", other: true },
        "retiring-unmeasured": { replacement: "exact", retiring: "unmeasured", other: true },
    };
    assert(Object.hasOwn(states, expected), `${label}: unknown input coverage expectation`);
    assert.deepEqual(actual, states[expected], `${label}: private input coverage differs`);
}

function assertEmptyInputMemory(value, label) {
    assert(value !== null, `${label}: empty input owner was reported as unmeasured`);
    assert.equal(value.entries.owners, 0, `${label}: retained Vec owner`);
    assert.equal(value.entries.length_slots, 0, `${label}: retained entries`);
    assert.equal(value.entries.capacity_slots, 0, `${label}: retained Vec backing`);
    assert.equal(value.paths.strings, 0, `${label}: retained path Strings`);
}

function assertEmptyInputSnapshot(value, label) {
    inputCoverage(value, "exact", `${label}: empty input coverage`);
    assertEmptyInputMemory(value.replacement, `${label}/replacement`);
    assertEmptyInputMemory(value.retiring, `${label}/retiring`);
}

function replacementV1EntriesMemory(value, label) {
    exactFields(value, V1_ENTRIES_MEMORY_FIELDS, label);
    assert.equal(value.counters_valid, true, `${label}: invalid V1 entry counters`);
    vecBackingMemory(value.input, `${label}/input`);
    vecBackingAggregateMemory(value.groups, `${label}/groups`);
    safeCount(value.sorting_rows, `${label}/sorting_rows`);
    vecBackingMemory(value.rows, `${label}/rows`);
    vecBackingMemory(value.leaf, `${label}/leaf`);
    vecBackingMemory(value.retiring_rows, `${label}/retiring_rows`);
    stringMemory(value.paths, `${label}/paths`);
    const count = value.input.length_slots + value.groups.length_slots + value.sorting_rows
        + value.rows.length_slots + value.leaf.length_slots + value.retiring_rows.length_slots;
    safeCount(count, `${label}/summed_rows`);
    assert.equal(count, value.paths.strings, `${label}: row/path conservation differs`);
    assert(value.groups.owners <= value.groups.capacity_slots,
        `${label}: grouped Vec owners exceed aggregate capacity`);
    const slotSizes = [value.input, value.groups, value.rows, value.leaf, value.retiring_rows]
        .filter(owner => owner.owners > 0).map(owner => owner.slot_size_bytes);
    assert(new Set(slotSizes).size <= 1, `${label}: V1 FileEntry slot sizes differ across owners`);
    return value;
}

function replacementV1EntriesSnapshot(tree, label) {
    const value = tree.replacement_v1_entries_memory_snapshot();
    exactFields(value, V1_ENTRIES_FIELDS, label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "replacement-v1-entries");
    assert.equal(typeof value.other_entry_owners_unmeasured, "boolean",
        `${label}: malformed V1 entry coverage flag`);
    for (const name of ["replacement", "retiring"]) {
        assert(value[name] === null || typeof value[name] === "object",
            `${label}/${name}: expected exact V1 entry owner or null`);
        if (value[name] !== null) replacementV1EntriesMemory(value[name], `${label}/${name}`);
    }
    assert(value.replacement !== null && value.retiring !== null
        || value.other_entry_owners_unmeasured,
    `${label}: nullable V1 entry owner reported as completely measured`);
    return value;
}

function assertEmptyV1EntriesMemory(value, label) {
    assert(value !== null, `${label}: empty V1 entry owner was reported as unmeasured`);
    for (const name of ["input", "rows", "leaf", "retiring_rows"]) {
        assert.equal(value[name].owners, 0, `${label}/${name}: retained Vec owner`);
        assert.equal(value[name].capacity_slots, 0, `${label}/${name}: retained Vec backing`);
    }
    assert.equal(value.groups.owners, 0, `${label}/groups: retained Vec owners`);
    assert.equal(value.groups.capacity_slots, 0, `${label}/groups: retained Vec backing`);
    assert.equal(value.sorting_rows, 0, `${label}: retained sorting rows`);
    assert.equal(value.paths.strings, 0, `${label}: retained path Strings`);
}

function assertEmptyV1EntriesSnapshot(value, label) {
    assert.equal(value.other_entry_owners_unmeasured, false,
        `${label}: empty V1 entry coverage is incomplete`);
    assertEmptyV1EntriesMemory(value.replacement, `${label}/replacement`);
    assertEmptyV1EntriesMemory(value.retiring, `${label}/retiring`);
}

function v1EntriesBackingBytes(value) {
    const total = value.input.backing_capacity_bytes + value.groups.backing_capacity_bytes
        + value.rows.backing_capacity_bytes + value.leaf.backing_capacity_bytes
        + value.retiring_rows.backing_capacity_bytes + value.paths.capacity_bytes;
    safeCount(total, "summed V1 entry backing");
    return total;
}

function assertV1EntriesRelations(entries, sort, planning, label) {
    for (const name of ["replacement", "retiring"]) {
        const owner = entries[name];
        if (owner === null) continue;
        const entrySorter = sort[name]?.entries;
        const v2Owner = planning[name] !== null
            && PLANNING_VECTORS.some(vector => planning[name][vector].owners === 1);
        if (owner.sorting_rows > 0) {
            assert(entrySorter?.sorters === 1,
                `${label}/${name}: sorting rows lack their exclusive sorter backing`);
            assert.equal(owner.sorting_rows, entrySorter.values.length_slots,
                `${label}/${name}: sorter/value row count differs`);
        }
        if (entrySorter?.sorters === 1 && !v2Owner) {
            assert.equal(owner.sorting_rows, entrySorter.values.length_slots,
                `${label}/${name}: V1 sorter/value row count differs`);
            if (entrySorter.values.length_slots > 0) {
                assert(owner.sorting_rows > 0,
                    `${label}/${name}: V1 entry sorter hid its nested path owners`);
            }
        }
        if (v2Owner) assertEmptyV1EntriesMemory(owner,
            `${label}/${name}: V2 replacement fabricated V1 entry ownership`);
    }
}

function assertV1EntriesTransfer(previous, current, label) {
    assertEmptyV1EntriesMemory(previous.retiring, `${label}: premature V1 entry retirement`);
    assertEmptyV1EntriesMemory(current.replacement, `${label}: V1 entry owner counted twice`);
    assert.deepEqual(current.retiring, previous.replacement,
        `${label}: V1 entry ownership changed across transfer`);
    assert.equal(current.other_entry_owners_unmeasured,
        previous.other_entry_owners_unmeasured, `${label}: V1 entry coverage changed across transfer`);
}

function assertV1EntriesRetirement(previous, current, units, label) {
    assert(previous !== null && current !== null, `${label}: exact V1 entry owner became unmeasured`);
    for (const name of ["input", "rows", "leaf", "retiring_rows"]) {
        assert(current[name].owners <= previous[name].owners, `${label}/${name}: Vec owner reappeared`);
        assert(current[name].length_slots <= previous[name].length_slots,
            `${label}/${name}: row count increased`);
        assert(current[name].capacity_slots <= previous[name].capacity_slots,
            `${label}/${name}: Vec capacity increased`);
        if (current[name].owners === 1) {
            assert.equal(previous[name].owners, 1, `${label}/${name}: Vec owner reappeared`);
            for (const field of ["capacity_slots", "slot_size_bytes", "backing_capacity_bytes"])
                assert.equal(current[name][field], previous[name][field],
                    `${label}/${name}: retained Vec ${field} changed`);
        }
    }
    assert(current.groups.owners <= previous.groups.owners, `${label}/groups: Vec owner reappeared`);
    assert(current.groups.length_slots <= previous.groups.length_slots,
        `${label}/groups: row count increased`);
    assert(current.groups.capacity_slots <= previous.groups.capacity_slots,
        `${label}/groups: Vec capacity increased`);
    if (current.groups.owners === previous.groups.owners) {
        for (const field of ["capacity_slots", "slot_size_bytes", "backing_capacity_bytes"])
            assert.equal(current.groups[field], previous.groups[field],
                `${label}/groups: retained aggregate ${field} changed`);
    }
    assert(current.sorting_rows <= previous.sorting_rows, `${label}: sorting row count increased`);
    for (const field of ["strings", "length_bytes", "capacity_bytes"])
        assert(current.paths[field] <= previous.paths[field], `${label}: path ${field} increased`);
    const retiredRows = previous.paths.strings - current.paths.strings;
    const releasedOwners = previous.input.owners - current.input.owners
        + previous.groups.owners - current.groups.owners
        + previous.rows.owners - current.rows.owners
        + previous.leaf.owners - current.leaf.owners
        + previous.retiring_rows.owners - current.retiring_rows.owners;
    assert(retiredRows + releasedOwners <= units,
        `${label}: retirement unit destroyed multiple V1 row/backing owners`);
    if (retiredRows === 0) assert.deepEqual(current.paths, previous.paths,
        `${label}: path allocation changed without a row retirement`);
    return retiredRows + releasedOwners;
}

function replacementV1GraphMemory(value, label) {
    exactFields(value, V1_GRAPH_MEMORY_FIELDS, label);
    assert.equal(value.counters_valid, true, `${label}: invalid V1 graph counters`);
    for (const name of V1_GRAPH_VECTORS) vecBackingMemory(value[name], `${label}/${name}`);
    for (const name of V1_GRAPH_STRINGS) stringMemory(value[name], `${label}/${name}`);
    safeCount(value.sorting_children, `${label}/sorting_children`);
    assert(value.prefix.strings <= 1 && value.identities.strings <= 2,
        `${label}: impossible prefix/identity owner count`);
    const children = value.children.length_slots + value.sorting_children + value.retiring_children.length_slots;
    safeCount(children, `${label}/summed_children`);
    assert.equal(children, value.child_labels.strings, `${label}: child/label ownership differs`);
    assert.equal(value.root_children.length_slots, value.root_child_labels.strings,
        `${label}: root child/label ownership differs`);
    for (const names of [["leaf_hashes", "hashes"], ["children", "retiring_children", "root_children"]]) {
        const sizes = names.filter(name => value[name].owners > 0).map(name => value[name].slot_size_bytes);
        assert(new Set(sizes).size <= 1, `${label}: same-type Vec slot sizes differ`);
    }
    v1GraphBackingBytes(value);
    return value;
}

function replacementV1GraphSnapshot(tree, label) {
    const value = tree.replacement_v1_graph_memory_snapshot();
    exactFields(value, V1_GRAPH_FIELDS, label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "replacement-v1-graph");
    // This component covers every retained V1 assembly owner, including while
    // a sorter owns the top-level child Vec. Missing owners are not zeroes.
    assert.equal(value.other_graph_owners_unmeasured, false, `${label}: V1 graph coverage became incomplete`);
    for (const name of ["replacement", "retiring"])
        replacementV1GraphMemory(value[name], `${label}/${name}`);
    return value;
}

function assertEmptyV1GraphMemory(value, label) {
    for (const name of V1_GRAPH_VECTORS)
        assert.equal(value[name].owners, 0, `${label}/${name}: retained graph backing owner`);
    for (const name of V1_GRAPH_STRINGS)
        assert.equal(value[name].strings, 0, `${label}/${name}: retained graph String owner`);
    assert.equal(value.sorting_children, 0, `${label}: retained sorting children`);
}

function assertEmptyV1GraphSnapshot(value, label) {
    for (const name of ["replacement", "retiring"]) assertEmptyV1GraphMemory(value[name], `${label}/${name}`);
}

function v1GraphBackingBytes(value) {
    const bytes = V1_GRAPH_VECTORS.reduce((sum, name) => sum + value[name].backing_capacity_bytes, 0)
        + V1_GRAPH_STRINGS.reduce((sum, name) => sum + value[name].capacity_bytes, 0);
    safeCount(bytes, "summed V1 graph backing");
    return bytes;
}

function assertV1GraphRelations(graph, entries, sort, planning, metadata, label) {
    for (const name of ["replacement", "retiring"]) {
        const value = graph[name], childSort = sort[name]?.children;
        const v2Owner = planning[name] !== null
            && PLANNING_VECTORS.some(vector => planning[name][vector].owners === 1);
        if (v2Owner) assertEmptyV1GraphMemory(value, `${label}/${name}: V2 fabricated V1 graph ownership`);
        assert.equal(value.sorting_children, childSort?.sorters === 1 ? childSort.values.length_slots : 0,
            `${label}/${name}: child sorter lost or duplicated its nested labels`);
        if (childSort?.sorters === 1) {
            assert.equal(value.children.owners, 0, `${label}/${name}: sorter values backing counted twice`);
            assert.equal(value.retiring_children.owners, 0, `${label}/${name}: sorter overlaps encoded children`);
            for (const vector of [value.root_children]) if (vector.owners > 0)
                assert.equal(vector.slot_size_bytes, childSort.values.slot_size_bytes,
                    `${label}/${name}: sorted/unsorted child slot sizes differ`);
        }
        if (entries[name] !== null) assert(value.group_keys.strings >= entries[name].groups.owners,
            `${label}/${name}: grouped entry Vec lacks its owning key`);
        if (metadata[name]?.committed.roots > 0)
            assertEmptyV1GraphMemory(value, `${label}/${name}: provisional/ready root counted in graph and metadata`);
    }
}

function assertV1GraphTransfer(previous, current, label) {
    assertEmptyV1GraphMemory(previous.retiring, `${label}: premature graph retirement`);
    assertEmptyV1GraphMemory(current.replacement, `${label}: graph owner counted twice`);
    assert.deepEqual(current.retiring, previous.replacement, `${label}: graph allocation transfer differs`);
}

function assertV1GraphMetadataTransfer(previous, current, metadata, label) {
    assertEmptyV1GraphMemory(current, `${label}: metadata transfer retained graph owners`);
    assert(metadata !== null && metadata.committed.roots === 1, `${label}: transferred root metadata is absent`);
    const root = metadata.committed;
    assert.equal(root.v1_children_length, previous.root_children.length_slots,
        `${label}: root child count changed during transfer`);
    assert.equal(root.v1_children_capacity, previous.root_children.capacity_slots,
        `${label}: root Vec capacity changed during transfer`);
    assert.equal(root.v1_children_backing_capacity_bytes, previous.root_children.backing_capacity_bytes,
        `${label}: root Vec bytes changed during transfer`);
    for (const field of ["strings", "length_bytes", "capacity_bytes"])
        assert.equal(root.strings[field], previous.root_child_labels[field] + previous.identities[field],
            `${label}: root String ${field} changed during transfer`);
}

function assertV1GraphRetirement(previous, current, units, label) {
    let released = 0;
    for (const name of V1_GRAPH_VECTORS) {
        const before = previous[name], after = current[name];
        assert(after.owners <= before.owners && after.length_slots <= before.length_slots,
            `${label}/${name}: graph Vec owner/rows reappeared`);
        if (after.owners === 1) for (const field of ["capacity_slots", "slot_size_bytes", "backing_capacity_bytes"])
            assert.equal(after[field], before[field], `${label}/${name}: retained backing ${field} changed`);
        released += before.owners - after.owners;
    }
    let retiredStrings = 0;
    for (const name of V1_GRAPH_STRINGS) {
        const before = previous[name], after = current[name];
        for (const field of ["strings", "length_bytes", "capacity_bytes"])
            assert(after[field] <= before[field], `${label}/${name}: String ${field} increased`);
        if (before.strings === after.strings) assert.deepEqual(after, before,
            `${label}/${name}: String allocation changed without a retired descriptor`);
        retiredStrings += before.strings - after.strings;
    }
    assert(current.sorting_children <= previous.sorting_children, `${label}: sorting children increased`);
    const retiredHashes = previous.leaf_hashes.length_slots - current.leaf_hashes.length_slots
        + previous.hashes.length_slots - current.hashes.length_slots;
    // Each child descriptor owns one label already counted above. Its inline
    // hash is not another allocation or another retirement work unit.
    const work = retiredStrings + retiredHashes + released;
    assert(work <= units, `${label}: one unit retired multiple graph descriptors/backings`);
    return work;
}

function replacementV2PostSortSnapshot(tree, label) {
    const value = tree.replacement_v2_post_sort_memory_snapshot();
    exactFields(value, POST_SORT_FIELDS, label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "replacement-v2-post-sort");
    assert.equal(typeof value.other_post_sort_owners_unmeasured, "boolean",
        `${label}: malformed post-sort coverage flag`);
    for (const name of ["replacement", "retiring"]) {
        if (value[name] !== null) replacementInputMemory(value[name], `${label}/${name}`);
    }
    assert.equal(value.other_post_sort_owners_unmeasured,
        value.replacement === null || value.retiring === null,
        `${label}: post-sort coverage differs from its nullable owners`);
    return value;
}

function assertEmptyV2PostSortSnapshot(value, label) {
    assert.equal(value.other_post_sort_owners_unmeasured, false,
        `${label}: empty post-sort coverage is incomplete`);
    for (const name of ["replacement", "retiring"])
        assertEmptyInputMemory(value[name], `${label}/${name}`);
}

function assertPostSortRelations(postSort, sort, planning, label) {
    for (const name of ["replacement", "retiring"]) {
        const v2Owner = planning[name] !== null
            && PLANNING_VECTORS.some(vector => planning[name][vector].owners === 1);
        const sorting = v2Owner && sort[name]?.entries.sorters === 1;
        if (sorting) {
            assert.equal(postSort[name], null,
                `${label}/${name}: sorter ownership was counted as exact post-sort entries`);
        } else {
            assert(postSort[name] !== null,
                `${label}/${name}: non-sorting post-sort owner was reported unmeasured`);
            if (!v2Owner) assertEmptyInputMemory(postSort[name],
                `${label}/${name}: non-V2 builder or drained job fabricated post-sort ownership`);
        }
    }
}

function assertPostSortTransfer(previous, current, label) {
    assertEmptyInputMemory(previous.retiring, `${label}: premature post-sort retirement`);
    assertEmptyInputMemory(current.replacement, `${label}: post-sort owner counted twice`);
    assert.deepEqual(current.retiring, previous.replacement,
        `${label}: post-sort entry/path ownership changed across transfer`);
    assert.equal(current.other_post_sort_owners_unmeasured,
        previous.other_post_sort_owners_unmeasured,
        `${label}: post-sort coverage changed across transfer`);
}

function assertPostSortFromInput(actual, accepted, ready, label) {
    assert(actual !== null, `${label}: post-sort owner is unmeasured`);
    if (!ready) {
        assert.deepEqual(actual, accepted,
            `${label}: sorting changed accepted entry backing/path ownership`);
        return;
    }
    assertEmptyInputMemory(actual, `${label}: ready builder retained post-sort backing`);
}

function assertInitialReplacementInput(value, expectedEntries, label) {
    inputCoverage(value, "exact", `${label}: initial input coverage`);
    assertEmptyInputMemory(value.retiring, `${label}/retiring`);
    const replacement = value.replacement;
    assert(replacement !== null, `${label}: initial replacement input was unmeasured`);
    assert.equal(replacement.entries.owners, 1, `${label}: reserved input Vec has no owner`);
    assert.equal(replacement.entries.length_slots, 0, `${label}: new input Vec is not empty`);
    assert(replacement.entries.capacity_slots >= expectedEntries,
        `${label}: input Vec did not reserve declared cardinality`);
    assert.equal(replacement.paths.strings, 0, `${label}: new input retained path Strings`);
}

function assertRetiringInputRows(value, initial, rows, retiredRows, label) {
    inputCoverage(value, "exact", `${label}: exact retirement input coverage`);
    assertEmptyInputMemory(value.replacement, `${label}/replacement`);
    assert(retiredRows >= 0 && retiredRows <= rows.length, `${label}: invalid retired row count`);
    const actual = value.retiring;
    assert(actual !== null, `${label}: retiring input was reported unmeasured`);
    const remaining = rows.length - retiredRows;
    assert.equal(actual.entries.owners, 1, `${label}: input Vec backing released before its unit`);
    assert.equal(actual.entries.length_slots, remaining, `${label}: retiring input row count differs`);
    for (const field of ["capacity_slots", "slot_size_bytes", "backing_capacity_bytes"])
        assert.equal(actual.entries[field], initial.entries[field], `${label}: input Vec ${field} changed`);
    assert.equal(actual.paths.strings, remaining, `${label}: retiring path owner count differs`);
    const remainingLength = rows.slice(retiredRows)
        .reduce((sum, row) => sum + Buffer.byteLength(row.path), 0);
    assert.equal(actual.paths.length_bytes, remainingLength, `${label}: retiring path bytes differ`);

    // Both qualification corpora deliberately use one fixed-size, unescaped
    // UTF-8 path shape. Equal construction gives an independent aggregate
    // capacity oracle without exposing any path or per-row native counters.
    const pathLengths = new Set(rows.map(row => Buffer.byteLength(row.path)));
    assert.equal(pathLengths.size, 1, `${label}: fixture paths do not have a uniform byte shape`);
    assert.equal(initial.paths.length_bytes,
        rows.reduce((sum, row) => sum + Buffer.byteLength(row.path), 0),
        `${label}: transferred input path bytes differ from the fixture`);
    assert.equal(initial.paths.capacity_bytes % rows.length, 0,
        `${label}: uniform fixture path capacity is not divisible by its owners`);
    const perPathCapacity = initial.paths.capacity_bytes / rows.length;
    assert.equal(actual.paths.capacity_bytes, perPathCapacity * remaining,
        `${label}: retiring path capacity differs`);
}

function indirectSortMemory(value, label) {
    exactFields(value, SORT_MEMORY_FIELDS, label);
    safeCount(value.sorters, `${label}/sorters`);
    assert(value.sorters <= 1, `${label}: unexpected sorter cardinality`);
    assert.equal(value.counters_valid, true, `${label}: invalid sort counters`);
    const values = vecBackingMemory(value.values, `${label}/values`);
    const source = vecBackingMemory(value.source_indices, `${label}/source_indices`);
    const target = vecBackingMemory(value.target_indices, `${label}/target_indices`);
    const expectedOwners = value.sorters;
    for (const [name, vector] of [["values", values], ["source", source], ["target", target]]) {
        assert.equal(vector.owners, expectedOwners,
            `${label}: ${name} Vec owner differs from sorter cardinality`);
    }
    if (value.sorters === 1) {
        assert.equal(source.slot_size_bytes, target.slot_size_bytes,
            `${label}: index Vec slot sizes differ`);
    }
    return { sorters: value.sorters, values, source_indices: source,
        target_indices: target, counters_valid: value.counters_valid };
}

function replacementSortMemory(value, label) {
    exactFields(value, SORT_OWNER_FIELDS, label);
    assert.equal(value.counters_valid, true, `${label}: invalid replacement sort counters`);
    const entries = indirectSortMemory(value.entries, `${label}/entries`);
    const children = indirectSortMemory(value.children, `${label}/children`);
    assert(entries.sorters + children.sorters <= 1,
        `${label}: simultaneous replacement sorters`);
    return { entries, children, counters_valid: value.counters_valid };
}

function replacementSortSnapshot(tree, label) {
    const value = tree.replacement_sort_memory_snapshot();
    exactFields(value, SORT_FIELDS, label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "replacement-sort");
    assert.equal(typeof value.other_sort_owners_unmeasured, "boolean",
        `${label}: malformed sort coverage flag`);
    for (const name of ["replacement", "retiring"]) {
        assert(value[name] === null || typeof value[name] === "object",
            `${label}/${name}: expected exact sort owner or null`);
        if (value[name] !== null) replacementSortMemory(value[name], `${label}/${name}`);
    }
    assert(value.replacement !== null && value.retiring !== null
        || value.other_sort_owners_unmeasured,
    `${label}: nullable sort owner reported as completely measured`);
    return value;
}

function replacementSortMemoryPlan(value, expectedEntries, label) {
    exactFields(value, SORT_PLAN_FIELDS, label);
    for (const field of ["schema", "entryCount", "indexSizeBytes", "sourceIndexRequestedBytes",
        "targetIndexRequestedBytes", "peakAdmissionBytes"])
        safeCount(value[field], `${label}/${field}`);
    const expectedIndexBytes = expectedEntries * 4;
    assert.equal(value.schema, 1, `${label}: unsupported schema`);
    assert.equal(value.scope, "v2-replacement-sort-indices", `${label}: wrong scope`);
    assert.equal(value.entryCount, expectedEntries, `${label}: native entry count differs from accepted input`);
    assert.equal(value.indexSizeBytes, 4, `${label}: packaged WASM index width is not wasm32`);
    assert.equal(value.sourceIndexRequestedBytes, expectedIndexBytes,
        `${label}: source request differs from independent 4*N oracle`);
    assert.equal(value.targetIndexRequestedBytes, expectedIndexBytes,
        `${label}: target request differs from independent 4*N oracle`);
    assert.equal(value.peakAdmissionBytes, expectedIndexBytes * 2,
        `${label}: peak request differs from independent 8*N oracle`);
    return value;
}

function startReplacementWithSortAdmission(context, tree, token, version, entryCount, label) {
    const input = replacementInputSnapshot(tree, `${label}/input-before`);
    const live = context.sample(`${label}/live-before`);
    if (version === 1) {
        assert.throws(() => tree.replacement_rebuild_sort_memory_plan_v1_job(token),
            `${label}: V1 exposed a V2 sort-memory plan`);
        assert.throws(() => tree.start_replacement_rebuild_sort_memory_v1_job(token, 0, 0),
            `${label}: V1 accepted admitted V2 sort start`);
        assert.deepEqual(replacementInputSnapshot(tree, `${label}/input-after-v1-rejection`), input,
            `${label}: V1 rejection changed the complete input owner`);
        sameLive(context.sample(`${label}/live-after-v1-rejection`), live,
            `${label}: V1 rejection retained native memory`);
        tree.start_replacement_rebuild_job(token);
        return null;
    }
    const plan = replacementSortMemoryPlan(
        tree.replacement_rebuild_sort_memory_plan_v1_job(token), entryCount, `${label}/plan`);
    assert.deepEqual(replacementSortMemoryPlan(
        tree.replacement_rebuild_sort_memory_plan_v1_job(token), entryCount, `${label}/plan-repeat`), plan,
    `${label}: repeated sort-memory plan changed`);
    const wrong = token === 0xffff_ffff ? token - 1 : token + 1;
    assert.throws(() => tree.replacement_rebuild_sort_memory_plan_v1_job(wrong),
        `${label}: wrong token exposed sort-memory plan`);
    assert.throws(() => tree.start_replacement_rebuild_sort_memory_v1_job(
        wrong, plan.sourceIndexRequestedBytes, plan.targetIndexRequestedBytes),
    `${label}: wrong token started V2 sorter`);
    for (let field = 0; field < 2; field++) {
        const witnesses = [plan.sourceIndexRequestedBytes, plan.targetIndexRequestedBytes];
        for (const bad of new Set([witnesses[field] - 1, witnesses[field] + 1,
            NaN, Infinity, -Infinity, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])) {
            const invalid = witnesses.slice(); invalid[field] = bad;
            assert.throws(() => tree.start_replacement_rebuild_sort_memory_v1_job(token, ...invalid),
                `${label}: invalid sort witness ${field}/${bad} started the builder`);
        }
    }
    assert.deepEqual(replacementInputSnapshot(tree, `${label}/input-after-rejections`), input,
        `${label}: rejected sort witnesses changed the complete input owner`);
    assertEmptySortSnapshot(replacementSortSnapshot(tree, `${label}/sort-after-rejections`),
        `${label}: rejected sort witnesses`);
    sameLive(context.sample(`${label}/live-after-rejections`), live,
        `${label}: rejected sort witnesses retained native memory`);

    tree.start_replacement_rebuild_sort_memory_v1_job(
        token, plan.sourceIndexRequestedBytes, plan.targetIndexRequestedBytes);
    const startedLive = context.sample(`${label}/live-started`);
    const identityBytes = Buffer.byteLength(VAULT) + Buffer.byteLength(DEVICE);
    assert.equal(startedLive.live_requested_bytes - live.live_requested_bytes,
        plan.peakAdmissionBytes + identityBytes,
    `${label}: native requested-live delta differs from two admitted indices plus builder IDs`);
    assert.equal(startedLive.live_allocations - live.live_allocations,
        2 + (entryCount === 0 ? 0 : 2),
    `${label}: native live-allocation delta differs from builder IDs plus nonempty indices`);
    const active = replacementSortSnapshot(tree, `${label}/sort-started`).replacement?.entries;
    assert.equal(active?.sorters, 1, `${label}: admitted start did not create the V2 entry sorter`);
    assert.equal(active.values.length_slots, entryCount, `${label}: admitted sorter lost input rows`);
    assert.equal(active.source_indices.length_slots, 0, `${label}: source indices were initialized at start`);
    assert.equal(active.target_indices.length_slots, 0, `${label}: target indices were initialized at start`);
    assert.equal(active.source_indices.slot_size_bytes, plan.indexSizeBytes,
        `${label}: source index slot size differs from plan`);
    assert.equal(active.target_indices.slot_size_bytes, plan.indexSizeBytes,
        `${label}: target index slot size differs from plan`);
    assert(active.source_indices.capacity_slots >= entryCount,
        `${label}: source index Vec capacity is below its exact request`);
    assert(active.target_indices.capacity_slots >= entryCount,
        `${label}: target index Vec capacity is below its exact request`);
    assert(active.source_indices.backing_capacity_bytes >= plan.sourceIndexRequestedBytes,
        `${label}: source index backing is below its admitted request`);
    assert(active.target_indices.backing_capacity_bytes >= plan.targetIndexRequestedBytes,
        `${label}: target index backing is below its admitted request`);
    assert.throws(() => tree.replacement_rebuild_sort_memory_plan_v1_job(token),
        `${label}: consumed sort-memory plan remained readable`);
    assert.throws(() => tree.start_replacement_rebuild_sort_memory_v1_job(
        token, plan.sourceIndexRequestedBytes, plan.targetIndexRequestedBytes),
    `${label}: admitted sort start replay succeeded`);
    return plan;
}

function sortCoverage(value, expected, label) {
    const actual = {
        replacement: value.replacement === null ? "unmeasured" : "exact",
        retiring: value.retiring === null ? "unmeasured" : "exact",
        other: value.other_sort_owners_unmeasured,
    };
    const states = {
        exact: { replacement: "exact", retiring: "exact", other: false },
        "replacement-unmeasured": { replacement: "unmeasured", retiring: "exact", other: true },
        "retiring-unmeasured": { replacement: "exact", retiring: "unmeasured", other: true },
        "other-unmeasured": { replacement: "exact", retiring: "exact", other: true },
    };
    assert(Object.hasOwn(states, expected), `${label}: unknown sort coverage expectation`);
    assert.deepEqual(actual, states[expected], `${label}: private sort coverage differs`);
}

function assertEmptyIndirectSort(value, label) {
    assert.equal(value.sorters, 0, `${label}: retained sorter`);
    for (const [name, vector] of [["values", value.values], ["source", value.source_indices],
        ["target", value.target_indices]]) {
        assert.equal(vector.owners, 0, `${label}: retained ${name} Vec owner`);
        assert.equal(vector.capacity_slots, 0, `${label}: retained ${name} Vec backing`);
    }
}

function assertEmptySortMemory(value, label) {
    assert(value !== null, `${label}: empty sort owner was reported as unmeasured`);
    assertEmptyIndirectSort(value.entries, `${label}/entries`);
    assertEmptyIndirectSort(value.children, `${label}/children`);
}

function assertEmptySortSnapshot(value, label) {
    sortCoverage(value, "exact", `${label}: empty sort coverage`);
    assertEmptySortMemory(value.replacement, `${label}/replacement`);
    assertEmptySortMemory(value.retiring, `${label}/retiring`);
}

function sortBackingBytes(value) {
    return value.values.backing_capacity_bytes + value.source_indices.backing_capacity_bytes
        + value.target_indices.backing_capacity_bytes;
}

function replacementV2PlanningMemory(value, label) {
    exactFields(value, PLANNING_MEMORY_FIELDS, label);
    assert.equal(value.counters_valid, true, `${label}: invalid V2 planning counters`);
    for (const name of PLANNING_VECTORS) vecBackingMemory(value[name], `${label}/${name}`);
    return value;
}

function replacementV2PlanningSnapshot(tree, label) {
    const value = tree.replacement_v2_planning_memory_snapshot();
    exactFields(value, PLANNING_FIELDS, label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "replacement-v2-planning");
    assert.equal(typeof value.other_planning_owners_unmeasured, "boolean",
        `${label}: malformed V2 planning coverage flag`);
    for (const name of ["replacement", "retiring"]) {
        assert(value[name] === null || typeof value[name] === "object",
            `${label}/${name}: expected exact V2 planning owner or null`);
        if (value[name] !== null)
            replacementV2PlanningMemory(value[name], `${label}/${name}`);
    }
    assert(value.replacement !== null && value.retiring !== null
        || value.other_planning_owners_unmeasured,
    `${label}: nullable V2 planning owner reported as completely measured`);
    return value;
}

function assertEmptyV2PlanningMemory(value, label) {
    assert(value !== null, `${label}: empty V2 planning owner was reported as unmeasured`);
    for (const name of PLANNING_VECTORS) {
        assert.equal(value[name].owners, 0, `${label}/${name}: retained Vec owner`);
        assert.equal(value[name].length_slots, 0, `${label}/${name}: retained descriptors`);
        assert.equal(value[name].capacity_slots, 0, `${label}/${name}: retained Vec backing`);
    }
}

function assertEmptyV2PlanningSnapshot(value, label) {
    assert.equal(value.other_planning_owners_unmeasured, false,
        `${label}: empty V2 planning coverage is incomplete`);
    assertEmptyV2PlanningMemory(value.replacement, `${label}/replacement`);
    assertEmptyV2PlanningMemory(value.retiring, `${label}/retiring`);
}

function assertActiveV2PlanningMemory(value, label) {
    assert(value !== null, `${label}: active V2 planning owner was reported as unmeasured`);
    for (const name of PLANNING_VECTORS)
        assert.equal(value[name].owners, 1, `${label}/${name}: active Vec owner is absent`);
}

function planningBackingBytes(value) {
    const total = PLANNING_VECTORS.reduce(
        (sum, name) => sum + value[name].backing_capacity_bytes, 0);
    assert(Number.isSafeInteger(total), "summed V2 planning backing lost precision");
    return total;
}

function rangesBackingBytes(value, label) {
    let total = 0;
    for (const bytes of [...RANGE_VECTORS.map(name => value[name].backing_capacity_bytes),
        value.vector_paths.capacity_bytes, value.descriptor_paths.capacity_bytes]) {
        total += bytes; safeCount(total, `${label}: summed range backing`);
    }
    // Descriptor count is not multiplied by a guessed HashMap bucket size.
    return total;
}

function replacementV2RangesMemory(value, label) {
    exactFields(value, RANGES_MEMORY_FIELDS, label);
    assert.equal(value.counters_valid, true, `${label}: invalid range counters`);
    let count = 0, slot;
    for (const name of RANGE_VECTORS) {
        const vector = vecBackingMemory(value[name], `${label}/${name}`);
        if (vector.owners !== 0) {
            assert(vector.capacity_slots > 0, `${label}/${name}: unallocated Vec advertised as an owner`);
            if (slot !== undefined) assert.equal(vector.slot_size_bytes, slot,
                `${label}/${name}: RangeRef layout differs across owners`);
            slot = vector.slot_size_bytes;
        }
        count += vector.length_slots; safeCount(count, `${label}: summed range count`);
    }
    stringMemory(value.vector_paths, `${label}/vector-paths`);
    stringMemory(value.descriptor_paths, `${label}/descriptor-paths`);
    safeCount(value.descriptor_ranges, `${label}/descriptor-ranges`);
    for (const [ranges, strings] of [[count, value.vector_paths.strings],
        [value.descriptor_ranges, value.descriptor_paths.strings]]) {
        assert(ranges <= Math.floor(Number.MAX_SAFE_INTEGER / 2), `${label}: endpoint multiplication overflow`);
        assert.equal(strings, ranges * 2, `${label}: endpoint count differs from physical RangeRefs`);
    }
    rangesBackingBytes(value, label);
    return value;
}

function replacementV2RangesSnapshot(tree, label) {
    const value = tree.replacement_v2_ranges_memory_snapshot();
    exactFields(value, RANGES_FIELDS, label);
    assert.equal(value.schema, 1); assert.equal(value.scope, "replacement-v2-ranges");
    // This replacement-only ABI intentionally excludes general/candidate jobs,
    // including when its own measured components happen to be empty.
    assert.equal(value.other_range_owners_unmeasured, true, `${label}: lost partial-scope caveat`);
    for (const name of ["replacement", "retiring"]) {
        if (value[name] !== null) replacementV2RangesMemory(value[name], `${label}/${name}`);
    }
    return value;
}

function assertEmptyV2RangesMemory(value, label) {
    assert(value !== null, `${label}: empty owner reported unmeasured`);
    for (const name of RANGE_VECTORS) assert.equal(value[name].owners, 0,
        `${label}/${name}: retained Vec backing`);
    assert.equal(value.vector_paths.strings, 0, `${label}: retained vector endpoints`);
    assert.equal(value.descriptor_ranges, 0, `${label}: retained descriptors`);
    assert.equal(value.descriptor_paths.strings, 0, `${label}: retained descriptor endpoints`);
}

function assertEmptyV2RangesSnapshot(value, label) {
    for (const name of ["replacement", "retiring"]) assertEmptyV2RangesMemory(value[name], `${label}/${name}`);
}

function assertV2RangesTransfer(before, after, label) {
    assertEmptyV2RangesMemory(before.retiring, `${label}: initial retirement`);
    assertEmptyV2RangesMemory(after.replacement, `${label}: transferred replacement`);
    assert.deepEqual(after.retiring, before.replacement, `${label}: lost/duplicated transferred RangeRef owners`);
}

function assertV2RangesRetirement(before, after, units, label) {
    assert(before !== null && after !== null, `${label}: measured range retirement became unavailable`);
    let retired = before.descriptor_ranges - after.descriptor_ranges, released = 0;
    assert(retired >= 0, `${label}: descriptor owner reappeared`);
    for (const name of RANGE_VECTORS) {
        const a = before[name], b = after[name];
        assert(b.owners <= a.owners && b.length_slots <= a.length_slots,
            `${label}/${name}: range owner reappeared`);
        if (b.owners !== 0) for (const field of ["capacity_slots", "slot_size_bytes", "backing_capacity_bytes"])
            assert.equal(b[field], a[field], `${label}/${name}: retained backing changed`);
        if (units === 1 && a.owners > b.owners) assert.equal(a.length_slots, 0,
            `${label}/${name}: backing released in the last range's unit`);
        retired += a.length_slots - b.length_slots;
        released += a.owners - b.owners;
    }
    for (const name of ["vector_paths", "descriptor_paths"]) {
        const a = before[name], b = after[name];
        for (const field of ["strings", "length_bytes", "capacity_bytes"])
            assert(b[field] <= a[field], `${label}/${name}: endpoint ownership increased`);
        if (a.strings === b.strings) assert.deepEqual(b, a,
            `${label}/${name}: String allocation changed without retiring its RangeRef`);
    }
    assert(retired + released <= units, `${label}: multiple disjoint ranges/backings retired in one unit`);
    return retired + released;
}

function assertV2RootRangeTransfer(before, after, metadata, label) {
    assert(before !== null && after !== null && metadata !== null, `${label}: missing exact root handoff`);
    const count = before.ranges.length_slots + before.next_ranges.length_slots;
    assert.equal(count, 1, `${label}: fixture did not expose the unique root child`);
    assert.equal(before.closure_pending.length_slots, 0);
    assert.equal(before.closure_expanding.length_slots, 0);
    assert.equal(before.descriptor_ranges, 0);
    assert.equal(after.ranges.owners, 0, `${label}: root transfer retained ranges Vec`);
    assert.equal(after.next_ranges.owners, 0, `${label}: root transfer retained next ranges Vec`);
    assert.equal(after.closure_pending.length_slots, 1, `${label}: closure clone is missing`);
    assert.equal(after.closure_expanding.length_slots, 0);
    assert.equal(after.descriptor_ranges, 0);
    // Original Strings MOVE into root metadata; closure pending CLONES them.
    // Equal endpoint text is not shared allocation, and clone capacity need
    // not equal the original's spare capacity.
    assert.equal(after.vector_paths.strings, 2);
    assert.equal(after.vector_paths.length_bytes, before.vector_paths.length_bytes);
    assert.equal(metadata.committed.roots, 1);
    assert.equal(metadata.committed.strings.strings, 4);
    assert.equal(metadata.committed.strings.length_bytes,
        Buffer.byteLength(VAULT) + Buffer.byteLength(DEVICE) + before.vector_paths.length_bytes);
    assert.equal(metadata.tree_ids.strings, 2);
    assert.equal(metadata.committed.v1_children_backing_capacity_bytes, 0);
}

function metadataCoverage(value, expected, label) {
    const actual = {
        replacement: value.replacement === null ? "unmeasured" : "exact",
        retiring: value.retiring === null ? "unmeasured" : "exact",
        other: value.other_private_jobs_unmeasured,
    };
    const states = {
        exact: { replacement: "exact", retiring: "exact", other: false },
        "replacement-unmeasured": { replacement: "unmeasured", retiring: "exact", other: true },
        "retiring-unmeasured": { replacement: "exact", retiring: "unmeasured", other: true },
        "other-unmeasured": { replacement: "exact", retiring: "exact", other: true },
    };
    assert(Object.hasOwn(states, expected), `${label}: unknown metadata coverage expectation`);
    assert.deepEqual(actual, states[expected], `${label}: private metadata coverage differs`);
}

function physicalMetadataBytes(value) {
    let total = value.wrapper_ids.capacity_bytes;
    for (const tree of [value.resident, value.replacement, value.retiring]) {
        if (tree === null) continue;
        total += tree.committed.strings.capacity_bytes
            + tree.committed.v1_children_backing_capacity_bytes
            + tree.candidate.strings.capacity_bytes
            + tree.candidate.v1_children_backing_capacity_bytes
            + tree.tree_ids.capacity_bytes;
    }
    assert(Number.isSafeInteger(total));
    return total;
}

function assertEmptyTreeMetadata(value, label) {
    assert(value !== null, `${label}: empty owner was reported as unmeasured`);
    for (const root of [value.committed, value.candidate]) {
        assert.equal(root.roots, 0, `${label}: retained root`);
        assert.equal(root.strings.strings, 0, `${label}: retained root strings`);
        assert.equal(root.v1_children_capacity, 0, `${label}: retained root Vec`);
    }
    assert.equal(value.tree_ids.strings, 0, `${label}: retained tree identities`);
    assert.equal(value.candidate_baseline.present, false, `${label}: retained baseline owner`);
}

function observe(context, tree, phase, expectedUnmeasured = false, expectedMetadataCoverage,
    expectedInputCoverage, expectedSortCoverage = "exact") {
    const components = chunkSnapshot(tree, `${context.label}/${phase}`, expectedUnmeasured);
    const metadata = metadataSnapshot(tree, `${context.label}/${phase}/metadata`);
    if (expectedMetadataCoverage !== undefined) {
        metadataCoverage(metadata, expectedMetadataCoverage, `${context.label}/${phase}/metadata`);
    }
    const input = replacementInputSnapshot(tree, `${context.label}/${phase}/input`);
    if (expectedInputCoverage !== undefined) {
        inputCoverage(input, expectedInputCoverage, `${context.label}/${phase}/input`);
    }
    const sort = replacementSortSnapshot(tree, `${context.label}/${phase}/sort`);
    sortCoverage(sort, expectedSortCoverage, `${context.label}/${phase}/sort`);
    const v1Entries = replacementV1EntriesSnapshot(
        tree, `${context.label}/${phase}/v1-entries`);
    const planning = replacementV2PlanningSnapshot(
        tree, `${context.label}/${phase}/v2-planning`);
    assertV1EntriesRelations(v1Entries, sort, planning,
        `${context.label}/${phase}/v1-entries`);
    const postSort = replacementV2PostSortSnapshot(tree, `${context.label}/${phase}/v2-post-sort`);
    assertPostSortRelations(postSort, sort, planning, `${context.label}/${phase}/v2-post-sort`);
    const v1Graph = replacementV1GraphSnapshot(tree, `${context.label}/${phase}/v1-graph`);
    assertV1GraphRelations(v1Graph, v1Entries, sort, planning, metadata,
        `${context.label}/${phase}/v1-graph`);
    const ranges = replacementV2RangesSnapshot(tree, `${context.label}/${phase}/v2-ranges`);
    for (const name of ["replacement", "retiring"]) {
        if (input[name]?.entries.owners === 1 || v1Graph[name]?.identities.strings > 0
            || v1Entries[name]?.paths.strings > 0 || sort[name]?.children.sorters === 1) {
            assertEmptyV2RangesMemory(ranges[name], `${phase}/${name}: input/V1 has no V2 ranges`);
        }
    }
    // Serialize component diagnostics first; the global sample is taken after
    // that call returned and released its transient encoding work. Reporting
    // may raise lifetime peaks; it is not claimed to be allocation-free.
    const memory = context.sample(phase);
    for (const field of ["payload_bytes", "buffer_capacity_bytes"]) {
        const total = STORES.reduce((sum, name) => sum + components[name][field], 0);
        assert(Number.isSafeInteger(total), `${phase}: summed component counter lost precision`);
        assert(total <= memory.live_requested_bytes, `${phase}: owned node ${field} exceeds instance requested-live`);
    }
    assert(physicalMetadataBytes(metadata) <= memory.live_requested_bytes,
        `${phase}: exact metadata buffers exceed instance requested-live`);
    for (const owner of [input.replacement, input.retiring]) {
        if (owner === null) continue;
        assert(owner.entries.backing_capacity_bytes + owner.paths.capacity_bytes <= memory.live_requested_bytes,
            `${phase}: exact input buffers exceed instance requested-live`);
    }
    for (const owner of [sort.replacement, sort.retiring]) {
        if (owner === null) continue;
        for (const component of [owner.entries, owner.children]) {
            assert(sortBackingBytes(component) <= memory.live_requested_bytes,
                `${phase}: exact sort buffers exceed instance requested-live`);
        }
    }
    for (const owner of [planning.replacement, planning.retiring]) {
        if (owner === null) continue;
        assert(planningBackingBytes(owner) <= memory.live_requested_bytes,
            `${phase}: exact V2 planning backing exceeds instance requested-live`);
    }
    for (const owner of [postSort.replacement, postSort.retiring]) {
        if (owner === null) continue;
        const bytes = owner.entries.backing_capacity_bytes + owner.paths.capacity_bytes;
        assert(Number.isSafeInteger(bytes), `${phase}: summed post-sort bytes lost precision`);
        assert(bytes <= memory.live_requested_bytes,
            `${phase}: exact post-sort buffers exceed instance requested-live`);
    }
    for (const owner of [v1Entries.replacement, v1Entries.retiring]) {
        if (owner === null) continue;
        assert(v1EntriesBackingBytes(owner) <= memory.live_requested_bytes,
            `${phase}: exact V1 entry buffers exceed instance requested-live`);
    }
    let additive = STORES.reduce((sum, name) => sum + components[name].buffer_capacity_bytes, 0)
        + physicalMetadataBytes(metadata);
    for (const owner of [input.replacement, input.retiring]) {
        if (owner !== null) additive += owner.entries.backing_capacity_bytes + owner.paths.capacity_bytes;
    }
    for (const owner of [sort.replacement, sort.retiring]) {
        if (owner !== null) additive += sortBackingBytes(owner.entries) + sortBackingBytes(owner.children);
    }
    for (const owner of [planning.replacement, planning.retiring]) {
        if (owner !== null) additive += planningBackingBytes(owner);
    }
    for (const owner of [postSort.replacement, postSort.retiring]) {
        if (owner !== null) additive += owner.entries.backing_capacity_bytes + owner.paths.capacity_bytes;
    }
    for (const owner of [v1Entries.replacement, v1Entries.retiring]) {
        if (owner !== null) additive += v1EntriesBackingBytes(owner);
    }
    for (const owner of [v1Graph.replacement, v1Graph.retiring]) additive += v1GraphBackingBytes(owner);
    for (const value of [ranges.replacement, ranges.retiring]) {
        if (value !== null) additive += rangesBackingBytes(value, phase);
    }
    safeCount(additive, `${phase}: additive component requested bytes`);
    assert(additive <= memory.live_requested_bytes,
        `${phase}: disjoint exact component backing exceeds instance requested-live`);
    context.recordComponents(phase, components);
    return { memory, components, metadata, input, sort, planning, postSort, v1Entries, v1Graph, ranges };
}

function sameStores(actual, expected, label) { assert.deepEqual(actual, expected, label); }
function noPrivate(components, label) {
    sameStores(components.replacement, EMPTY_STORE, `${label}: unexpected replacement buffers`);
    sameStores(components.retiring, EMPTY_STORE, `${label}: retirement is not fully drained`);
}

function canonicalComponent(component, expected, label) {
    assert.equal(component.chunks, expected.chunks.length, `${label}: canonical node count differs`);
    const bytes = expected.chunks.reduce((sum, [, hex]) => sum + hex.length / 2, 0);
    assert.equal(component.payload_bytes, bytes, `${label}: canonical payload total differs`);
}

function residentComponent(module, tree, hashes, component, label) {
    assert.equal(new Set(hashes).size, hashes.length, `${label}: duplicate resident hashes`);
    assert.equal(component.chunks, hashes.length, `${label}: candidate/resident nodes counted twice or omitted`);
    const bytes = hashes.reduce((sum, hash) => {
        const length = module.wasm_tree_chunk_byte_length(tree, hash);
        assert(Number.isSafeInteger(length) && length >= 0, `${label}: missing resident chunk length`);
        return sum + length;
    }, 0);
    assert.equal(component.payload_bytes, bytes, `${label}: resident payload differs from independent lengths`);
}

async function loadVariant(name) {
    const stem = name === "scalar" ? "sync_core" : "sync_core_simd";
    const bytes = await readFile(resolve(outputDir, `${stem}_bg.wasm`));
    assert(WebAssembly.validate(bytes), `${name}: packaged module does not validate on this host`);
    return { name, stem, bytes };
}

async function fresh(variant, label) {
    const url = pathToFileURL(resolve(outputDir, `${variant.stem}.js`));
    url.searchParams.set("memory-verification-instance", String(++importSequence));
    const module = await import(url.href);
    assert.equal(typeof module.wasm_memory_snapshot, "function", `${label}: missing wasm_memory_snapshot (stale artifact)`);
    const native = await module.default({ module_or_path: variant.bytes });
    assert(native.memory instanceof WebAssembly.Memory, `${label}: native memory export is unavailable`);
    // The raw snapshot precedes serde/wasm-bindgen result encoding. Its first
    // encoding initializes permanent reporting state and may grow a page AFTER
    // sampling linear size. Exclude this one unmeasured initialization call,
    // then demand exact post-return linear equality on every measured sample.
    module.wasm_memory_snapshot();
    // The additive component serializer has its own first-use state. Warm a
    // disposable empty tree BEFORE the same fixed instance baseline; never
    // reset that baseline after input/build/cancel/publication/retirement.
    const warmTree = requiredTree(module);
    try {
        const warm = chunkSnapshot(warmTree, `${label}/component-warmup`);
        for (const name of STORES) sameStores(warm[name], EMPTY_STORE, `${label}: empty warmup store`);
        const metadata = metadataSnapshot(warmTree, `${label}/metadata-warmup`);
        assert.equal(metadata.wrapper_ids.strings, 2);
        assert.equal(metadata.resident.tree_ids.strings, 2);
        assert.equal(metadata.resident.committed.roots, 0);
        assertEmptyInputSnapshot(replacementInputSnapshot(warmTree, `${label}/input-warmup`),
            `${label}/input-warmup`);
        assertEmptyV1EntriesSnapshot(replacementV1EntriesSnapshot(
            warmTree, `${label}/v1-entries-warmup`), `${label}/v1-entries-warmup`);
        assertEmptyV1GraphSnapshot(replacementV1GraphSnapshot(
            warmTree, `${label}/v1-graph-warmup`), `${label}/v1-graph-warmup`);
        assertEmptySortSnapshot(replacementSortSnapshot(warmTree, `${label}/sort-warmup`),
            `${label}/sort-warmup`);
        assertEmptyV2PlanningSnapshot(replacementV2PlanningSnapshot(
            warmTree, `${label}/v2-planning-warmup`), `${label}/v2-planning-warmup`);
        assertEmptyV2PostSortSnapshot(replacementV2PostSortSnapshot(
            warmTree, `${label}/v2-post-sort-warmup`), `${label}/v2-post-sort-warmup`);
        assertEmptyV2RangesSnapshot(replacementV2RangesSnapshot(
            warmTree, `${label}/v2-ranges-warmup`), `${label}/v2-ranges-warmup`);
        const sortPlanWarmToken = tokenValue(warmTree.begin_replacement_rebuild_job(2, 0, 2));
        replacementSortMemoryPlan(warmTree.replacement_rebuild_sort_memory_plan_v1_job(sortPlanWarmToken),
            0, `${label}/sort-plan-warmup`);
        warmTree.cancel_replacement_rebuild_job_deferred(sortPlanWarmToken);
        while (!warmTree.step_tree_retirement(sortPlanWarmToken, 256).done) { /* warm and release */ }
        warmCandidateOpen(module, warmTree);
        warmCandidateChunkPlan(module, warmTree);
        warmMutationOutput(module, warmTree);
    } finally { warmTree.free(); }
    let previous;
    const phases = new Map();
    function sample(phase) {
        const value = module.wasm_memory_snapshot();
        exactFields(value, FIELDS, `${label}/${phase}`);
        assert.equal(value.schema, 1); assert.equal(value.scope, "wasm-instance");
        assert.equal(value.enabled, true); assert.equal(value.counters_valid, true);
        assert.equal(value.precision_lost, false);
        for (const field of NUMBERS) assert(typeof value[field] === "number" &&
            Number.isSafeInteger(value[field]) && value[field] >= 0, `${label}/${phase}: unsafe ${field}`);
        assert(value.live_requested_bytes <= value.peak_requested_bytes, `${label}/${phase}: live exceeds lifetime peak`);
        assert.equal(value.linear_memory_bytes, native.memory.buffer.byteLength, `${label}/${phase}: native linear bytes differ`);
        assert.equal(value.linear_memory_bytes % 65_536, 0);
        assert.equal(value.allocation_failures, 0, `${label}/${phase}: actual allocation failure`);
        assert.equal(value.reallocation_failures, 0, `${label}/${phase}: actual reallocation failure`);
        if (previous) for (const field of MONOTONIC)
            assert(value[field] >= previous[field], `${label}/${phase}: ${field} went backwards`);
        previous = value;
        let row = phases.get(phase);
        if (!row) {
            row = { samples: 0, sampled_live_peak_bytes: 0, sampled_live_allocation_peak: 0,
                lifetime_requested_peak_bytes: 0, linear_pages_peak: 0 };
            phases.set(phase, row);
        }
        row.samples++;
        row.sampled_live_peak_bytes = Math.max(row.sampled_live_peak_bytes, value.live_requested_bytes);
        row.sampled_live_allocation_peak = Math.max(row.sampled_live_allocation_peak, value.live_allocations);
        row.lifetime_requested_peak_bytes = value.peak_requested_bytes;
        row.linear_pages_peak = value.linear_memory_bytes / 65_536;
        return value;
    }
    // Reporting was initialized exactly once above. Now require stable live
    // ownership; never widen a tolerance or reset after an operation to excuse
    // a leak.
    sample("report-warmup");
    const baseline = sample("report-warmup");
    sameLive(sample("report-warmup"), baseline, `${label}: repeated reporting retains live allocations`);
    function recordComponents(phase, components) {
        const row = phases.get(phase);
        assert(row, `${label}: component phase has no allocation sample`);
        row.chunk_stores ??= {};
        for (const name of STORES) {
            const maxima = row.chunk_stores[name] ??= {};
            for (const field of STORE_NUMBERS)
                maxima[`sampled_max_${field}`] = Math.max(maxima[`sampled_max_${field}`] ?? 0, components[name][field]);
        }
    }
    return { module, native, sample, baseline, label, recordComponents,
        report: () => Object.fromEntries(phases), snapshot: () => previous };
}

function sameLive(actual, expected, label) {
    assert.equal(actual.live_requested_bytes, expected.live_requested_bytes, `${label}: requested-live leak/drift`);
    assert.equal(actual.live_allocations, expected.live_allocations, `${label}: live-allocation leak/drift`);
}

function requiredTree(module) {
    const tree = new module.WasmTree(VAULT, DEVICE);
    try {
        for (const method of ["begin_replacement_rebuild_job", "append_replacement_rebuild_job",
            "start_replacement_rebuild_job", "step_replacement_rebuild_job",
            "replacement_rebuild_sort_memory_plan_v1_job", "start_replacement_rebuild_sort_memory_v1_job",
            "replacement_rebuild_plan_job", "resume_replacement_rebuild_job",
            "step_replacement_rebuild_output_memory_v1_job",
            "replacement_rebuild_output_memory_plan_v1_job", "resume_replacement_rebuild_output_memory_v1_job",
            "cancel_replacement_rebuild_job_deferred", "finish_replacement_rebuild_job_deferred",
            "step_tree_retirement", "committed_revision", "candidate_revision", "chunk_memory_snapshot",
            "metadata_memory_snapshot", "replacement_input_memory_snapshot",
            "replacement_v1_entries_memory_snapshot",
            "replacement_v1_graph_memory_snapshot",
            "replacement_sort_memory_snapshot", "replacement_v2_planning_memory_snapshot",
            "replacement_v2_post_sort_memory_snapshot",
            "replacement_v2_ranges_memory_snapshot",
            "begin_candidate_job", "step_tree_job", "begin_candidate_update_job",
            "step_reachability_job_deferred", "finish_candidate_job_deferred",
            "cancel_reachability_job_deferred", "step_reachability_retirement",
            "finish_reachability_retirement", "candidate_open_memory_plan_v1_job",
            "resume_candidate_open_memory_v1_job", "begin_candidate_chunks_job",
            "candidate_chunks_sort_memory_plan_v1_job", "resume_candidate_chunks_sort_memory_v1_job",
            "step_candidate_chunks_sort_v1_job", "candidate_chunks_plan_info_v1_job",
            "read_candidate_chunks_page_v1_job", "finish_candidate_chunks_plan_v1_job",
            "cancel_tree_job", "free",
            ...MUTATION_OUTPUT_METHODS])
            assert.equal(typeof tree[method], "function", `missing packaged native ${method}`);
        return tree;
    } catch (error) { tree.free(); throw error; }
}

function corpus(kind) {
    const result = [];
    for (let index = COUNT - 1; index >= 0; index--) {
        const path = kind === "one-prefix" ? `notes/${String(index).padStart(5, "0")}.md`
            : `group-${String(index).padStart(5, "0")}/note.md`;
        result.push({ path, hash: (index + 1).toString(16).padStart(64, "0"),
            mtime_ms: 1_700_000_000_000 + index, size: 128 + index % 4096 });
    }
    assert(result.some((row, index) => index && result[index - 1].path > row.path), "corpus is accidentally sorted");
    assert.equal(new Set(result.map(row => row.path)).size, COUNT);
    const json = JSON.stringify(result), bytes = Buffer.byteLength(json);
    assert(bytes <= INPUT_BYTES, "synthetic corpus exceeds actual native input admission");
    return { rows: result, json, bytes };
}

function progress(value, previous, build, label) {
    exactFields(value, build ? ["done", "units", "completed", "phase"] : ["done", "units", "completed"], label);
    assert.equal(typeof value.done, "boolean");
    assert(Number.isSafeInteger(value.units) && value.units >= 0 && value.units <= 256, `${label}: invalid units`);
    assert(value.done || value.units > 0, `${label}: unfinished zero-work step`);
    assert(Number.isSafeInteger(value.completed) && value.completed === previous + value.units, `${label}: invalid cumulative work`);
    if (build) assert(typeof value.phase === "string" && value.phase.length > 0 && value.phase.length <= 64);
    return value;
}

function reachabilityProgress(value, previous, label) {
    exactFields(value, ["done", "units", "completed", "remaining", "reachable"], label);
    assert.equal(typeof value.done, "boolean", `${label}: malformed done flag`);
    for (const field of ["units", "completed", "remaining", "reachable"])
        safeCount(value[field], `${label}/${field}`);
    assert(value.units <= 256, `${label}: exceeded the native step cap`);
    assert(value.done || value.units > 0, `${label}: unfinished zero-work step`);
    assert.equal(value.completed, previous + value.units, `${label}: invalid cumulative work`);
    return value;
}

function candidateOpenRows(count) {
    return Array.from({ length: count }, (_, index) => ({
        path: `candidate-open/${String(index).padStart(8, "0")}.md`,
        hash: (index + 1).toString(16).padStart(64, "0"),
        mtime_ms: 1_931_000_000_000 + index,
        size: 64 + index % 4_096,
    }));
}

function candidateOpenExpected(module, tree, rows, vault = VAULT, device = DEVICE) {
    const paths = rows.map(row => Buffer.from(row.path, "utf8"))
        .sort((left, right) => Buffer.compare(left, right));
    const rootIdentityRequestedBytes = Buffer.byteLength(vault) + Buffer.byteLength(device);
    const rootEndpointRequestedBytes = paths.length === 0 ? 0 :
        paths[0].byteLength + paths[paths.length - 1].byteLength;
    const rootStringRequestedBytes = rootIdentityRequestedBytes + rootEndpointRequestedBytes;
    return { residentChunkCount: module.wasm_tree_committed_chunk_hashes(tree).length,
        rootStringCount: paths.length === 0 ? 2 : 4,
        rootIdentityRequestedBytes, rootEndpointRequestedBytes, rootStringRequestedBytes,
        baselineKeySnapshotRequestedBytes: 0, peakAdmissionBytes: rootStringRequestedBytes };
}

function stepCandidateOpenToReady(tree, token, label, budget = 256) {
    let previous = 0, turns = 0, state;
    do {
        assert(++turns < 100_000, `${label}: candidate-open traversal did not converge`);
        state = reachabilityProgress(tree.step_reachability_job_deferred(token, budget), previous,
            `${label}/step${turns}`);
        assert(state.units <= budget, `${label}: traversal exceeded requested budget`);
        previous = state.completed;
    } while (!state.done);
    const replay = reachabilityProgress(tree.step_reachability_job_deferred(token, budget), previous,
        `${label}/ready-replay`);
    assert.equal(replay.done, true, `${label}: READY replay regressed`);
    assert.equal(replay.units, 0, `${label}: READY replay performed work`);
    assert.equal(replay.completed, previous, `${label}: READY replay changed completion`);
    assert.equal(replay.reachable, state.reachable, `${label}: READY replay changed reachability`);
    return state;
}

function drainCandidateOpenRetirement(tree, token, label, budget = 256) {
    tree.cancel_reachability_job_deferred(token);
    tree.cancel_reachability_job_deferred(token);
    let completed = 0, turns = 0, state;
    do {
        assert(++turns < 100_000, `${label}: candidate-open retirement did not converge`);
        state = progress(tree.step_reachability_retirement(token, completed, budget), completed,
            false, `${label}/step${turns}`);
        assert(state.units <= budget, `${label}: retirement exceeded requested budget`);
        const replay = progress(tree.step_reachability_retirement(token, completed, budget), completed,
            false, `${label}/replay${turns}`);
        assert.deepEqual(replay, state, `${label}: retirement replay changed result`);
        completed = state.completed;
    } while (!state.done);
    assert.throws(() => tree.finish_reachability_retirement(token, completed + 1),
        `${label}: wrong retirement witness accepted`);
    assert.equal(tree.finish_reachability_retirement(token, completed), undefined,
        `${label}: retirement acknowledgement returned a value`);
    assert.equal(tree.finish_reachability_retirement(token, completed), undefined,
        `${label}: retirement acknowledgement was not idempotent`);
    return { turns, completed };
}

function warmCandidateOpen(module, tree) {
    tree.rebuild_from_entries_in_version(2, "[]");
    const expected = candidateOpenExpected(module, tree, []);
    const token = tokenValue(tree.begin_candidate_job());
    stepCandidateOpenToReady(tree, token, "candidate-open warmup");
    const plan = candidateOpenMemoryPlan(tree.candidate_open_memory_plan_v1_job(token), expected,
        "candidate-open warmup/plan");
    tree.resume_candidate_open_memory_v1_job(token, plan.rootIdentityRequestedBytes,
        plan.rootEndpointRequestedBytes, plan.rootStringRequestedBytes);
    drainCandidateOpenRetirement(tree, token, "candidate-open warmup/retirement");
}

function warmCandidateChunkPlan(module, tree) {
    tree.begin_candidate();
    const token = tokenValue(tree.begin_candidate_chunks_job());
    const ready = stepCandidateOpenToReady(tree, token, "candidate chunk plan warmup");
    const plan = candidateChunkSortMemoryPlan(tree.candidate_chunks_sort_memory_plan_v1_job(token),
        ready.reachable, "candidate chunk plan warmup/plan");
    tree.resume_candidate_chunks_sort_memory_v1_job(token,
        plan.sourceHashesRequestedBytes, plan.scratchHashesRequestedBytes);
    const sort = candidateChunkSortProgress(
        tree.step_candidate_chunks_sort_v1_job(token, CANDIDATE_CHUNK_SORT_UNITS), 0,
        ready.reachable, "candidate chunk plan warmup/sort");
    assert.equal(sort.done, true, "empty candidate chunk warmup did work");
    const info = tree.candidate_chunks_plan_info_v1_job(token);
    exactFields(info, ["schema", "scope", "allCount", "pageMaxHashes"],
        "candidate chunk plan warmup/info");
    assert.equal(info.allCount, 0); assert.equal(info.pageMaxHashes, CANDIDATE_CHUNK_PAGE_HASHES);
    const page = candidateChunkPage(tree.read_candidate_chunks_page_v1_job(
        token, 0, CANDIDATE_CHUNK_PAGE_HASHES), 0, 0, "candidate chunk plan warmup/page");
    assert.equal(page.done, true); assert.deepEqual(page.all, []); assert.deepEqual(page.fresh, []);
    tree.finish_candidate_chunks_plan_v1_job(token);
    drainCandidateOpenRetirement(tree, token, "candidate chunk plan warmup/retirement");
    tree.abort_candidate();
}

function tokenValue(token) { assert(Number.isSafeInteger(token) && token > 0 && token <= 0xffff_ffff); return token; }

function payloadPlan(value, label) {
    exactFields(value, ["nodeCount", "leafCount", "internalCount", "nodePayloadBytes", "maxNodeBytes", "storedRootBytes"], label);
    for (const field of Object.keys(value)) assert(Number.isSafeInteger(value[field]) && value[field] >= 0, `${label}: invalid ${field}`);
    assert.equal(value.nodeCount, value.leafCount + value.internalCount, `${label}: inconsistent node counts`);
    assert(value.storedRootBytes >= 64 && value.storedRootBytes <= 16 * 1024, `${label}: invalid root bytes`);
    if (value.nodeCount === 0) {
        assert.equal(value.nodePayloadBytes, 0); assert.equal(value.maxNodeBytes, 0);
    } else {
        assert(value.leafCount > 0 && value.maxNodeBytes >= 8 && value.maxNodeBytes <= 256 * 1024);
        assert(value.nodePayloadBytes >= value.maxNodeBytes);
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

function canonicalV1ReplacementOutputMemoryPlan(entryCount, inputJsonBytes, label) {
    const identityBytes = Buffer.byteLength(VAULT) + Buffer.byteLength(DEVICE);
    const nodePayloadBytes = outputPlanSum(inputJsonBytes * 2, entryCount * 256 + 128 * 1024, label);
    const rootRequestedBytes = outputPlanSum(inputJsonBytes * 2,
        entryCount * 128 + identityBytes * 4 + 128 * 1024, label);
    return replacementOutputMemoryPlan({ schema: 1, scope: "v1-replacement-output",
        nodePayloadBytes, rangeEndpointPeakRequestedBytes: rootRequestedBytes,
        rangeEndpointResidentRequestedBytes: rootRequestedBytes,
        peakAdmissionBytes: outputPlanSum(nodePayloadBytes, rootRequestedBytes, label),
        residentAdmissionBytes: outputPlanSum(nodePayloadBytes, rootRequestedBytes, label) },
    label, "v1-replacement-output");
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

function owner(tree) {
    return { version: tree.tree_version(), committed: tree.committed_revision(), candidate: tree.candidate_revision(),
        active: tree.has_candidate(), count: tree.total_files() };
}

function visible(module, tree) {
    return { ...owner(tree), hash: tree.root_hash_hex(), root: Buffer.from(tree.root_bytes()).toString("hex"),
        candidateHash: tree.candidate_root_hash_hex(),
        candidateRoot: tree.has_candidate() ? Buffer.from(tree.candidate_root_bytes()).toString("hex") : null,
        chunks: module.wasm_tree_chunk_hashes(tree).slice().sort() };
}

function canonical(module, tree) {
    assert.equal(tree.has_candidate(), false);
    const root = tree.root_bytes(); assert(root instanceof Uint8Array);
    const hashes = module.wasm_tree_committed_chunk_hashes(tree).slice().sort();
    assert.equal(new Set(hashes).size, hashes.length);
    const chunks = hashes.map(hash => {
        assert(/^[0-9a-f]{64}$/.test(hash));
        const bytes = module.wasm_tree_get_chunk(tree, hash);
        assert(bytes instanceof Uint8Array);
        assert.equal(module.wasm_tree_chunk_byte_length(tree, hash), bytes.length);
        assert.equal(module.wasm_hash(bytes), hash);
        return [hash, Buffer.from(bytes).toString("hex")];
    });
    return { version: tree.tree_version(), hash: tree.root_hash_hex(), count: tree.total_files(),
        root: Buffer.from(root).toString("hex"), chunks };
}

function canonicalPayloadPlan(expected, label) {
    if (expected.version !== 2) return null;
    let leafCount = 0, internalCount = 0, nodePayloadBytes = 0, maxNodeBytes = 0;
    for (const [, hex] of expected.chunks) {
        const bytes = Buffer.from(hex, "hex");
        if (bytes.subarray(0, 4).equals(Buffer.from("OVL2"))) leafCount++;
        else if (bytes.subarray(0, 4).equals(Buffer.from("OVI2"))) internalCount++;
        else assert.fail(`${label}: canonical graph contains an unknown V2 node`);
        nodePayloadBytes += bytes.length;
        maxNodeBytes = Math.max(maxNodeBytes, bytes.length);
    }
    const plan = { nodeCount: expected.chunks.length, leafCount, internalCount,
        nodePayloadBytes, maxNodeBytes, storedRootBytes: expected.root.length / 2 };
    payloadPlan(plan, `${label}/canonical-plan`);
    return plan;
}

async function oracleFor(variant, kind, version, input) {
    const context = await fresh(variant, `${variant.name}/${kind}/v${version}/oracle`);
    const tree = requiredTree(context.module);
    try {
        tree.rebuild_from_entries_in_version(version, input.json);
        const expected = canonical(context.module, tree);
        const observed = observe(context, tree, "oracle-resident", false, "exact", "exact");
        noPrivate(observed.components, context.label);
        assertEmptyInputSnapshot(observed.input, `${context.label}: oracle`);
        assertEmptyV2PlanningSnapshot(observed.planning, `${context.label}: oracle`);
        assertEmptyV2RangesSnapshot(observed.ranges, `${context.label}: oracle ranges`);
        canonicalComponent(observed.components.resident, expected, context.label);
        return expected;
    } finally {
        tree.free();
        sameLive(context.sample("oracle-freed"), context.baseline, `${context.label}: oracle free`);
    }
}

// Finite diagnostic runaway guard derived generously from the row count and
// comparison-sort depth. This is NOT a heap/RSS or timing acceptance limit.
const workGuard = rows => 64 * (rows + 1) * (Math.ceil(Math.log2(rows + 1)) + 1) + 1024;

function feed(context, tree, token, rows, resident, prefix) {
    let offset = 0, pathBytes = 0, accepted;
    while (offset < rows.length) {
        const page = rows.slice(offset, offset + PAGE_ENTRIES), json = JSON.stringify(page);
        assert(Buffer.byteLength(json) <= PAGE_BYTES, "fixture emits oversized native page");
        offset += page.length;
        pathBytes += page.reduce((sum, row) => sum + Buffer.byteLength(row.path), 0);
        assert.equal(tree.append_replacement_rebuild_job(token, offset - page.length, json), offset);
        const { components, input, v1Graph } = observe(context, tree, `${prefix}/input`, false,
            "replacement-unmeasured", "exact");
        assertEmptyV1GraphSnapshot(v1Graph, `${prefix}: input-only graph ownership`);
        sameStores(components.resident, resident, `${prefix}: input changed resident component`);
        noPrivate(components, `${prefix}: input-only job`);
        assert(input.replacement !== null, `${prefix}: accepted input was reported unmeasured`);
        assert.equal(input.replacement.entries.length_slots, offset, `${prefix}: input entry count differs`);
        assert(input.replacement.entries.capacity_slots >= rows.length, `${prefix}: input capacity shrank`);
        assert.equal(input.replacement.paths.strings, offset, `${prefix}: input path count differs`);
        assert.equal(input.replacement.paths.length_bytes, pathBytes, `${prefix}: input path bytes differ`);
        assertEmptyInputMemory(input.retiring, `${prefix}: input feed retirement`);
        accepted = input.replacement;
    }
    return accepted;
}

async function advanceToEntrySort(context, tree, token, rows, before, resident, prefix) {
    let completed = 0, turns = 0;
    for (;;) {
        // V1 first groups every input row. Stop exactly at that boundary, then
        // use unit steps so even a one-row wide-prefix sorter cannot be skipped.
        const remainingGrouping = Math.max(rows - completed, 0);
        const budget = before.version === 1 && remainingGrouping > 0
            ? Math.min(256, remainingGrouping) : 1;
        const state = progress(tree.step_replacement_rebuild_job(token, budget), completed, true,
            `${prefix}/advance-to-entry-sort`);
        assert(state.units <= budget, `${prefix}: entry-sort search exceeded its requested budget`);
        completed = state.completed; turns++;
        const observed = observe(context, tree, `${prefix}/advance:${state.phase}`, false,
            "replacement-unmeasured", "replacement-unmeasured");
        sameStores(observed.components.resident, resident,
            `${prefix}: entry-sort search changed resident component`);
        sameStores(observed.components.retiring, EMPTY_STORE,
            `${prefix}: entry-sort search prematurely retired nodes`);
        assert.deepEqual(owner(tree), before, `${prefix}: entry-sort search changed visible owner`);
        const active = observed.sort.replacement?.entries;
        if (active?.sorters === 1) {
            assert.equal(observed.sort.replacement.children.sorters, 0,
                `${prefix}: entry and child sorters overlap`);
            assert(active.values.length_slots > 0,
                `${prefix}: active entry sorter owns no values`);
            assert(active.values.capacity_slots >= active.values.length_slots,
                `${prefix}: entry values capacity is too small`);
            assert(active.source_indices.capacity_slots >= active.values.length_slots,
                `${prefix}: source index capacity is too small`);
            assert(active.target_indices.capacity_slots >= active.values.length_slots,
                `${prefix}: target index capacity is too small`);
            return { turns, completed, phase: state.phase, sort: observed.sort };
        }
        assert(!state.done, `${prefix}: builder reached ready without exposing an entry sorter`);
        assert(completed <= workGuard(rows), `${prefix}: entry-sort search work runaway`);
        if (turns % 256 === 0) await tick();
    }
}

async function build(context, tree, token, rows, before, resident, expected, prefix,
    inputJsonBytes, partial = false) {
    let completed = 0, turns = 0, plan = null, outputPlan = null;
    let previousPostSort, previousPlanningMemory, previousPhase;
    let previousGraph, previousEntries, sawGraphMetadataTransfer = false;
    const graphVectors = new Set();
    let sawGraphChildSort = false;
    let sawDrainedEntryBacking = false, sawEntryBackingRelease = false;
    let previousRanges, previousMetadata, sawRootRangeTransfer = false;
    let uniformPathBytes;
    let sawDescriptorPeak = false, sawExpansionMove = false, sawExpansionTail = false, sawPendingTail = false;
    let sawParentEndpointOwnershipConserved = false;
    const rangeVectors = new Set();
    const expectedPlan = canonicalPayloadPlan(expected, prefix);
    const expectedOutputPlan = expected.version === 2
        ? canonicalReplacementOutputMemoryPlan(expected.chunks, expected.root, prefix)
        : canonicalV1ReplacementOutputMemoryPlan(rows, inputJsonBytes, prefix);
    if (expected.version === 2) rejectReplacementOutputPlanPhase(tree, token, `${prefix}/before-plan`);
    assertReplacementOutputOwnersEmpty(tree, `${prefix}/before-plan`);
    if (expected.version === 1) {
        const beforeResume = context.sample(`${prefix}/v1-output-resume-before`);
        outputPlan = qualifyReplacementOutputPlan(tree, token, null, expectedOutputPlan,
            256, 0, `${prefix}/v1-output-plan`, 1);
        tree.resume_replacement_rebuild_output_memory_v1_job(token, ...outputMemoryWitnesses(outputPlan));
        const afterResume = context.sample(`${prefix}/v1-output-resume-after`);
        const identityBytes = Buffer.byteLength(VAULT) + Buffer.byteLength(DEVICE);
        assert.equal(afterResume.live_requested_bytes - beforeResume.live_requested_bytes, identityBytes,
            `${prefix}: V1 identity copy escaped output admission`);
        assert.equal(afterResume.live_allocations - beforeResume.live_allocations, 2,
            `${prefix}: V1 output resume did not allocate exactly two identities`);
        const resumed = observe(context, tree, `${prefix}/v1-output-resumed`, false,
            "replacement-unmeasured", "replacement-unmeasured");
        assert.deepEqual(resumed.v1Graph.replacement.identities,
            { strings: 2, length_bytes: identityBytes, capacity_bytes: identityBytes,
                counters_valid: true }, `${prefix}: V1 admitted identity owner differs`);
        rejectReplacementOutputPlanPhase(tree, token, `${prefix}/v1-output-resumed`, outputPlan);
    }
    for (;;) {
        // Stop exactly after the final row, then give backing release its own
        // unit. Otherwise a 256-unit sample could conceal premature release.
        const finalLeafDecision = previousPhase === "move-leaf"
            && previousEntries?.rows.owners === 0 && previousEntries?.leaf.owners === 0
            && previousEntries?.retiring_rows.owners === 0;
        // Only the small graph-assembly tail is unit-stepped: a 25k single
        // prefix has 25 child labels, not 25k additional sorting probes.
        const graphBoundary = expected.version === 1 && (previousPhase === "move-internal"
            || previousPhase === "sort-internal" || previousPhase === "retire-internal"
            || (previousGraph?.group_keys.strings === 0
                && (previousPhase === "next-group" || finalLeafDecision)));
        const rangeBoundary = expected.version === 2 && previousRanges !== undefined
            && (RANGE_VECTORS.some(name => previousRanges[name].owners !== 0)
                || previousRanges.descriptor_ranges !== 0);
        const budget = graphBoundary || rangeBoundary ? 1
            : previousPhase === "cleanup entries" && previousPostSort?.entries.owners === 1
                ? Math.max(1, Math.min(256, previousPostSort.entries.length_slots)) : 256;
        const state = progress(tree.step_replacement_rebuild_job(token, budget), completed, true, `${prefix}/build`);
        assert(state.units <= budget, `${prefix}: build exceeded its requested work budget`);
        completed = state.completed; turns++;
        const rootMetadataReady = state.done
            || ["validate closure", "cleanup closure", "cleanup entries", "ready"].includes(state.phase)
            || (expected.version === 1 && ["seed-root", "validate"].includes(state.phase));
        const { components, planning, postSort, v1Graph, v1Entries, metadata, ranges } = observe(context, tree,
            `${prefix}/build:${state.phase}`, false,
            rootMetadataReady ? "exact" : "replacement-unmeasured", "replacement-unmeasured");
        if (expected.version === 1) {
            assertEmptyV2RangesSnapshot(ranges, `${prefix}: V1 build ranges`);
            const graph = v1Graph.replacement;
            for (const name of V1_GRAPH_VECTORS) if (graph[name].length_slots > 0) graphVectors.add(name);
            sawGraphChildSort ||= graph.sorting_children > 0;
            if (previousGraph?.identities.strings === 2 && graph.identities.strings === 0) {
                assert.equal(budget, 1, `${prefix}: root metadata handoff was not isolated`);
                assertV1GraphMetadataTransfer(previousGraph, graph, metadata.replacement,
                    `${prefix}: graph→metadata`);
                sawGraphMetadataTransfer = true;
            }
            previousGraph = graph;
            previousEntries = v1Entries.replacement;
        } else assertEmptyV1GraphSnapshot(v1Graph, `${prefix}: V2 build graph component`);
        if (expected.version === 2) {
            const activeRanges = ranges.replacement;
            assert(activeRanges !== null, `${prefix}: V2 range owner reported unavailable`);
            const entryOwner = postSort.replacement;
            if (uniformPathBytes === undefined && entryOwner?.entries.length_slots > 0) {
                // These existing corpora use uniform unescaped ASCII paths.
                // feed() already checked their UTF-8 lengths from JS input;
                // post-sort transfer preserves that independent input witness.
                uniformPathBytes = entryOwner.paths.length_bytes / entryOwner.entries.length_slots;
                assert(Number.isSafeInteger(uniformPathBytes) && uniformPathBytes > 0,
                    `${prefix}: uniform-path fixture precondition changed`);
            }
            if (uniformPathBytes !== undefined) {
                // Parent emission moves populated endpoint allocations and
                // leaves real empty String values in the old child slots.
                // Every non-empty endpoint in these corpora still has the
                // independently witnessed uniform input length.
                assert.equal(activeRanges.vector_paths.length_bytes % uniformPathBytes, 0,
                    `${prefix}/vector_paths: endpoint UTF-8 bytes differ from accepted corpus`);
                assert(activeRanges.vector_paths.length_bytes / uniformPathBytes
                    <= activeRanges.vector_paths.strings,
                `${prefix}/vector_paths: more populated endpoints than physical String values`);
                // Descriptor maps receive complete RangeRefs; unlike retired
                // child vectors they never contain moved-from endpoints.
                assert.equal(activeRanges.descriptor_paths.length_bytes,
                    activeRanges.descriptor_paths.strings * uniformPathBytes,
                    `${prefix}/descriptor_paths: endpoint UTF-8 bytes differ from accepted corpus`);
            }
            assertEmptyV2RangesMemory(ranges.retiring, `${prefix}: premature range retirement`);
            for (const name of RANGE_VECTORS) if (activeRanges[name].length_slots > 0) rangeVectors.add(name);
            if (previousRanges && activeRanges.ranges.length_slots === previousRanges.ranges.length_slots
                && activeRanges.next_ranges.length_slots === previousRanges.next_ranges.length_slots + 1) {
                assert.equal(activeRanges.vector_paths.strings, previousRanges.vector_paths.strings + 2,
                    `${prefix}: parent move did not retain its two empty donor String values`);
                assert.equal(activeRanges.vector_paths.length_bytes, previousRanges.vector_paths.length_bytes,
                    `${prefix}: parent move cloned or dropped endpoint bytes`);
                assert.equal(activeRanges.vector_paths.capacity_bytes, previousRanges.vector_paths.capacity_bytes,
                    `${prefix}: parent move cloned or dropped endpoint capacity`);
                sawParentEndpointOwnershipConserved = true;
            }
            if (previousRanges && previousMetadata?.replacement === null && metadata.replacement !== null) {
                assert.equal(budget, 1, `${prefix}: root range handoff was not isolated`);
                assertV2RootRangeTransfer(previousRanges, activeRanges, metadata.replacement,
                    `${prefix}: range→metadata/closure clone`);
                sawRootRangeTransfer = true;
            }
            if (previousRanges && state.phase === previousPhase
                && ["retire range level", "cleanup closure"].includes(state.phase)) {
                assertV2RangesRetirement(previousRanges, activeRanges, state.units,
                    `${prefix}: normal range cleanup`);
            }
            if (previousRanges && previousPhase === "validate closure") {
                if (previousRanges.closure_expanding.length_slots > 0) {
                    assert.equal(activeRanges.closure_expanding.length_slots,
                        previousRanges.closure_expanding.length_slots - 1, `${prefix}: expansion did not move one range`);
                    assert.equal(activeRanges.closure_pending.length_slots,
                        previousRanges.closure_pending.length_slots + 1, `${prefix}: pending lost moved range`);
                    assert.deepEqual(activeRanges.vector_paths, previousRanges.vector_paths,
                        `${prefix}: expansion move cloned/dropped endpoint allocations`);
                    assert.deepEqual(activeRanges.descriptor_paths, previousRanges.descriptor_paths);
                    assert.equal(activeRanges.closure_expanding.backing_capacity_bytes,
                        previousRanges.closure_expanding.backing_capacity_bytes,
                        `${prefix}: expansion backing released in last-range unit`);
                    sawExpansionMove = true;
                }
                for (const [name, mark] of [["closure_expanding", "expanding"], ["closure_pending", "pending"]]) {
                    if (previousRanges[name].owners === 1 && activeRanges[name].owners === 0) {
                        assert.equal(state.units, 1);
                        assert.equal(previousRanges[name].length_slots, 0,
                            `${prefix}/${name}: no retained-empty checkpoint before backing release`);
                        assert.equal(assertV2RangesRetirement(previousRanges, activeRanges, state.units,
                            `${prefix}/${name}: tail release`), 1);
                        assert.deepEqual(metadata, previousMetadata, `${prefix}: range tail changed metadata`);
                        assert.deepEqual(planning.replacement, previousPlanningMemory,
                            `${prefix}: range tail changed planning`);
                        assert.deepEqual(postSort.replacement, previousPostSort,
                            `${prefix}: range tail changed entries`);
                        if (mark === "expanding") sawExpansionTail = true; else sawPendingTail = true;
                    }
                }
            }
            if (activeRanges.descriptor_ranges === expectedPlan.nodeCount && expectedPlan.nodeCount > 0) {
                sawDescriptorPeak = true;
                assert.equal(activeRanges.closure_pending.length_slots, 0,
                    `${prefix}: canonical descriptor closure still has unvisited pending ranges`);
                assert.equal(activeRanges.closure_expanding.length_slots, 0);
            }
            const current = postSort.replacement;
            if (previousPostSort !== undefined && previousPostSort !== null) {
                assertPostSortRetirement(previousPostSort, current, state.units, undefined, 0,
                    `${prefix}: normal post-sort cleanup`);
                if (previousPostSort.entries.owners === 1 && current.entries.owners === 0) {
                    assert.equal(previousPostSort.entries.length_slots, 0,
                        `${prefix}: normal cleanup released backing before the empty checkpoint`);
                    assert.equal(budget, 1, `${prefix}: backing release was not qualified separately`);
                    assert.equal(state.units, 1, `${prefix}: backing release consumed no native unit`);
                    assert.deepEqual(planning.replacement, previousPlanningMemory,
                        `${prefix}: entry backing release also changed planning ownership`);
                    assert.deepEqual(activeRanges, previousRanges,
                        `${prefix}: entry backing release also changed range ownership`);
                    sawEntryBackingRelease = true;
                }
            }
            if (current?.entries.owners === 1 && current.entries.length_slots === 0) {
                assert.equal(state.phase, "cleanup entries", `${prefix}: entries vanished before cleanup`);
                assert(current.entries.capacity_slots > 0, `${prefix}: empty checkpoint lost input capacity`);
                assert.equal(current.paths.strings, 0, `${prefix}: empty checkpoint retained paths`);
                sawDrainedEntryBacking = true;
            }
            previousPostSort = current;
            previousRanges = activeRanges;
        }
        previousMetadata = metadata;
        previousPlanningMemory = planning.replacement;
        previousPhase = state.phase;
        sameStores(components.resident, resident, `${prefix}: private build changed resident component`);
        sameStores(components.retiring, EMPTY_STORE, `${prefix}: private build prematurely retired nodes`);
        assert.deepEqual(owner(tree), before, `${prefix}: private build changed visible owner`);
        assert(completed <= workGuard(rows), `${prefix}: build work runaway`);
        if (state.phase === "plan ready") {
            assert.equal(expected.version, 2, `${prefix}: V1 exposed a repeated output plan barrier`);
            assert.equal(plan, null, `${prefix}: V2 exposed more than one node plan barrier`);
            sameStores(components.replacement, EMPTY_STORE, `${prefix}: V2 plan allocated node output`);
            assertEmptyV2RangesSnapshot(ranges, `${prefix}: V2 plan allocated RangeRef owners`);
            plan = payloadPlan(tree.replacement_rebuild_plan_job(token), `${prefix}/plan`);
            assert.deepEqual(plan, expectedPlan, `${prefix}: plan differs from canonical chunks/root`);
            outputPlan = qualifyReplacementOutputPlan(tree, token, plan, expectedOutputPlan,
                budget, completed, `${prefix}/output-plan`, 2);
            for (const witness of [NaN, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, plan.nodePayloadBytes + 1]) {
                assert.throws(() => tree.resume_replacement_rebuild_job(token, witness));
            }
            sameStores(observe(context, tree, `${prefix}/plan-rejected`, false,
                "replacement-unmeasured", "replacement-unmeasured").components.replacement,
                EMPTY_STORE, `${prefix}: rejected witness allocated node output`);
            tree.resume_replacement_rebuild_output_memory_v1_job(token, ...outputMemoryWitnesses(outputPlan));
            rejectReplacementOutputPlanPhase(tree, token, `${prefix}/resumed`, outputPlan);
        }
        // Stop partial construction after reaching graph emission, not merely
        // the first scalar input-validation tick. Ready cancellation is separate.
        const emitted = /retire-leaf|move-internal|emit leaves|validate range|emit internal/.test(state.phase);
        const closureCheckpoint = partial === "closure" && ranges.replacement?.closure_pending.length_slots > 0
            && ranges.replacement.closure_expanding.length_slots > 0 && ranges.replacement.descriptor_ranges > 0;
        const parentCheckpoint = partial === "parent" && sawParentEndpointOwnershipConserved
            && ranges.replacement?.ranges.length_slots > 0
            && ranges.replacement.next_ranges.length_slots > 0;
        if (state.done || (partial === true && emitted) || closureCheckpoint || parentCheckpoint) {
            if (partial) assert(!state.done, `${prefix}: partial fixture only reached ready`);
            if (partial === "closure") assert(closureCheckpoint, `${prefix}: missed active closure cancellation checkpoint`);
            if (partial === "parent") assert(parentCheckpoint,
                `${prefix}: missed moved-parent cancellation checkpoint`);
            assert.equal(plan !== null, expected.version === 2,
                `${prefix}: node plan barrier count differs from tree version contract`);
            assert(outputPlan !== null, `${prefix}: output memory plan barrier was skipped`);
            if (state.done && expected.version === 2) {
                assert(sawDrainedEntryBacking && sawEntryBackingRelease,
                    `${prefix}: ready skipped empty-entry/backing-release checkpoints`);
                assertEmptyV2PostSortSnapshot(postSort, `${prefix}: ready post-sort cleanup`);
                assertEmptyV2RangesSnapshot(ranges, `${prefix}: ready range cleanup`);
                assert(sawRootRangeTransfer && sawDescriptorPeak && sawExpansionMove && sawExpansionTail
                    && sawPendingTail && sawParentEndpointOwnershipConserved,
                    `${prefix}: missing root/closure/move/backing checkpoints`);
                assert.deepEqual([...rangeVectors].sort(), RANGE_VECTORS.slice().sort(),
                    `${prefix}: full V2 build did not expose all four range vectors`);
            }
            if (state.done && expected.version === 1) {
                assert(sawGraphMetadataTransfer, `${prefix}: V1 skipped graph→metadata transfer checkpoint`);
                assertEmptyV1GraphSnapshot(v1Graph, `${prefix}: ready graph cleanup`);
                assert(components.replacement.payload_bytes <= outputPlan.nodePayloadBytes,
                    `${prefix}: V1 ready node output exceeded admission`);
                const retainedRootBytes = metadata.replacement.committed.strings.capacity_bytes
                    + metadata.replacement.committed.v1_children_backing_capacity_bytes
                    + metadata.replacement.tree_ids.capacity_bytes;
                assert(retainedRootBytes <= outputPlan.rangeEndpointResidentRequestedBytes,
                    `${prefix}: V1 ready root output exceeded admission`);
                assert(components.replacement.payload_bytes + retainedRootBytes
                    <= outputPlan.residentAdmissionBytes,
                `${prefix}: V1 ready output total exceeded resident admission`);
            }
            return { turns, completed, phase: state.phase, plan, output_plan: outputPlan,
                graph: expected.version === 1 ? { vectors: [...graphVectors].sort(),
                    child_sort: sawGraphChildSort, metadata_transfer: sawGraphMetadataTransfer } : null,
                ranges: expected.version === 2 ? { vectors: [...rangeVectors].sort(), root_transfer: sawRootRangeTransfer,
                    descriptor_peak: sawDescriptorPeak, expansion_move: sawExpansionMove,
                    expansion_tail: sawExpansionTail, pending_tail: sawPendingTail,
                    parent_endpoint_ownership_conserved: sawParentEndpointOwnershipConserved } : null,
                post_sort_cleanup: expected.version === 2
                    ? { empty_backing_observed: sawDrainedEntryBacking, release_observed: sawEntryBackingRelease }
                    : null };
        }
        if (turns % 256 === 0) await tick();
    }
}

function sortBackingAllocations(value) {
    return [value.values, value.source_indices, value.target_indices]
        .filter(vector => vector.capacity_slots > 0).length;
}

function assertSortRetirement(previous, current, units, label) {
    let released = 0, retiredValues = 0;
    for (const kind of ["entries", "children"]) {
        const before = previous[kind], after = current[kind];
        assert(after.sorters <= before.sorters, `${label}/${kind}: sorter reappeared during retirement`);
        if (before.sorters === 0) {
            assertEmptyIndirectSort(after, `${label}/${kind}`);
            continue;
        }
        if (after.sorters === 1) {
            for (const field of ["values", "source_indices", "target_indices"]) {
                assert(after[field].length_slots <= before[field].length_slots,
                    `${label}/${kind}/${field}: Vec length increased`);
                assert(after[field].capacity_slots <= before[field].capacity_slots,
                    `${label}/${kind}/${field}: Vec capacity increased`);
                assert(after[field].backing_capacity_bytes <= before[field].backing_capacity_bytes,
                    `${label}/${kind}/${field}: Vec backing increased`);
                assert.equal(after[field].slot_size_bytes, before[field].slot_size_bytes,
                    `${label}/${kind}/${field}: Vec slot size changed`);
            }
        }
        retiredValues += before.values.length_slots - after.values.length_slots;
        released += sortBackingAllocations(before) - sortBackingAllocations(after);
    }
    assert(retiredValues >= 0, `${label}: sort values reappeared`);
    assert(retiredValues <= units, `${label}: one retirement unit destroyed multiple sort values`);
    assert(released >= 0, `${label}: sort backing allocation count increased`);
    assert(released <= units, `${label}: one retirement unit released multiple sort backings`);
}

function assertV2PlanningRetirement(previous, current, units, label) {
    let retiredDescriptors = 0, releasedOwners = 0;
    for (const name of PLANNING_VECTORS) {
        const before = previous[name], after = current[name];
        assert(after.owners <= before.owners, `${label}/${name}: Vec owner reappeared`);
        assert(after.length_slots <= before.length_slots,
            `${label}/${name}: descriptor length increased`);
        if (after.owners === 1) {
            assert.equal(before.owners, 1, `${label}/${name}: Vec owner reappeared`);
            assert.equal(after.capacity_slots, before.capacity_slots,
                `${label}/${name}: retained Vec capacity changed`);
            assert.equal(after.slot_size_bytes, before.slot_size_bytes,
                `${label}/${name}: retained Vec slot size changed`);
            assert.equal(after.backing_capacity_bytes, before.backing_capacity_bytes,
                `${label}/${name}: retained Vec backing changed`);
        } else assert.equal(after.length_slots, 0,
            `${label}/${name}: absent Vec retained descriptors`);
        if (units === 1 && before.owners === 1 && after.owners === 0) {
            assert.equal(before.length_slots, 0,
                `${label}/${name}: Vec backing released with live descriptors`);
        }
        retiredDescriptors += before.length_slots - after.length_slots;
        releasedOwners += before.owners - after.owners;
    }
    assert(retiredDescriptors + releasedOwners <= units,
        `${label}: retirement unit destroyed multiple planning owners/descriptors`);
}

function assertPostSortRetirement(previous, current, units, initial, completed, label) {
    if (previous === null) {
        // Cancelling a sorter destroys its rows; it must never advertise a
        // populated post-sort owner while the sorter is being retired.
        if (current !== null) assertEmptyInputMemory(current, `${label}: cancelled sorter tail`);
        return;
    }
    assert(current !== null, `${label}: exact post-sort ownership became unmeasured`);
    const before = previous.entries, after = current.entries;
    assert(after.owners <= before.owners, `${label}: entry Vec owner reappeared`);
    assert(after.length_slots <= before.length_slots, `${label}: entry count increased`);
    const retired = before.length_slots - after.length_slots;
    const released = before.owners - after.owners;
    assert(retired + released <= units, `${label}: entry/drop release exceeded native units`);
    if (after.owners === 1) {
        for (const field of ["capacity_slots", "slot_size_bytes", "backing_capacity_bytes"])
            assert.equal(after[field], before[field], `${label}: retained entry ${field} changed`);
    } else assertEmptyInputMemory(current, `${label}: released post-sort owner`);
    for (const field of ["strings", "length_bytes", "capacity_bytes"])
        assert(current.paths[field] <= previous.paths[field], `${label}: retained path ${field} increased`);
    if (retired === 0) assert.deepEqual(current.paths, previous.paths,
        `${label}: path allocation changed without an entry retirement`);

    if (initial?.entries.owners !== 1) return;
    const count = initial.entries.length_slots;
    if (completed > count) {
        assertEmptyInputMemory(current, `${label}: entry backing outlived its release unit`);
        return;
    }
    const remaining = count - completed;
    assert.equal(after.owners, 1, `${label}: entry backing released in its final row unit`);
    assert.equal(after.length_slots, remaining, `${label}: post-sort rows retired out of order`);
    assert.equal(current.paths.strings, remaining, `${label}: post-sort path count differs`);
    // Existing corpora and the planning probe have uniform, unescaped path
    // shapes. Input→post-sort equality supplies the initial capacity oracle.
    for (const field of ["length_bytes", "capacity_bytes"]) {
        if (count === 0) assert.equal(current.paths[field], 0, `${label}: empty path ${field}`);
        else {
            assert.equal(initial.paths[field] % count, 0, `${label}: nonuniform path ${field} oracle`);
            assert.equal(current.paths[field], initial.paths[field] / count * remaining,
                `${label}: post-sort path ${field} differs from remaining rows`);
        }
    }
}

function cleanupRowsFor(input, rows, fallback) {
    const exact = input.retiring?.entries.owners === 1;
    if (!exact) return fallback;
    assert(Array.isArray(rows), "failure cleanup lacks an exact accepted-row oracle");
    const count = input.retiring.entries.length_slots;
    assert(count <= rows.length, "failure cleanup input exceeds its accepted-row oracle");
    return rows.slice(0, count);
}

async function retire(context, tree, token, rows, before, transferred, prefix, transferredInput,
    transferredSort, transferredPlanning, maxBudget = 256) {
    assert(Number.isSafeInteger(maxBudget) && maxBudget >= 1 && maxBudget <= 256,
        `${prefix}: invalid retirement qualification budget`);
    const rowCount = Array.isArray(rows) ? rows.length : rows;
    const exactInputRows = transferredInput?.retiring?.entries.owners === 1 ? rows : null;
    assert(exactInputRows === null || Array.isArray(exactInputRows),
        `${prefix}: exact input retirement requires its row oracle`);
    const initialInput = exactInputRows === null ? null : transferredInput.retiring;
    const postSortStart = replacementV2PostSortSnapshot(tree, `${prefix}: post-sort retirement start`);
    assertEmptyInputMemory(postSortStart.replacement, `${prefix}: transferred post-sort replacement`);
    let previousPostSort = postSortStart.retiring;
    const initialPostSort = previousPostSort;
    const v1EntriesStart = replacementV1EntriesSnapshot(tree, `${prefix}: V1 entry retirement start`);
    assertEmptyV1EntriesMemory(v1EntriesStart.replacement,
        `${prefix}: transferred V1 entry replacement`);
    let previousV1Entries = v1EntriesStart.retiring;
    const graphStart = replacementV1GraphSnapshot(tree, `${prefix}: graph retirement start`);
    assertEmptyV1GraphMemory(graphStart.replacement, `${prefix}: transferred graph replacement`);
    let previousGraph = graphStart.retiring;
    const rangesStart = replacementV2RangesSnapshot(tree, `${prefix}: ranges retirement start`);
    assertEmptyV2RangesMemory(rangesStart.replacement, `${prefix}: transferred range replacement`);
    let previousRanges = rangesStart.retiring;
    let completed = 0, turns = 0;
    let previous = transferred.retiring;
    let previousMetadata = metadataSnapshot(tree, `${prefix}: metadata retirement start`);
    let previousSort = transferredSort?.retiring;
    if (previousSort !== undefined) {
        sortCoverage(transferredSort, "exact", `${prefix}: transferred sort coverage`);
        assertEmptySortMemory(transferredSort.replacement, `${prefix}: transferred replacement sort`);
        assert(previousSort !== null, `${prefix}: retiring sort was reported unmeasured`);
    }
    let previousPlanning = transferredPlanning?.retiring;
    if (previousPlanning !== undefined) {
        assert.equal(transferredPlanning.other_planning_owners_unmeasured, false,
            `${prefix}: transferred V2 planning coverage is incomplete`);
        assertEmptyV2PlanningMemory(transferredPlanning.replacement,
            `${prefix}: transferred replacement V2 planning`);
        assert(previousPlanning !== null,
            `${prefix}: retiring V2 planning was reported unmeasured`);
    }
    let inputExact = transferredInput !== undefined && transferredInput.retiring !== null;
    for (;;) {
        const remainingInputRows = initialInput === null ? null : Math.max(rowCount - completed, 0);
        const remainingPostSortRows = initialPostSort?.entries.owners === 1
            ? Math.max(initialPostSort.entries.length_slots - completed, 0) : null;
        const remainingSortRows = previousSort?.entries.sorters === 1
            ? previousSort.entries.values.length_slots : null;
        const remainingRows = remainingInputRows ?? remainingPostSortRows ?? remainingSortRows;
        const isolatingChildSorter = previousSort?.children.sorters === 1;
        const isolatingRanges = remainingRows === null && previousRanges !== null
            && (RANGE_VECTORS.some(name => previousRanges[name].owners !== 0)
                || previousRanges.descriptor_ranges !== 0);
        const budget = isolatingChildSorter || isolatingRanges ? 1 : remainingRows === null
            ? maxBudget : Math.max(1, Math.min(maxBudget, remainingRows));
        const state = progress(tree.step_tree_retirement(token, budget), completed, false, `${prefix}/retirement`);
        assert(state.units <= budget, `${prefix}: retirement exceeded its requested work budget`);
        completed = state.completed; turns++;
        const { components, metadata, input, sort, planning, postSort, v1Entries, v1Graph, ranges } = observe(
            context, tree, `${prefix}/retirement`);
        const beforeComponents = previous;
        const beforeMetadata = previousMetadata;
        const beforeV1Entries = previousV1Entries;
        const beforePostSort = previousPostSort;
        assertEmptyV2RangesMemory(ranges.replacement, `${prefix}: retirement range replacement`);
        const rangeWork = assertV2RangesRetirement(previousRanges, ranges.retiring, state.units,
            `${prefix}: RangeRef retirement`);
        if (rangeWork > 0 && state.units === 1) {
            assert.deepEqual(components.retiring, beforeComponents,
                `${prefix}: range unit also retired chunk buffers`);
            assert.deepEqual(metadata, beforeMetadata, `${prefix}: range unit also retired root metadata`);
            assert.deepEqual(postSort.retiring, beforePostSort, `${prefix}: range unit also retired entries`);
            assert.deepEqual(v1Entries.retiring, beforeV1Entries, `${prefix}: range unit changed V1 entries`);
            assert.deepEqual(v1Graph.retiring, previousGraph, `${prefix}: range unit changed V1 graph`);
            if (previousSort !== undefined) assert.deepEqual(sort.retiring, previousSort,
                `${prefix}: range unit also retired sort backing`);
            if (previousPlanning !== undefined) assert.deepEqual(planning.retiring, previousPlanning,
                `${prefix}: range unit also retired planning backing`);
        }
        assertEmptyV1EntriesMemory(v1Entries.replacement,
            `${prefix}: retirement V1 entry replacement`);
        const entryWork = assertV1EntriesRetirement(beforeV1Entries, v1Entries.retiring, state.units,
            `${prefix}: V1 entry retirement`);
        assertEmptyV1GraphMemory(v1Graph.replacement, `${prefix}: retirement graph replacement`);
        const graphWork = assertV1GraphRetirement(previousGraph, v1Graph.retiring, state.units,
            `${prefix}: graph retirement`);
        let sortBackings = 0;
        if (previousSort !== undefined) for (const kind of ["entries", "children"])
            sortBackings += sortBackingAllocations(previousSort[kind])
                - sortBackingAllocations(sort.retiring[kind]);
        assert(entryWork + graphWork + rangeWork + sortBackings <= state.units,
            `${prefix}: one unit retired multiple disjoint entry/graph/range/sort owners`);
        assertEmptyInputMemory(postSort.replacement, `${prefix}: retirement post-sort replacement`);
        const releasedPostSort = beforePostSort?.entries.owners === 1
            && postSort.retiring?.entries.owners === 0;
        assertPostSortRetirement(beforePostSort, postSort.retiring, state.units,
            initialPostSort, completed, `${prefix}: post-sort retirement`);
        if (releasedPostSort) {
            assert.equal(state.units, 1,
                `${prefix}: post-sort backing release was not isolated to one unit`);
            if (previousSort !== undefined) {
                assert.deepEqual(sort.retiring, previousSort,
                    `${prefix}: post-sort backing release also changed sort ownership`);
            }
            if (previousPlanning !== undefined) {
                assert.deepEqual(planning.retiring, previousPlanning,
                    `${prefix}: post-sort backing release also changed planning ownership`);
            }
            assert.deepEqual(ranges.retiring, previousRanges,
                `${prefix}: post-sort backing release also changed ranges`);
        }
        assertEmptySortMemory(sort.replacement, `${prefix}: retirement replacement sort`);
        if (previousSort !== undefined) {
            assert(sort.retiring !== null, `${prefix}: retirement sort became unmeasured`);
            const releasedEntrySorter = previousSort.entries.sorters === 1
                && sort.retiring.entries.sorters === 0;
            const releasedChildSorter = previousSort.children.sorters === 1
                && sort.retiring.children.sorters === 0;
            assertSortRetirement(previousSort, sort.retiring, state.units,
                `${prefix}: retirement sort`);
            if (releasedEntrySorter || releasedChildSorter) {
                assert.equal(state.units, 1,
                    `${prefix}: sorter values backing release was not isolated to one unit`);
                assert.deepEqual(components.retiring, beforeComponents,
                    `${prefix}: sorter tail also retired a chunk owner`);
                assert.deepEqual(metadata, beforeMetadata,
                    `${prefix}: sorter tail also changed metadata ownership`);
                assert.deepEqual(v1Graph.retiring, previousGraph,
                    `${prefix}: sorter backing tail also changed graph ownership`);
                assert.deepEqual(ranges.retiring, previousRanges,
                    `${prefix}: sorter backing tail also changed range ownership`);
            }
            if (releasedEntrySorter) {
                assertEmptyInputMemory(postSort.retiring,
                    `${prefix}: sorter tail fabricated post-sort rows`);
            }
            if (releasedChildSorter) {
                assert.deepEqual(v1Entries.retiring, beforeV1Entries,
                    `${prefix}: child sorter tail also retired a V1 entry owner`);
                assert.deepEqual(postSort.retiring, beforePostSort,
                    `${prefix}: child sorter tail also retired post-sort ownership`);
            }
            if ((releasedEntrySorter || releasedChildSorter)
                && previousPlanning !== undefined) {
                assert.deepEqual(planning.retiring, previousPlanning,
                    `${prefix}: sorter tail also retired a planning owner`);
            }
            previousSort = sort.retiring;
        }
        previousV1Entries = v1Entries.retiring;
        previousGraph = v1Graph.retiring;
        previousRanges = ranges.retiring;
        previousPostSort = postSort.retiring;
        previousMetadata = metadata;
        assertEmptyV2PlanningMemory(planning.replacement,
            `${prefix}: retirement replacement V2 planning`);
        if (previousPlanning !== undefined) {
            assert(planning.retiring !== null,
                `${prefix}: retirement V2 planning became unmeasured`);
            assertV2PlanningRetirement(previousPlanning, planning.retiring, state.units,
                `${prefix}: retirement V2 planning`);
            previousPlanning = planning.retiring;
        }
        assertEmptyInputMemory(input.replacement, `${prefix}: retirement replacement input`);
        if (initialInput !== null && completed <= rowCount) {
            assertRetiringInputRows(input, initialInput, exactInputRows, completed,
                `${prefix}: retirement input`);
        } else if (initialInput !== null) {
            inputCoverage(input, "exact", `${prefix}: released input coverage`);
            assertEmptyInputMemory(input.retiring, `${prefix}: released input backing`);
        } else if (inputExact) {
            inputCoverage(input, "exact", `${prefix}: retirement input coverage`);
            assertEmptyInputMemory(input.retiring, `${prefix}: exact non-input retirement`);
        }
        else if (input.retiring === null) inputCoverage(input, "retiring-unmeasured",
            `${prefix}: retirement input coverage`);
        else {
            inputCoverage(input, "exact", `${prefix}: retirement input coverage`);
            assertEmptyInputMemory(input.retiring, `${prefix}: measured non-input retirement`);
            inputExact = true;
        }
        sameStores(components.resident, transferred.resident, `${prefix}: retirement changed resident component`);
        sameStores(components.replacement, EMPTY_STORE, `${prefix}: retired nodes returned to replacement`);
        for (const field of STORE_NUMBERS)
            assert(components.retiring[field] <= previous[field], `${prefix}: retiring ${field} increased`);
        previous = components.retiring;
        assert.deepEqual(owner(tree), before, `${prefix}: retirement changed published/candidate owner`);
        assert(completed <= workGuard(rowCount), `${prefix}: retirement work runaway`);
        if (state.done) {
            assertEmptyV2RangesSnapshot(ranges, `${prefix}: finished range retirement`);
            assertEmptyV1GraphSnapshot(v1Graph, `${prefix}: finished graph retirement`);
            sameStores(components.retiring, EMPTY_STORE, `${prefix}: finished retirement retained a component`);
            assertEmptyTreeMetadata(metadata.retiring, `${prefix}: finished metadata retirement`);
            assert.equal(metadata.other_private_jobs_unmeasured, false,
                `${prefix}: drained retirement remained partially measured`);
            assertEmptyInputMemory(input.retiring, `${prefix}: finished input retirement`);
            assert.equal(input.other_input_owners_unmeasured, false,
                `${prefix}: drained input retirement remained partially measured`);
            assertEmptySortMemory(sort.retiring, `${prefix}: finished sort retirement`);
            assert.equal(sort.other_sort_owners_unmeasured, false,
                `${prefix}: drained sort retirement remained partially measured`);
            assertEmptyV2PlanningMemory(planning.retiring,
                `${prefix}: finished V2 planning retirement`);
            assert.equal(planning.other_planning_owners_unmeasured, false,
                `${prefix}: drained V2 planning retirement remained partially measured`);
            assertEmptyV2PostSortSnapshot(postSort, `${prefix}: finished post-sort retirement`);
            assertEmptyV1EntriesSnapshot(v1Entries,
                `${prefix}: finished V1 entry retirement`);
            return { turns, completed };
        }
        if (turns % 256 === 0) await tick();
    }
}

async function qualifyHandlesAndIsolation(variant) {
    const first = await fresh(variant, `${variant.name}/handles`), second = await fresh(variant, `${variant.name}/isolation`);
    assert.notEqual(first.native.memory.buffer, second.native.memory.buffer, "two imports share one native memory instance");
    const secondBefore = second.sample("unrelated-before");
    const tree = requiredTree(first.module), hasher = new first.module.Hasher();
    try {
        const payload = new Uint8Array(256 * 1024).fill(0x57);
        hasher.update(payload);
        assert.equal(hasher.finalize(), first.module.wasm_hash(payload));
        const empty = observe(first, tree, "handles-empty-tree", false, "exact", "exact");
        for (const name of STORES) sameStores(empty.components[name], EMPTY_STORE, `handles: unexpected ${name} nodes`);
        assertEmptyInputSnapshot(empty.input, "handles: empty tree input");
        assertEmptyV1EntriesSnapshot(empty.v1Entries, "handles: empty tree V1 entries");
        assertEmptySortSnapshot(empty.sort, "handles: empty tree sort");
        assertEmptyV2PlanningSnapshot(empty.planning, "handles: empty tree V2 planning");
        assertEmptyV2RangesSnapshot(empty.ranges, "handles: empty tree V2 ranges");
        tree.rebuild_from_entries_in_version(1, "[]");
        tree.begin_candidate();
        const graphToken = tokenValue(tree.begin_candidate_chunks_job());
        try {
            const graph = observe(first, tree, "other-private-graph-job", true, "other-unmeasured", "exact");
            assertEmptyV2RangesSnapshot(graph.ranges, "handles: general graph job is outside replacement ranges");
            for (const name of STORES) sameStores(graph.components[name], EMPTY_STORE, `graph job: fabricated ${name} nodes`);
        } finally { tree.cancel_tree_job(graphToken); }
        const mutationToken = tokenValue(tree.begin_candidate_update_job(JSON.stringify([{
            path: "notes/mutation.md", hash: "a5".repeat(32),
            mtime_ms: 1_700_000_000_000, size: 128,
        }])));
        try {
            const mutation = observe(first, tree, "other-private-sort-job", true,
                "other-unmeasured", "exact", "replacement-unmeasured");
            assert.equal(mutation.sort.replacement, null,
                "candidate mutation fabricated exact replacement-sort ownership");
            assert.equal(mutation.sort.other_sort_owners_unmeasured, true,
                "candidate mutation did not expose its unmeasured sort owner");
        } finally { tree.cancel_tree_job(mutationToken); }
        tree.abort_candidate();
        const reported = observe(first, tree, "handles-live", false, "exact", "exact");
        for (const name of STORES) sameStores(reported.components[name], EMPTY_STORE, `handles: cancelled graph left ${name}`);
        assertEmptyInputSnapshot(reported.input, "handles: live tree input");
        assertEmptyV1EntriesSnapshot(reported.v1Entries, "handles: live tree V1 entries");
        assertEmptySortSnapshot(reported.sort, "handles: live tree sort");
        assertEmptyV2PlanningSnapshot(reported.planning, "handles: live tree V2 planning");
        assertEmptyV2RangesSnapshot(reported.ranges, "handles: live tree V2 ranges");
        sameLive(observe(first, tree, "component-report-repeat").memory, reported.memory,
            "repeated component reporting retains live allocations");
        const live = reported.memory;
        assert(live.live_requested_bytes > first.baseline.live_requested_bytes);
        assert(live.live_allocations > first.baseline.live_allocations);
        const independent = second.sample("unrelated-after");
        sameLive(independent, secondBefore, "another WASM instance changed live metrics");
        assert.equal(independent.peak_requested_bytes, secondBefore.peak_requested_bytes,
            "another WASM instance changed lifetime allocation peak");
        assert.equal(independent.linear_memory_bytes, secondBefore.linear_memory_bytes);
    } finally { hasher.free(); tree.free(); }
    const final = first.sample("handles-freed");
    sameLive(final, first.baseline, `${variant.name}: freed real handles`);
    sameLive(first.sample("handles-freed-repeat"), final, "reporting itself leaks requested-live bytes");
    return { variant: variant.name, baseline_requested_bytes: first.baseline.live_requested_bytes,
        baseline_live_allocations: first.baseline.live_allocations, independent_instances: true,
        phases: first.report() };
}

function mutationMemoryPlan(value, label, expectedScope = "v2-candidate-mutation-output") {
    const fields = ["schema", "scope", "nodePayloadBytes", "rangeEndpointPeakRequestedBytes",
        "rangeEndpointResidentRequestedBytes", "peakAdmissionBytes", "residentAdmissionBytes"];
    exactFields(value, fields, label);
    assert.equal(value.schema, 1); assert.equal(value.scope, expectedScope);
    for (const field of fields.slice(2)) safeCount(value[field], `${label}/${field}`);
    assert(value.rangeEndpointResidentRequestedBytes <= value.rangeEndpointPeakRequestedBytes);
    safeCount(value.nodePayloadBytes + value.rangeEndpointPeakRequestedBytes, label);
    safeCount(value.nodePayloadBytes + value.rangeEndpointResidentRequestedBytes, label);
    assert.equal(value.peakAdmissionBytes, value.nodePayloadBytes + value.rangeEndpointPeakRequestedBytes);
    assert.equal(value.residentAdmissionBytes, value.nodePayloadBytes + value.rangeEndpointResidentRequestedBytes);
    return value;
}

function mutationMemoryReady(value, plan, label) {
    exactFields(value, ["schema", "scope", "stagedNodePayloadBytes",
        "rangeEndpointResidentRequestedBytes", "residentAdmissionBytes"], label);
    assert.equal(value.schema, 1); assert.equal(value.scope, plan.scope);
    for (const field of ["stagedNodePayloadBytes", "rangeEndpointResidentRequestedBytes", "residentAdmissionBytes"])
        safeCount(value[field], `${label}/${field}`);
    assert(value.stagedNodePayloadBytes <= plan.nodePayloadBytes);
    if (plan.scope === "v1-candidate-mutation-output")
        assert(value.rangeEndpointResidentRequestedBytes <= plan.rangeEndpointResidentRequestedBytes);
    else assert.equal(value.rangeEndpointResidentRequestedBytes, plan.rangeEndpointResidentRequestedBytes);
    safeCount(value.stagedNodePayloadBytes + value.rangeEndpointResidentRequestedBytes, label);
    assert.equal(value.residentAdmissionBytes, value.stagedNodePayloadBytes + value.rangeEndpointResidentRequestedBytes);
    assert(value.residentAdmissionBytes <= plan.residentAdmissionBytes);
    return value;
}

function canonicalV1MutationOutputMemoryPlan(tree, row, label) {
    const candidate = metadataSnapshot(tree, `${label}/metadata`).resident.candidate;
    assert.equal(candidate.roots, 1, `${label}: fixture candidate root missing`);
    const slotBytes = candidate.v1_children_capacity === 0 ? 0
        : candidate.v1_children_backing_capacity_bytes / candidate.v1_children_capacity;
    assert(Number.isSafeInteger(slotBytes), `${label}: invalid V1 child slot width`);
    const rootCloneRequestedBytes = candidate.strings.length_bytes
        + candidate.v1_children_length * slotBytes;
    const resident = chunkSnapshot(tree, `${label}/chunks`, false).resident;
    const pathBytes = Buffer.byteLength(row.path);
    const nodePayloadBytes = resident.payload_bytes * 2 + pathBytes * 4 + 512 + 128 * 1024;
    const rootResident = rootCloneRequestedBytes * 2 + pathBytes * 4 + 256 + 128 * 1024;
    const rootPeak = rootCloneRequestedBytes + rootResident;
    const plan = mutationMemoryPlan({ schema: 1, scope: "v1-candidate-mutation-output",
        nodePayloadBytes, rangeEndpointPeakRequestedBytes: rootPeak,
        rangeEndpointResidentRequestedBytes: rootResident,
        peakAdmissionBytes: nodePayloadBytes + rootPeak,
        residentAdmissionBytes: nodePayloadBytes + rootResident }, label,
    "v1-candidate-mutation-output");
    return { plan, rootCloneRequestedBytes,
        rootCloneAllocations: candidate.strings.strings + Number(candidate.v1_children_length > 0) };
}

function v2OutputSettlementTarget(module, tree, outcome, label) {
    assert.equal(tree.tree_version(), 2, `${label}: settlement target is not V2`);
    assert.equal(tree.has_candidate(), true, `${label}: settlement target lacks candidate`);
    const commit = outcome === "commit";
    const rootBytes = commit ? tree.candidate_root_bytes() : tree.root_bytes();
    const rootHash = commit ? tree.candidate_root_hash_hex() : tree.root_hash_hex();
    const totalFiles = commit ? tree.candidate_total_files() : tree.total_files();
    assert(rootBytes instanceof Uint8Array && typeof rootHash === "string", `${label}: missing target root`);
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
    return { root: Buffer.from(rootBytes).toString("hex"), rootHash, totalFiles, hashes,
        nodePayloadBytes: memory.nodePayloadBytes,
        rangeEndpointResidentRequestedBytes: memory.rangeEndpointResidentRequestedBytes,
        residentAdmissionBytes: memory.residentAdmissionBytes };
}

function v2OutputSettlementReport(value, expected, label) {
    exactFields(value, V2_OUTPUT_SETTLEMENT_FIELDS, label);
    for (const field of ["treeVersion", "before", "reachable", "removed", "after", "bytesRemoved",
        "committedRevision", "candidateRevision", "nodePayloadBytes",
        "rangeEndpointResidentRequestedBytes", "residentAdmissionBytes"])
        safeCount(value[field], `${label}/${field}`);
    assert.equal(value.schema, 1); assert.equal(value.scope, "v2-stable-tree-output");
    assert.equal(value.outcome, expected.outcome); assert.equal(value.treeVersion, 2);
    assert.equal(value.countersValid, true);
    assert.equal(value.after, value.reachable);
    assert.equal(value.removed, value.before - value.after);
    safeCount(value.nodePayloadBytes + value.rangeEndpointResidentRequestedBytes, label);
    assert.equal(value.residentAdmissionBytes,
        value.nodePayloadBytes + value.rangeEndpointResidentRequestedBytes);
    assert.deepEqual(value, expected, `${label}: report differs from independent physical oracle`);
    return value;
}

function settleV2OutputMemory(module, tree, outcome, label,
    readStore = phase => chunkSnapshot(tree, `${label}/${phase}`).resident) {
    const target = v2OutputSettlementTarget(module, tree, outcome, `${label}/target`);
    const beforeStore = readStore("before");
    const beforeCommitted = tree.committed_revision(), beforeCandidate = tree.candidate_revision();
    for (const [field, value] of [["committedRevision", beforeCommitted],
        ["candidateRevision", beforeCandidate]]) safeCount(value, `${label}/${field}`);
    assert.equal(beforeStore.counters_valid, true);
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
        safeCount(expected[field], `${label}/expected/${field}`);
    const raw = tree[outcome === "commit" ? "commit_candidate_output_settlement_v1"
        : "abort_candidate_output_settlement_v1"]();
    const report = v2OutputSettlementReport(raw, expected, `${label}/report`);
    const afterStore = readStore("after");
    assert.equal(tree.has_candidate(), false, `${label}: settlement retained candidate`);
    assert.equal(tree.committed_revision(), report.committedRevision);
    assert.equal(tree.candidate_revision(), report.candidateRevision);
    assert.equal(tree.root_hash_hex(), target.rootHash);
    assert.equal(tree.total_files(), target.totalFiles);
    assert.equal(Buffer.from(tree.root_bytes()).toString("hex"), target.root);
    assert.deepEqual(module.wasm_tree_committed_chunk_hashes(tree).slice().sort(), target.hashes,
        `${label}: committed reachability differs from target`);
    assert.equal(afterStore.counters_valid, true);
    assert.equal(afterStore.chunks, target.hashes.length);
    assert.equal(afterStore.payload_bytes, target.nodePayloadBytes);
    return report;
}

function rejectV1OutputSettlementMemory(module, tree, label) {
    const row = { path: "v1-settlement.md", hash: "cd".repeat(32), size: 2, mtime_ms: 2 };
    tree.rebuild_from_entries_in_version(1, JSON.stringify([row]));
    tree.begin_candidate();
    tree.candidate_update_batch(JSON.stringify([{ ...row, hash: "ef".repeat(32) }]));
    const before = visible(module, tree);
    const beforeStore = chunkSnapshot(tree, `${label}/before`).resident;
    for (const method of ["commit_candidate_output_settlement_v1",
        "abort_candidate_output_settlement_v1"]) {
        assert.throws(() => tree[method](), `${label}: ${method} accepted V1`);
        assert.deepEqual(visible(module, tree), before, `${label}: ${method} changed V1 ownership`);
        sameStores(chunkSnapshot(tree, `${label}/${method}`).resident, beforeStore,
            `${label}: ${method} changed the V1 physical store`);
    }
    tree.abort_candidate();
}

function mutationMemoryStep(value, previous, budget, label) {
    exactFields(value, ["done", "units", "completed", "remaining", "reachable", "phase"], label);
    assert.equal(typeof value.done, "boolean"); assert.equal(typeof value.phase, "string");
    for (const field of ["units", "completed", "remaining", "reachable"]) safeCount(value[field], `${label}/${field}`);
    assert(value.units <= budget); assert.equal(value.completed, previous.completed + value.units);
    safeCount(previous.completed + value.units, label);
    assert(value.reachable >= previous.reachable && value.reachable <= value.completed);
    assert.equal(value.remaining, value.done ? 0 : 1);
    assert(value.done || value.units > 0 || value.phase === "plan ready");
    return value;
}

// Warm the new serde field names before, never after, the fixed instance
// baseline. This disposable fixture also pins the required complete ABI.
function warmMutationOutput(module, tree) {
    tree.rebuild_from_entries_in_version(2, "[]"); tree.begin_candidate();
    const token = tree.begin_candidate_update_job(JSON.stringify([{
        path: "warm.md", hash: "ab".repeat(32), size: 1, mtime_ms: 1 }]));
    let previous = { completed: 0, reachable: 0 }, plan;
    for (let turns = 0; ; turns++) {
        assert(turns < 10_000, "mutation warmup did not converge");
        previous = mutationMemoryStep(tree.step_candidate_mutation_output_memory_v1_job(token, 256), previous, 256, "warmup");
        if (previous.phase === "plan ready") {
            assert.equal(plan, undefined);
            plan = mutationMemoryPlan(tree.candidate_mutation_output_memory_plan_v1_job(token), "warmup");
            tree.resume_candidate_mutation_output_memory_v1_job(token, ...outputMemoryWitnesses(plan));
        }
        if (previous.done) break;
    }
    assert(plan); mutationMemoryReady(tree.candidate_mutation_output_memory_ready_v1_job(token), plan, "warmup");
    tree.finish_candidate_mutation_job_deferred(token);
    let completed = 0;
    for (let turns = 0; ; turns++) {
        assert(turns < 10_000, "mutation warmup retirement did not converge");
        const state = progress(tree.step_tree_retirement(token, 256), completed, false, "warmup");
        assert(state.units <= 256); completed = state.completed;
        if (state.done) break;
    }
    settleV2OutputMemory(module, tree, "commit", "warmup-settlement");
    rejectV1OutputSettlementMemory(module, tree, "warmup-v1-settlement");
}

async function qualifyV1CandidateMutationOutput(context, oracleContext) {
    const tree = requiredTree(context.module), oracle = requiredTree(oracleContext.module);
    const budget = 1;
    try {
        const rows = Array.from({ length: 32 }, (_, index) => ({
            path: `v1-output/${String(index).padStart(3, "0")}.md`,
            hash: (index + 1).toString(16).padStart(64, "0"), size: 64 + index,
            mtime_ms: 1_700_000_000_000 + index,
        }));
        const changed = { ...rows[7], hash: "ef".repeat(32), size: 7 };
        const payload = JSON.stringify([changed]);
        for (const target of [tree, oracle]) {
            target.rebuild_from_entries_in_version(1, JSON.stringify(rows));
            target.begin_candidate();
        }
        oracle.candidate_update_batch(payload);
        const expectedRoot = Buffer.from(oracle.candidate_root_bytes()).toString("hex");
        const initial = visible(context.module, tree);
        const baseline = context.sample("v1-mutation/baseline");
        const expected = canonicalV1MutationOutputMemoryPlan(tree, changed, "v1-mutation/oracle");

        const drain = async (target, token, visibleState, phase) => {
            let completed = 0;
            for (let turns = 0; ; turns++) {
                target.cancel_candidate_mutation_job_deferred(token);
                const state = progress(target.step_tree_retirement(token, budget), completed, false, phase);
                assert(state.units <= budget, `${phase}: retirement exceeded its native step budget`);
                completed = state.completed;
                assert.deepEqual(visible(context.module, target), visibleState,
                    `${phase}: retirement changed visible state`);
                if (state.done) return { turns: turns + 1, completed };
                assert(turns < 100_000, `${phase}: retirement runaway`);
                if ((turns & 255) === 0) await tick();
            }
        };

        // A separate wide V1 owner is interrupted after two externally
        // bounded canonical-leaf validations. The second 256-unit call must
        // report exactly one unit, proving cancellation starts while
        // ValidateShape still owns the remaining hashes and previous bound.
        // Retirement itself stays at budget 1 so those owners cannot disappear
        // behind one bulk drop. This runs for both packaged variants through
        // qualifyCandidateMutationOutput's scalar/SIMD loop.
        const beforeShapeOwner = context.sample("v1-mutation/shape-cancel/before-owner");
        const shapeTree = requiredTree(context.module);
        let shapeCancellation;
        try {
            const shapeRows = Array.from({ length: 2_001 }, (_, index) => ({
                path: `v1-shape/${String(index).padStart(4, "0")}.md`,
                hash: (index + 1).toString(16).padStart(64, "0"), size: 96 + index,
                mtime_ms: 1_710_000_000_000 + index,
            }));
            const shapeChanged = { ...shapeRows[7], hash: "ad".repeat(32), size: 11 };
            shapeTree.rebuild_from_entries_in_version(1, JSON.stringify(shapeRows));
            shapeTree.begin_candidate();
            const shapeVisible = visible(context.module, shapeTree);
            const shapeBaseline = context.sample("v1-mutation/shape-cancel/baseline");
            const shapeToken = shapeTree.begin_candidate_update_job(JSON.stringify([shapeChanged]));
            const shapePaused = mutationMemoryStep(
                shapeTree.step_candidate_mutation_output_memory_v1_job(shapeToken, 256),
                { completed: 0, reachable: 0 }, 256, "v1-mutation/shape-cancel/plan-ready");
            assert.equal(shapePaused.phase, "plan ready");
            const shapePlan = mutationMemoryPlan(
                shapeTree.candidate_mutation_output_memory_plan_v1_job(shapeToken),
                "v1-mutation/shape-cancel/plan", "v1-candidate-mutation-output");
            shapeTree.resume_candidate_mutation_output_memory_v1_job(
                shapeToken, ...outputMemoryWitnesses(shapePlan));
            const firstShapeStep = mutationMemoryStep(
                shapeTree.step_candidate_mutation_output_memory_v1_job(shapeToken, 256),
                shapePaused, 256, "v1-mutation/shape-cancel/first-leaf");
            assert(!firstShapeStep.done && firstShapeStep.units > 0 && firstShapeStep.units < 256,
                "wide V1 fixture did not stop at its first leaf-validation boundary");
            const secondShapeStep = mutationMemoryStep(
                shapeTree.step_candidate_mutation_output_memory_v1_job(shapeToken, 256),
                firstShapeStep, 256, "v1-mutation/shape-cancel/second-leaf");
            assert.equal(secondShapeStep.units, 1,
                "wide V1 fixture did not return after exactly one additional leaf validation");
            assert.equal(secondShapeStep.done, false,
                "wide V1 fixture reached Ready before mid-ValidateShape cancellation");
            const beforeShapeTransfer = context.sample("v1-mutation/shape-cancel/before-transfer");
            shapeTree.cancel_candidate_mutation_job_deferred(shapeToken);
            sameLive(context.sample("v1-mutation/shape-cancel/after-transfer"), beforeShapeTransfer,
                "mid-ValidateShape cancellation dropped ownership instead of deferring retirement");
            const cancellation = await drain(shapeTree, shapeToken, shapeVisible,
                "v1-mutation/shape-cancel/retirement");
            sameLive(context.sample("v1-mutation/shape-cancel/cancelled"), shapeBaseline,
                "mid-ValidateShape cancellation did not retire the wide V1 owner");
            shapeCancellation = { entries: shapeRows.length,
                first_units: firstShapeStep.units, second_units: secondShapeStep.units,
                cancellation };
        } finally {
            if (shapeTree.has_candidate()) shapeTree.abort_candidate();
            shapeTree.free();
        }
        sameLive(context.sample("v1-mutation/shape-cancel/freed"), beforeShapeOwner,
            "mid-ValidateShape fixture did not release its tree owner");

        const build = async (exerciseRefusal, phase) => {
            const token = tree.begin_candidate_update_job(payload);
            const parsed = context.sample(`${phase}/parsed-input`);
            const paused = mutationMemoryStep(
                tree.step_candidate_mutation_output_memory_v1_job(token, budget),
                { completed: 0, reachable: 0 }, budget, `${phase}/plan-ready`);
            assert.deepEqual(paused, { done: false, units: 0, completed: 0,
                remaining: 1, reachable: 0, phase: "plan ready" });
            sameLive(context.sample(`${phase}/plan-ready`), parsed,
                `${phase}: PlanReady allocated output before admission`);
            const plan = mutationMemoryPlan(tree.candidate_mutation_output_memory_plan_v1_job(token),
                `${phase}/plan`, "v1-candidate-mutation-output");
            assert.deepEqual(plan, expected.plan, `${phase}: conservative plan differs from oracle`);
            if (exerciseRefusal) {
                for (let field = 0; field < 3; field++) {
                    const invalid = outputMemoryWitnesses(plan); invalid[field]++;
                    assert.throws(() => tree.resume_candidate_mutation_output_memory_v1_job(token, ...invalid));
                    sameLive(context.sample(`${phase}/refused-${field}`), parsed,
                        `${phase}: rejected witness changed pending owner`);
                    assert.deepEqual(tree.step_candidate_mutation_output_memory_v1_job(token, budget), paused,
                        `${phase}: rejected witness consumed PlanReady`);
                }
            }
            const beforeResume = context.sample(`${phase}/resume-before`);
            tree.resume_candidate_mutation_output_memory_v1_job(token, ...outputMemoryWitnesses(plan));
            const afterResume = context.sample(`${phase}/resume-after`);
            assert.equal(afterResume.live_requested_bytes - beforeResume.live_requested_bytes,
                expected.rootCloneRequestedBytes, `${phase}: root clone escaped admission`);
            assert.equal(afterResume.live_allocations - beforeResume.live_allocations,
                expected.rootCloneAllocations, `${phase}: root clone allocation count differs`);
            assert.equal(afterResume.successful_reallocations - beforeResume.successful_reallocations, 0,
                `${phase}: root clone unexpectedly reallocated`);
            let current = paused;
            for (let turns = 0; ; turns++) {
                current = mutationMemoryStep(
                    tree.step_candidate_mutation_output_memory_v1_job(token, budget), current,
                    budget, `${phase}/build`);
                assert.deepEqual(visible(context.module, tree), initial,
                    `${phase}: provisional output became visible`);
                if (current.done) break;
                assert(turns < 100_000, `${phase}: build runaway`);
                if ((turns & 255) === 0) await tick();
            }
            const ready = mutationMemoryReady(tree.candidate_mutation_output_memory_ready_v1_job(token),
                plan, `${phase}/ready`);
            assert(ready.rangeEndpointResidentRequestedBytes > 0,
                `${phase}: Ready omitted resident root charge`);
            return { token, plan, ready, completed: current.completed };
        };

        const cancelledBuild = await build(true, "v1-mutation/cancel");
        const cancellation = await drain(tree, cancelledBuild.token, initial,
            "v1-mutation/cancel-retirement");
        sameLive(context.sample("v1-mutation/cancelled"), baseline,
            "V1 mutation cancellation did not retire parsed/root/output owners");

        const publishedBuild = await build(false, "v1-mutation/publish");
        tree.finish_candidate_mutation_job_deferred(publishedBuild.token);
        assert.equal(Buffer.from(tree.candidate_root_bytes()).toString("hex"), expectedRoot,
            "V1 mutation publication differs from synchronous oracle");
        const published = visible(context.module, tree);
        const retirement = await drain(tree, publishedBuild.token, published,
            "v1-mutation/publish-retirement");
        return { version: 1, plan: publishedBuild.plan, ready: publishedBuild.ready,
            shapeCancellation, cancellation, retirement, completed: publishedBuild.completed };
    } finally {
        oracle.free();
        tree.free();
    }
}

async function qualifyCandidateMutationOutput(variant) {
    const context = await fresh(variant, `${variant.name}/candidate-output`);
    // A separate WASM instance keeps the synchronous oracle outside measured
    // native live/peak counters, rather than subtracting an estimated oracle.
    const oracleContext = await fresh(variant, `${variant.name}/candidate-output-oracle`);
    assert.notEqual(context.native.memory.buffer, oracleContext.native.memory.buffer);
    const reports = [];
    for (const budget of [1, 256]) {
        const tree = requiredTree(context.module), oracle = requiredTree(oracleContext.module);
        try {
            const rows = Array.from({ length: 32 }, (_, i) => ({ path: `output/${String(i).padStart(3, "0")}.md`,
                hash: (i + 1).toString(16).padStart(64, "0"), size: 128 + i, mtime_ms: 1_700_000_000_000 + i }));
            const changed = { ...rows[7], hash: "cd".repeat(32), size: 3 };
            for (const target of [tree, oracle]) { target.rebuild_from_entries_in_version(2, JSON.stringify(rows)); target.begin_candidate(); }
            const residentHashes = new Set(context.module.wasm_tree_committed_chunk_hashes(tree));
            const sample = (phase, unmeasured) => {
                const components = chunkSnapshot(tree, `${budget}/${phase}`, unmeasured);
                const memory = context.sample(`budget${budget}/${phase}`);
                // The overlay is intentionally not a replacement-store
                // component. Its unmeasured flag must not become a zero claim.
                noPrivate(components, phase);
                assert(components.resident.buffer_capacity_bytes <= memory.live_requested_bytes);
                context.recordComponents(`budget${budget}/${phase}`, components);
                return { components, memory };
            };
            const retireMutation = async (token, before, phase) => {
                let completed = 0, turns = 0;
                assert.throws(() => tree.step_tree_retirement(token + 1, budget));
                for (;;) {
                    tree.cancel_candidate_mutation_job_deferred(token);
                    const state = progress(tree.step_tree_retirement(token, budget), completed, false, phase);
                    assert(state.units <= budget); completed = state.completed; turns++;
                    assert.deepEqual(visible(context.module, tree), before, `${phase}: retirement changed visible ownership`);
                    const raw = tree.chunk_memory_snapshot();
                    assert.equal(typeof raw.other_private_jobs_unmeasured, "boolean");
                    const observed = sample(`${phase}/retire`, raw.other_private_jobs_unmeasured);
                    residentComponent(context.module, tree, [...residentHashes], observed.components.resident, phase);
                    assert(turns <= 20_000, `${phase}: retirement runaway`);
                    if (state.done) {
                        assert.equal(raw.other_private_jobs_unmeasured, false, `${phase}: drained cursor remains unmeasured`);
                        break;
                    }
                    if ((turns & 63) === 0) await tick();
                }
                assert.throws(() => tree.step_tree_retirement(token, budget));
                assert.throws(() => tree.cancel_candidate_mutation_job_deferred(token));
                return { turns, completed };
            };
            const operations = [];
            for (const [index, operation] of [{ kind: "update", rows: [changed] },
                { kind: "update", rows: [rows[7]] }, { kind: "delete", rows: [rows[3].path] },
                { kind: "update", rows: [] }, { kind: "delete", rows: [] }].entries()) {
                const payload = JSON.stringify(operation.rows), noop = operation.rows.length === 0;
                oracle[operation.kind === "update" ? "candidate_update_batch" : "candidate_delete_batch"](payload);
                const expectedRoot = Buffer.from(oracle.candidate_root_bytes()).toString("hex");
                const expectedChunks = oracleContext.module.wasm_tree_candidate_chunk_hashes(oracle).sort().map(hash =>
                    [hash, Buffer.from(oracleContext.module.wasm_tree_get_chunk(oracle, hash)).toString("hex")]);
                assert.equal(expectedChunks.length, 1, "mutation byte oracle requires one leaf");
                assert.equal(Buffer.from(expectedChunks[0][1], "hex").subarray(0, 4).toString(), "OVL2");
                const independent = canonicalReplacementOutputMemoryPlan(expectedChunks, expectedRoot, "mutation oracle");
                const expectedPlan = { ...independent, scope: "v2-candidate-mutation-output" };
                const actualBytes = expectedChunks.reduce((sum, [hash, hex]) => sum + (residentHashes.has(hash) ? 0 : hex.length / 2), 0);
                const cancellations = [];
                for (const cancellation of [...(index === 0 ? ["plan-refusal", "staged", "ready"] : []), null]) {
                    // Explicitly interrupt before Ready even when a tiny
                    // output fits the normal budget-256 native call.
                    const buildBudget = cancellation === "staged" ? 1 : budget;
                    const before = visible(context.module, tree), beforeStore = sample("before", false).components.resident;
                    const beforeLive = context.sample("before-mutation");
                    const token = tree[operation.kind === "update" ? "begin_candidate_update_job" : "begin_candidate_delete_job"](payload);
                    let previous = { completed: 0, reachable: 0 }, plan, ready, turns = 0, stopped = false;
                    for (;;) {
                        previous = mutationMemoryStep(tree.step_candidate_mutation_output_memory_v1_job(token, buildBudget), previous, buildBudget, "mutation");
                        turns++; assert(turns <= 20_000, "mutation work runaway");
                        assert.deepEqual(visible(context.module, tree), before, "mutation exposed provisional owner");
                        const sampled = sample("build", true);
                        sameStores(sampled.components.resident, beforeStore, "mutation wrote resident nodes before finish");
                        if (previous.phase === "plan ready") {
                            assert.equal(plan, undefined); assert.equal(previous.reachable, 0);
                            plan = mutationMemoryPlan(tree.candidate_mutation_output_memory_plan_v1_job(token), "PlanReady");
                            assert.deepEqual(plan, expectedPlan, "mutation plan differs from canonical bytes");
                            for (let field = 0; field < 3; field++) for (const delta of [-1, 1]) {
                                const invalid = outputMemoryWitnesses(plan); invalid[field] += delta;
                                assert.throws(() => tree.resume_candidate_mutation_output_memory_v1_job(token, ...invalid));
                                assert.deepEqual(tree.step_candidate_mutation_output_memory_v1_job(token, budget),
                                    { done: false, units: 0, completed: previous.completed, remaining: 1, reachable: 0, phase: "plan ready" });
                                sameStores(sample("refused", true).components.resident, beforeStore, "refused witness changed resident");
                            }
                            if (cancellation === "plan-refusal") { stopped = true; break; }
                            tree.resume_candidate_mutation_output_memory_v1_job(token, ...outputMemoryWitnesses(plan));
                            assert.throws(() => tree.resume_candidate_mutation_output_memory_v1_job(token, ...outputMemoryWitnesses(plan)));
                        }
                        if (cancellation === "staged" && previous.reachable > 0) {
                            assert.equal(previous.done, false, "staged cancellation accidentally duplicated Ready cancellation");
                            stopped = true; break;
                        }
                        if (previous.done) {
                            if (noop) {
                                assert.equal(plan, undefined);
                                assert.deepEqual(previous, { done: true, units: 0, completed: 0, remaining: 0, reachable: 0, phase: "ready" });
                                assert.throws(() => tree.candidate_mutation_output_memory_ready_v1_job(token));
                            } else {
                                assert(plan); ready = mutationMemoryReady(tree.candidate_mutation_output_memory_ready_v1_job(token), plan, "Ready");
                                assert.equal(ready.stagedNodePayloadBytes, actualBytes);
                                // A disjoint subset only: this is not the full
                                // mutation owner, module heap or admitted quota.
                                assert(beforeStore.payload_bytes + ready.residentAdmissionBytes <=
                                    context.sample("ready-output-subset").live_requested_bytes);
                            }
                            if (cancellation === "ready") stopped = true;
                            break;
                        }
                        if ((turns & 63) === 0) await tick();
                    }
                    if (stopped) {
                        tree.cancel_candidate_mutation_job_deferred(token);
                        const retired = await retireMutation(token, before, cancellation);
                        sameStores(sample("cancelled", false).components.resident, beforeStore, "cancel changed resident capacity/payload");
                        sameLive(context.sample("cancelled-live"), beforeLive, "cancelled output did not free exact native cursor allocations");
                        cancellations.push({ stage: cancellation, build_step_budget: buildBudget, ...retired }); continue;
                    }
                    tree.finish_candidate_mutation_job_deferred(token);
                    assert.equal(Buffer.from(tree.candidate_root_bytes()).toString("hex"), expectedRoot, "publication differs from separate synchronous oracle");
                    for (const [hash] of expectedChunks) residentHashes.add(hash);
                    const published = visible(context.module, tree), retired = await retireMutation(token, published, "published");
                    const afterStore = sample("attached", false).components.resident;
                    assert.equal(afterStore.payload_bytes - beforeStore.payload_bytes, noop ? 0 : actualBytes,
                        "Ready did not match actual resident payload transfer");
                    if (index === 1) {
                        assert(plan.nodePayloadBytes > 0); assert.equal(ready.stagedNodePayloadBytes, 0);
                        sameStores(afterStore, beforeStore, "resident duplicate was overwritten or retained twice");
                    }
                    operations.push({ kind: operation.kind, noop, turns, completed: previous.completed,
                        plan: plan ?? null, ready: ready ?? null, retirement: retired, cancellations });
                }
            }
            const outcome = budget === 1 ? "abort" : "commit";
            const settlement = settleV2OutputMemory(context.module, tree, outcome,
                `${context.label}/budget${budget}/settlement-${outcome}`,
                phase => sample(`settlement-${outcome}-${phase}`, false).components.resident);
            assert(settlement.removed > 0, `${outcome}: fixture did not exercise physical sweep`);
            if (outcome === "commit") oracle.commit_candidate();
            else oracle.abort_candidate();
            assert.deepEqual(canonical(context.module, tree), canonical(oracleContext.module, oracle),
                `${outcome}: atomic settlement differs from legacy oracle`);
            reports.push({ budget, operations, settlement });
        } finally { oracle.free(); tree.free(); }
        sameLive(context.sample(`budget${budget}/free`), context.baseline, "candidate lifecycle did not return fixed requested-live baseline");
        sameLive(oracleContext.sample(`budget${budget}/free`), oracleContext.baseline, "candidate oracle native owner leaked");
    }
    reports.unshift(await qualifyV1CandidateMutationOutput(context, oracleContext));
    sameLive(context.sample("v1-mutation/free"), context.baseline,
        "V1 candidate output lifecycle leaked native owners");
    sameLive(oracleContext.sample("v1-mutation/oracle-free"), oracleContext.baseline,
        "V1 candidate output oracle leaked native owners");
    return { variant: variant.name, versions: [1, 2], entries: 32, independent_oracle_instance: true,
        baseline_requested_bytes: context.baseline.live_requested_bytes,
        baseline_live_allocations: context.baseline.live_allocations,
        runs: reports, phases: compactPhases(context.report()), final: context.snapshot() };
}

function assertCandidateOpenResumeDelta(before, after, plan, label) {
    assert.equal(after.live_requested_bytes - before.live_requested_bytes,
        plan.rootStringRequestedBytes, `${label}: requested-live root clone delta`);
    assert.equal(after.live_allocations - before.live_allocations,
        plan.rootStringCount, `${label}: live allocation root clone delta`);
    assert.equal(after.successful_allocations - before.successful_allocations,
        plan.rootStringCount, `${label}: successful allocation root clone delta`);
    assert.equal(after.successful_reallocations - before.successful_reallocations, 0,
        `${label}: root clone unexpectedly reallocated`);
}

async function qualifyCandidateOpenMemory(variant) {
    const context = await fresh(variant, `${variant.name}/candidate-open`);
    const runs = [];
    for (const count of [0, 1, COUNT]) {
        const rows = candidateOpenRows(count), tree = requiredTree(context.module);
        let token, primaryError;
        try {
            tree.rebuild_from_entries_in_version(2, JSON.stringify(rows));
            const resident = context.sample(`candidate-open/${count}/resident`);
            const initial = visible(context.module, tree);
            const expected = candidateOpenExpected(context.module, tree, rows);
            assert.equal(expected.rootStringCount, count === 0 ? 2 : 4);
            if (count === COUNT) {
                assert(expected.residentChunkCount > 1,
                    `${context.label}: wide fixture has no N-dependent resident key population`);
            }

            token = tokenValue(tree.begin_candidate_job());
            const beforePhaseRefusal = context.sample(`candidate-open/${count}/phase-refusal-before`);
            assert.throws(() => tree.candidate_open_memory_plan_v1_job(token),
                `${context.label}: pre-READY candidate-open plan was visible`);
            assert.throws(() => tree.resume_candidate_open_memory_v1_job(token,
                expected.rootIdentityRequestedBytes, expected.rootEndpointRequestedBytes,
                expected.rootStringRequestedBytes),
            `${context.label}: pre-READY candidate-open resume succeeded`);
            sameLive(context.sample(`candidate-open/${count}/phase-refusal-after`), beforePhaseRefusal,
                `${context.label}: pre-READY refusal changed native owner`);
            assert.deepEqual(visible(context.module, tree), initial,
                `${context.label}: pre-READY refusal changed visible tree`);

            const ready = stepCandidateOpenToReady(tree, token,
                `${context.label}/candidate-open/${count}/cancel-ready`);
            const plan = candidateOpenMemoryPlan(tree.candidate_open_memory_plan_v1_job(token),
                expected, `${context.label}/candidate-open/${count}/cancel-plan`);
            assert.equal(ready.reachable, plan.residentChunkCount,
                `${context.label}: plan resident count differs from traversal`);
            const witnesses = [plan.rootIdentityRequestedBytes, plan.rootEndpointRequestedBytes,
                plan.rootStringRequestedBytes];
            const beforeWitnessRefusal = context.sample(`candidate-open/${count}/witness-refusal-before`);
            for (let field = 0; field < witnesses.length; field++) {
                const invalid = witnesses.slice(); invalid[field]++;
                assert.throws(() => tree.resume_candidate_open_memory_v1_job(token, ...invalid),
                    `${context.label}: wrong candidate-open witness ${field} accepted`);
            }
            sameLive(context.sample(`candidate-open/${count}/witness-refusal-after`), beforeWitnessRefusal,
                `${context.label}: witness refusal changed native owner`);
            assert.deepEqual(visible(context.module, tree), initial,
                `${context.label}: witness refusal changed visible tree`);

            const beforeResume = context.sample(`candidate-open/${count}/cancel-resume-before`);
            assert.equal(tree.resume_candidate_open_memory_v1_job(token, ...witnesses), undefined);
            const afterResume = context.sample(`candidate-open/${count}/cancel-resume-after`);
            assertCandidateOpenResumeDelta(beforeResume, afterResume, plan,
                `${context.label}/candidate-open/${count}/cancel-resume`);
            assert.throws(() => tree.candidate_open_memory_plan_v1_job(token),
                `${context.label}: prepared candidate-open plan remained visible`);
            assert.throws(() => tree.resume_candidate_open_memory_v1_job(token, ...witnesses),
                `${context.label}: duplicate candidate-open resume succeeded`);
            assert.throws(() => tree.cancel_tree_job(token),
                `${context.label}: generic cancel consumed prepared candidate-open`);
            assert.deepEqual(visible(context.module, tree), initial,
                `${context.label}: prepared candidate-open changed visible tree`);
            const cancelRetirement = drainCandidateOpenRetirement(tree, token,
                `${context.label}/candidate-open/${count}/cancel-retirement`, count <= 1 ? 1 : 256);
            token = undefined;
            assert(cancelRetirement.completed >= plan.rootStringCount,
                `${context.label}: prepared root was not cooperatively retired`);
            sameLive(context.sample(`candidate-open/${count}/cancelled`), resident,
                `${context.label}: prepared cancellation did not restore resident baseline`);
            assert.deepEqual(visible(context.module, tree), initial,
                `${context.label}: cancellation changed visible tree`);

            token = tokenValue(tree.begin_candidate_job());
            const publishReady = stepCandidateOpenToReady(tree, token,
                `${context.label}/candidate-open/${count}/publish-ready`);
            const publishPlan = candidateOpenMemoryPlan(tree.candidate_open_memory_plan_v1_job(token),
                expected, `${context.label}/candidate-open/${count}/publish-plan`);
            const publishWitnesses = [publishPlan.rootIdentityRequestedBytes,
                publishPlan.rootEndpointRequestedBytes, publishPlan.rootStringRequestedBytes];
            const beforePublishResume = context.sample(`candidate-open/${count}/publish-resume-before`);
            tree.resume_candidate_open_memory_v1_job(token, ...publishWitnesses);
            const afterPublishResume = context.sample(`candidate-open/${count}/publish-resume-after`);
            assertCandidateOpenResumeDelta(beforePublishResume, afterPublishResume, publishPlan,
                `${context.label}/candidate-open/${count}/publish-resume`);
            const reachable = tree.finish_candidate_job_deferred(token);
            assert.equal(reachable, publishReady.reachable,
                `${context.label}: candidate-open publication reachable count`);
            assert.equal(reachable, publishPlan.residentChunkCount,
                `${context.label}: candidate-open publication resident count`);
            const published = visible(context.module, tree);
            assert.equal(published.active, true, `${context.label}: candidate-open did not publish`);
            assert.equal(published.committed, initial.committed,
                `${context.label}: candidate-open advanced committed revision`);
            assert.equal(published.candidate, initial.candidate + 1,
                `${context.label}: candidate-open revision did not advance exactly once`);
            assert.equal(published.hash, initial.hash, `${context.label}: committed root hash changed`);
            assert.equal(published.root, initial.root, `${context.label}: committed root bytes changed`);
            assert.equal(published.candidateHash, initial.hash, `${context.label}: candidate root hash differs`);
            assert.equal(published.candidateRoot, initial.root, `${context.label}: candidate root bytes differ`);
            const publishRetirement = drainCandidateOpenRetirement(tree, token,
                `${context.label}/candidate-open/${count}/publish-retirement`, count <= 1 ? 1 : 256);
            token = undefined;
            const retained = context.sample(`candidate-open/${count}/published`);
            assert.equal(retained.live_requested_bytes - resident.live_requested_bytes,
                publishPlan.rootStringRequestedBytes,
                `${context.label}: published candidate retained the wrong requested bytes`);
            assert.equal(retained.live_allocations - resident.live_allocations,
                publishPlan.rootStringCount,
                `${context.label}: published candidate retained the wrong String allocations`);
            const metadata = metadataSnapshot(tree, `${context.label}/candidate-open/${count}/metadata`);
            assert.equal(metadata.resident.candidate.roots, 1,
                `${context.label}: published root missing from metadata`);
            assert.equal(metadata.resident.candidate.strings.strings, publishPlan.rootStringCount,
                `${context.label}: published String count differs from plan`);
            assert.equal(metadata.resident.candidate.strings.capacity_bytes,
                publishPlan.rootStringRequestedBytes,
                `${context.label}: published String capacity differs from requested bytes`);
            assert.equal(metadata.resident.candidate_baseline.present, true,
                `${context.label}: generation baseline is absent`);
            assert.equal(metadata.resident.candidate_baseline.hashes, 0,
                `${context.label}: generation baseline retained physical hashes`);
            assert.equal(metadata.resident.candidate_baseline.capacity_slots, 0,
                `${context.label}: generation baseline retained HashSet capacity`);
            assert.equal(metadata.resident.candidate_baseline.logical_hash_bytes, 0,
                `${context.label}: generation baseline retained logical key bytes`);

            const beforeAbortRevision = tree.candidate_revision();
            tree.abort_candidate();
            assert.equal(tree.has_candidate(), false, `${context.label}: candidate abort failed`);
            assert.equal(tree.candidate_revision(), beforeAbortRevision + 1,
                `${context.label}: candidate abort revision did not advance`);
            sameLive(context.sample(`candidate-open/${count}/aborted`), resident,
                `${context.label}: abort did not restore resident baseline`);
            runs.push({ count, plan: publishPlan,
                cancel_resume_requested_delta: afterResume.live_requested_bytes - beforeResume.live_requested_bytes,
                cancel_resume_allocation_delta: afterResume.live_allocations - beforeResume.live_allocations,
                publish_resume_requested_delta:
                    afterPublishResume.live_requested_bytes - beforePublishResume.live_requested_bytes,
                publish_resume_allocation_delta:
                    afterPublishResume.live_allocations - beforePublishResume.live_allocations,
                cancelled_retirement_units: cancelRetirement.completed,
                published_retirement_units: publishRetirement.completed });
        } catch (error) {
            primaryError = error;
            throw error;
        } finally {
            let cleanupError;
            if (token !== undefined) {
                try { tree.cancel_reachability_job_deferred(token); }
                catch (error) { cleanupError = error; }
            }
            try {
                tree.free();
                sameLive(context.sample(`candidate-open/${count}/tree-freed`), context.baseline,
                    `${context.label}: candidate-open tree free did not restore module baseline`);
            } catch (error) { cleanupError ??= error; }
            if (primaryError === undefined && cleanupError !== undefined) throw cleanupError;
        }
    }

    const v1 = requiredTree(context.module);
    let v1Token, v1Error;
    try {
        const rows = candidateOpenRows(1);
        v1.rebuild_from_entries_in_version(1, JSON.stringify(rows));
        const resident = context.sample("candidate-open/v1/resident");
        const initial = visible(context.module, v1);
        const committed = metadataSnapshot(v1, "candidate-open/v1/committed").resident.committed;
        const identity = Buffer.byteLength(VAULT) + Buffer.byteLength(DEVICE);
        const slotBytes = committed.v1_children_capacity === 0 ? 0 :
            committed.v1_children_backing_capacity_bytes / committed.v1_children_capacity;
        assert(Number.isSafeInteger(slotBytes), `${context.label}: invalid V1 root child slot width`);
        const endpoint = committed.strings.length_bytes - identity +
            committed.v1_children_length * slotBytes;
        const expected = { scope: "v1-candidate-open-root",
            residentChunkCount: context.module.wasm_tree_committed_chunk_hashes(v1).length,
            rootStringCount: committed.strings.strings,
            rootIdentityRequestedBytes: identity, rootEndpointRequestedBytes: endpoint,
            rootStringRequestedBytes: identity + endpoint,
            baselineKeySnapshotRequestedBytes: 0, peakAdmissionBytes: identity + endpoint };
        v1Token = tokenValue(v1.begin_candidate_job());
        const ready = stepCandidateOpenToReady(v1, v1Token, `${context.label}/candidate-open/v1/ready`);
        const plan = candidateOpenMemoryPlan(v1.candidate_open_memory_plan_v1_job(v1Token),
            expected, `${context.label}/candidate-open/v1/plan`);
        assert.equal(ready.reachable, plan.residentChunkCount);
        const witnesses = [plan.rootIdentityRequestedBytes, plan.rootEndpointRequestedBytes,
            plan.rootStringRequestedBytes];
        const beforeRefusal = context.sample("candidate-open/v1/refusal-before");
        for (let field = 0; field < witnesses.length; field++) {
            const invalid = witnesses.slice(); invalid[field]++;
            assert.throws(() => v1.resume_candidate_open_memory_v1_job(v1Token, ...invalid),
                `${context.label}: V1 accepted changed candidate-open witness ${field}`);
        }
        sameLive(context.sample("candidate-open/v1/refusal-after"), beforeRefusal,
            `${context.label}: V1 witness refusal changed ready owner`);
        const beforeResume = context.sample("candidate-open/v1/resume-before");
        v1.resume_candidate_open_memory_v1_job(v1Token, ...witnesses);
        const afterResume = context.sample("candidate-open/v1/resume-after");
        assert.equal(afterResume.live_requested_bytes - beforeResume.live_requested_bytes,
            plan.rootStringRequestedBytes, `${context.label}: V1 clone escaped its admitted request`);
        assert.equal(afterResume.live_allocations - beforeResume.live_allocations,
            plan.rootStringCount + Number(committed.v1_children_length > 0),
        `${context.label}: V1 clone allocation ownership differs from root shape`);
        assert.deepEqual(visible(context.module, v1), initial,
            `${context.label}: V1 prepared root changed visible state`);
        const retirement = drainCandidateOpenRetirement(v1, v1Token,
            `${context.label}/candidate-open/v1/retirement`, 1);
        v1Token = undefined;
        sameLive(context.sample("candidate-open/v1/cancelled"), resident,
            `${context.label}: V1 cancellation did not restore resident baseline`);

        v1Token = tokenValue(v1.begin_candidate_job());
        const publishReady = stepCandidateOpenToReady(v1, v1Token,
            `${context.label}/candidate-open/v1/publish-ready`);
        const publishPlan = candidateOpenMemoryPlan(v1.candidate_open_memory_plan_v1_job(v1Token),
            expected, `${context.label}/candidate-open/v1/publish-plan`);
        v1.resume_candidate_open_memory_v1_job(v1Token, ...witnesses);
        assert.equal(v1.finish_candidate_job_deferred(v1Token), publishReady.reachable);
        const publishRetirement = drainCandidateOpenRetirement(v1, v1Token,
            `${context.label}/candidate-open/v1/publish-retirement`, 1);
        v1Token = undefined;
        const published = metadataSnapshot(v1, "candidate-open/v1/published").resident;
        assert.equal(published.candidate.roots, 1);
        assert.equal(published.candidate.strings.capacity_bytes,
            committed.strings.length_bytes);
        assert.equal(published.candidate_baseline.hashes, 0);
        assert.equal(published.candidate_baseline.capacity_slots, 0);
        v1.abort_candidate();
        sameLive(context.sample("candidate-open/v1/aborted"), resident,
            `${context.label}: V1 abort did not restore resident baseline`);
        runs.push({ version: 1, plan: publishPlan,
            retirement_units: retirement.completed,
            published_retirement_units: publishRetirement.completed });
    } catch (error) {
        v1Error = error;
        throw error;
    } finally {
        let cleanupError;
        if (v1Token !== undefined) {
            try { v1.cancel_reachability_job_deferred(v1Token); }
            catch (error) { cleanupError = error; }
        }
        try {
            v1.free();
            sameLive(context.sample("candidate-open/v1/tree-freed"), context.baseline,
                `${context.label}: V1 tree free did not restore module baseline`);
        } catch (error) { cleanupError ??= error; }
        if (v1Error === undefined && cleanupError !== undefined) throw cleanupError;
    }
    return { variant: variant.name, runs, phases: compactPhases(context.report()),
        final: context.snapshot() };
}

async function qualifyCandidateChunkPlans(variant) {
    const context = await fresh(variant, `${variant.name}/candidate-chunk-plan`);
    const runs = [];
    for (const version of [1, 2]) {
        const tree = requiredTree(context.module);
        let token, primaryError;
        try {
            const rows = candidateOpenRows(COUNT);
            tree.rebuild_from_entries_in_version(version, JSON.stringify(rows));
            const committed = context.sample(`candidate-chunk/v${version}/committed`);
            tree.begin_candidate();
            const candidateBaseline = context.sample(`candidate-chunk/v${version}/candidate`);
            const expectedAll = context.module.wasm_tree_candidate_chunk_hashes(tree);
            const expectedFresh = context.module.wasm_tree_new_candidate_chunk_hashes(tree);
            assert(expectedAll.length > 0, `${context.label}/v${version}: wide graph has no chunks`);
            assert.deepEqual(expectedFresh, [], `${context.label}/v${version}: unchanged candidate has fresh chunks`);
            let previous;
            for (const hash of expectedAll) {
                assert.match(hash, /^[0-9a-f]{64}$/);
                assert(previous === undefined || previous < hash,
                    `${context.label}/v${version}: synchronous oracle is not canonical`);
                previous = hash;
            }

            for (let cycle = 0; cycle < 3; cycle++) {
                token = tokenValue(tree.begin_candidate_chunks_job());
                const ready = stepCandidateOpenToReady(tree, token,
                    `${context.label}/v${version}/cycle${cycle}/traversal`);
                assert.equal(ready.reachable, expectedAll.length,
                    `${context.label}/v${version}/cycle${cycle}: traversal count`);
                const plan = candidateChunkSortMemoryPlan(
                    tree.candidate_chunks_sort_memory_plan_v1_job(token), expectedAll.length,
                    `${context.label}/v${version}/cycle${cycle}/plan`);
                const beforeRefusal = context.sample(`candidate-chunk/v${version}/cycle${cycle}/refusal-before`);
                assert.throws(() => tree.resume_candidate_chunks_sort_memory_v1_job(token,
                    plan.sourceHashesRequestedBytes + 32, plan.scratchHashesRequestedBytes),
                `${context.label}/v${version}/cycle${cycle}: wrong source witness accepted`);
                sameLive(context.sample(`candidate-chunk/v${version}/cycle${cycle}/refusal-after`),
                    beforeRefusal, `${context.label}/v${version}/cycle${cycle}: refusal retained memory`);

                const beforeResume = context.sample(`candidate-chunk/v${version}/cycle${cycle}/resume-before`);
                tree.resume_candidate_chunks_sort_memory_v1_job(token,
                    plan.sourceHashesRequestedBytes, plan.scratchHashesRequestedBytes);
                const afterResume = context.sample(`candidate-chunk/v${version}/cycle${cycle}/resume-after`);
                assert.equal(afterResume.live_requested_bytes - beforeResume.live_requested_bytes,
                    plan.peakAdmissionBytes,
                    `${context.label}/v${version}/cycle${cycle}: admitted workspace requested delta`);
                assert.equal(afterResume.live_allocations - beforeResume.live_allocations, 2,
                    `${context.label}/v${version}/cycle${cycle}: admitted workspace allocation delta`);
                assert.equal(afterResume.successful_allocations - beforeResume.successful_allocations, 2,
                    `${context.label}/v${version}/cycle${cycle}: workspace allocation count`);

                let completed = 0, state, turns = 0;
                const sortBudget = cycle === 0 ? 1 : CANDIDATE_CHUNK_SORT_UNITS;
                do {
                    assert(++turns < 100_000,
                        `${context.label}/v${version}/cycle${cycle}: sort did not converge`);
                    state = candidateChunkSortProgress(tree.step_candidate_chunks_sort_v1_job(
                        token, sortBudget), completed, expectedAll.length,
                    `${context.label}/v${version}/cycle${cycle}/sort${turns}`);
                    completed = state.completed;
                    const live = context.sample(`candidate-chunk/v${version}/cycle${cycle}/sort`);
                    assert(live.live_requested_bytes <= afterResume.live_requested_bytes,
                        `${context.label}/v${version}/cycle${cycle}: sort exceeded admitted workspace`);
                    if (cycle === 0 && turns === 1) break;
                } while (!state.done);

                if (cycle === 0) {
                    assert.equal(state.done, false,
                        `${context.label}/v${version}: cancellation fixture finished in one sort turn`);
                } else {
                    assert.equal(state.done, true, `${context.label}/v${version}: sort not ready`);
                    const replay = candidateChunkSortProgress(tree.step_candidate_chunks_sort_v1_job(
                        token, CANDIDATE_CHUNK_SORT_UNITS), completed, expectedAll.length,
                    `${context.label}/v${version}/cycle${cycle}/sort-ready-replay`);
                    assert.equal(replay.units, 0,
                        `${context.label}/v${version}/cycle${cycle}: READY replay did work`);
                    assert.deepEqual({ ...replay, units: state.units }, state,
                        `${context.label}/v${version}/cycle${cycle}: READY replay changed`);
                    const info = tree.candidate_chunks_plan_info_v1_job(token);
                    exactFields(info, ["schema", "scope", "allCount", "pageMaxHashes"],
                        `${context.label}/v${version}/cycle${cycle}/info`);
                    assert.equal(info.schema, 1); assert.equal(info.scope, "candidate-chunk-plan-pages");
                    assert.equal(info.allCount, expectedAll.length);
                    assert.equal(info.pageMaxHashes, CANDIDATE_CHUNK_PAGE_HASHES);
                    let offset = 0, pageCount = 0;
                    const actualAll = [], actualFresh = [];
                    do {
                        const beforePage = context.sample(
                            `candidate-chunk/v${version}/cycle${cycle}/page-before`);
                        const page = candidateChunkPage(tree.read_candidate_chunks_page_v1_job(
                            token, offset, CANDIDATE_CHUNK_PAGE_HASHES), offset, expectedAll.length,
                        `${context.label}/v${version}/cycle${cycle}/page${pageCount}`);
                        sameLive(context.sample(`candidate-chunk/v${version}/cycle${cycle}/page-after`),
                            beforePage,
                            `${context.label}/v${version}/cycle${cycle}: page retained native allocation`);
                        actualAll.push(...page.all); actualFresh.push(...page.fresh);
                        offset = page.nextOffset; pageCount++;
                        if (cycle === 2 || page.done) break;
                    } while (pageCount <= expectedAll.length + 1);
                    if (cycle === 1) {
                        assert.deepEqual(actualAll, expectedAll,
                            `${context.label}/v${version}: paged all differs from oracle`);
                        assert.deepEqual(actualFresh, expectedFresh,
                            `${context.label}/v${version}: paged fresh differs from oracle`);
                        tree.finish_candidate_chunks_plan_v1_job(token);
                    }
                }
                const retirement = drainCandidateOpenRetirement(tree, token,
                    `${context.label}/v${version}/cycle${cycle}/retirement`);
                token = undefined;
                sameLive(context.sample(`candidate-chunk/v${version}/cycle${cycle}/drained`),
                    candidateBaseline,
                    `${context.label}/v${version}/cycle${cycle}: plan did not return to candidate baseline`);
                runs.push({ version, cycle, plan, sort_turns: turns,
                    cancelled_stage: cycle === 0 ? "sort" : cycle === 2 ? "page" : null,
                    retirement_units: retirement.completed });
            }
            tree.abort_candidate();
            sameLive(context.sample(`candidate-chunk/v${version}/aborted`), committed,
                `${context.label}/v${version}: candidate abort did not restore committed baseline`);
        } catch (error) {
            primaryError = error;
            throw error;
        } finally {
            let cleanupError;
            if (token !== undefined) {
                try { tree.cancel_reachability_job_deferred(token); }
                catch (error) { cleanupError = error; }
            }
            try {
                tree.free();
                sameLive(context.sample(`candidate-chunk/v${version}/tree-freed`), context.baseline,
                    `${context.label}/v${version}: tree free did not restore module baseline`);
            } catch (error) { cleanupError ??= error; }
            if (primaryError === undefined && cleanupError !== undefined) throw cleanupError;
        }
    }
    return { variant: variant.name, entries: COUNT, runs,
        phases: compactPhases(context.report()), final: context.snapshot() };
}

async function qualifyV2ReachabilityAllocations(variant) {
    const rowCount = 2_000;
    const rows = Array.from({ length: rowCount }, (_, index) => ({
        path: `reachability/${String(index).padStart(8, "0")}.md`,
        hash: (index + 1).toString(16).padStart(64, "0"),
        mtime_ms: 1_700_000_000_000 + index,
        size: 128 + index,
    }));
    const context = await fresh(variant, `${variant.name}/v2-reachability`);
    const tree = requiredTree(context.module);
    let token, primaryError;
    try {
        tree.rebuild_from_entries_in_version(2, JSON.stringify(rows));
        const nodeCount = context.module.wasm_tree_committed_chunk_hashes(tree).length;
        assert(nodeCount > 2, `${context.label}: fixture did not build a multi-child root`);
        const resident = context.sample("v2-reachability/resident");
        const reportRepeat = context.sample("v2-reachability/report-repeat");
        assert.equal(reportRepeat.successful_allocations, resident.successful_allocations,
            `${context.label}: warmed allocation reporting allocated unexpectedly`);
        assert.equal(reportRepeat.successful_reallocations, resident.successful_reallocations,
            `${context.label}: warmed allocation reporting reallocated unexpectedly`);

        token = tokenValue(tree.begin_candidate_job());
        let state = reachabilityProgress(tree.step_tree_job(token, 1), 0,
            `${context.label}/seed`);
        assert.equal(state.completed, 1);
        assert.equal(state.reachable, 0);
        assert.equal(state.remaining, 1);

        const beforeRoot = context.sample("v2-reachability/root-before");
        state = reachabilityProgress(tree.step_tree_job(token, 1), state.completed,
            `${context.label}/root`);
        const afterRoot = context.sample("v2-reachability/root-after");
        assert.equal(state.reachable, 1);
        assert(state.remaining > 1, `${context.label}: decoded root has too few children`);
        const rootAllocations = afterRoot.successful_allocations - beforeRoot.successful_allocations;
        const rootReallocations = afterRoot.successful_reallocations - beforeRoot.successful_reallocations;
        // One allocation per decoded endpoint String. The six fixed owners are
        // run_local's pinned future, async-trait get(), the returned node-byte
        // clone, decoded children Vec, and the first reachable/descriptor hash
        // table allocations. A temporary RangeRef would add two more String
        // allocations and fail this actual packaged-WASM gate.
        const expectedRootAllocations = state.remaining * 2 + 6;
        assert.equal(rootAllocations, expectedRootAllocations,
            `${context.label}: root validation allocated endpoint clones or changed fixed owners`);
        assert.equal(rootReallocations, 0,
            `${context.label}: root validation unexpectedly reallocated an owner`);

        const rootChildren = state.remaining;
        state = reachabilityProgress(tree.step_tree_job(token, 1), state.completed,
            `${context.label}/partial-expansion`);
        assert.equal(state.remaining, rootChildren,
            `${context.label}: child transfer changed total pending work`);
        assert.equal(state.reachable, 1,
            `${context.label}: child transfer also validated a node`);
        tree.cancel_tree_job(token); token = undefined;
        sameLive(context.sample("v2-reachability/partial-cancel"), resident,
            `${context.label}: partial expansion cancellation retained native owners`);

        token = tokenValue(tree.begin_candidate_job());
        let completed = 0;
        do {
            state = reachabilityProgress(tree.step_tree_job(token, 256), completed,
                `${context.label}/complete`);
            completed = state.completed;
            assert(completed <= nodeCount * 2,
                `${context.label}: complete traversal exceeded its exact work bound`);
        } while (!state.done);
        assert.equal(state.reachable, nodeCount,
            `${context.label}: complete traversal reachable count differs`);
        assert.equal(completed, nodeCount * 2,
            `${context.label}: move-only traversal changed its exact work units`);

        const beforeIdle = context.sample("v2-reachability/idle-before");
        const idle = reachabilityProgress(tree.step_tree_job(token, 1), completed,
            `${context.label}/idle`);
        const afterIdle = context.sample("v2-reachability/idle-after");
        assert.equal(idle.done, true);
        assert.equal(idle.units, 0);
        assert.equal(afterIdle.successful_allocations, beforeIdle.successful_allocations,
            `${context.label}: ready no-op step allocated unexpectedly`);
        assert.equal(afterIdle.successful_reallocations, beforeIdle.successful_reallocations,
            `${context.label}: ready no-op step reallocated unexpectedly`);
        tree.cancel_tree_job(token); token = undefined;
        sameLive(context.sample("v2-reachability/complete-cancel"), resident,
            `${context.label}: completed traversal cancellation retained native owners`);
        return { variant: variant.name, version: 2, entries: rowCount,
            nodes: nodeCount, root_children: rootChildren,
            root_step_successful_allocations: rootAllocations,
            expected_root_step_allocations: expectedRootAllocations,
            root_step_successful_reallocations: rootReallocations,
            traversal_units: completed, partial_expansion_cancelled: true,
            phases: context.report() };
    } catch (error) {
        primaryError = error;
        throw error;
    } finally {
        let cleanupError;
        if (token !== undefined) {
            try { tree.cancel_tree_job(token); }
            catch (error) { cleanupError = error; }
        }
        try {
            tree.free();
            sameLive(context.sample("v2-reachability/tree-freed"), context.baseline,
                `${context.label}: tree free did not return to fixed baseline`);
        } catch (error) { cleanupError ??= error; }
        if (primaryError === undefined && cleanupError !== undefined) throw cleanupError;
    }
}

async function qualifyV1ChildSort(variant) {
    const largeGroupRows = 1_001, remainingGroupRows = 7;
    const rowCount = largeGroupRows + remainingGroupRows;
    const rows = [];
    for (let index = largeGroupRows - 1; index >= 0; index--) {
        rows.push({ path: `a/${String(index).padStart(5, "0")}.md`,
            hash: (index + 1).toString(16).padStart(64, "0"),
            mtime_ms: 1_700_000_000_000 + index, size: 128 + index });
    }
    for (let index = remainingGroupRows - 1; index >= 0; index--) {
        const value = largeGroupRows + index;
        rows.push({ path: `z/${String(index).padStart(5, "0")}.md`,
            hash: (value + 1).toString(16).padStart(64, "0"),
            mtime_ms: 1_700_000_000_000 + value, size: 128 + value });
    }
    const json = JSON.stringify(rows), bytes = Buffer.byteLength(json);
    assert(bytes <= INPUT_BYTES);
    const context = await fresh(variant, `${variant.name}/v1-child-sort`);
    const tree = requiredTree(context.module);
    let token, retiring = false, primaryError;
    try {
        const before = owner(tree);
        const idle = observe(context, tree, "child-sort/idle", false, "exact", "exact");
        const baseline = idle.memory, resident = idle.components.resident;
        noPrivate(idle.components, "child-sort idle");
        assertEmptyInputSnapshot(idle.input, "child-sort idle input");
        assertEmptyV1GraphSnapshot(idle.v1Graph, "child-sort idle graph");
        assertEmptySortSnapshot(idle.sort, "child-sort idle sort");
        assertEmptyV2PlanningSnapshot(idle.planning, "child-sort idle V2 planning");
        token = tokenValue(tree.begin_replacement_rebuild_job(1, rowCount, bytes));
        feed(context, tree, token, rows, resident, "child-sort");
        startReplacementWithSortAdmission(context, tree, token, 1, rowCount, "child-sort/start");

        let completed = 0, turns = 0, prepared;
        for (;;) {
            const state = progress(tree.step_replacement_rebuild_job(token, 1), completed, true,
                "child-sort/build");
            completed = state.completed; turns++;
            if (state.phase === "sort-internal") {
                const observed = observe(context, tree, "child-sort/active", false,
                    "replacement-unmeasured", "replacement-unmeasured");
                if (observed.sort.replacement?.children.sorters === 1) {
                    prepared = observed;
                    break;
                }
            }
            assert(!state.done, "child-sort: V1 reached ready without exposing its child sorter");
            assert(completed <= workGuard(rowCount), "child-sort: build work runaway");
            if (turns % 256 === 0) await tick();
        }
        const active = prepared.sort.replacement.children;
        assert.equal(prepared.sort.replacement.entries.sorters, 0,
            "child-sort: entry and child sorters overlap");
        assert.equal(active.values.length_slots, 2,
            "child-sort: 1001 one-prefix rows did not create two child labels");
        assert(active.source_indices.capacity_slots >= active.values.length_slots);
        assert(active.target_indices.capacity_slots >= active.values.length_slots);
        const liveEntries = prepared.v1Entries.replacement;
        assert(liveEntries !== null, "child-sort: V1 entries became unmeasured");
        assert.equal(liveEntries.groups.owners, 1,
            "child-sort: remaining group owner was not retained");
        assert.equal(liveEntries.groups.length_slots, remainingGroupRows,
            "child-sort: remaining group row count differs");
        assert.equal(liveEntries.paths.strings, remainingGroupRows,
            "child-sort: remaining nested path count differs");
        assert.equal(liveEntries.sorting_rows, 0,
            "child-sort: encoded rows remained in the entry sorter component");
        const graph = prepared.v1Graph.replacement;
        assert.equal(graph.group_keys.strings, 1, "child-sort: remaining group key is absent");
        assert.equal(graph.prefix.strings, 1, "child-sort: active prefix is absent");
        assert.equal(graph.sorting_children, 2, "child-sort: child labels were not transferred to sorter");
        assert.equal(graph.child_labels.strings, 2, "child-sort: nested labels missing");
        assert.equal(graph.child_labels.length_bytes, Buffer.byteLength("a/0") + Buffer.byteLength("a/1"),
            "child-sort: generated label bytes differ from the input-derived oracle");
        assert.equal(graph.children.owners, 0, "child-sort: top-level values counted in graph and sort");
        sameStores(prepared.components.resident, resident,
            "child-sort: private build changed resident component");
        assert.deepEqual(owner(tree), before, "child-sort: private build changed visible owner");

        tree.cancel_replacement_rebuild_job_deferred(token); retiring = true;
        const transferred = observe(context, tree, "child-sort/transferred", false,
            "retiring-unmeasured", "retiring-unmeasured");
        assertEmptySortMemory(transferred.sort.replacement,
            "child-sort: replacement owner survived transfer");
        assert.deepEqual(transferred.sort.retiring, prepared.sort.replacement,
            "child-sort: child sorter changed across replacement→retirement transfer");
        assertEmptyV2PlanningSnapshot(transferred.planning,
            "child-sort: V1 fabricated V2 planning ownership");
        assertV1EntriesTransfer(prepared.v1Entries, transferred.v1Entries,
            "child-sort: V1 entry transfer");
        assertV1GraphTransfer(prepared.v1Graph, transferred.v1Graph, "child-sort: graph transfer");
        sameStores(transferred.components.resident, resident,
            "child-sort: transfer changed resident component");
        sameStores(transferred.components.replacement, EMPTY_STORE,
            "child-sort: transfer double-counted replacement chunks");
        sameStores(transferred.components.retiring, prepared.components.replacement,
            "child-sort: transfer lost private chunks");
        const retired = await retire(context, tree, token, rowCount, before, transferred.components,
            "child-sort", transferred.input, transferred.sort, transferred.planning);
        token = undefined; retiring = false;
        const drained = observe(context, tree, "child-sort/drained", false, "exact", "exact");
        noPrivate(drained.components, "child-sort drained");
        assertEmptyInputSnapshot(drained.input, "child-sort drained input");
        assertEmptySortSnapshot(drained.sort, "child-sort drained sort");
        assertEmptyV2PlanningSnapshot(drained.planning, "child-sort drained V2 planning");
        assertEmptyV1EntriesSnapshot(drained.v1Entries, "child-sort drained V1 entries");
        assertEmptyV1GraphSnapshot(drained.v1Graph, "child-sort drained graph");
        assert.deepEqual(owner(tree), before, "child-sort: cancel changed visible owner");
        sameLive(drained.memory, baseline, "child-sort: drained allocation baseline differs");
        return { variant: variant.name, version: 1, entries: rowCount,
            child_values: active.values.length_slots, remaining_group_rows: remainingGroupRows,
            build_units: completed,
            retirement_units: retired.completed, phases: context.report() };
    } catch (error) {
        primaryError = error;
        throw error;
    } finally {
        try {
            if (token !== undefined) {
                try {
                    if (!retiring) tree.cancel_replacement_rebuild_job_deferred(token);
                    const remaining = observe(context, tree, "child-sort/failure-cleanup");
                    const cleanupRows = cleanupRowsFor(remaining.input, rows, rowCount + 1);
                    await retire(context, tree, token, cleanupRows, owner(tree), remaining.components,
                        "child-sort/failure-cleanup", remaining.input, remaining.sort,
                        remaining.planning);
                } catch (cleanupError) {
                    if (primaryError === undefined) throw cleanupError;
                }
            }
        } finally { tree.free(); }
        sameLive(context.sample("child-sort/tree-freed"), context.baseline,
            `${variant.name}: child-sort tree free`);
    }
}

async function qualifyV2Planning(variant) {
    const rowCount = 5_000;
    const rows = [];
    for (let index = rowCount - 1; index >= 0; index--) {
        rows.push({ path: `planning/${String(index).padStart(8, "0")}.md`,
            hash: (index + 1).toString(16).padStart(64, "0"),
            mtime_ms: 1_700_000_000_000 + index, size: 128 + index });
    }
    const json = JSON.stringify(rows), bytes = Buffer.byteLength(json);
    assert(bytes <= INPUT_BYTES);
    const context = await fresh(variant, `${variant.name}/v2-planning`);
    const tree = requiredTree(context.module);
    let token, retiring = false;
    try {
        const before = owner(tree);
        const idle = observe(context, tree, "v2-planning/idle", false, "exact", "exact");
        const baseline = idle.memory, resident = idle.components.resident;
        noPrivate(idle.components, "v2-planning idle");
        assertEmptyV2PlanningSnapshot(idle.planning, "v2-planning idle");
        assertEmptyV1EntriesSnapshot(idle.v1Entries, "v2-planning idle V1 entries");
        assertEmptyV1GraphSnapshot(idle.v1Graph, "v2-planning idle V1 graph");
        assertEmptyV2RangesSnapshot(idle.ranges, "v2-planning idle ranges");

        token = tokenValue(tree.begin_replacement_rebuild_job(2, rowCount, bytes));
        const begun = observe(context, tree, "v2-planning/begin", false,
            "replacement-unmeasured", "exact");
        assertEmptyV2PlanningSnapshot(begun.planning, "v2-planning input owner");
        const acceptedInput = feed(context, tree, token, rows, resident, "v2-planning");
        startReplacementWithSortAdmission(context, tree, token, 2, rowCount, "v2-planning/start");
        const started = observe(context, tree, "v2-planning/start", false,
            "replacement-unmeasured", "replacement-unmeasured");
        assertActiveV2PlanningMemory(started.planning.replacement,
            "v2-planning active builder");
        assertEmptyV1EntriesSnapshot(started.v1Entries,
            "v2-planning V2 builder fabricated V1 entries");
        assertEmptyV2PlanningMemory(started.planning.retiring,
            "v2-planning active retirement");

        const seen = Object.fromEntries(PLANNING_VECTORS.map(name => [name, false]));
        let completed = 0, turns = 0, unitProbe = false, prepared;
        for (;;) {
            const budget = unitProbe ? 1 : 256;
            const state = progress(tree.step_replacement_rebuild_job(token, budget), completed,
                true, "v2-planning/build");
            assert(state.units <= budget,
                "v2-planning build exceeded its requested unit budget");
            completed = state.completed; turns++;
            unitProbe ||= !["sort", "validate entries"].includes(state.phase);
            const observed = observe(context, tree, `v2-planning/build:${state.phase}`, false,
                "replacement-unmeasured", "replacement-unmeasured");
            assertEmptyV2RangesSnapshot(observed.ranges, "v2-planning allocated ranges before resume");
            assertActiveV2PlanningMemory(observed.planning.replacement,
                "v2-planning stepped builder");
            for (const name of PLANNING_VECTORS) {
                const component = observed.planning.replacement[name];
                seen[name] ||= component.length_slots > 0 && component.capacity_slots > 0;
            }
            assert.deepEqual(owner(tree), before,
                "v2-planning probe changed visible owner");
            sameStores(observed.components.resident, resident,
                "v2-planning probe changed resident components");
            sameStores(observed.components.replacement, EMPTY_STORE,
                "v2-planning probe allocated node output before admission");
            assert(completed <= workGuard(rowCount), "v2-planning build work runaway");
            if (PLANNING_VECTORS.every(name => seen[name])) {
                prepared = observed;
                assertPostSortFromInput(prepared.postSort.replacement, acceptedInput, false,
                    "v2-planning: accepted input moved through sort");
                break;
            }
            assert(!state.done && state.phase !== "plan ready",
                "v2-planning fixture skipped a planning Vec");
            if (turns % 256 === 0) await tick();
        }
        assert(unitProbe, "v2-planning qualification never entered unit-step mode");

        tree.cancel_replacement_rebuild_job_deferred(token); retiring = true;
        const transferred = observe(context, tree, "v2-planning/transferred", false,
            "retiring-unmeasured", "retiring-unmeasured");
        assertEmptyV2PlanningMemory(transferred.planning.replacement,
            "v2-planning transferred replacement");
        assert.deepEqual(transferred.planning.retiring, prepared.planning.replacement,
            "v2-planning owner changed across replacement→retirement transfer");
        assertPostSortTransfer(prepared.postSort, transferred.postSort,
            "v2-planning: post-sort transfer");
        assertV1EntriesTransfer(prepared.v1Entries, transferred.v1Entries,
            "v2-planning: V1 entry transfer");
        assertEmptyV1GraphSnapshot(prepared.v1Graph, "v2-planning: active V1 graph");
        assertV1GraphTransfer(prepared.v1Graph, transferred.v1Graph, "v2-planning: graph transfer");
        sameStores(transferred.components.replacement, EMPTY_STORE,
            "v2-planning transfer retained replacement chunks");
        sameStores(transferred.components.retiring, EMPTY_STORE,
            "v2-planning transfer fabricated retiring chunks");

        const retired = await retire(context, tree, token, rowCount, before,
            transferred.components, "v2-planning", transferred.input, transferred.sort,
            transferred.planning, 1);
        token = undefined; retiring = false;
        const drained = observe(context, tree, "v2-planning/drained", false, "exact", "exact");
        noPrivate(drained.components, "v2-planning drained");
        assertEmptyV2PlanningSnapshot(drained.planning, "v2-planning drained");
        assertEmptyV1EntriesSnapshot(drained.v1Entries, "v2-planning drained V1 entries");
        assert.deepEqual(owner(tree), before, "v2-planning cancel changed visible owner");
        sameLive(drained.memory, baseline, "v2-planning drained allocation baseline differs");
        return { variant: variant.name, version: 2, entries: rowCount,
            observed_nonempty_vectors: seen, build_units: completed,
            retirement_units: retired.completed, build_turns: turns,
            phases: context.report() };
    } finally {
        try {
            if (token !== undefined) {
                if (!retiring) tree.cancel_replacement_rebuild_job_deferred(token);
                const remaining = observe(context, tree, "v2-planning/failure-cleanup");
                await retire(context, tree, token, rowCount, owner(tree), remaining.components,
                    "v2-planning/failure-cleanup", remaining.input, remaining.sort,
                    remaining.planning, 1);
            }
        } finally { tree.free(); }
        sameLive(context.sample("v2-planning/tree-freed"), context.baseline,
            `${variant.name}: V2 planning tree free`);
    }
}

async function qualifyV2SortBoundaries(variant) {
    const context = await fresh(variant, `${variant.name}/v2-sort-boundaries`), runs = [];
    for (const count of [0, 1]) {
        const tree = requiredTree(context.module);
        let token, retiring = false;
        try {
            const before = owner(tree), treeBaseline = context.sample(`sort-boundary-${count}/baseline`);
            const rows = count === 0 ? [] : [{ path: "boundary/é-𐀀.md", hash: "a5".repeat(32),
                mtime_ms: 1_700_000_000_001, size: 1 }];
            const json = JSON.stringify(rows), bytes = Buffer.byteLength(json);
            token = tokenValue(tree.begin_replacement_rebuild_job(2, count, bytes));
            if (count !== 0) {
                const incomplete = replacementInputSnapshot(tree, `sort-boundary-${count}/incomplete`);
                const incompleteLive = context.sample(`sort-boundary-${count}/incomplete-live`);
                assert.throws(() => tree.replacement_rebuild_sort_memory_plan_v1_job(token),
                    "incomplete V2 input exposed a sort-memory plan");
                assert.throws(() => tree.start_replacement_rebuild_sort_memory_v1_job(token, 4, 4),
                    "incomplete V2 input accepted admitted sort start");
                assert.deepEqual(replacementInputSnapshot(tree, `sort-boundary-${count}/incomplete-after`), incomplete,
                    "incomplete admitted start changed input ownership");
                sameLive(context.sample(`sort-boundary-${count}/incomplete-rejected`), incompleteLive,
                    "incomplete admitted start retained native memory");
                assert.equal(tree.append_replacement_rebuild_job(token, 0, json), count);
            }
            const plan = startReplacementWithSortAdmission(
                context, tree, token, 2, count, `sort-boundary-${count}/start`);
            if (count === 1) {
                const state = progress(tree.step_replacement_rebuild_job(token, 1), 0, true,
                    `sort-boundary-${count}/partial-sort`);
                assert.deepEqual(state, { done: false, units: 1, completed: 1, phase: "sort" },
                    "one-row sorter did not expose its first bounded index unit");
            }
            const prepared = replacementSortSnapshot(tree, `sort-boundary-${count}/prepared`);
            assert.equal(prepared.replacement.entries.sorters, 1);
            assert(prepared.replacement.entries.source_indices.capacity_slots >= count);
            assert(prepared.replacement.entries.target_indices.capacity_slots >= count);
            tree.cancel_replacement_rebuild_job_deferred(token); retiring = true;
            const transferred = replacementSortSnapshot(tree, `sort-boundary-${count}/transferred`);
            assertEmptySortMemory(transferred.replacement, `sort-boundary-${count}/transferred replacement`);
            assert.deepEqual(transferred.retiring, prepared.replacement,
                `sort-boundary-${count}: sorter owner changed during deferred cancellation`);
            let completed = 0, turns = 0;
            for (;;) {
                const state = progress(tree.step_tree_retirement(token, 1), completed, false,
                    `sort-boundary-${count}/retirement`);
                completed = state.completed; turns++;
                assert.deepEqual(owner(tree), before,
                    `sort-boundary-${count}: retirement changed visible tree ownership`);
                if (state.done) break;
                assert(turns < 64, `sort-boundary-${count}: retirement did not converge`);
            }
            token = undefined; retiring = false;
            assertEmptySortSnapshot(replacementSortSnapshot(tree, `sort-boundary-${count}/drained`),
                `sort-boundary-${count}/drained`);
            assertEmptyInputSnapshot(replacementInputSnapshot(tree, `sort-boundary-${count}/drained-input`),
                `sort-boundary-${count}/drained-input`);
            sameLive(context.sample(`sort-boundary-${count}/drained-live`), treeBaseline,
                `sort-boundary-${count}: deferred cleanup did not restore the tree baseline`);
            runs.push({ count, plan, retirementUnits: completed });
        } finally {
            if (token !== undefined) {
                try {
                    if (!retiring) tree.cancel_replacement_rebuild_job_deferred(token);
                    while (!tree.step_tree_retirement(token, 256).done) { /* bounded native cleanup */ }
                } catch { /* retain the primary assertion */ }
            }
            tree.free();
        }
        sameLive(context.sample(`sort-boundary-${count}/tree-freed`), context.baseline,
            `${variant.name}: sort-boundary tree free`);
    }
    return { variant: variant.name, runs };
}

async function qualifyCorpus(variant, kind, version) {
    const started = performance.now(), input = corpus(kind);
    const expected = await oracleFor(variant, kind, version, input);
    const context = await fresh(variant, `${variant.name}/${kind}/v${version}`), cycles = [];
    for (let cycle = 0; cycle < 3; cycle++) {
        const tree = requiredTree(context.module);
        let activeToken, cleanupRows, retiring = false;
        try {
            // A real populated committed graph plus a distinct candidate is
            // present for every repair, not an empty-tree allocation shortcut.
            tree.rebuild_from_entries_in_version(version, input.json);
            tree.begin_candidate();
            tree.candidate_update_batch(JSON.stringify([{ ...input.rows[0], hash: "f3".repeat(32), mtime_ms: 1_800_000_000_000 }]));
            assert(tree.has_candidate()); assert.notEqual(tree.candidate_root_hash_hex(), tree.root_hash_hex());
            const visibleBefore = visible(context.module, tree), ownerBefore = owner(tree);
            const seeded = observe(context, tree, "seeded-committed-and-candidate", false, "exact", "exact");
            const baseline = seeded.memory, resident = seeded.components.resident;
            noPrivate(seeded.components, "seeded tree");
            assertEmptyInputSnapshot(seeded.input, "seeded tree input");
            assertEmptyV1EntriesSnapshot(seeded.v1Entries, "seeded tree V1 entries");
            assertEmptyV1GraphSnapshot(seeded.v1Graph, "seeded tree graph");
            assertEmptyV2PlanningSnapshot(seeded.planning, "seeded tree V2 planning");
            assertEmptyV2RangesSnapshot(seeded.ranges, "seeded tree V2 ranges");
            assert.equal(seeded.metadata.resident.committed.roots, 1);
            assert.equal(seeded.metadata.resident.candidate.roots, 1);
            assert.equal(seeded.metadata.resident.candidate_baseline.present, true);
            // This fixture has one initial rebuild and one candidate update.
            // The active-graph export is candidate reachability, not all
            // resident nodes: include superseded committed nodes, deduplicating
            // the shared ones. Length queries do not copy the node payloads.
            const committedHashes = context.module.wasm_tree_committed_chunk_hashes(tree);
            const candidateHashes = new Set(visibleBefore.chunks);
            assert(committedHashes.some(hash => !candidateHashes.has(hash)),
                `${context.label}: fixture lacks a committed-only resident node`);
            const residentHashes = [...new Set([...committedHashes, ...candidateHashes])].sort();
            residentComponent(context.module, tree, residentHashes, resident, context.label);
            sameLive(observe(context, tree, "seeded-component-verified").memory, baseline,
                `${context.label}: resident length queries/reporting retained native scratch`);
            const cancelled = [];
            for (const stage of ["empty-input", "partial-input", "input", "sorting", "partial", "ready",
                ...(version === 2 ? ["parent", "closure", "failed-duplicate"] : [])]) {
                const rows = stage === "failed-duplicate" ? [...input.rows, { ...input.rows[0] }] : input.rows;
                const expectedRows = stage === "empty-input" ? [] : rows;
                const acceptedRows = stage === "partial-input" ? rows.slice(0, 17) : expectedRows;
                const inputOnly = ["empty-input", "partial-input", "input"].includes(stage);
                const jsonBytes = stage === "failed-duplicate" ? Buffer.byteLength(JSON.stringify(rows))
                    : stage === "empty-input" ? 2 : input.bytes;
                assert(jsonBytes <= INPUT_BYTES);
                cleanupRows = acceptedRows;
                activeToken = tokenValue(tree.begin_replacement_rebuild_job(
                    version, expectedRows.length, jsonBytes));
                rejectReplacementOutputPlanPhase(tree, activeToken, `${stage}/input-phase`);
                const begun = observe(context, tree, `${stage}/begin`, false,
                    "replacement-unmeasured", "exact");
                sameStores(begun.components.resident, resident, `${stage}: begin changed resident store`);
                noPrivate(begun.components, `${stage}: input reservation`);
                assertInitialReplacementInput(begun.input, expectedRows.length, `${stage}: begin input`);
                assertEmptyV1EntriesSnapshot(begun.v1Entries, `${stage}: begin V1 entries`);
                assertEmptyV1GraphSnapshot(begun.v1Graph, `${stage}: begin graph`);
                assertEmptyV2RangesSnapshot(begun.ranges, `${stage}: begin ranges`);
                if (stage === "input") {
                    const full257 = JSON.stringify(rows.slice(0, 257));
                    const malformed257 = JSON.stringify(rows.slice(0, 256)).slice(0, -1) + ', {"path": [malformed]';
                    const oversizedRow = JSON.stringify([{ ...rows[0], path: "x".repeat(PAGE_BYTES) }]);
                    assert(Buffer.byteLength(full257) < PAGE_BYTES);
                    assert(Buffer.byteLength(malformed257) < PAGE_BYTES);
                    assert(Buffer.byteLength(oversizedRow) > PAGE_BYTES);
                    for (const [name, rejected, reason] of [
                        ["full-257-rows", full257, "replacement rebuild feed entry count is invalid"],
                        ["malformed-257th-row", malformed257, "replacement rebuild feed entry count is invalid"],
                        ["oversized-row", oversizedRow, "replacement rebuild feed JSON bound is invalid"],
                    ]) {
                        const beforeBadPage = observe(context, tree, `${name}/before`, false,
                            "replacement-unmeasured", "exact");
                        assert.throws(() => tree.append_replacement_rebuild_job(activeToken, 0, rejected),
                            error => !(error instanceof assert.AssertionError) && String(error).includes(reason),
                            `${name}: native preflight did not reject the intended boundary`);
                        const afterBadPage = observe(context, tree, `${name}/after`, false,
                            "replacement-unmeasured", "exact");
                        assert.deepEqual(afterBadPage.input, beforeBadPage.input,
                            `${context.label}/${name}: rejected page changed input ownership`);
                        assert.deepEqual(afterBadPage.v1Graph, beforeBadPage.v1Graph,
                            `${context.label}/${name}: rejected page changed graph ownership`);
                        assert.deepEqual(afterBadPage.ranges, beforeBadPage.ranges,
                            `${context.label}/${name}: rejected page changed range ownership`);
                        sameLive(afterBadPage.memory, beforeBadPage.memory,
                            `${context.label}/${name}: rejected page retained parsed scratch`);
                        sameStores(afterBadPage.components, beforeBadPage.components,
                            `${context.label}/${name}: rejected page changed component ownership`);
                        assert.deepEqual(owner(tree), ownerBefore);
                    }
                }
                const acceptedInput = feed(context, tree, activeToken, acceptedRows, resident, stage)
                    ?? begun.input.replacement;
                let built;
                if (!inputOnly) {
                    startReplacementWithSortAdmission(context, tree, activeToken, version, rows.length,
                        `${stage}/start`);
                    const started = observe(context, tree, `${stage}/start`, false,
                        "replacement-unmeasured", "replacement-unmeasured");
                    sameStores(started.components.resident, resident, `${stage}: start changed resident store`);
                    noPrivate(started.components, `${stage}: builder constructor`);
                    assertEmptyV2RangesSnapshot(started.ranges, `${stage}: builder constructor ranges`);
                    if (version === 2) {
                        assertActiveV2PlanningMemory(started.planning.replacement,
                            `${stage}: V2 builder constructor`);
                        assertEmptyV2PlanningMemory(started.planning.retiring,
                            `${stage}: V2 builder constructor retirement`);
                    } else assertEmptyV2PlanningSnapshot(started.planning,
                        `${stage}: V1 builder fabricated V2 planning ownership`);
                    if (version === 1) {
                        assert.equal(started.v1Graph.replacement.identities.strings, 0,
                            `${stage}: V1 constructor allocated identities before output admission`);
                        assert.equal(started.v1Graph.replacement.identities.length_bytes, 0,
                            `${stage}: V1 constructor allocated identity bytes before output admission`);
                        assert.equal(started.v1Entries.replacement.input.length_slots,
                            acceptedInput.entries.length_slots,
                            `${stage}: accepted input count changed at V1 start`);
                        assert.deepEqual(started.v1Entries.replacement.paths, acceptedInput.paths,
                            `${stage}: accepted path ownership changed at V1 start`);
                    } else assertEmptyV1EntriesSnapshot(started.v1Entries,
                        `${stage}: V2 builder fabricated V1 entry ownership`);
                    if (version === 2) assertEmptyV1GraphSnapshot(started.v1Graph,
                        `${stage}: V2 constructor fabricated V1 graph ownership`);
                    if (stage === "failed-duplicate") {
                        await assert.rejects(build(context, tree, activeToken, rows.length, ownerBefore,
                            resident, expected, stage, jsonBytes),
                            error => !(error instanceof assert.AssertionError) &&
                                String(error).includes("Tree v2 entries are not strictly ordered"),
                            "duplicate v2 entries must fail before publication");
                        const failed = observe(context, tree, `${stage}/failed`, false,
                            "replacement-unmeasured", "replacement-unmeasured");
                        sameStores(failed.components.resident, resident, `${stage}: failed build changed resident`);
                        assert.throws(() => tree.finish_replacement_rebuild_job_deferred(activeToken));
                    } else if (stage === "sorting") {
                        const active = started.sort.replacement?.entries;
                        built = active?.sorters === 1
                            ? { turns: 0, completed: 0, phase: "sort-entries", sort: started.sort }
                            : await advanceToEntrySort(context, tree, activeToken, rows.length,
                                ownerBefore, resident, stage);
                    } else built = await build(context, tree, activeToken, rows.length, ownerBefore, resident,
                        expected, stage, jsonBytes, stage === "closure" ? "closure"
                            : stage === "parent" ? "parent" : stage === "partial");
                }
                const preparedObservation = observe(context, tree, `${stage}/before-cancel`, false,
                    stage === "ready" || stage === "closure" ? "exact" : "replacement-unmeasured",
                    inputOnly ? "exact" : "replacement-unmeasured");
                const prepared = preparedObservation.components;
                if (version === 2 || inputOnly) assertEmptyV1EntriesSnapshot(
                    preparedObservation.v1Entries, `${stage}: non-V1-builder entry ownership`);
                else assert.equal(preparedObservation.v1Entries.replacement.paths.strings,
                    stage === "ready" ? 0 : preparedObservation.v1Entries.replacement.input.length_slots
                        + preparedObservation.v1Entries.replacement.groups.length_slots
                        + preparedObservation.v1Entries.replacement.sorting_rows
                        + preparedObservation.v1Entries.replacement.rows.length_slots
                        + preparedObservation.v1Entries.replacement.leaf.length_slots
                        + preparedObservation.v1Entries.replacement.retiring_rows.length_slots,
                    `${stage}: V1 path conservation changed`);
                if (version === 2 && !inputOnly && stage !== "sorting") {
                    assertPostSortFromInput(preparedObservation.postSort.replacement, acceptedInput,
                        stage === "ready", `${stage}: accepted post-sort input`);
                } else if (inputOnly || version === 1) {
                    assertEmptyV2PostSortSnapshot(preparedObservation.postSort,
                        `${stage}: input-only/V1 post-sort coverage`);
                }
                sameStores(prepared.resident, resident, `${stage}: build changed resident before cancel`);
                sameStores(prepared.retiring, EMPTY_STORE, `${stage}: premature retirement before cancel`);
                if (["partial", "parent", "ready", "closure"].includes(stage)) assert(prepared.replacement.chunks > 0,
                    `${stage}: cancellation did not exercise private node buffers`);
                if (stage === "ready") canonicalComponent(prepared.replacement, expected, `${context.label}: private ready`);
                if (stage === "sorting") {
                    const entrySort = preparedObservation.sort.replacement?.entries;
                    assert.equal(entrySort?.sorters, 1,
                        `${stage}: cancellation did not exercise an entry sorter`);
                    assert.equal(preparedObservation.sort.replacement.children.sorters, 0,
                        `${stage}: entry and child sorters overlap`);
                    const expectedSortRows = version === 2 || kind === "one-prefix" ? rows.length : 1;
                    assert.equal(entrySort.values.length_slots, expectedSortRows,
                        `${stage}: active entry-sort values differ from the corpus partition`);
                    if (version === 2) {
                        assert(entrySort.source_indices.capacity_slots >= expectedSortRows,
                            `${stage}: admitted source index capacity is below N`);
                        assert(entrySort.target_indices.capacity_slots >= expectedSortRows,
                            `${stage}: admitted target index capacity is below N`);
                        assert(entrySort.source_indices.backing_capacity_bytes >= 4 * expectedSortRows,
                            `${stage}: admitted source index backing is below 4*N`);
                        assert(entrySort.target_indices.backing_capacity_bytes >= 4 * expectedSortRows,
                            `${stage}: admitted target index backing is below 4*N`);
                    } else {
                        assert(entrySort.source_indices.capacity_slots >= expectedSortRows,
                            `${stage}: source index backing is smaller than the active partition`);
                        assert(entrySort.target_indices.capacity_slots >= expectedSortRows,
                            `${stage}: target index backing is smaller than the active partition`);
                    }
                } else {
                    assertEmptySortMemory(preparedObservation.sort.replacement,
                        `${stage}: unexpected replacement sorter before cancel`);
                }
                tree.cancel_replacement_rebuild_job_deferred(activeToken); retiring = true;
                rejectReplacementOutputPlanPhase(tree, activeToken, `${stage}/retiring-phase`, built?.output_plan);
                const transferredObservation = observe(context, tree, `${stage}/cancel-transferred`, false,
                    stage === "ready" || stage === "closure" ? "exact" : "retiring-unmeasured",
                    inputOnly ? "exact" : "retiring-unmeasured");
                const transferred = transferredObservation.components;
                sameStores(transferred.resident, prepared.resident, `${stage}: cancel changed resident ownership`);
                sameStores(transferred.replacement, EMPTY_STORE, `${stage}: cancel counted both replacement and retiring`);
                sameStores(transferred.retiring, prepared.replacement, `${stage}: cancel lost/duplicated private node owners`);
                assertEmptyInputMemory(transferredObservation.input.replacement,
                    `${stage}: transferred replacement input`);
                assertEmptySortMemory(transferredObservation.sort.replacement,
                    `${stage}: transferred replacement sort`);
                assert.deepEqual(transferredObservation.sort.retiring,
                    preparedObservation.sort.replacement,
                    `${stage}: exact sort owner changed across replacement→retirement transfer`);
                assertEmptyV2PlanningMemory(transferredObservation.planning.replacement,
                    `${stage}: transferred replacement V2 planning`);
                assert.deepEqual(transferredObservation.planning.retiring,
                    preparedObservation.planning.replacement,
                    `${stage}: exact V2 planning owner changed across replacement→retirement transfer`);
                assertPostSortTransfer(preparedObservation.postSort, transferredObservation.postSort,
                    `${stage}: exact post-sort transfer`);
                assertV1EntriesTransfer(preparedObservation.v1Entries,
                    transferredObservation.v1Entries, `${stage}: exact V1 entry transfer`);
                assertV1GraphTransfer(preparedObservation.v1Graph,
                    transferredObservation.v1Graph, `${stage}: exact graph transfer`);
                assertV2RangesTransfer(preparedObservation.ranges,
                    transferredObservation.ranges, `${stage}: exact ranges transfer`);
                if (preparedObservation.metadata.replacement !== null) {
                    assert.deepEqual(transferredObservation.metadata.retiring,
                        preparedObservation.metadata.replacement,
                        `${stage}: metadata owner changed across replacement→retirement transfer`);
                } else {
                    assert.equal(transferredObservation.metadata.retiring, null,
                        `${stage}: unmeasured replacement was fabricated as exact retirement`);
                    assert.equal(transferredObservation.metadata.other_private_jobs_unmeasured, true,
                        `${stage}: private metadata owner was fabricated as exact`);
                }
                if (inputOnly) {
                    assert.deepEqual(transferredObservation.input.retiring,
                        preparedObservation.input.replacement,
                        `${stage}: exact input owner changed across replacement→retirement transfer`);
                }
                const retired = await retire(context, tree, activeToken, acceptedRows, ownerBefore, transferred,
                    stage, transferredObservation.input, transferredObservation.sort,
                    transferredObservation.planning);
                rejectReplacementOutputPlanPhase(tree, activeToken, `${stage}/drained-token`, built?.output_plan);
                activeToken = undefined; retiring = false;
                const drained = observe(context, tree, `${stage}/drained`, false, "exact", "exact");
                assertEmptyInputSnapshot(drained.input, `${stage}: drained input`);
                assertEmptyV2RangesSnapshot(drained.ranges, `${stage}: drained ranges`);
                assertEmptySortSnapshot(drained.sort, `${stage}: drained sort`);
                assertEmptyV2PlanningSnapshot(drained.planning,
                    `${stage}: drained V2 planning`);
                assertEmptyV1EntriesSnapshot(drained.v1Entries,
                    `${stage}: drained V1 entries`);
                noPrivate(drained.components, `${stage}: cancelled and drained`);
                sameLive(drained.memory, baseline, `${context.label}/${stage}/cycle${cycle}`);
                assert.deepEqual(visible(context.module, tree), visibleBefore, `${stage}: cancel changed exact visible roots/chunks`);
                sameLive(observe(context, tree, `${stage}/verified`).memory, baseline, `${context.label}/${stage}: verification scratch leak`);
                cancelled.push({ stage, build: built ?? null, retired, before_cancel: prepared,
                    sort_before_cancel: preparedObservation.sort, after_cancel: transferred,
                    sort_after_cancel: transferredObservation.sort,
                    post_sort_before_cancel: preparedObservation.postSort,
                    post_sort_after_cancel: transferredObservation.postSort,
                    ranges_before_cancel: preparedObservation.ranges,
                    ranges_after_cancel: transferredObservation.ranges,
                    after_drain: drained.components });
            }
            cleanupRows = input.rows;
            activeToken = tokenValue(tree.begin_replacement_rebuild_job(version, input.rows.length, input.bytes));
            const publishBegin = observe(context, tree, "publish/begin", false,
                "replacement-unmeasured", "exact");
            sameStores(publishBegin.components.resident, resident, "publish begin changed resident");
            noPrivate(publishBegin.components, "publish begin");
            assertInitialReplacementInput(publishBegin.input, input.rows.length, "publish begin input");
            assertEmptyV1EntriesSnapshot(publishBegin.v1Entries, "publish begin V1 entries");
            assertEmptyV2RangesSnapshot(publishBegin.ranges, "publish begin ranges");
            const acceptedPublishInput = feed(context, tree, activeToken, input.rows, resident, "publish");
            startReplacementWithSortAdmission(context, tree, activeToken, version, input.rows.length,
                "publish/start");
            const publishStart = observe(context, tree, "publish/start", false,
                "replacement-unmeasured", "replacement-unmeasured");
            sameStores(publishStart.components.resident, resident, "publish start changed resident");
            noPrivate(publishStart.components, "publish start");
            assertEmptyV2RangesSnapshot(publishStart.ranges, "publish start ranges");
            if (version === 2) assertActiveV2PlanningMemory(
                publishStart.planning.replacement, "publish start V2 planning");
            else assertEmptyV2PlanningSnapshot(publishStart.planning,
                "publish start V1 planning");
            if (version === 1) {
                assert.equal(publishStart.v1Entries.replacement.input.length_slots, COUNT,
                    "publish start V1 input count changed");
                assert.deepEqual(publishStart.v1Entries.replacement.paths, acceptedPublishInput.paths,
                    "publish start V1 path ownership changed");
            } else assertEmptyV1EntriesSnapshot(publishStart.v1Entries,
                "publish start V2 fabricated V1 entries");
            const built = await build(context, tree, activeToken, COUNT, ownerBefore, resident,
                expected, "publish", input.bytes);
            if (version === 1) {
                assert(built.graph.metadata_transfer, "publish: graph→metadata handoff was not qualified");
                assert(built.graph.vectors.includes("root_children"), "publish: root Vec was never observed");
                if (kind === "one-prefix") {
                    assert.deepEqual(built.graph.vectors, V1_GRAPH_VECTORS.slice().sort(),
                        "publish: 25k single-prefix fixture skipped a graph Vec owner");
                    assert(built.graph.child_sort, "publish: 25 child labels never reached sorting");
                } else assert.equal(built.graph.child_sort, false,
                    "publish: one-file groups unexpectedly produced child sorters");
            }
            const preparedObservation = observe(context, tree, "publish/ready-components", false,
                "exact", "replacement-unmeasured");
            const prepared = preparedObservation.components;
            sameStores(prepared.resident, resident, "publish ready changed old resident");
            sameStores(prepared.retiring, EMPTY_STORE, "publish ready prematurely retired old resident");
            canonicalComponent(prepared.replacement, expected, `${context.label}: published private graph`);
            assert(preparedObservation.metadata.replacement !== null,
                "ready replacement metadata owner is not exact");
            if (version === 2) assertPostSortFromInput(preparedObservation.postSort.replacement,
                acceptedPublishInput, true, "publish: ready post-sort backing");
            else assertEmptyV2PostSortSnapshot(preparedObservation.postSort,
                "publish: V1 ready post-sort coverage");
            assertEmptyV1EntriesMemory(preparedObservation.v1Entries.replacement,
                "publish: ready V1 entry owner");
            tree.finish_replacement_rebuild_job_deferred(activeToken); retiring = true;
            const publishedOwner = owner(tree), publication = observe(context, tree,
                "publish/swapped-retiring", false, "other-unmeasured", "retiring-unmeasured");
            const published = publication.memory, transferred = publication.components;
            sameStores(transferred.resident, prepared.replacement, "swap changed prepared node ownership");
            sameStores(transferred.replacement, EMPTY_STORE, "swap double-counted prepared nodes");
            sameStores(transferred.retiring, prepared.resident, "swap lost/duplicated old resident nodes");
            assert.deepEqual(publication.metadata.resident, preparedObservation.metadata.replacement,
                "swap changed prepared metadata ownership");
            assert.deepEqual(publication.metadata.retiring, seeded.metadata.resident,
                "swap lost or duplicated old resident metadata ownership");
            assert.deepEqual(publication.metadata.wrapper_ids, seeded.metadata.wrapper_ids,
                "inner swap changed wrapper identity ownership");
            assertEmptyV2PlanningMemory(publication.planning.replacement,
                "swap retained replacement V2 planning owner");
            assert.deepEqual(publication.planning.retiring,
                preparedObservation.planning.replacement,
                "swap changed prepared V2 planning ownership");
            assertPostSortTransfer(preparedObservation.postSort, publication.postSort,
                "swap: post-sort transfer");
            assertV1EntriesTransfer(preparedObservation.v1Entries, publication.v1Entries,
                "swap: V1 entry transfer");
            assertV1GraphTransfer(preparedObservation.v1Graph, publication.v1Graph, "swap: graph transfer");
            assertV2RangesTransfer(preparedObservation.ranges, publication.ranges, "swap: range transfer");
            assert.equal(publishedOwner.committed, ownerBefore.committed + 1);
            assert.equal(publishedOwner.candidate, ownerBefore.candidate + 1);
            assert.equal(publishedOwner.active, false); assert.equal(publishedOwner.count, COUNT);
            const retired = await retire(context, tree, activeToken, input.rows, publishedOwner, transferred,
                "publish", publication.input, publication.sort, publication.planning);
            activeToken = undefined; retiring = false;
            const drained = observe(context, tree, "publish/drained-new-graph-live", false, "exact", "exact");
            assertEmptyInputSnapshot(drained.input, "publish: drained input");
            assertEmptyV2RangesSnapshot(drained.ranges, "publish: drained ranges");
            assertEmptySortSnapshot(drained.sort, "publish: drained sort");
            assertEmptyV2PlanningSnapshot(drained.planning,
                "publish: drained V2 planning");
            assertEmptyV1EntriesSnapshot(drained.v1Entries,
                "publish: drained V1 entries");
            const retained = drained.memory;
            noPrivate(drained.components, "published and drained");
            sameStores(drained.components.resident, prepared.replacement, "retirement changed published nodes");
            canonicalComponent(drained.components.resident, expected, `${context.label}: final resident`);
            assert(retained.live_requested_bytes > context.baseline.live_requested_bytes, "live published graph is not tracked");
            assert(retained.live_allocations > context.baseline.live_allocations);
            assert(retained.live_requested_bytes < published.live_requested_bytes, "old populated graph did not retire");
            assert.deepEqual(canonical(context.module, tree), expected, `${context.label}: rebuilt root/chunks differ from legacy`);
            sameLive(observe(context, tree, "publish/verified").memory, retained, `${context.label}: canonical export retains native scratch`);
            cycles.push({ cycle, cancelled, published: { built, retired,
                retained_requested_bytes: retained.live_requested_bytes, retained_live_allocations: retained.live_allocations,
                before_swap: prepared, after_swap: transferred,
                post_sort_before_swap: preparedObservation.postSort,
                post_sort_after_swap: publication.postSort,
                ranges_before_swap: preparedObservation.ranges, ranges_after_swap: publication.ranges,
                after_drain: drained.components } });
        } finally {
            // Failure remains a failure. Best-effort native cleanup is joined;
            // explicit free is only this verifier's owned disposable instance,
            // never a production wrapper or an admission/drain success claim.
            try {
                if (activeToken !== undefined) {
                    if (!retiring) tree.cancel_replacement_rebuild_job_deferred(activeToken);
                    const remaining = observe(context, tree, "failure-cleanup/transfer");
                    const rows = cleanupRowsFor(remaining.input, cleanupRows, COUNT + 1);
                    await retire(context, tree, activeToken, rows, owner(tree), remaining.components,
                        "failure-cleanup", remaining.input, remaining.sort, remaining.planning);
                }
            } finally { tree.free(); }
        }
        sameLive(context.sample("cycle-tree-freed"), context.baseline, `${context.label}: cycle${cycle} free`);
        sameLive(context.sample("cycle-tree-freed-repeat"), context.baseline, `${context.label}: repeated snapshot`);
        console.error(`WASM allocation verification: ${context.label} cycle ${cycle + 1}/3 passed`);
        await tick();
    }
    return { variant: variant.name, corpus: kind, version, entries: COUNT,
        distinct_top_level_prefixes: kind === "one-prefix" ? 1 : COUNT, input_json_bytes: input.bytes,
        baseline_requested_bytes: context.baseline.live_requested_bytes,
        baseline_live_allocations: context.baseline.live_allocations,
        phases: context.report(), cycles,
        final: context.snapshot(), elapsed_ms: Math.round(performance.now() - started),
        elapsed_scope: "corpus + separate legacy oracle + three complete lifecycle cycles + diagnostics" };
}

function compactPhases(phases) {
    const summary = {
        samples: 0, sampled_live_peak_bytes: 0, sampled_live_allocation_peak: 0,
        lifetime_requested_peak_bytes: 0, linear_memory_peak_bytes: 0,
        chunk_stores: Object.fromEntries(STORES.map(name => [name,
            Object.fromEntries(STORE_NUMBERS.map(field => [`sampled_max_${field}`, 0]))])),
    };
    for (const phase of Object.values(phases)) {
        summary.samples += phase.samples;
        summary.sampled_live_peak_bytes = Math.max(summary.sampled_live_peak_bytes,
            phase.sampled_live_peak_bytes);
        summary.sampled_live_allocation_peak = Math.max(summary.sampled_live_allocation_peak,
            phase.sampled_live_allocation_peak);
        summary.lifetime_requested_peak_bytes = Math.max(summary.lifetime_requested_peak_bytes,
            phase.lifetime_requested_peak_bytes);
        summary.linear_memory_peak_bytes = Math.max(summary.linear_memory_peak_bytes,
            phase.linear_pages_peak * 65_536);
        for (const name of STORES) for (const field of STORE_NUMBERS) {
            const key = `sampled_max_${field}`;
            summary.chunk_stores[name][key] = Math.max(summary.chunk_stores[name][key],
                phase.chunk_stores?.[name]?.[key] ?? 0);
        }
    }
    return summary;
}

function compactHandle(result) {
    return {
        variant: result.variant,
        baseline_requested_bytes: result.baseline_requested_bytes,
        baseline_live_allocations: result.baseline_live_allocations,
        independent_instances: result.independent_instances,
        phases: compactPhases(result.phases),
    };
}

function compactChildSort(result) {
    return { variant: result.variant, version: result.version, entries: result.entries,
        child_values: result.child_values, build_units: result.build_units,
        retirement_units: result.retirement_units, phases: compactPhases(result.phases) };
}

function compactV2Planning(result) {
    return { variant: result.variant, version: result.version, entries: result.entries,
        observed_nonempty_vectors: result.observed_nonempty_vectors,
        build_units: result.build_units, retirement_units: result.retirement_units,
        build_turns: result.build_turns, phases: compactPhases(result.phases) };
}

function compactV2Reachability(result) {
    return { variant: result.variant, version: result.version, entries: result.entries,
        nodes: result.nodes, root_children: result.root_children,
        root_step_successful_allocations: result.root_step_successful_allocations,
        expected_root_step_allocations: result.expected_root_step_allocations,
        root_step_successful_reallocations: result.root_step_successful_reallocations,
        traversal_units: result.traversal_units,
        partial_expansion_cancelled: result.partial_expansion_cancelled,
        phases: compactPhases(result.phases) };
}

function compactCorpus(result) {
    const cancelledStages = result.cycles[0]?.cancelled.map(stage => stage.stage) ?? [];
    for (const cycle of result.cycles) {
        assert.deepEqual(cycle.cancelled.map(stage => stage.stage), cancelledStages,
            `${result.variant}/${result.corpus}/v${result.version}: lifecycle stages differ by cycle`);
    }
    return {
        variant: result.variant,
        corpus: result.corpus,
        version: result.version,
        entries: result.entries,
        distinct_top_level_prefixes: result.distinct_top_level_prefixes,
        input_json_bytes: result.input_json_bytes,
        baseline_requested_bytes: result.baseline_requested_bytes,
        baseline_live_allocations: result.baseline_live_allocations,
        phases: compactPhases(result.phases),
        cycle_count: result.cycles.length,
        cancelled_stages: cancelledStages,
        published_range_checkpoints: result.cycles.map(cycle => cycle.published.built.ranges),
        published_retained_requested_bytes: result.cycles.map(cycle =>
            cycle.published.retained_requested_bytes),
        published_retained_live_allocations: result.cycles.map(cycle =>
            cycle.published.retained_live_allocations),
        final: result.final,
        elapsed_ms: result.elapsed_ms,
        elapsed_scope: result.elapsed_scope,
    };
}

const variants = [await loadVariant("scalar"), await loadVariant("simd")];
const handles = [], v2Reachability = [], childSorts = [], v2Planning = [], v2SortBoundaries = [],
    candidateOutputs = [], candidateOpens = [], candidateChunkPlans = [], corpora = [];
async function qualify(label, run) {
    try {
        return await run();
    } catch (error) {
        throw new Error(`${label}: ${String(error)}`, { cause: error });
    }
}
// V1 persists creation time in canonical root bytes. The independent oracle
// and every stepped build must share that input; advisory elapsed timing still
// uses performance.now(). This standalone process never changes a real client.
const dateNow = Date.now;
Date.now = () => 1_930_000_000_000;
try {
    for (const variant of variants) {
        handles.push(await qualify(`${variant.name}/handles`,
            () => qualifyHandlesAndIsolation(variant)));
        v2Reachability.push(await qualify(`${variant.name}/v2-reachability`,
            () => qualifyV2ReachabilityAllocations(variant)));
        childSorts.push(await qualify(`${variant.name}/v1-child-sort`,
            () => qualifyV1ChildSort(variant)));
        v2Planning.push(await qualify(`${variant.name}/v2-planning`,
            () => qualifyV2Planning(variant)));
        v2SortBoundaries.push(await qualify(`${variant.name}/v2-sort-boundaries`,
            () => qualifyV2SortBoundaries(variant)));
        candidateOutputs.push(await qualify(`${variant.name}/candidate-output`,
            () => qualifyCandidateMutationOutput(variant)));
        candidateOpens.push(await qualify(`${variant.name}/candidate-open`,
            () => qualifyCandidateOpenMemory(variant)));
        candidateChunkPlans.push(await qualify(`${variant.name}/candidate-chunk-plan`,
            () => qualifyCandidateChunkPlans(variant)));
        for (const kind of ["one-prefix", "wide-prefix"]) for (const version of [1, 2])
            corpora.push(await qualify(`${variant.name}/${kind}/v${version}`,
                () => qualifyCorpus(variant, kind, version)));
    }
} finally { Date.now = dateNow; }
assert.equal(candidateOutputs.length, 2);
assert.deepEqual(candidateOutputs[0].runs, candidateOutputs[1].runs,
    "scalar/SIMD candidate output plans, actuals, progress and retirement differ");
assert.equal(candidateOpens.length, 2);
assert.deepEqual(candidateOpens[0].runs, candidateOpens[1].runs,
    "scalar/SIMD candidate-open plans, allocation deltas and retirement differ");
assert.equal(candidateChunkPlans.length, 2);
assert.deepEqual(candidateChunkPlans[0].runs, candidateChunkPlans[1].runs,
    "scalar/SIMD candidate chunk plans, pages and retirement differ");
assert.equal(v2SortBoundaries.length, 2);
assert.deepEqual(v2SortBoundaries[0].runs, v2SortBoundaries[1].runs,
    "scalar/SIMD V2 sort admission plans or boundary retirement differ");
const detailedReport = process.env.OBSETYNC_WASM_MEMORY_DETAILS === "1";
console.log(JSON.stringify({ schema: 1, verification: "packaged-wasm-requested-allocation-lifecycle",
    advisory_only: true, includes_js_rss_or_admission: false,
    scope: "fresh wasm instance per variant/corpus/version; aggregate phase samples, monotonic lifetime peaks",
    chunk_component_scope: "node Vec payload/capacity only; candidate shares resident store once; excludes root/input/sort/codec/map metadata; map capacity is logical entries, not allocated bytes",
    metadata_component_scope: "exact root/path/identity String capacity, v1 root Vec backing, and candidate baseline logical keys/slots; excludes allocator and HashSet bucket bytes plus private input/sort/plan/codec worksets",
    input_component_scope: "exact accepted replacement Vec backing and path String capacity before builder start and during input-only retirement; builder ownership moves to version-specific components",
    v1_entries_component_scope: "exact Tree v1 original FileEntry redistribution across input/groups/sorter logical rows/rows/leaf/encoded retirement; sorter Vec backing stays exclusive to sort component; group keys/map nodes/child labels/codec/allocator overhead and admission excluded",
    v1_graph_component_scope: "exact retained V1 hash/child Vec backing and group-key/prefix/child-label/identity String capacity; child-sort Vec backing remains in sort, provisional root allocations transfer once to metadata; excludes inline hashes, BTree nodes, closure, codec scratch, allocator overhead and admission",
    sort_component_scope: "exact active replacement indirect-sort values/source/target Vec backing, including versioned V2 wasm32 requested-index admission, replacement→retirement transfer and bounded release; excludes nested Strings and other builder worksets",
    planning_component_scope: "exact six Tree v2 replacement planning POD descriptor Vec backings, including unit-step replacement→retirement transfer and release; excludes entries/path Strings, RangeRef endpoints, closure/maps, codec scratch, allocator overhead and admission",
    post_sort_component_scope: "exact Tree v2 post-sort entry Vec backing and nested path String capacity; nullable while sorter owns rows, exact through cleanup and replacement→retirement transfer; excludes RangeRef/closure/codec/allocator overhead and admission",
    ranges_component_scope: "exact four Tree v2 replacement RangeRef Vec backings and retained vector/map endpoint String capacities; root endpoints move to metadata while closure clones remain distinct; descriptors are logical count only, not HashMap bucket/inline bytes; general/candidate ranges, HashSet buckets, codec scratch, allocator overhead and admission remain excluded",
    reporting_scope: "component serialization may allocate transiently; warmed before the fixed instance baseline; disjoint additive component totals compared with a later global requested-live sample",
    report_detail: detailedReport ? "full phase/cycle detail" : "compact maxima; set OBSETYNC_WASM_MEMORY_DETAILS=1 for full detail",
    handles: detailedReport ? handles : handles.map(compactHandle),
    v2_reachability: detailedReport ? v2Reachability : v2Reachability.map(compactV2Reachability),
    v1_child_sort: detailedReport ? childSorts : childSorts.map(compactChildSort),
    v2_planning: detailedReport ? v2Planning : v2Planning.map(compactV2Planning),
    v2_sort_boundaries: v2SortBoundaries,
    candidate_mutation_output: candidateOutputs,
    candidate_mutation_output_scope: "bounded one-leaf V2 output plan and actual staged/resident transfer, plus scalar/SIMD V1 cancellation during multi-leaf canonical-shape validation and complete native cursor retirement; other mutation buffers remain explicitly unmeasured by component snapshots; no total-heap or quota claim",
    candidate_open_memory: candidateOpens,
    candidate_open_memory_scope: "exact V2 candidate-open root identity/endpoint requested bytes around a warmed resume; insertion-generation baseline uses no key snapshot; includes prepared-root cancellation, publication retirement, abort baseline and V1 rejection; not RSS or total-heap admission",
    candidate_chunk_plan_memory: candidateChunkPlans,
    candidate_chunk_plan_memory_scope: "exact two 32-byte-per-hash radix workspaces after admission on 25k-entry V1/V2 candidates; bounded 256-hash pages retain no native allocation; sort/page cancellation and success drain to the same candidate baseline across three cycles; reachable HashSet, page bridge values, allocator metadata, JS heap and RSS remain outside this scope",
    v2_output_settlement_scope: "exact post-commit/abort reachable node Vec payload lengths plus surviving root RangeRef UTF-8 endpoint lengths; excludes capacities, map buckets, identities, metadata, scratch, linear memory and RSS",
    corpora: detailedReport ? corpora : corpora.map(compactCorpus) }, null, 2));
