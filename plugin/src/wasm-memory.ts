/** Optional, synchronous allocator diagnostics from one instantiated WASM module. */
export interface WasmMemoryModule {
    wasm_memory_snapshot?: () => unknown;
}

/** Optional, synchronous accounting for chunk stores owned by one WasmTree. */
export interface WasmTreeChunkMemorySource {
    chunk_memory_snapshot?: () => unknown;
}

/** Optional root/identity/baseline metadata owned by one exact WasmTree. */
export interface WasmTreeMetadataMemorySource {
    metadata_memory_snapshot?: () => unknown;
}

/** Optional accepted replacement-input Vec/path ownership components. */
export interface WasmTreeReplacementInputMemorySource {
    replacement_input_memory_snapshot?: () => unknown;
}

/** Optional original FileEntry redistribution ownership in a V1 replacement. */
export interface WasmTreeReplacementV1EntriesMemorySource {
    replacement_v1_entries_memory_snapshot?: () => unknown;
}

/** Optional V1 replacement graph-assembly Vec/String ownership. */
export interface WasmTreeReplacementV1GraphMemorySource {
    replacement_v1_graph_memory_snapshot?: () => unknown;
}

/** Optional top-level Vec backing owned by replacement indirect sorters. */
export interface WasmTreeReplacementSortMemorySource {
    replacement_sort_memory_snapshot?: () => unknown;
}

/** Optional POD descriptor Vec backing retained by Tree v2 replacement planning. */
export interface WasmTreeReplacementV2PlanningMemorySource {
    replacement_v2_planning_memory_snapshot?: () => unknown;
}

/** Optional V2 entry Vec/path ownership after replacement sorting. */
export interface WasmTreeReplacementV2PostSortMemorySource {
    replacement_v2_post_sort_memory_snapshot?: () => unknown;
}

/** Optional V2 replacement RangeRef Vec and nested endpoint String ownership. */
export interface WasmTreeReplacementV2RangesMemorySource {
    replacement_v2_ranges_memory_snapshot?: () => unknown;
}

const COUNTERS = [
    "live_requested_bytes", "peak_requested_bytes", "live_allocations",
    "successful_allocations", "successful_reallocations", "allocation_failures",
    "reallocation_failures",
] as const;
const MISSING = Symbol("missing WASM memory field");
const SCOPE = "WASM scope:        this instance only · not RSS · not a quota";
const EXCLUSIONS = "WASM requested:    excludes stack/static, JS, other workers/instances, allocator overhead and realloc overlap";
const TREE_FIELDS = [
    "schema", "scope", "resident", "replacement", "retiring", "other_private_jobs_unmeasured",
] as const;
const TREE_SUMMARY_FIELDS = [
    "chunks", "map_capacity", "payload_bytes", "buffer_capacity_bytes", "counters_valid",
] as const;
const TREE_SCOPE = "Tree chunk scope:  root/candidate shared store counted once · node buffers summed across physically owned chunks";
const TREE_LIMITS = "Tree chunk limits: map cap is logical capacity, not bytes/allocated buckets · excludes metadata/sort/root strings · not total heap/RSS/admission";
const TREE_AGGREGATE = "Tree chunk total:  already included in WASM instance memory; do not add · other private jobs unmeasured: ";

function ownData(value: object, key: string): unknown {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor && "value" in descriptor ? descriptor.value : MISSING;
}

function safeCount(value: unknown): value is number {
    return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function bytes(value: number): string {
    if (value < 1024) return `${value} B`;
    const [divisor, unit] = value < 1024 ** 2 ? [1024, "KiB"]
        : value < 1024 ** 3 ? [1024 ** 2, "MiB"] : [1024 ** 3, "GiB"];
    return `${(value / Number(divisor)).toFixed(2)} ${unit} (${value} B)`;
}

function linearLine(value: number | null): string {
    return `WASM linear:       ${value === null ? "unavailable" : bytes(value)} · not RSS`;
}

function invalid(reason: string, linear: number | null = null): string[] {
    return [`WASM memory:       invalid (${reason})`, linearLine(linear), SCOPE, EXCLUSIONS];
}

function exactOwnFields(value: object, expected: readonly string[]): boolean {
    const keys = Reflect.ownKeys(value);
    return keys.length === expected.length && keys.every(key =>
        typeof key === "string" && (expected as readonly string[]).includes(key));
}

interface TreeChunkSummary {
    chunks: number;
    map_capacity: number;
    payload_bytes: number;
    buffer_capacity_bytes: number;
    counters_valid: boolean;
}

function treeSummary(value: unknown): TreeChunkSummary | null {
    if (typeof value !== "object" || value === null || Array.isArray(value)
        || !exactOwnFields(value, TREE_SUMMARY_FIELDS)) return null;
    const chunks = ownData(value, "chunks"), mapCapacity = ownData(value, "map_capacity");
    const payloadBytes = ownData(value, "payload_bytes");
    const bufferCapacityBytes = ownData(value, "buffer_capacity_bytes");
    const countersValid = ownData(value, "counters_valid");
    if (!safeCount(chunks) || !safeCount(mapCapacity) || !safeCount(payloadBytes)
        || !safeCount(bufferCapacityBytes) || typeof countersValid !== "boolean"
        || mapCapacity < chunks || bufferCapacityBytes < payloadBytes) return null;
    return {
        chunks, map_capacity: mapCapacity, payload_bytes: payloadBytes,
        buffer_capacity_bytes: bufferCapacityBytes, counters_valid: countersValid,
    };
}

function treeLine(label: string, value: TreeChunkSummary): string {
    const chunks = `${value.chunks} ${value.chunks === 1 ? "chunk" : "chunks"}`;
    const slots = `${value.map_capacity} ${value.map_capacity === 1 ? "slot" : "slots"}`;
    return `Tree chunks ${label}: ${chunks} · map cap ${slots} · `
        + `payload ${bytes(value.payload_bytes)} · buffer cap ${bytes(value.buffer_capacity_bytes)}`;
}

function invalidTree(reason: string): string[] {
    return [`Tree chunks:        invalid (${reason})`, TREE_SCOPE, TREE_LIMITS];
}

/**
 * Path-free, advisory diagnostics, not a ResourceBudget reservation or heap/RSS
 * measurement. The export is called once with its module receiver. Only known
 * own data fields are read; neither arbitrary payloads nor error text escape.
 * A malformed/invalid counter cannot become a fabricated zero. Independently
 * valid linear-memory size remains useful even when allocator counters fail.
 */
export function formatWasmMemoryDebug(module?: WasmMemoryModule): string[] {
    let raw: unknown;
    try {
        const snapshot = module?.wasm_memory_snapshot;
        if (snapshot === undefined) return ["WASM memory:       unsupported (snapshot export unavailable)"];
        if (typeof snapshot !== "function") return invalid("malformed snapshot export");
        raw = Reflect.apply(snapshot, module, []);
    } catch {
        return invalid("snapshot export failed");
    }

    let linear: number | null = null;
    try {
        if (typeof raw !== "object" || raw === null || Array.isArray(raw)
            || ownData(raw, "schema") !== 1 || ownData(raw, "scope") !== "wasm-instance") {
            return invalid("malformed snapshot");
        }
        const linearValue = ownData(raw, "linear_memory_bytes");
        if (safeCount(linearValue)) linear = linearValue;
        const enabled = ownData(raw, "enabled");
        const countersValid = ownData(raw, "counters_valid");
        const precisionLost = ownData(raw, "precision_lost");
        const values = COUNTERS.map(key => ownData(raw, key));
        if (typeof enabled !== "boolean" || typeof countersValid !== "boolean"
            || typeof precisionLost !== "boolean"
            || (linearValue !== null && !safeCount(linearValue))
            || values.some(value => value !== null && !safeCount(value))) {
            return invalid("malformed snapshot", linear);
        }
        if (!enabled) {
            if (!countersValid || precisionLost || linearValue !== null || values.some(value => value !== 0)) {
                return invalid("inconsistent disabled snapshot", linear);
            }
            return ["WASM memory:       disabled (allocator telemetry unavailable)", SCOPE, EXCLUSIONS];
        }
        if (precisionLost) return invalid("counter precision lost", linear);
        if (!countersValid) return invalid("native counters marked invalid", linear);
        if (!values.every(safeCount)) return invalid("missing counter values", linear);
        const [live, peak, allocations, successful, reallocations, allocationFailures, reallocationFailures] = values;
        if (peak < live) return invalid("inconsistent live/peak counters", linear);
        return [
            `WASM memory:       requested live ${bytes(live)} · peak ${bytes(peak)}`,
            `WASM allocations:  ${allocations} live · ${successful} alloc/zeroed successes · ${reallocations} realloc successes`,
            `WASM failures:     ${allocationFailures} alloc/zeroed · ${reallocationFailures} realloc`,
            linearLine(linear), SCOPE, EXCLUSIONS,
        ];
    } catch {
        return invalid("malformed snapshot", linear);
    }
}

/**
 * Path-free view of exact native chunk-store counters. These counters cover
 * physically owned node buffers only and are a subset of the module allocator
 * report above, never an independent total or an admission decision.
 */
export function formatTreeChunkMemoryDebug(tree?: WasmTreeChunkMemorySource): string[] {
    let raw: unknown;
    try {
        const snapshot = tree?.chunk_memory_snapshot;
        if (snapshot === undefined) {
            return ["Tree chunks:        unsupported (snapshot export unavailable)"];
        }
        if (typeof snapshot !== "function") return invalidTree("malformed snapshot export");
        raw = Reflect.apply(snapshot, tree, []);
    } catch {
        return invalidTree("snapshot export failed");
    }

    try {
        if (typeof raw !== "object" || raw === null || Array.isArray(raw)
            || !exactOwnFields(raw, TREE_FIELDS)
            || ownData(raw, "schema") !== 1
            || ownData(raw, "scope") !== "tree-chunk-stores") {
            return invalidTree("malformed snapshot");
        }
        const resident = treeSummary(ownData(raw, "resident"));
        const replacement = treeSummary(ownData(raw, "replacement"));
        const retiring = treeSummary(ownData(raw, "retiring"));
        const otherPrivate = ownData(raw, "other_private_jobs_unmeasured");
        if (!resident || !replacement || !retiring || typeof otherPrivate !== "boolean") {
            return invalidTree("malformed snapshot");
        }
        if (![resident, replacement, retiring].every(value => value.counters_valid)) {
            return invalidTree("native counters marked invalid");
        }
        return [
            treeLine("resident", resident),
            treeLine("private replacement", replacement),
            treeLine("retiring", retiring),
            TREE_SCOPE,
            TREE_LIMITS,
            TREE_AGGREGATE + (otherPrivate ? "yes" : "no"),
        ];
    } catch {
        return invalidTree("malformed snapshot");
    }
}

const METADATA_FIELDS = ["schema", "scope", "wrapper_ids", "resident", "replacement", "retiring", "other_private_jobs_unmeasured"] as const;
const STRING_FIELDS = ["strings", "length_bytes", "capacity_bytes", "counters_valid"] as const;
const ROOT_METADATA_FIELDS = ["roots", "strings", "v1_children_length", "v1_children_capacity", "v1_children_backing_capacity_bytes", "counters_valid"] as const;
const BASELINE_FIELDS = ["present", "hashes", "capacity_slots", "logical_hash_bytes", "counters_valid"] as const;
const TREE_METADATA_FIELDS = ["committed", "candidate", "tree_ids", "candidate_baseline", "counters_valid"] as const;
const METADATA_SCOPE = "Tree metadata scope: exact tree owner · already included in WASM instance memory; do not add · not total heap/RSS/admission";
const METADATA_LIMITS = "Tree metadata limits: baseline hash bytes are logical keys; set capacity is slots, not bucket bytes · excludes node buffers, input/sort/codec/other private scratch and allocator overhead";
const INPUT_FIELDS = ["schema", "scope", "replacement", "retiring", "other_input_owners_unmeasured"] as const;
const INPUT_MEMORY_FIELDS = ["entries", "paths", "counters_valid"] as const;
const VEC_MEMORY_FIELDS = ["owners", "length_slots", "capacity_slots", "slot_size_bytes", "backing_capacity_bytes", "counters_valid"] as const;
const INPUT_SCOPE = "Replacement input scope: accepted native FileEntry Vec + path Strings only · already included in WASM instance memory; do not add";
const INPUT_LIMITS = "Replacement input limits: excludes page/serde bridge, builder sort/group/plan/range/closure, allocator overhead, total heap/RSS and admission";
const V1_ENTRIES_FIELDS = ["schema", "scope", "replacement", "retiring", "other_entry_owners_unmeasured"] as const;
const V1_ENTRIES_MEMORY_FIELDS = ["input", "groups", "sorting_rows", "rows", "leaf", "retiring_rows", "paths", "counters_valid"] as const;
const V1_ENTRIES_SCOPE = "Replacement V1 entries scope: original FileEntry Vec backing + path Strings · sorter backing stays in replacement-sort · already included in WASM instance memory; do not add";
const V1_ENTRIES_LIMITS = "Replacement V1 entries limits: excludes group keys/map nodes, child labels/hashes, root/closure/codec scratch, allocator overhead, total heap/RSS and admission";
const V1_GRAPH_FIELDS = ["schema", "scope", "replacement", "retiring", "other_graph_owners_unmeasured"] as const;
const V1_GRAPH_MEMORY_FIELDS = ["group_keys", "prefix", "leaf_hashes", "hashes", "children", "sorting_children", "retiring_children", "child_labels", "root_children", "root_child_labels", "identities", "counters_valid"] as const;
const V1_GRAPH_SCOPE = "Replacement V1 graph scope: assembly Vec backing + owned Strings until provisional root enters metadata · already included in WASM instance memory; do not add";
const V1_GRAPH_LIMITS = "Replacement V1 graph limits: sorter backing is separate · excludes BTree/HashSet buckets, reachability closure, codecs, allocator overhead, total heap/RSS and admission";
const SORT_FIELDS = ["schema", "scope", "replacement", "retiring", "other_sort_owners_unmeasured"] as const;
const SORT_OWNER_FIELDS = ["entries", "children", "counters_valid"] as const;
const SORT_MEMORY_FIELDS = ["sorters", "values", "source_indices", "target_indices", "counters_valid"] as const;
const SORT_SCOPE = "Replacement sort scope: active top-level values/source/target Vec backing only · already included in WASM instance memory; do not add";
const SORT_LIMITS = "Replacement sort limits: excludes nested Strings, grouping/plan/range/closure/codec, allocator overhead, total heap/RSS and admission";
const PLANNING_FIELDS = ["schema", "scope", "replacement", "retiring", "other_planning_owners_unmeasured"] as const;
const PLANNING_MEMORY_FIELDS = ["spans", "leaf_spans", "planned_ranges", "next_planned_ranges", "planned_spans", "planned_internal_spans", "counters_valid"] as const;
const PLANNING_SCOPE = "Replacement V2 planning scope: six POD descriptor Vec backings only · already included in WASM instance memory; do not add";
const PLANNING_LIMITS = "Replacement V2 planning limits: excludes entries/path Strings, RangeRef endpoints, closure/maps, codec scratch, allocator overhead, total heap/RSS and admission";
const POST_SORT_FIELDS = ["schema", "scope", "replacement", "retiring", "other_post_sort_owners_unmeasured"] as const;
const POST_SORT_SCOPE = "Replacement V2 post-sort scope: exact FileEntry Vec + path Strings after sorting · already included in WASM instance memory; do not add";
const POST_SORT_LIMITS = "Replacement V2 post-sort limits: nullable during sorter ownership · excludes ranges/closure/maps/codec, allocator overhead, total heap/RSS and admission";
const RANGES_FIELDS = ["schema", "scope", "replacement", "retiring", "other_range_owners_unmeasured"] as const;
const RANGES_VECTORS = ["ranges", "next_ranges", "closure_pending", "closure_expanding"] as const;
const RANGES_MEMORY_FIELDS = [...RANGES_VECTORS, "vector_paths", "descriptor_ranges", "descriptor_paths", "counters_valid"] as const;
const RANGES_SCOPE = "Replacement V2 ranges scope: retained RangeRef Vec backing + vector/map endpoint Strings · already included in WASM instance memory; do not add";
const RANGES_LIMITS = "Replacement V2 ranges limits: descriptor count is logical, not map bytes · root endpoints stay in metadata · excludes HashMap/HashSet buckets, entries/sort/planning, codec scratch, allocator overhead, total heap/RSS and admission";

interface StringMemory {
    strings: number;
    length_bytes: number;
    capacity_bytes: number;
    counters_valid: boolean;
}
interface RootMetadata {
    roots: number;
    strings: StringMemory;
    v1_children_length: number;
    v1_children_capacity: number;
    v1_children_backing_capacity_bytes: number;
    counters_valid: boolean;
}
interface BaselineMemory {
    present: boolean;
    hashes: number;
    capacity_slots: number;
    logical_hash_bytes: number;
    counters_valid: boolean;
}
interface TreeMetadata {
    committed: RootMetadata;
    candidate: RootMetadata;
    tree_ids: StringMemory;
    candidate_baseline: BaselineMemory;
    counters_valid: boolean;
}

function metadataRecord(value: unknown, fields: readonly string[]): Record<string, unknown> | null {
    if (typeof value !== "object" || value === null || Array.isArray(value) || !exactOwnFields(value, fields)) return null;
    const record: Record<string, unknown> = {};
    for (const field of fields) {
        const entry = ownData(value, field);
        if (entry === MISSING) return null;
        record[field] = entry;
    }
    return record;
}

function stringMemory(value: unknown): StringMemory | null {
    const row = metadataRecord(value, STRING_FIELDS);
    if (!row || !safeCount(row.strings) || !safeCount(row.length_bytes) || !safeCount(row.capacity_bytes)
        || typeof row.counters_valid !== "boolean" || row.capacity_bytes < row.length_bytes
        || (row.strings === 0 && (row.length_bytes !== 0 || row.capacity_bytes !== 0))) return null;
    return row as unknown as StringMemory;
}

function rootMetadata(value: unknown): RootMetadata | null {
    const row = metadataRecord(value, ROOT_METADATA_FIELDS);
    if (!row) return null;
    const strings = stringMemory(row.strings);
    if (!strings || !safeCount(row.roots) || !safeCount(row.v1_children_length)
        || !safeCount(row.v1_children_capacity) || !safeCount(row.v1_children_backing_capacity_bytes)
        || typeof row.counters_valid !== "boolean" || row.v1_children_length > row.v1_children_capacity
        || (row.v1_children_capacity === 0 ? row.v1_children_backing_capacity_bytes !== 0
            : row.v1_children_backing_capacity_bytes < row.v1_children_capacity)
        || (row.roots === 0 && (strings.strings !== 0 || row.v1_children_length !== 0
            || row.v1_children_capacity !== 0 || row.v1_children_backing_capacity_bytes !== 0))) return null;
    return { ...row, strings } as unknown as RootMetadata;
}

function baselineMemory(value: unknown): BaselineMemory | null {
    const row = metadataRecord(value, BASELINE_FIELDS);
    if (!row || typeof row.present !== "boolean" || typeof row.counters_valid !== "boolean"
        || !safeCount(row.hashes) || !safeCount(row.capacity_slots) || !safeCount(row.logical_hash_bytes)
        || row.hashes > row.capacity_slots || row.hashes > Math.floor(Number.MAX_SAFE_INTEGER / 32)
        || row.logical_hash_bytes !== row.hashes * 32
        || (!row.present && (row.hashes !== 0 || row.capacity_slots !== 0 || row.logical_hash_bytes !== 0))) return null;
    return row as unknown as BaselineMemory;
}

function treeMetadata(value: unknown): TreeMetadata | null {
    const row = metadataRecord(value, TREE_METADATA_FIELDS);
    if (!row || typeof row.counters_valid !== "boolean") return null;
    const committed = rootMetadata(row.committed), candidate = rootMetadata(row.candidate);
    const treeIds = stringMemory(row.tree_ids), baseline = baselineMemory(row.candidate_baseline);
    if (!committed || !candidate || !treeIds || !baseline) return null;
    return { committed, candidate, tree_ids: treeIds, candidate_baseline: baseline, counters_valid: row.counters_valid };
}

function metadataValid(value: TreeMetadata): boolean {
    return value.counters_valid && value.committed.counters_valid && value.committed.strings.counters_valid
        && value.candidate.counters_valid && value.candidate.strings.counters_valid
        && value.tree_ids.counters_valid && value.candidate_baseline.counters_valid;
}

function stringMemoryText(value: StringMemory): string {
    return `${value.strings} strings · length ${bytes(value.length_bytes)} · capacity ${bytes(value.capacity_bytes)}`;
}

function metadataLines(label: string, value: TreeMetadata | null): string[] {
    if (!value) return [`Tree metadata ${label}: unmeasured`];
    const rootLine = (kind: string, root: RootMetadata) => `Tree metadata ${label} ${kind}: ${root.roots} roots · `
        + `${stringMemoryText(root.strings)} · v1 children ${root.v1_children_length}/${root.v1_children_capacity}`
        + ` · vector capacity ${bytes(root.v1_children_backing_capacity_bytes)}`;
    const baseline = value.candidate_baseline;
    return [rootLine("committed", value.committed), rootLine("candidate", value.candidate),
        `Tree metadata ${label} IDs: ${stringMemoryText(value.tree_ids)}`,
        `Tree metadata ${label} baseline: ${baseline.present ? "present" : "absent"} · ${baseline.hashes} hashes`
            + ` · ${baseline.capacity_slots} capacity slots · logical keys ${bytes(baseline.logical_hash_bytes)}`];
}

function invalidMetadata(reason: string): string[] {
    return [`Tree metadata:     invalid (${reason})`, METADATA_SCOPE, METADATA_LIMITS];
}

/** Advisory physical-owner metadata components, never a heap/quota total.
 * Unknown replacement/retirement coverage remains explicit, not fabricated zero.
 * Only exact own data fields are consumed; paths, IDs and arbitrary errors are
 * never echoed. Calling/serializing the native diagnostic may itself allocate. */
export function formatTreeMetadataMemoryDebug(tree?: WasmTreeMetadataMemorySource): string[] {
    let raw: unknown;
    try {
        const snapshot = tree?.metadata_memory_snapshot;
        if (snapshot === undefined) return ["Tree metadata:     unsupported (snapshot export unavailable)"];
        if (typeof snapshot !== "function") return invalidMetadata("malformed snapshot export");
        raw = Reflect.apply(snapshot, tree, []);
    } catch { return invalidMetadata("snapshot export failed"); }
    try {
        const row = metadataRecord(raw, METADATA_FIELDS);
        if (!row || row.schema !== 1 || row.scope !== "tree-metadata"
            || typeof row.other_private_jobs_unmeasured !== "boolean") return invalidMetadata("malformed snapshot");
        const wrapper = stringMemory(row.wrapper_ids), resident = treeMetadata(row.resident);
        const replacement = row.replacement === null ? null : treeMetadata(row.replacement);
        const retiring = row.retiring === null ? null : treeMetadata(row.retiring);
        if (!wrapper || !resident || (row.replacement !== null && !replacement) || (row.retiring !== null && !retiring)) {
            return invalidMetadata("malformed snapshot");
        }
        if ((row.replacement === null || row.retiring === null) && !row.other_private_jobs_unmeasured) {
            return invalidMetadata("contradictory private coverage");
        }
        if (!wrapper.counters_valid || !metadataValid(resident) || (replacement && !metadataValid(replacement))
            || (retiring && !metadataValid(retiring))) return invalidMetadata("native counters marked invalid");
        return [`Tree metadata wrapper IDs: ${stringMemoryText(wrapper)}`,
            ...metadataLines("resident", resident), ...metadataLines("private replacement", replacement),
            ...metadataLines("retiring", retiring), METADATA_SCOPE, METADATA_LIMITS,
            `Tree metadata other private jobs unmeasured: ${row.other_private_jobs_unmeasured ? "yes" : "no"}`];
    } catch { return invalidMetadata("malformed snapshot"); }
}

interface VecBackingMemory {
    owners: number;
    length_slots: number;
    capacity_slots: number;
    slot_size_bytes: number;
    backing_capacity_bytes: number;
    counters_valid: boolean;
}

interface ReplacementInputMemory {
    entries: VecBackingMemory;
    paths: StringMemory;
    counters_valid: boolean;
}

type VecBackingAggregateMemory = VecBackingMemory;

interface ReplacementV1EntriesMemory {
    input: VecBackingMemory;
    groups: VecBackingAggregateMemory;
    sorting_rows: number;
    rows: VecBackingMemory;
    leaf: VecBackingMemory;
    retiring_rows: VecBackingMemory;
    paths: StringMemory;
    counters_valid: boolean;
}

interface ReplacementV1GraphMemory {
    group_keys: StringMemory;
    prefix: StringMemory;
    leaf_hashes: VecBackingMemory;
    hashes: VecBackingMemory;
    children: VecBackingMemory;
    sorting_children: number;
    retiring_children: VecBackingMemory;
    child_labels: StringMemory;
    root_children: VecBackingMemory;
    root_child_labels: StringMemory;
    identities: StringMemory;
    counters_valid: boolean;
}

function vecBackingMemory(value: unknown): VecBackingMemory | null {
    const row = metadataRecord(value, VEC_MEMORY_FIELDS);
    if (!row || !safeCount(row.owners) || row.owners > 1 || !safeCount(row.length_slots)
        || !safeCount(row.capacity_slots) || !safeCount(row.slot_size_bytes)
        || !safeCount(row.backing_capacity_bytes) || typeof row.counters_valid !== "boolean"
        || row.length_slots > row.capacity_slots) return null;
    if (row.owners === 0) {
        if (row.length_slots !== 0 || row.capacity_slots !== 0 || row.slot_size_bytes !== 0
            || row.backing_capacity_bytes !== 0) return null;
    } else {
        if (row.slot_size_bytes === 0
            || row.capacity_slots > Math.floor(Number.MAX_SAFE_INTEGER / row.slot_size_bytes)
            || row.backing_capacity_bytes !== row.capacity_slots * row.slot_size_bytes) return null;
    }
    return row as unknown as VecBackingMemory;
}

function vecBackingAggregateMemory(value: unknown): VecBackingAggregateMemory | null {
    const row = metadataRecord(value, VEC_MEMORY_FIELDS);
    if (!row || !safeCount(row.owners) || !safeCount(row.length_slots)
        || !safeCount(row.capacity_slots) || !safeCount(row.slot_size_bytes)
        || !safeCount(row.backing_capacity_bytes) || typeof row.counters_valid !== "boolean"
        || row.length_slots > row.capacity_slots) return null;
    if (row.owners === 0) {
        if (row.length_slots !== 0 || row.capacity_slots !== 0 || row.slot_size_bytes !== 0
            || row.backing_capacity_bytes !== 0) return null;
    } else if (row.slot_size_bytes === 0
        || row.capacity_slots > Math.floor(Number.MAX_SAFE_INTEGER / row.slot_size_bytes)
        || row.backing_capacity_bytes !== row.capacity_slots * row.slot_size_bytes) return null;
    return row as unknown as VecBackingAggregateMemory;
}

function replacementInputMemory(value: unknown): ReplacementInputMemory | null {
    const row = metadataRecord(value, INPUT_MEMORY_FIELDS);
    if (!row || typeof row.counters_valid !== "boolean") return null;
    const entries = vecBackingMemory(row.entries), paths = stringMemory(row.paths);
    if (!entries || !paths || entries.length_slots !== paths.strings) return null;
    return { entries, paths, counters_valid: row.counters_valid };
}

function replacementInputLine(label: string, value: ReplacementInputMemory | null): string {
    if (!value) return `Replacement input ${label}: unmeasured`;
    return `Replacement input ${label}: ${value.entries.length_slots}/${value.entries.capacity_slots} entries`
        + ` · slot ${bytes(value.entries.slot_size_bytes)} · Vec backing ${bytes(value.entries.backing_capacity_bytes)}`
        + ` · paths ${stringMemoryText(value.paths)}`;
}

function invalidReplacementInput(reason: string): string[] {
    return [`Replacement input:  invalid (${reason})`, INPUT_SCOPE, INPUT_LIMITS];
}

/** Exact accepted input owner components until start redistributes its rows.
 * Nullable builder coverage is explicit and cannot masquerade as measured zero. */
export function formatReplacementInputMemoryDebug(tree?: WasmTreeReplacementInputMemorySource): string[] {
    let raw: unknown;
    try {
        const snapshot = tree?.replacement_input_memory_snapshot;
        if (snapshot === undefined) return ["Replacement input:  unsupported (snapshot export unavailable)"];
        if (typeof snapshot !== "function") return invalidReplacementInput("malformed snapshot export");
        raw = Reflect.apply(snapshot, tree, []);
    } catch { return invalidReplacementInput("snapshot export failed"); }
    try {
        const row = metadataRecord(raw, INPUT_FIELDS);
        if (!row || row.schema !== 1 || row.scope !== "replacement-input"
            || typeof row.other_input_owners_unmeasured !== "boolean") {
            return invalidReplacementInput("malformed snapshot");
        }
        const replacement = row.replacement === null ? null : replacementInputMemory(row.replacement);
        const retiring = row.retiring === null ? null : replacementInputMemory(row.retiring);
        if ((row.replacement !== null && !replacement) || (row.retiring !== null && !retiring)
            || ((row.replacement === null || row.retiring === null) && !row.other_input_owners_unmeasured)) {
            return invalidReplacementInput("malformed snapshot");
        }
        if ((replacement && (!replacement.counters_valid || !replacement.entries.counters_valid
            || !replacement.paths.counters_valid)) || (retiring && (!retiring.counters_valid
            || !retiring.entries.counters_valid || !retiring.paths.counters_valid))) {
            return invalidReplacementInput("native counters marked invalid");
        }
        return [replacementInputLine("private", replacement), replacementInputLine("retiring", retiring),
            INPUT_SCOPE, INPUT_LIMITS,
            `Replacement input other owners unmeasured: ${row.other_input_owners_unmeasured ? "yes" : "no"}`];
    } catch { return invalidReplacementInput("malformed snapshot"); }
}

function replacementV1EntriesMemory(value: unknown): ReplacementV1EntriesMemory | null {
    const row = metadataRecord(value, V1_ENTRIES_MEMORY_FIELDS);
    if (!row || !safeCount(row.sorting_rows) || typeof row.counters_valid !== "boolean") return null;
    const input = vecBackingMemory(row.input), groups = vecBackingAggregateMemory(row.groups);
    const rows = vecBackingMemory(row.rows), leaf = vecBackingMemory(row.leaf);
    const retiringRows = vecBackingMemory(row.retiring_rows), paths = stringMemory(row.paths);
    if (!input || !groups || !rows || !leaf || !retiringRows || !paths) return null;
    const counts = [input.length_slots, groups.length_slots, row.sorting_rows,
        rows.length_slots, leaf.length_slots, retiringRows.length_slots];
    const total = counts.reduce((sum, count) => sum + count, 0);
    const slotSizes = [input, groups, rows, leaf, retiringRows]
        .filter(owner => owner.owners > 0).map(owner => owner.slot_size_bytes);
    if (!safeCount(total) || total !== paths.strings || groups.owners > groups.capacity_slots
        || new Set(slotSizes).size > 1) return null;
    return { input, groups, sorting_rows: row.sorting_rows, rows, leaf,
        retiring_rows: retiringRows, paths, counters_valid: row.counters_valid };
}

function replacementV1EntriesLine(label: string, value: ReplacementV1EntriesMemory | null): string {
    if (!value) return `Replacement V1 entries ${label}: unmeasured`;
    const owner = (name: string, row: VecBackingMemory) => `${name} ${row.length_slots}/${row.capacity_slots}`
        + ` (${bytes(row.backing_capacity_bytes)})`;
    return `Replacement V1 entries ${label}: ${owner("input", value.input)}`
        + ` · groups ${value.groups.owners} owners ${value.groups.length_slots}/${value.groups.capacity_slots}`
        + ` (${bytes(value.groups.backing_capacity_bytes)}) · sorting ${value.sorting_rows}`
        + ` · ${owner("rows", value.rows)} · ${owner("leaf", value.leaf)}`
        + ` · ${owner("encoded", value.retiring_rows)} · paths ${stringMemoryText(value.paths)}`;
}

function invalidReplacementV1Entries(reason: string): string[] {
    return [`Replacement V1 entries: invalid (${reason})`, V1_ENTRIES_SCOPE, V1_ENTRIES_LIMITS];
}

/** Exact retained original-entry owners for a private V1 replacement. */
export function formatReplacementV1EntriesMemoryDebug(
    tree?: WasmTreeReplacementV1EntriesMemorySource,
): string[] {
    let raw: unknown;
    try {
        const snapshot = tree?.replacement_v1_entries_memory_snapshot;
        if (snapshot === undefined) {
            return ["Replacement V1 entries: unsupported (snapshot export unavailable)"];
        }
        if (typeof snapshot !== "function") return invalidReplacementV1Entries("malformed snapshot export");
        raw = Reflect.apply(snapshot, tree, []);
    } catch { return invalidReplacementV1Entries("snapshot export failed"); }
    try {
        const row = metadataRecord(raw, V1_ENTRIES_FIELDS);
        if (!row || row.schema !== 1 || row.scope !== "replacement-v1-entries"
            || typeof row.other_entry_owners_unmeasured !== "boolean") {
            return invalidReplacementV1Entries("malformed snapshot");
        }
        const replacement = row.replacement === null ? null : replacementV1EntriesMemory(row.replacement);
        const retiring = row.retiring === null ? null : replacementV1EntriesMemory(row.retiring);
        if ((row.replacement !== null && !replacement) || (row.retiring !== null && !retiring)
            || ((row.replacement === null || row.retiring === null)
                && !row.other_entry_owners_unmeasured)) {
            return invalidReplacementV1Entries("malformed snapshot");
        }
        const invalid = (owner: ReplacementV1EntriesMemory | null): boolean => !!owner
            && (!owner.counters_valid || !owner.input.counters_valid || !owner.groups.counters_valid
                || !owner.rows.counters_valid || !owner.leaf.counters_valid
                || !owner.retiring_rows.counters_valid || !owner.paths.counters_valid);
        if (invalid(replacement) || invalid(retiring)) {
            return invalidReplacementV1Entries("native counters marked invalid");
        }
        return [replacementV1EntriesLine("private", replacement),
            replacementV1EntriesLine("retiring", retiring), V1_ENTRIES_SCOPE, V1_ENTRIES_LIMITS,
            `Replacement V1 entries other owners unmeasured: ${row.other_entry_owners_unmeasured ? "yes" : "no"}`];
    } catch { return invalidReplacementV1Entries("malformed snapshot"); }
}

function replacementV1GraphMemory(value: unknown): ReplacementV1GraphMemory | null {
    const row = metadataRecord(value, V1_GRAPH_MEMORY_FIELDS);
    if (!row || !safeCount(row.sorting_children) || typeof row.counters_valid !== "boolean") return null;
    const groupKeys = stringMemory(row.group_keys), prefix = stringMemory(row.prefix);
    const leafHashes = vecBackingMemory(row.leaf_hashes), hashes = vecBackingMemory(row.hashes);
    const children = vecBackingMemory(row.children);
    const retiringChildren = vecBackingMemory(row.retiring_children);
    const childLabels = stringMemory(row.child_labels);
    const rootChildren = vecBackingMemory(row.root_children);
    const rootChildLabels = stringMemory(row.root_child_labels);
    const identities = stringMemory(row.identities);
    if (!groupKeys || !prefix || !leafHashes || !hashes || !children || !retiringChildren
        || !childLabels || !rootChildren || !rootChildLabels || !identities
        || prefix.strings > 1 || identities.strings > 2) return null;
    const childRows = children.length_slots + row.sorting_children + retiringChildren.length_slots;
    const hashRows = leafHashes.length_slots + hashes.length_slots;
    if (!safeCount(childRows) || !safeCount(hashRows) || childRows !== childLabels.strings
        || rootChildren.length_slots !== rootChildLabels.strings) return null;
    const aggregateBytes = [groupKeys.capacity_bytes, prefix.capacity_bytes,
        leafHashes.backing_capacity_bytes, hashes.backing_capacity_bytes,
        children.backing_capacity_bytes, retiringChildren.backing_capacity_bytes,
        childLabels.capacity_bytes, rootChildren.backing_capacity_bytes,
        rootChildLabels.capacity_bytes, identities.capacity_bytes]
        .reduce((sum, amount) => sum + amount, 0);
    if (!safeCount(aggregateBytes)) return null;
    const sameLiveSlotSize = (owners: VecBackingMemory[]): boolean =>
        new Set(owners.filter(owner => owner.owners > 0).map(owner => owner.slot_size_bytes)).size <= 1;
    if (!sameLiveSlotSize([leafHashes, hashes])
        || !sameLiveSlotSize([children, retiringChildren, rootChildren])) return null;
    return {
        group_keys: groupKeys, prefix, leaf_hashes: leafHashes, hashes,
        children, sorting_children: row.sorting_children,
        retiring_children: retiringChildren, child_labels: childLabels,
        root_children: rootChildren, root_child_labels: rootChildLabels,
        identities, counters_valid: row.counters_valid,
    };
}

function replacementV1GraphLine(label: string, value: ReplacementV1GraphMemory | null): string {
    if (!value) return `Replacement V1 graph ${label}: unmeasured`;
    const hashRows = value.leaf_hashes.length_slots + value.hashes.length_slots;
    const childRows = value.children.length_slots + value.sorting_children
        + value.retiring_children.length_slots;
    const vecBytes = value.leaf_hashes.backing_capacity_bytes + value.hashes.backing_capacity_bytes
        + value.children.backing_capacity_bytes + value.retiring_children.backing_capacity_bytes
        + value.root_children.backing_capacity_bytes;
    const stringBytes = value.group_keys.capacity_bytes + value.prefix.capacity_bytes
        + value.child_labels.capacity_bytes + value.root_child_labels.capacity_bytes
        + value.identities.capacity_bytes;
    return `Replacement V1 graph ${label}: groups ${value.group_keys.strings} · hashes ${hashRows}`
        + ` · children ${childRows} · root children ${value.root_children.length_slots}`
        + ` · Vec backing ${bytes(vecBytes)} · String capacity ${bytes(stringBytes)}`;
}

function invalidReplacementV1Graph(reason: string): string[] {
    return [`Replacement V1 graph: invalid (${reason})`, V1_GRAPH_SCOPE, V1_GRAPH_LIMITS];
}

/** Exact V1 graph-assembly components, never a total builder/native budget. */
export function formatReplacementV1GraphMemoryDebug(
    tree?: WasmTreeReplacementV1GraphMemorySource,
): string[] {
    let raw: unknown;
    try {
        const snapshot = tree?.replacement_v1_graph_memory_snapshot;
        if (snapshot === undefined) {
            return ["Replacement V1 graph: unsupported (snapshot export unavailable)"];
        }
        if (typeof snapshot !== "function") return invalidReplacementV1Graph("malformed snapshot export");
        raw = Reflect.apply(snapshot, tree, []);
    } catch { return invalidReplacementV1Graph("snapshot export failed"); }
    try {
        const row = metadataRecord(raw, V1_GRAPH_FIELDS);
        if (!row || row.schema !== 1 || row.scope !== "replacement-v1-graph"
            || typeof row.other_graph_owners_unmeasured !== "boolean") {
            return invalidReplacementV1Graph("malformed snapshot");
        }
        const replacement = row.replacement === null ? null : replacementV1GraphMemory(row.replacement);
        const retiring = row.retiring === null ? null : replacementV1GraphMemory(row.retiring);
        if ((row.replacement !== null && !replacement) || (row.retiring !== null && !retiring)
            || ((row.replacement === null || row.retiring === null)
                && !row.other_graph_owners_unmeasured)) {
            return invalidReplacementV1Graph("malformed snapshot");
        }
        const invalid = (owner: ReplacementV1GraphMemory | null): boolean => !!owner
            && (!owner.counters_valid || !owner.group_keys.counters_valid || !owner.prefix.counters_valid
                || !owner.leaf_hashes.counters_valid || !owner.hashes.counters_valid
                || !owner.children.counters_valid || !owner.retiring_children.counters_valid
                || !owner.child_labels.counters_valid || !owner.root_children.counters_valid
                || !owner.root_child_labels.counters_valid || !owner.identities.counters_valid);
        if (invalid(replacement) || invalid(retiring)) {
            return invalidReplacementV1Graph("native counters marked invalid");
        }
        return [replacementV1GraphLine("private", replacement),
            replacementV1GraphLine("retiring", retiring), V1_GRAPH_SCOPE, V1_GRAPH_LIMITS,
            `Replacement V1 graph other owners unmeasured: ${row.other_graph_owners_unmeasured ? "yes" : "no"}`];
    } catch { return invalidReplacementV1Graph("malformed snapshot"); }
}

function postSortLine(label: string, value: ReplacementInputMemory | null): string {
    if (!value) return `Replacement V2 post-sort ${label}: unmeasured`;
    return `Replacement V2 post-sort ${label}: ${value.entries.length_slots}/${value.entries.capacity_slots} entries`
        + ` · slot ${bytes(value.entries.slot_size_bytes)} · Vec backing ${bytes(value.entries.backing_capacity_bytes)}`
        + ` · paths ${stringMemoryText(value.paths)}`;
}

function invalidReplacementV2PostSort(reason: string): string[] {
    return [`Replacement V2 post-sort: invalid (${reason})`, POST_SORT_SCOPE, POST_SORT_LIMITS];
}

/** Exact post-sort V2 entry owner components, never total builder/native memory. */
export function formatReplacementV2PostSortMemoryDebug(
    tree?: WasmTreeReplacementV2PostSortMemorySource,
): string[] {
    let raw: unknown;
    try {
        const snapshot = tree?.replacement_v2_post_sort_memory_snapshot;
        if (snapshot === undefined) {
            return ["Replacement V2 post-sort: unsupported (snapshot export unavailable)"];
        }
        if (typeof snapshot !== "function") {
            return invalidReplacementV2PostSort("malformed snapshot export");
        }
        raw = Reflect.apply(snapshot, tree, []);
    } catch { return invalidReplacementV2PostSort("snapshot export failed"); }
    try {
        const row = metadataRecord(raw, POST_SORT_FIELDS);
        if (!row || row.schema !== 1 || row.scope !== "replacement-v2-post-sort"
            || typeof row.other_post_sort_owners_unmeasured !== "boolean") {
            return invalidReplacementV2PostSort("malformed snapshot");
        }
        const replacement = row.replacement === null ? null : replacementInputMemory(row.replacement);
        const retiring = row.retiring === null ? null : replacementInputMemory(row.retiring);
        if ((row.replacement !== null && !replacement) || (row.retiring !== null && !retiring)
            || ((row.replacement === null || row.retiring === null)
                && !row.other_post_sort_owners_unmeasured)) {
            return invalidReplacementV2PostSort("malformed snapshot");
        }
        const invalid = (owner: ReplacementInputMemory | null): boolean => !!owner
            && (!owner.counters_valid || !owner.entries.counters_valid || !owner.paths.counters_valid);
        if (invalid(replacement) || invalid(retiring)) {
            return invalidReplacementV2PostSort("native counters marked invalid");
        }
        return [postSortLine("private", replacement), postSortLine("retiring", retiring),
            POST_SORT_SCOPE, POST_SORT_LIMITS,
            `Replacement V2 post-sort other owners unmeasured: ${row.other_post_sort_owners_unmeasured ? "yes" : "no"}`];
    } catch { return invalidReplacementV2PostSort("malformed snapshot"); }
}

interface IndirectSortMemory {
    sorters: number;
    values: VecBackingMemory;
    source_indices: VecBackingMemory;
    target_indices: VecBackingMemory;
    counters_valid: boolean;
}

interface ReplacementSortMemory {
    entries: IndirectSortMemory;
    children: IndirectSortMemory;
    counters_valid: boolean;
}

function indirectSortMemory(value: unknown): IndirectSortMemory | null {
    const row = metadataRecord(value, SORT_MEMORY_FIELDS);
    if (!row || !safeCount(row.sorters) || row.sorters > 1
        || typeof row.counters_valid !== "boolean") return null;
    const values = vecBackingMemory(row.values), source = vecBackingMemory(row.source_indices);
    const target = vecBackingMemory(row.target_indices);
    if (!values || !source || !target) return null;
    const owners = [values.owners, source.owners, target.owners];
    if (row.sorters === 0 ? owners.some(owner => owner !== 0) : owners.some(owner => owner !== 1)) {
        return null;
    }
    if (row.sorters === 1 && source.slot_size_bytes !== target.slot_size_bytes) return null;
    return { sorters: row.sorters, values, source_indices: source, target_indices: target,
        counters_valid: row.counters_valid };
}

function replacementSortMemory(value: unknown): ReplacementSortMemory | null {
    const row = metadataRecord(value, SORT_OWNER_FIELDS);
    if (!row || typeof row.counters_valid !== "boolean") return null;
    const entries = indirectSortMemory(row.entries), children = indirectSortMemory(row.children);
    if (!entries || !children || entries.sorters + children.sorters > 1) return null;
    return { entries, children, counters_valid: row.counters_valid };
}

function sortMemoryLine(owner: string, kind: string, value: IndirectSortMemory): string {
    if (value.sorters === 0) return `Replacement sort ${owner} ${kind}: none`;
    const vector = (name: string, row: VecBackingMemory) => `${name} ${row.length_slots}/${row.capacity_slots}`
        + ` × ${bytes(row.slot_size_bytes)} · backing ${bytes(row.backing_capacity_bytes)}`;
    return `Replacement sort ${owner} ${kind}: ${vector("values", value.values)}`
        + ` · ${vector("source", value.source_indices)} · ${vector("target", value.target_indices)}`;
}

function sortOwnerLines(label: string, value: ReplacementSortMemory | null): string[] {
    if (!value) return [`Replacement sort ${label}: unmeasured`];
    return [sortMemoryLine(label, "entries", value.entries),
        sortMemoryLine(label, "children", value.children)];
}

function invalidReplacementSort(reason: string): string[] {
    return [`Replacement sort:   invalid (${reason})`, SORT_SCOPE, SORT_LIMITS];
}

/** Exact active replacement-sort Vec backing, never total builder/native memory. */
export function formatReplacementSortMemoryDebug(tree?: WasmTreeReplacementSortMemorySource): string[] {
    let raw: unknown;
    try {
        const snapshot = tree?.replacement_sort_memory_snapshot;
        if (snapshot === undefined) return ["Replacement sort:   unsupported (snapshot export unavailable)"];
        if (typeof snapshot !== "function") return invalidReplacementSort("malformed snapshot export");
        raw = Reflect.apply(snapshot, tree, []);
    } catch { return invalidReplacementSort("snapshot export failed"); }
    try {
        const row = metadataRecord(raw, SORT_FIELDS);
        if (!row || row.schema !== 1 || row.scope !== "replacement-sort"
            || typeof row.other_sort_owners_unmeasured !== "boolean") {
            return invalidReplacementSort("malformed snapshot");
        }
        const replacement = row.replacement === null ? null : replacementSortMemory(row.replacement);
        const retiring = row.retiring === null ? null : replacementSortMemory(row.retiring);
        if ((row.replacement !== null && !replacement) || (row.retiring !== null && !retiring)
            || ((row.replacement === null || row.retiring === null) && !row.other_sort_owners_unmeasured)) {
            return invalidReplacementSort("malformed snapshot");
        }
        const invalid = (owner: ReplacementSortMemory | null): boolean => !!owner
            && (!owner.counters_valid || !owner.entries.counters_valid || !owner.children.counters_valid
                || !owner.entries.values.counters_valid || !owner.entries.source_indices.counters_valid
                || !owner.entries.target_indices.counters_valid || !owner.children.values.counters_valid
                || !owner.children.source_indices.counters_valid || !owner.children.target_indices.counters_valid);
        if (invalid(replacement) || invalid(retiring)) {
            return invalidReplacementSort("native counters marked invalid");
        }
        return [...sortOwnerLines("private", replacement), ...sortOwnerLines("retiring", retiring),
            SORT_SCOPE, SORT_LIMITS,
            `Replacement sort other owners unmeasured: ${row.other_sort_owners_unmeasured ? "yes" : "no"}`];
    } catch { return invalidReplacementSort("malformed snapshot"); }
}

interface ReplacementV2PlanningMemory {
    spans: VecBackingMemory;
    leaf_spans: VecBackingMemory;
    planned_ranges: VecBackingMemory;
    next_planned_ranges: VecBackingMemory;
    planned_spans: VecBackingMemory;
    planned_internal_spans: VecBackingMemory;
    counters_valid: boolean;
}

function replacementV2PlanningMemory(value: unknown): ReplacementV2PlanningMemory | null {
    const row = metadataRecord(value, PLANNING_MEMORY_FIELDS);
    if (!row || typeof row.counters_valid !== "boolean") return null;
    const spans = vecBackingMemory(row.spans), leafSpans = vecBackingMemory(row.leaf_spans);
    const plannedRanges = vecBackingMemory(row.planned_ranges);
    const nextPlannedRanges = vecBackingMemory(row.next_planned_ranges);
    const plannedSpans = vecBackingMemory(row.planned_spans);
    const plannedInternalSpans = vecBackingMemory(row.planned_internal_spans);
    if (!spans || !leafSpans || !plannedRanges || !nextPlannedRanges
        || !plannedSpans || !plannedInternalSpans) return null;
    return { spans, leaf_spans: leafSpans, planned_ranges: plannedRanges,
        next_planned_ranges: nextPlannedRanges, planned_spans: plannedSpans,
        planned_internal_spans: plannedInternalSpans, counters_valid: row.counters_valid };
}

function planningVectorLine(owner: string, name: string, value: VecBackingMemory): string {
    if (value.owners === 0) return `Replacement V2 planning ${owner} ${name}: none`;
    return `Replacement V2 planning ${owner} ${name}: ${value.length_slots}/${value.capacity_slots}`
        + ` × ${bytes(value.slot_size_bytes)} · backing ${bytes(value.backing_capacity_bytes)}`;
}

function planningOwnerLines(owner: string, value: ReplacementV2PlanningMemory | null): string[] {
    if (!value) return [`Replacement V2 planning ${owner}: unmeasured`];
    return [planningVectorLine(owner, "spans", value.spans),
        planningVectorLine(owner, "leaf spans", value.leaf_spans),
        planningVectorLine(owner, "planned ranges", value.planned_ranges),
        planningVectorLine(owner, "next planned ranges", value.next_planned_ranges),
        planningVectorLine(owner, "planned spans", value.planned_spans),
        planningVectorLine(owner, "planned internal spans", value.planned_internal_spans)];
}

function invalidReplacementV2Planning(reason: string): string[] {
    return [`Replacement V2 planning: invalid (${reason})`, PLANNING_SCOPE, PLANNING_LIMITS];
}

/** Exact retained POD descriptor Vec backing, never total V2 builder memory. */
export function formatReplacementV2PlanningMemoryDebug(
    tree?: WasmTreeReplacementV2PlanningMemorySource,
): string[] {
    let raw: unknown;
    try {
        const snapshot = tree?.replacement_v2_planning_memory_snapshot;
        if (snapshot === undefined) {
            return ["Replacement V2 planning: unsupported (snapshot export unavailable)"];
        }
        if (typeof snapshot !== "function") {
            return invalidReplacementV2Planning("malformed snapshot export");
        }
        raw = Reflect.apply(snapshot, tree, []);
    } catch { return invalidReplacementV2Planning("snapshot export failed"); }
    try {
        const row = metadataRecord(raw, PLANNING_FIELDS);
        if (!row || row.schema !== 1 || row.scope !== "replacement-v2-planning"
            || typeof row.other_planning_owners_unmeasured !== "boolean") {
            return invalidReplacementV2Planning("malformed snapshot");
        }
        const replacement = row.replacement === null ? null
            : replacementV2PlanningMemory(row.replacement);
        const retiring = row.retiring === null ? null
            : replacementV2PlanningMemory(row.retiring);
        if ((row.replacement !== null && !replacement) || (row.retiring !== null && !retiring)
            || ((row.replacement === null || row.retiring === null)
                && !row.other_planning_owners_unmeasured)) {
            return invalidReplacementV2Planning("malformed snapshot");
        }
        const invalid = (owner: ReplacementV2PlanningMemory | null): boolean => !!owner
            && (!owner.counters_valid || !owner.spans.counters_valid
                || !owner.leaf_spans.counters_valid || !owner.planned_ranges.counters_valid
                || !owner.next_planned_ranges.counters_valid || !owner.planned_spans.counters_valid
                || !owner.planned_internal_spans.counters_valid);
        if (invalid(replacement) || invalid(retiring)) {
            return invalidReplacementV2Planning("native counters marked invalid");
        }
        return [...planningOwnerLines("private", replacement),
            ...planningOwnerLines("retiring", retiring), PLANNING_SCOPE, PLANNING_LIMITS,
            `Replacement V2 planning other owners unmeasured: ${row.other_planning_owners_unmeasured ? "yes" : "no"}`];
    } catch { return invalidReplacementV2Planning("malformed snapshot"); }
}

interface ReplacementV2RangesMemory {
    ranges: VecBackingMemory;
    next_ranges: VecBackingMemory;
    closure_pending: VecBackingMemory;
    closure_expanding: VecBackingMemory;
    vector_paths: StringMemory;
    descriptor_ranges: number;
    descriptor_paths: StringMemory;
    counters_valid: boolean;
}

function replacementV2RangesMemory(value: unknown): ReplacementV2RangesMemory | null {
    const row = metadataRecord(value, RANGES_MEMORY_FIELDS);
    if (!row || typeof row.counters_valid !== "boolean" || !safeCount(row.descriptor_ranges)) return null;
    const ranges = vecBackingMemory(row.ranges), nextRanges = vecBackingMemory(row.next_ranges);
    const pending = vecBackingMemory(row.closure_pending), expanding = vecBackingMemory(row.closure_expanding);
    const vectorPaths = stringMemory(row.vector_paths), descriptorPaths = stringMemory(row.descriptor_paths);
    if (!ranges || !nextRanges || !pending || !expanding || !vectorPaths || !descriptorPaths) return null;
    let rangeCount = 0, capacityBytes = 0, slotSize: number | undefined;
    for (const vector of [ranges, nextRanges, pending, expanding]) {
        if (vector.owners !== 0) {
            // This ABI counts allocated Vec backing, not an empty field header.
            // The ABI supplies the layout; the host only checks consistency.
            if (vector.capacity_slots === 0 || (slotSize !== undefined && slotSize !== vector.slot_size_bytes)) return null;
            slotSize = vector.slot_size_bytes;
        }
        rangeCount += vector.length_slots;
        capacityBytes += vector.backing_capacity_bytes;
        if (!safeCount(rangeCount) || !safeCount(capacityBytes)) return null;
    }
    const halfSafe = Math.floor(Number.MAX_SAFE_INTEGER / 2);
    if (rangeCount > halfSafe || row.descriptor_ranges > halfSafe
        || vectorPaths.strings !== rangeCount * 2 || descriptorPaths.strings !== row.descriptor_ranges * 2) return null;
    for (const paths of [vectorPaths, descriptorPaths]) {
        capacityBytes += paths.capacity_bytes;
        if (!safeCount(capacityBytes)) return null;
    }
    return { ranges, next_ranges: nextRanges, closure_pending: pending, closure_expanding: expanding,
        vector_paths: vectorPaths, descriptor_ranges: row.descriptor_ranges,
        descriptor_paths: descriptorPaths, counters_valid: row.counters_valid };
}

function rangesVectorLine(owner: string, name: string, value: VecBackingMemory): string {
    if (value.owners === 0) return `Replacement V2 ranges ${owner} ${name}: none`;
    return `Replacement V2 ranges ${owner} ${name}: ${value.length_slots}/${value.capacity_slots}`
        + ` × ${bytes(value.slot_size_bytes)} · backing ${bytes(value.backing_capacity_bytes)}`;
}

function rangesOwnerLines(owner: string, value: ReplacementV2RangesMemory | null): string[] {
    if (!value) return [`Replacement V2 ranges ${owner}: unmeasured`];
    return [rangesVectorLine(owner, "ranges", value.ranges),
        rangesVectorLine(owner, "next ranges", value.next_ranges),
        rangesVectorLine(owner, "closure pending", value.closure_pending),
        rangesVectorLine(owner, "closure expanding", value.closure_expanding),
        `Replacement V2 ranges ${owner} vector paths: ${value.vector_paths.strings} strings`
            + ` · length ${bytes(value.vector_paths.length_bytes)} · capacity ${bytes(value.vector_paths.capacity_bytes)}`,
        `Replacement V2 ranges ${owner} descriptors: ${value.descriptor_ranges} logical ranges`
            + ` · ${value.descriptor_paths.strings} strings · length ${bytes(value.descriptor_paths.length_bytes)}`
            + ` · capacity ${bytes(value.descriptor_paths.capacity_bytes)}`];
}

function invalidReplacementV2Ranges(reason: string): string[] {
    return [`Replacement V2 ranges: invalid (${reason})`, RANGES_SCOPE, RANGES_LIMITS];
}

/**
 * Disjoint retained vectors and endpoint Strings only, not an allocation cap.
 * Root-owned endpoints belong to metadata, even when separate closure clones
 * have equal text. Map RangeRef count is logical; no bucket bytes are inferred.
 */
export function formatReplacementV2RangesMemoryDebug(
    tree?: WasmTreeReplacementV2RangesMemorySource,
): string[] {
    let raw: unknown;
    try {
        const snapshot = tree?.replacement_v2_ranges_memory_snapshot;
        if (snapshot === undefined) {
            return ["Replacement V2 ranges: unsupported (snapshot export unavailable)"];
        }
        if (typeof snapshot !== "function") return invalidReplacementV2Ranges("malformed snapshot export");
        raw = Reflect.apply(snapshot, tree, []);
    } catch { return invalidReplacementV2Ranges("snapshot export failed"); }
    try {
        const row = metadataRecord(raw, RANGES_FIELDS);
        if (!row || row.schema !== 1 || row.scope !== "replacement-v2-ranges"
            || typeof row.other_range_owners_unmeasured !== "boolean") {
            return invalidReplacementV2Ranges("malformed snapshot");
        }
        const replacement = row.replacement === null ? null : replacementV2RangesMemory(row.replacement);
        const retiring = row.retiring === null ? null : replacementV2RangesMemory(row.retiring);
        if ((row.replacement !== null && !replacement) || (row.retiring !== null && !retiring)
            || ((row.replacement === null || row.retiring === null) && !row.other_range_owners_unmeasured)) {
            return invalidReplacementV2Ranges("malformed snapshot");
        }
        for (const owner of [replacement, retiring]) {
            if (owner && (!owner.counters_valid || !owner.vector_paths.counters_valid
                || !owner.descriptor_paths.counters_valid
                || RANGES_VECTORS.some(name => !owner[name].counters_valid))) {
                return invalidReplacementV2Ranges("native counters marked invalid");
            }
        }
        return [...rangesOwnerLines("private", replacement), ...rangesOwnerLines("retiring", retiring),
            RANGES_SCOPE, RANGES_LIMITS,
            `Replacement V2 ranges other owners unmeasured: ${row.other_range_owners_unmeasured ? "yes" : "no"}`];
    } catch { return invalidReplacementV2Ranges("malformed snapshot"); }
}
