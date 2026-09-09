import type { DirtyFileChange } from "./dirty-set";
import type { FileChange } from "./push";
import type { RenameDependencySnapshot } from "./deferred-changes";
import { isSafeVaultPath } from "./delta-validation";

export type RootBatchDependency = RenameDependencySnapshot;
export const ROOT_BATCH_LIMITS = {
    paths: 256, cuts: 256, metadataBytes: 256 * 1024,
    headerBytes: 1024, entryFixedBytes: 320, cutFixedBytes: 96,
} as const;
export interface RootBatchLimits { maxPaths?: number; maxCuts?: number; maxMetadataBytes?: number }
export type RootBatchHoldReason = "missing-peer" | "unresolved-dependency" | "stale-capture" |
    "component-limit" | "transaction-limit";
export type RootBatchDeferrals = Record<RootBatchHoldReason, { components: number; paths: number }>;
export interface RootBatchSelection {
    /** Detached metadata, in the supplied materialized priority order. */
    ready: FileChange[];
    /** Exactly selected original detached hint objects, preserving DirtyPathSet
     * take/restore provenance. Caller must keep that snapshot immutable. No ACK. */
    queued: DirtyFileChange[];
    retained: DirtyFileChange[];
    unselected: FileChange[];
    /** Disjoint transitive groups with >1 path; total membership <=maxPaths.
     * If a source defers later, omit its WHOLE group before building candidate
     * updates/deletes. Independent groups can still publish. */
    selectedGroups: readonly (readonly string[])[];
    estimatedMetadataBytes: number;
    cutCount: number;
    /** Fixed-shape aggregate diagnostics, never another per-path backlog. */
    deferred: RootBatchDeferrals;
}
export class RootBatchError extends Error {
    readonly code = "ROOT_BATCH_INPUT";
    constructor(message: string) { super(message); this.name = "RootBatchError"; }
}
function invalid(message: string): never { throw new RootBatchError(message); }
function limit(value: number | undefined, maximum: number): number {
    if (value === undefined) return maximum;
    if (!Number.isSafeInteger(value) || value < 1 || value > maximum) invalid("invalid root batch limits");
    return value;
}
function generation(value: number | undefined): void {
    if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0 || value >= Number.MAX_SAFE_INTEGER)) {
        invalid("invalid root batch journal generation");
    }
}
function path(value: unknown): asserts value is string {
    if (!isSafeVaultPath(value)) invalid("invalid root batch path");
}
function validateMetadata(change: FileChange): void {
    path(change?.path);
    if (change.action !== "created" && change.action !== "modified" && change.action !== "deleted") invalid("invalid root batch action");
    if (change.mtime !== undefined && !Number.isFinite(change.mtime)) invalid("invalid root batch mtime");
    if (change.size !== undefined && (!Number.isSafeInteger(change.size) || change.size < 0)) invalid("invalid root batch size");
    if (change.hash !== undefined && (typeof change.hash !== "string" || !/^[0-9a-f]{64}$/.test(change.hash))) {
        invalid("invalid root batch hash hint");
    }
}
function metadata(change: FileChange, validated = false): FileChange {
    if (!validated) validateMetadata(change);
    return { action: change.action, path: change.path,
        ...(change.hash === undefined ? {} : { hash: change.hash }),
        ...(change.mtime === undefined ? {} : { mtime: change.mtime }),
        ...(change.size === undefined ? {} : { size: change.size }) };
}
/** Internal shared graph model. The cooperative planner and the original
 * synchronous selector must use the same validation/group/hold rules. */
export interface RootBatchGraphNode {
    path: string;
    parent: number;
    weight: number;
    queued?: DirtyFileChange;
    /** Scalar witness; never a replacement for the original hint's proof. */
    witness?: DirtyFileChange;
    ready?: FileChange;
    queuedOrder: number;
    readyOrder: number;
    component?: RootBatchGraphComponent;
}
export interface RootBatchGraphComponent {
    paths: number;
    cuts: number;
    bytes: number;
    missing: boolean;
    unresolved: boolean;
    stale: boolean;
    ready: RootBatchGraphNode[];
    queued: RootBatchGraphNode[];
}
export interface RootBatchGraph {
    nodes: RootBatchGraphNode[];
    ready: RootBatchGraphNode[];
    queued: RootBatchGraphNode[];
    /** First materialized appearance, then wholly omitted queued groups. */
    components: RootBatchGraphComponent[];
}
export interface ResolvedRootBatchLimits { maxPaths: number; maxCuts: number; maxMetadataBytes: number }
export function resolveRootBatchLimits(limits: RootBatchLimits = {}): ResolvedRootBatchLimits {
    return { maxPaths: limit(limits.maxPaths, ROOT_BATCH_LIMITS.paths),
        maxCuts: limit(limits.maxCuts, ROOT_BATCH_LIMITS.cuts),
        maxMetadataBytes: limit(limits.maxMetadataBytes, ROOT_BATCH_LIMITS.metadataBytes) };
}
export function emptyRootBatchDeferrals(): RootBatchDeferrals {
    return { "missing-peer": { components: 0, paths: 0 }, "unresolved-dependency": { components: 0, paths: 0 },
        "stale-capture": { components: 0, paths: 0 }, "component-limit": { components: 0, paths: 0 },
        "transaction-limit": { components: 0, paths: 0 } };
}
export function rootBatchComponentHold(component: RootBatchGraphComponent,
    limits: ResolvedRootBatchLimits): Exclude<RootBatchHoldReason, "transaction-limit"> | undefined {
    if (component.missing) return "missing-peer";
    if (component.unresolved) return "unresolved-dependency";
    if (component.stale) return "stale-capture";
    if (component.paths > limits.maxPaths || component.cuts > limits.maxCuts ||
        component.bytes + ROOT_BATCH_LIMITS.headerBytes > limits.maxMetadataBytes) return "component-limit";
    return undefined;
}

/** UTF8 bytes of JSON.stringify(path), without a whole-string encoder buffer.
 * Each cooperative step scans at most 4096 UTF16 code units. */
export function* rootBatchPathBytesSteps(value: string): Generator<void, number> {
    let bytes = 2, scanned = 0;
    for (let index = 0; index < value.length; index++) {
        const code = value.charCodeAt(index);
        if (code === 34 || code === 92 || code === 8 || code === 9 || code === 10 || code === 12 || code === 13) bytes += 2;
        else if (code < 32) bytes += 6;
        else if (code < 128) bytes++;
        else if (code < 2048) bytes += 2;
        else if (code >= 0xd800 && code <= 0xdbff && index + 1 < value.length &&
            value.charCodeAt(index + 1) >= 0xdc00 && value.charCodeAt(index + 1) <= 0xdfff) {
            bytes += 4; index++; scanned++;
        } else if (code >= 0xd800 && code <= 0xdfff) bytes += 6;
        else bytes += 3;
        if (++scanned >= 4096) { scanned = 0; yield; }
    }
    return bytes;
}

export type RootBatchGraphStep = "queued" | "ready" | "dependency" | "classify" | "link-state" | "order" | "string";
export interface RootBatchGraphHooks {
    /** Called BEFORE retaining/copying that row/node. Cooperative callers use
     * this to recheck admission after a host turn, not just in preflight. */
    beforeRow?(kind: "queued" | "ready", change: FileChange): void;
    beforeDependency?(edge: RootBatchDependency): void;
    beforeNode?(nextNodeCount: number): void;
    /** Planner-only interning; synchronous selector outputs stay unchanged. */
    internReady?: boolean;
    beforeReadyCapture?(change: FileChange, witness: DirtyFileChange, shared: boolean): void;
}

/** Build once. Synchronous callers drain steps immediately; async callers
 * await their actual host cooperation port between bounded step windows.
 * Inputs are an immutable reviewed epoch: hooks/copies do not confer review,
 * live-generation or journal-ACK authority. */
export function* buildRootBatchGraphSteps(
    ready: readonly FileChange[], queued: readonly DirtyFileChange[], dependencies: readonly RootBatchDependency[],
    hooks: RootBatchGraphHooks = {},
): Generator<RootBatchGraphStep, RootBatchGraph> {
    if (!Array.isArray(ready) || !Array.isArray(queued) || !Array.isArray(dependencies)) invalid("invalid root batch collections");
    const queuedLength = queued.length, readyLength = ready.length, dependencyLength = dependencies.length;
    const nodes: RootBatchGraphNode[] = [], byPath = new Map<string, number>();
    const queuedNodes: RootBatchGraphNode[] = [], readyNodes: RootBatchGraphNode[] = [];
    const ensure = (name: string): number => {
        const known = byPath.get(name); if (known !== undefined) return known;
        hooks.beforeNode?.(nodes.length + 1);
        const id = nodes.length;
        nodes.push({ path: name, parent: id, weight: 1, queuedOrder: -1, readyOrder: -1 }); byPath.set(name, id); return id;
    };
    const root = (id: number): number => {
        let parent = id;
        while (nodes[parent].parent !== parent) parent = nodes[parent].parent;
        while (id !== parent) { const next = nodes[id].parent; nodes[id].parent = parent; id = next; }
        return parent;
    };
    const union = (left: number, right: number) => {
        let a = root(left), b = root(right); if (a === b) return;
        if (nodes[a].weight < nodes[b].weight) [a, b] = [b, a];
        nodes[b].parent = a; nodes[a].weight += nodes[b].weight;
    };
    const firstByGeneration = new Map<number, number>();
    for (let index = 0; index < queuedLength; index++) {
        const hint = queued[index];
        hooks.beforeRow?.("queued", hint);
        const captured = metadata(hint); generation(hint.journalId);
        if ("data" in hint) invalid("root batch queued hints must not contain file data");
        const id = ensure(captured.path), current = nodes[id];
        if (current.queued) invalid("duplicate root batch queued path");
        current.queued = hint; current.witness = { ...captured, journalId: hint.journalId };
        current.queuedOrder = queuedNodes.length; queuedNodes.push(current);
        if (hint.journalId !== undefined) {
            const previous = firstByGeneration.get(hint.journalId);
            if (previous === undefined) firstByGeneration.set(hint.journalId, id);
            else union(previous, id);
        }
        yield "queued";
    }
    for (let index = 0; index < readyLength; index++) {
        const change = ready[index];
        hooks.beforeRow?.("ready", change);
        validateMetadata(change);
        const id = byPath.get(change.path);
        if (id === undefined || !nodes[id].queued) invalid("materialized root batch path lacks its queued owner");
        if (nodes[id].ready) invalid("duplicate root batch materialized path");
        const current = nodes[id], witness = current.witness!;
        if (hooks.internReady) {
            const shared = change.action === witness.action && change.hash === witness.hash &&
                Object.is(change.mtime, witness.mtime) && Object.is(change.size, witness.size);
            hooks.beforeReadyCapture?.(change, witness, shared);
            // A matching ready row is the SAME captured scalar object. An
            // override retains only genuinely different scalar metadata and
            // canonicalizes equal path/hash values to the existing witness.
            current.ready = shared ? witness : { action: change.action, path: current.path,
                ...(change.hash === undefined ? {} : { hash: change.hash === witness.hash ? witness.hash : change.hash }),
                ...(change.mtime === undefined ? {} : { mtime: change.mtime }),
                ...(change.size === undefined ? {} : { size: change.size }) };
        } else current.ready = metadata(change, true);
        current.readyOrder = readyNodes.length; readyNodes.push(current);
        yield "ready";
    }
    // Capture the fixed scalar link state so later host turns cannot silently
    // mutate already-visited edges into a different reviewed generation.
    const links: RootBatchDependency[] = [];
    for (let index = 0; index < dependencyLength; index++) {
        const edge = dependencies[index];
        hooks.beforeDependency?.(edge);
        path(edge?.left); path(edge.right);
        if (!Number.isSafeInteger(edge.generation) || edge.generation < 0 || edge.generation >= Number.MAX_SAFE_INTEGER ||
            typeof edge.pending !== "boolean" || typeof edge.uncertain !== "boolean") invalid("invalid root batch dependency");
        union(ensure(edge.left), ensure(edge.right));
        links.push({ left: edge.left, right: edge.right, generation: edge.generation, pending: edge.pending, uncertain: edge.uncertain });
        yield "dependency";
    }
    const byRoot = new Map<number, RootBatchGraphComponent>();
    for (let id = 0; id < nodes.length; id++) {
        const node = nodes[id], key = root(id);
        let current = byRoot.get(key);
        if (!current) {
            current = { paths: 0, cuts: 0, bytes: 0, missing: false, unresolved: false, stale: false, ready: [], queued: [] };
            byRoot.set(key, current);
        }
        node.component = current;
        if (!node.queued || !node.ready) current.missing = true;
        if (node.queued) {
            current.paths++;
            if (node.witness!.journalId !== undefined) current.cuts++;
            const scan = rootBatchPathBytesSteps(node.path);
            let step = scan.next();
            while (!step.done) { yield "string"; step = scan.next(); }
            current.bytes += ROOT_BATCH_LIMITS.entryFixedBytes + step.value;
            if (node.witness!.journalId !== undefined) current.bytes += ROOT_BATCH_LIMITS.cutFixedBytes + step.value;
        }
        yield "classify";
    }
    for (const edge of links) {
        const left = nodes[byPath.get(edge.left)!], right = nodes[byPath.get(edge.right)!];
        const current = left.component!;
        if (edge.pending || edge.uncertain || edge.generation === 0) current.unresolved = true;
        if ((left.witness?.journalId ?? 0) < edge.generation || (right.witness?.journalId ?? 0) < edge.generation) current.stale = true;
        yield "link-state";
    }
    const components: RootBatchGraphComponent[] = [], ordered = new Set<RootBatchGraphComponent>();
    for (const node of readyNodes) {
        const current = node.component!; current.ready.push(node);
        if (!ordered.has(current)) { ordered.add(current); components.push(current); }
        yield "order";
    }
    for (const node of queuedNodes) {
        const current = node.component!; current.queued.push(node);
        if (!ordered.has(current)) { ordered.add(current); components.push(current); }
        yield "order";
    }
    return { nodes, ready: readyNodes, queued: queuedNodes, components };
}

/** Select ONLY AFTER the whole materialized recovery snapshot passed the
 * caller's bulk/deletion review and cooldown partition. This is not review
 * authorization, source admission, journal acknowledgement or a durable plan.
 * Supply ALL detached queued hints, even peers removed from ready by ignore,
 * cooldown or other materialization rules: omissions hold their linked group.
 *
 * Union current explicit links (including non-queued intermediate paths) and
 * equal journal IDs; no component can be split by count, bytes or priority.
 * Fit components by first appearance, skipping those which do not fit so
 * unrelated work can progress. Ready order itself remains unchanged.
 *
 * Selected metadata has explicit count/encoded-byte bounds. Graph/retained
 * metadata is O(current backlog + current path-pair edges), not per-history;
 * this synchronous planner is not a full heap/RSS/UI-latency bound. Queued
 * inputs must already be detached metadata-only hints; their object identity
 * is retained for owner-local take/restore provenance. Ready file data is
 * neither inspected nor retained. Final exact publication/request byte and
 * applicability checks remain mandatory after hashes/source state are known. */
export function selectRootBatch(
    ready: readonly FileChange[], queued: readonly DirtyFileChange[],
    dependencies: readonly RootBatchDependency[] = [], limits: RootBatchLimits = {},
): RootBatchSelection {
    if (!Array.isArray(ready) || !Array.isArray(queued) || !Array.isArray(dependencies)) invalid("invalid root batch collections");
    const resolved = resolveRootBatchLimits(limits);
    const build = buildRootBatchGraphSteps(ready, queued, dependencies);
    let step = build.next();
    while (!step.done) step = build.next();
    const graph = step.value, selected = new Set<RootBatchGraphComponent>();
    const deferred = emptyRootBatchDeferrals();
    let selectedCount = 0, selectedCuts = 0, selectedBytes = 0;
    for (const current of graph.components) {
        if (current.paths === 0) continue;
        let reason: RootBatchHoldReason | undefined = rootBatchComponentHold(current, resolved);
        if (!reason && (selectedCount + current.paths > resolved.maxPaths ||
            selectedCuts + current.cuts > resolved.maxCuts ||
            selectedBytes + current.bytes + ROOT_BATCH_LIMITS.headerBytes > resolved.maxMetadataBytes)) {
            reason = "transaction-limit";
        }
        if (reason) { deferred[reason].components++; deferred[reason].paths += current.paths; continue; }
        selected.add(current); selectedCount += current.paths;
        selectedCuts += current.cuts; selectedBytes += current.bytes;
    }
    const selectedReady: FileChange[] = [], unselected: FileChange[] = [];
    const selectedQueued: DirtyFileChange[] = [], retained: DirtyFileChange[] = [];
    const groups = new Map<RootBatchGraphComponent, string[]>();
    for (const node of graph.ready) {
        const current = node.component!, included = selected.has(current);
        (included ? selectedReady : unselected).push(node.ready!);
        if (included && current.paths > 1) {
            const group = groups.get(current) ?? [];
            group.push(node.path); groups.set(current, group);
        }
    }
    for (const node of graph.queued) (selected.has(node.component!) ? selectedQueued : retained).push(node.queued!);
    return { ready: selectedReady, queued: selectedQueued, retained, unselected,
        selectedGroups: Object.freeze([...groups.values()].map(group => Object.freeze(group))),
        estimatedMetadataBytes: selectedCount ? selectedBytes + ROOT_BATCH_LIMITS.headerBytes : 0,
        cutCount: selectedCuts, deferred };
}
