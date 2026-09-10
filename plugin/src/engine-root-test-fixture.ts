import { strict as assert } from "node:assert";
import { blake3 } from "@noble/hashes/blake3";
import { TFile } from "obsidian";
import { ObsetyncSyncEngine } from "./sync";
import { ObsetyncSyncBase, SYNC_BASE_STORE_PATH } from "./sync-base";
import { ObsetyncJournal, JOURNAL_STORE_PATH } from "./journal";
import { RootIntentStore, ROOT_INTENT_PATH } from "./root-intent";
import type { RootCommitIntent, RootOutcomeCapabilities, RootOutcomeIdentity, RootTerminalOutcome } from "./root-outcome";
import type { RootSyncIdentity } from "./root-sync-runtime";
import type { ResourceVisibilityGate } from "./resource-governor";
import { MemorySegmentedIO, type StoreTestBoundary } from "./segmented-store-test-io";

const h = (value: number) => value.toString(16).padStart(64, "0");
const SCOPE = h(1), INCARNATION = h(2);
const dataFor = (version: number) => new Uint8Array([version]);
const hash = (data: Uint8Array) => Buffer.from(blake3(data)).toString("hex");
const hashFor = (version: number) => hash(dataFor(version));
const pathAt = (index: number) => `note-${String(index).padStart(4, "0")}.md`;
type Entry = { hash: string; mtime: number; size: number };
type RootExportBoundary = { scope: "candidate" | "committed";
    phase: "begin" | "plan" | "admit" | "build" | "info" | "read" | "finish" | "cancel";
    completed?: number; offset?: number };
function treeBytes(entries: Map<string, Entry>): Uint8Array {
    return new TextEncoder().encode(JSON.stringify({ version: 1,
        entries: [...entries].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0) }));
}
function decodeTree(bytes: Uint8Array): Map<string, Entry> {
    const value = JSON.parse(new TextDecoder().decode(bytes));
    assert.equal(value.version, 1); assert(Array.isArray(value.entries));
    return new Map(value.entries);
}
const treeRoot = (entries: Map<string, Entry>) => hash(treeBytes(entries));
function gate() {
    let enter!: () => void, resolve!: () => void, reject!: (error: unknown) => void;
    const entered = new Promise<void>(yes => { enter = yes; });
    const held = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
    return { entered, resolve, reject, wait: async () => { enter(); await held; } };
}
const rejection = (promise: Promise<unknown>) => promise.then(
    () => { throw new Error("expected engine operation rejection"); }, error => error,
);
const turns = async () => { for (let count = 0; count < 15; count++) await Promise.resolve(); };
function observe<T>(promise: Promise<T>) {
    const state: { settled: boolean; value?: T; error?: unknown } = { settled: false };
    const done = promise.then(value => { state.settled = true; state.value = value; }, error => { state.settled = true; state.error = error; });
    return { state, done };
}
const CAPS: RootOutcomeCapabilities = { serverIncarnation: INCARNATION, maxSequence: Number.MAX_SAFE_INTEGER,
    commitBytes: 704 * 1024, rootBytes: 512 * 1024, queryBytes: 4096, cancelBytes: 4096,
    receiptBytes: 64 * 1024, streamsPerVault: 128 };
function rows(event: StoreTestBoundary, directory: string): any[] {
    return event.method === "write" && event.phase === "before" && event.path.startsWith(`${directory}/wal-`)
        ? JSON.parse(JSON.parse(event.data!).payload).rows : [];
}
function currentEntries(base: ObsetyncSyncBase): Map<string, Entry> {
    return new Map(base.allPaths().map(path => [path, { ...base.getEntry(path)! }]));
}

/** Actual constructor, capture/replay, root runtime, push, recovery and native
 * drains. Only adapter/source bytes, server requests and the transactional JSON
 * tree/portable BLAKE3 are test ports. No WASM/Merkle/server/device conformance
 * is inferred from this fixture. */
async function fixture(count = 1) {
    (globalThis as any).window ??= globalThis;
    const storage = new MemorySegmentedIO(), events: string[] = [], source = new Map<string, number>();
    const refs = new Set<{ event: string; callback: (...args: any[]) => Promise<void> }>();
    let cached: Uint8Array | null = null;
    const control = {
        boundary: undefined as undefined | ((event: StoreTestBoundary) => void | Promise<void>),
        cache: undefined as undefined | ((bytes: ArrayBuffer) => void | Promise<void>),
        rootExport: undefined as undefined | ((boundary: RootExportBoundary) => void),
        checkpoint: undefined as undefined | ((phase: string) => void | Promise<void>),
        statBulk: undefined as undefined | (() => void),
        beforeRootAnswer: undefined as undefined | (() => Promise<void>),
        beforeQueryAnswer: undefined as undefined | (() => Promise<void>),
        assertApiScope: undefined as undefined | (() => void),
        statErrors: new Map<string, Error>(),
        beforeRead: undefined as undefined | ((path: string) => void | Promise<void>),
        beforeContentCheck: undefined as undefined | ((hashes: string[]) => void | Promise<void>),
        beforePut: undefined as undefined | ((signal?: AbortSignal) => void | Promise<void>),
        contentMissing: false,
        listConfig: undefined as undefined | (() => Promise<Map<string, { mtime: number; size: number }>>),
        activePath: null as string | null,
        commitFailure: false,
        caps: { ...CAPS } as RootOutcomeCapabilities | null,
    };
    storage.onBoundary = async event => {
        if (rows(event, SYNC_BASE_STORE_PATH).some(row => row.op === "root-publication")) events.push("base-publication");
        if (rows(event, JOURNAL_STORE_PATH).some(row => row.op === "ack")) events.push("journal-ack");
        if (rows(event, ROOT_INTENT_PATH).some(row => row.op === "terminal")) events.push("terminal-write");
        await control.boundary?.(event);
    };
    const adapter = Object.assign(storage, {
        async writeBinary(_path: string, bytes: ArrayBuffer) {
            events.push("cache-write"); await control.cache?.(bytes); cached = new Uint8Array(bytes).slice();
        },
    });
    const app = { vault: { adapter,
        on(event: string, callback: (...args: any[]) => Promise<void>) {
            events.push(`capture:${event}`); const ref = { event, callback }; refs.add(ref); return ref;
        },
        offref(ref: any) { refs.delete(ref); },
        getFiles() { return [...source].map(([path, version]) => ({ path, stat: { size: 1, mtime: version } })); },
    }, workspace: { offref() {}, getActiveFile() {
        return control.activePath === null ? null : { path: control.activePath };
    } } };
    const base = new ObsetyncSyncBase(app as any), journal = new ObsetyncJournal(app as any);
    await base.load(); await journal.load();
    for (let index = 0; index < count; index++) {
        const path = pathAt(index); source.set(path, 1); base.setEntry(path, hashFor(1), 1, 1);
        // Seed through the same bounded publication cadence as production;
        // the fixture must not manufacture an unbounded pre-checkpoint owner.
        if ((index + 1) % 256 === 0) await base.save();
    }
    base.setTreeBaseRoot(treeRoot(currentEntries(base))); base.setLastSyncTimestamp(1); await base.save();
    cached = treeBytes(currentEntries(base));
    const server = { entries: currentEntries(base), root: base.treeBaseRoot!, floor: 0,
        receipt: null as RootTerminalOutcome | null, requests: [] as RootCommitIntent[],
        publicationSizes: [] as number[], cuts: [] as Array<Array<{ path: string; throughId: number }>>,
        queries: 0, legacy: 0, diffs: 0, checks: 0, preflights: 0,
        dataTransportInvalidations: 0,
        uploadPriorities: [] as Array<string | undefined>, uploadedKinds: [] as number[] };
    const engines: any[] = [];
    const hashers: Array<{ freed: number }> = [];
    class Hasher {
        private readonly inner = blake3.create({});
        readonly owner = { freed: 0 };
        constructor() { hashers.push(this.owner); }
        update(bytes: Uint8Array) { assert.equal(this.owner.freed, 0); assert(bytes.length <= 65536); this.inner.update(bytes); }
        finalize() { return Buffer.from(this.inner.digest()).toString("hex"); }
        free() { assert.equal(++this.owner.freed, 1); this.inner.destroy(); }
    }
    const create = async (name: string, options: { active?: boolean; staleCache?: boolean; ignorePatterns?: string[]; syncConfig?: boolean;
        autoSync?: boolean; captureDebounce?: (callback: () => void) => void;
        visibilityGate?: ResourceVisibilityGate } = {}) => {
        let committed = options.staleCache ? decodeTree(cached!) : currentEntries(base);
        let candidate: Map<string, Entry> | null = null;
        type RootJob = { kind: "root"; token: number; entries: Map<string, Entry>;
            scope: "candidate" | "committed";
            phase: "plan" | "build" | "ready"; completed: number; processed: number;
            arenaLimit: number; offset: number; bytes?: Uint8Array };
        type BeginJob = { kind: "begin"; token: number; done: boolean };
        type ChunkJob = { kind: "chunks"; token: number; done: boolean; resumed: boolean };
        type MutationJob = { kind: "mutation"; token: number; operation: "update" | "delete";
            payload: string; done: boolean };
        type RetireJob = { kind: "retire"; token: number };
        let job: RootJob | BeginJob | ChunkJob | MutationJob | RetireJob | null = null;
        let nextToken = 1, committedRevision = 0, candidateRevision = 0, rootHashReads = 0;
        const treeState = { commits: 0, aborts: 0, repairs: 0,
            rootExports: 0, rootBuilds: 0, rootReads: 0, rootFinishes: 0, rootCancels: 0,
            get committedRevision() { return committedRevision; },
            get rootHashReads() { return rootHashReads; },
            get activeJob() { return job?.kind ?? null; } };
        const idle = () => assert.equal(job, null, "synthetic tree mutated while a native job owned it");
        const rootJob = (token: number): RootJob => {
            assert(job?.kind === "root"); assert.equal(job.token, token); return job;
        };
        const boundary = (value: RootExportBoundary) => {
            events.push(`${name}:${value.scope}-root-export-${value.phase}`); control.rootExport?.(value);
        };
        const rootMaximum = (entries: Map<string, Entry>) => {
            let maximum = 1024;
            for (const path of entries.keys()) maximum += 256 + path.length * 6;
            return maximum;
        };
        const beginRoot = (entries: Map<string, Entry>, arenaLimit: number,
            scope: "candidate" | "committed") => {
            idle(); boundary({ scope, phase: "begin" });
            assert(Number.isInteger(arenaLimit) && arenaLimit >= 1 && arenaLimit <= 0xffff_ffff);
            job = { kind: "root", token: nextToken++, entries, scope, phase: "plan",
                completed: 0, processed: 0, arenaLimit, offset: 0 };
            treeState.rootExports++; return job.token;
        };
        const fromRows = (json: string) => new Map<string, Entry>((JSON.parse(json) as any[])
            .map(row => [row.path, { hash: row.hash, mtime: row.mtime_ms, size: row.size }]));
        const tree = {
            tree_version: () => 1,
            committed_revision: () => committedRevision,
            candidate_revision: () => candidateRevision,
            bump_committed_revision_for_test: () => { committedRevision++; },
            set_committed_revision_for_test: (value: number) => { committedRevision = value; },
            root_hash_hex: () => { rootHashReads++; return treeRoot(committed); },
            root_bytes: () => treeBytes(committed), total_files: () => committed.size,
            begin_candidate() { idle(); assert.equal(candidate, null); candidate = new Map(committed); }, has_candidate: () => candidate !== null,
            // Root export has a dedicated byte-budgeted step but still shares
            // the single native owner/cancel fence with candidate preparation.
            begin_candidate_job() {
                idle(); assert.equal(candidate, null);
                job = { kind: "begin", token: nextToken++, done: false }; return job.token;
            },
            finish_candidate_job(token: number) {
                assert(job?.kind === "begin" && job.done); assert.equal(job.token, token);
                candidate = new Map(committed); job = null; candidateRevision++; return 0;
            },
            begin_candidate_chunks_job() {
                idle(); assert(candidate);
                job = { kind: "chunks", token: nextToken++, done: false, resumed: false }; return job.token;
            },
            finish_candidate_chunks_job(token: number) {
                assert(job?.kind === "chunks" && job.done); assert.equal(job.token, token);
                job = null; return { all: [], fresh: [] };
            },
            begin_candidate_update_job(payload: string) {
                idle(); assert(candidate);
                job = { kind: "mutation", token: nextToken++, operation: "update", payload, done: false };
                return job.token;
            },
            begin_candidate_delete_job(payload: string) {
                idle(); assert(candidate);
                job = { kind: "mutation", token: nextToken++, operation: "delete", payload, done: false };
                return job.token;
            },
            finish_candidate_mutation_job(token: number) {
                assert(job?.kind === "mutation" && job.done); assert.equal(job.token, token); assert(candidate);
                if (job.operation === "update") {
                    for (const [path, entry] of fromRows(job.payload)) candidate.set(path, entry);
                } else {
                    for (const path of JSON.parse(job.payload)) candidate.delete(path);
                }
                job = null; candidateRevision++;
            },
            begin_candidate_root_export_job(arenaLimit: number) {
                assert(candidate); return beginRoot(candidate, arenaLimit, "candidate");
            },
            begin_committed_root_export_job(arenaLimit: number) {
                assert.equal(candidate, null); return beginRoot(committed, arenaLimit, "committed");
            },
            step_tree_job(token: number, maxUnits: number) {
                assert(job); assert.equal(job.token, token);
                assert(Number.isInteger(maxUnits) && maxUnits >= 1 && maxUnits <= 256);
                assert(job.kind === "begin" || job.kind === "mutation");
                assert.equal(job.done, false); job.done = true;
                return { done: true, units: 1, completed: 1, remaining: 0, reachable: 0 };
            },
            step_reachability_job_deferred(token: number, maxUnits: number) {
                assert(job?.kind === "begin" || job?.kind === "chunks");
                assert.equal(job.token, token); assert.equal(maxUnits, 1); assert.equal(job.done, false);
                job.done = true;
                return { done: true, units: 0, completed: 0, remaining: 0, reachable: 0 };
            },
            finish_candidate_job_deferred(token: number) {
                const finish = tree.finish_candidate_job;
                const reachable = finish.call(tree, token);
                job = { kind: "retire", token };
                return reachable;
            },
            finish_candidate_chunks_job_deferred(token: number) {
                const finish = tree.finish_candidate_chunks_job;
                const result = finish.call(tree, token);
                job = { kind: "retire", token };
                return result;
            },
            cancel_reachability_job_deferred(token: number) {
                assert(job); assert.equal(job.token, token);
                job = { kind: "retire", token };
            },
            step_reachability_retirement(token: number, completed: number, maxUnits: number) {
                assert(job?.kind === "retire"); assert.equal(job.token, token);
                assert.equal(completed, 0); assert.equal(maxUnits, 256);
                return { done: true, units: 0, completed: 0 };
            },
            finish_reachability_retirement(token: number, completed: number) {
                assert(job?.kind === "retire"); assert.equal(job.token, token); assert.equal(completed, 0);
                job = null;
            },
            candidate_chunks_sort_memory_plan_v1_job(token: number) {
                assert(job?.kind === "chunks" && job.done); assert.equal(job.token, token);
                return { schema: 1, scope: "candidate-chunk-plan-sort-workspace", hashCount: 0,
                    hashSizeBytes: 32, sourceHashesRequestedBytes: 0, scratchHashesRequestedBytes: 0,
                    peakAdmissionBytes: 0, reachableSetUnmeasured: true, pageOutputUnmeasured: true,
                    sortStrategy: "stable-lsd-radix-v1", pageMaxHashes: 256 };
            },
            resume_candidate_chunks_sort_memory_v1_job(token: number, source: number, scratch: number) {
                assert(job?.kind === "chunks" && job.done); assert.equal(job.token, token);
                assert.equal(source, 0); assert.equal(scratch, 0); job.resumed = true;
            },
            step_candidate_chunks_sort_v1_job() {
                throw new Error("empty synthetic tree unexpectedly sorted candidate chunks");
            },
            candidate_chunks_plan_info_v1_job(token: number) {
                assert(job?.kind === "chunks" && job.done && job.resumed); assert.equal(job.token, token);
                return { schema: 1, scope: "candidate-chunk-plan-pages", allCount: 0, pageMaxHashes: 256 };
            },
            read_candidate_chunks_page_v1_job() {
                throw new Error("empty synthetic tree unexpectedly read a candidate chunk page");
            },
            finish_candidate_chunks_plan_v1_job(token: number) {
                assert(job?.kind === "chunks" && job.done && job.resumed); assert.equal(job.token, token);
                job = { kind: "retire", token };
            },
            step_root_export_job(token: number, maxUnits: number, maxBytes: number) {
                const current = rootJob(token);
                assert(Number.isInteger(maxUnits) && maxUnits >= 1 && maxUnits <= 4096);
                assert(Number.isInteger(maxBytes) && maxBytes >= 1 && maxBytes <= 16 * 1024 * 1024);
                try {
                    assert(current.phase !== "ready");
                    boundary({ scope: current.scope, phase: current.phase, completed: current.completed });
                    current.completed++;
                    current.processed += 256;
                    if (current.phase === "plan") {
                        if (rootMaximum(current.entries) > current.arenaLimit) {
                            throw new Error("synthetic root export arena ceiling exceeded");
                        }
                        assert.equal(current.completed, 1);
                        return { done: true, units: 1, bytes: 256, completed: 1, processed: 256 };
                    }
                    // Synthetic JSON serialization is NOT a native performance
                    // model. Allocate only in the admitted build phase; retain
                    // output under its matching token until finish/cancel.
                    current.bytes ??= treeBytes(current.entries);
                    const done = current.completed === 2;
                    if (done) current.phase = "ready";
                    return { done, units: 1, bytes: 256,
                        completed: current.completed, processed: current.processed };
                } catch (error) {
                    // The actual WASM dispatcher drops failed Plan/Build jobs.
                    job = null; throw error;
                }
            },
            root_export_workset(token: number) {
                const current = rootJob(token); assert.equal(current.phase, "plan"); assert.equal(current.completed, 1);
                // Conservative JSON bound from metadata, without serializing
                // the complete root before shared-memory admission.
                return { max_length: rootMaximum(current.entries), offset_bytes: current.entries.size * 4 };
            },
            start_root_export_build_job(token: number) {
                const current = rootJob(token); assert.equal(current.phase, "plan"); assert.equal(current.completed, 1);
                boundary({ scope: current.scope, phase: "admit" }); current.phase = "build";
                current.completed = 0; current.processed = 0; treeState.rootBuilds++;
            },
            root_export_info(token: number) {
                const current = rootJob(token); assert.equal(current.phase, "ready"); assert(current.bytes);
                boundary({ scope: current.scope, phase: "info" });
                return { length: current.bytes.length, version: 1, hash: hash(current.bytes) };
            },
            read_root_export_job(token: number, offset: number, maxBytes: number) {
                const current = rootJob(token); assert.equal(current.phase, "ready"); assert(current.bytes);
                assert.equal(offset, current.offset); assert.equal(maxBytes, 65536);
                boundary({ scope: current.scope, phase: "read", offset });
                const bytes = current.bytes.slice(offset, offset + maxBytes);
                current.offset += bytes.length; treeState.rootReads++; return bytes;
            },
            finish_root_export_job(token: number) {
                const current = rootJob(token); assert.equal(current.phase, "ready"); assert(current.bytes);
                assert.equal(current.offset, current.bytes.length);
                boundary({ scope: current.scope, phase: "finish" });
                const scope = current.scope;
                job = null; treeState.rootFinishes++; events.push(`${name}:${scope}-root-export-finished`);
            },
            cancel_tree_job(token: number) {
                assert(job); assert.equal(job.token, token);
                if (job.kind === "root") {
                    boundary({ scope: job.scope, phase: "cancel" }); treeState.rootCancels++;
                }
                job = null;
            },
            candidate_root_hash_hex: () => candidate ? treeRoot(candidate) : null,
            candidate_root_bytes: () => candidate ? treeBytes(candidate) : null,
            candidate_total_files: () => candidate?.size ?? 0,
            candidate_update_batch(json: string) { idle(); assert(candidate); for (const [path, entry] of fromRows(json)) candidate.set(path, entry); },
            candidate_delete_batch(json: string) { idle(); assert(candidate); for (const path of JSON.parse(json)) candidate.delete(path); },
            commit_candidate() {
                idle(); assert(candidate); events.push(`${name}:candidate-commit`);
                if (control.commitFailure) { control.commitFailure = false; throw new Error("native candidate commit failed"); }
                committed = candidate; candidate = null; treeState.commits++; committedRevision++; candidateRevision++;
                return { before: 1, after: 1, reachable: 1, removed: 0 };
            },
            abort_candidate() {
                idle(); candidate = null; treeState.aborts++; candidateRevision++;
                return { before: 1, after: 1, reachable: 1, removed: 0 };
            },
            rebuild_from_entries_in_version(version: number, json: string) {
                idle(); assert.equal(version, 1); assert.equal(candidate, null); events.push(`${name}:repair`);
                committed = fromRows(json); treeState.repairs++; committedRevision++;
            },
            build_from_entries(json: string) { idle(); committed = fromRows(json); committedRevision++; },
        };
        const api = {
            async ensureTransportReady() { server.preflights++; },
            closeDataLane() { events.push(`${name}:lane-close`); },
            invalidateDataTransportAfterResume() {
                server.dataTransportInvalidations++;
                events.push(`${name}:data-transport-resume`);
            },
            async negotiateRootOutcomes() { events.push(`${name}:capabilities`); return control.caps && { ...control.caps }; },
            async queryRootOutcome(_vault: string, identity?: RootOutcomeIdentity) {
                if (!identity) return { protocol_version: 1, server_incarnation: INCARNATION, status: "stream",
                    last_sequence: server.floor, current_root_hash: server.root };
                events.push(`${name}:outcome-query`); server.queries++; await control.beforeQueryAnswer?.();
                assert(server.receipt); assert.equal(identity.sequence, server.receipt.sequence);
                assert.equal(identity.request_hash, server.receipt.request_hash);
                events.push(`${name}:outcome-answer`); return structuredClone(server.receipt);
            },
            async commitRootOutcome(_vault: string, request: RootCommitIntent) {
                assert.equal(request.sequence, server.floor + 1); server.requests.push(structuredClone(request));
                const disk = new RootIntentStore(storage.clone(), SCOPE, hash); await disk.load();
                const pending = disk.pending()!;
                assert.deepEqual(pending.intent.request, request, "root request preceded verified local intent");
                server.publicationSizes.push(pending.intent.publication.entries.length);
                server.cuts.push(structuredClone([...pending.intent.journalCuts])); await disk.closeAndDrain();
                server.entries = decodeTree(new Uint8Array(Buffer.from(request.root, "base64")));
                server.root = treeRoot(server.entries); server.floor = request.sequence;
                server.receipt = { protocol_version: 1, server_incarnation: INCARNATION, status: "accepted",
                    sequence: request.sequence, mutation_id: request.mutation_id, request_hash: request.request_hash,
                    result: { accepted: true, root_hash: server.root } };
                events.push(`${name}:root-accepted`); await control.beforeRootAnswer?.();
                events.push(`${name}:root-answer`); return structuredClone(server.receipt);
            },
            async cancelRootOutcome() { throw new Error("accepted receipt must not be cancelled"); },
            async putRoot() { server.legacy++; throw new Error("durable engine used legacy putRoot"); },
            async checkContent(hashes: string[]) {
                server.checks++; await control.beforeContentCheck?.([...hashes]);
                return control.contentMissing ? [...hashes] : [];
            },
            async putObjects(records: Array<{ kind: number }>, _perf?: unknown, _memory?: unknown,
                _signal?: AbortSignal, options?: { priority?: string }) {
                await control.beforePut?.(_signal);
                server.uploadPriorities.push(options?.priority);
                server.uploadedKinds.push(...records.map(record => record.kind));
            },
            async getDiff() { server.diffs++; events.push(`${name}:ordinary-diff`); return null; },
            async getRoot() { return treeBytes(server.entries); },
            observeTreeVersion() {},
            async ping() { events.push(`${name}:ping`); return { serverUrl: "http://synthetic", ok: true, transport: "synthetic" }; },
        };
        const io = {
            getAbsolutePath: () => null,
            async stat(path: string) {
                const error = control.statErrors.get(path); if (error) throw error;
                const version = source.get(path); return version === undefined ? null : { mtime: version, size: 1 };
            },
            async readFile(path: string) { events.push(`${name}:source-read`); await control.beforeRead?.(path); return dataFor(source.get(path)!); },
            async listObsidianConfig() { events.push(`${name}:config-list`); return control.listConfig ? control.listConfig() : new Map(); },
            statBulk() {
                events.push(`${name}:stat-bulk`); control.statBulk?.();
                return new Map([...source].map(([path, version]) => [path, { mtime: version, size: 1 }]));
            },
        };
        const wasm = {
            Hasher, wasm_should_chunk: () => false,
            wasm_hash_batch: (data: Uint8Array, offsets: Uint32Array, sizes: Uint32Array) =>
                [...offsets].map((offset, index) => hash(data.subarray(offset, offset + sizes[index]))),
            wasm_root_hash_from_bytes: (data: Uint8Array) => { decodeTree(data); return hash(data); },
            wasm_root_version_from_bytes: (data: Uint8Array) => { decodeTree(data); return 1; },
            wasm_tree_committed_chunk_hashes: () => [], wasm_tree_candidate_chunk_hashes: () => [],
            wasm_tree_new_candidate_chunk_hashes: () => [], wasm_tree_chunk_hashes: () => [],
        };
        const checkpoint = {
            async begin(phase: string) { events.push(`${name}:body:${phase}`); await control.checkpoint?.(phase); return "fixture-checkpoint"; },
            progress() {}, async complete() {},
        };
        const identity: RootSyncIdentity = { vaultId: "vault", deviceId: "device", scopeHash: Promise.resolve(SCOPE),
            assertApiScope() { control.assertApiScope?.(); } };
        const engine = new ObsetyncSyncEngine(app as any, api as any, io as any, base, journal, wasm as any, tree as any,
            "vault", 30000, "oldest", () => {}, base.treeBaseRoot, options.syncConfig ?? false, name, false, false, options.ignorePatterns ?? [],
            checkpoint as any, options.autoSync ?? false, null, options.visibilityGate, undefined, identity) as any;
        engines.push(engine);
        if (options.active !== false) {
            const globals = globalThis as any, previousDebounce = globals.__obsetyncTestDebounce;
            const capture = options.captureDebounce ? (callback: () => void, wait: number, immediate: boolean) => {
                assert.equal(wait, 3000); assert.equal(immediate, true);
                options.captureDebounce!(callback); return () => {};
            } : undefined;
            if (capture) globals.__obsetyncTestDebounce = capture;
            try { await engine.prepareLocal(); }
            finally {
                if (capture && globals.__obsetyncTestDebounce === capture) globals.__obsetyncTestDebounce = previousDebounce;
            }
            Object.assign(engine, { startupReplayInProgress: false, startupCompleted: true, state: "idle" });
        }
        return { engine, tree, treeState, treeHashAt: (path: string) => committed.get(path)?.hash };
    };
    return { storage, app, source, base, journal, events, control, server, refs, create,
        cached: () => cached!.slice(),
        async queue(engine: any, paths: string[], version: number) {
            for (const path of paths) source.set(path, version);
            const ids = await Promise.all(paths.map(path => journal.append({ action: "modified", path, ts: version, synced: false })));
            paths.forEach((path, index) => engine.pendingChanges.add({ action: "modified", path, hash: hashFor(version), mtime: version, size: 1 }, ids[index]));
            return ids;
        },
        async event(engine: any, path: string, version: number) {
            source.set(path, version);
            const ref = engine.eventRefs.find((item: any) => item.event === "modify"); assert(ref);
            await ref.callback(Object.assign(new TFile(), { path, stat: { size: 1, mtime: version } }));
            return journal.unsynced().find(row => row.path === path)!.id;
        },
        async cold() {
            const disk = storage.clone(), coldApp = { vault: { adapter: disk } } as any;
            const coldBase = new ObsetyncSyncBase(coldApp), coldJournal = new ObsetyncJournal(coldApp);
            const intents = new RootIntentStore(disk, SCOPE, hash);
            await coldBase.load(); await coldJournal.load(); await intents.load();
            const result = { base: coldBase, journal: coldJournal, pending: intents.pending(), sequence: intents.lastSequence };
            await intents.closeAndDrain(); return result;
        },
        async close() { for (const engine of engines) await engine.stopAndDrain(); for (const owner of hashers) assert.equal(owner.freed, 1); },
    };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;

export { fixture, hashFor, pathAt, gate, rejection, turns, observe, rows };
export type { Fixture };
