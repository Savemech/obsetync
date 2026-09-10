import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { push, type FileChange, type PushPreparedContext } from "./push";
import { BulkObjectKind, type BulkUploadRecord } from "./bulk-codec";
import { PerfTrace } from "./perf-trace";
import { PreparedDesktopManifestError } from "./prepared-desktop-manifest";
import { HashWorkerFileDriftError } from "./desktop-hash-workers";
import { PreparedTransferPlan } from "./transfer-plan";
import { ObjectConfirmationStore } from "./object-confirmation-store";
import { MemorySegmentedIO } from "./segmented-store-test-io";
import type { DesktopFileFingerprint } from "./hash-worker-protocol";
import type { FileManifest } from "./api";
import type { TransientWorkScope } from "./transient-memory";
import { ObsetyncSyncEngine } from "./sync";
import { ObsetyncJournal } from "./journal";
import { DirtyPathSet } from "./dirty-set";
import { DeferredChangeTracker } from "./deferred-changes";
import { LocalEventGuards } from "./local-event-guards";

(globalThis as any).window ??= globalThis;

let assertions = 0;
function check(value: unknown, message: string): asserts value {
    assertions++;
    if (!value) throw new Error(message);
}
const MIB = 1024 * 1024;
const PATH = "large.bin";
const SCOPE = "a".repeat(64);
const BASE = "b".repeat(64);
const PREVIOUS = "c".repeat(64);
const INDEX = "d".repeat(64);
const digest = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
function deferred<T>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>(yes => { resolve = yes; });
    return { promise, resolve };
}
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value));

// Synthetic hash/worker implementations intentionally use one consistent real
// digest without requiring generated WASM in the fast unit-test CI job. These
// are pipeline/lifetime tests, not Blake3/FastCDC protocol parity tests.
function manifestOf(bytes: Uint8Array): FileManifest {
    return {
        file_hash: digest(bytes), total_size: bytes.byteLength,
        chunks: [0, MIB].map(offset => ({
            hash: digest(bytes.subarray(offset, offset + MIB)), offset, size: MIB,
        })),
    };
}
async function fingerprintOf(path: string): Promise<DesktopFileFingerprint> {
    const info = await stat(path);
    return { size: info.size, mtime: info.mtimeMs, ctime: info.ctimeMs, device: info.dev, inode: info.ino };
}

async function fixture(directory: string) {
    const absolutePath = join(directory, PATH);
    const original = new Uint8Array(2 * MIB);
    original.fill(17, 0, MIB);
    original.fill(23, MIB);
    await writeFile(absolutePath, original);
    const originalManifest = manifestOf(original);
    const originalFingerprint = await fingerprintOf(absolutePath);
    const trace = new PerfTrace({ monitorEventLoop: false });
    const entries = new Map([[PATH, { hash: PREVIOUS, mtime: 1, size: original.length }]]);
    let committed = clone([...entries]);
    let candidate: typeof committed | null = null;
    const events: string[] = [];
    const state = {
        generation: 41, rendererReads: 0, workerBytes: 0, rangeHashBytes: 0,
        rangeHashers: 0, freedRangeHashers: 0, rootCalls: 0, saves: 0, commits: 0, aborts: 0,
        index: false, baseRoot: BASE, workerModes: [] as string[], chunkChecks: [] as string[][],
        uploads: [] as Array<{ kind: BulkObjectKind; hash: string }>, roots: [] as typeof committed[],
        present: new Set<string>(), manifests: new Map<string, FileManifest>(),
        storeIO: new MemorySegmentedIO(), plan: null as unknown as PreparedTransferPlan,
        beforeStore: undefined as undefined | ((stage: string) => void | Promise<void>),
        afterWorker: undefined as undefined | ((mode: string) => void | Promise<void>),
        afterPut: undefined as undefined | ((records: readonly BulkUploadRecord[]) => void | Promise<void>),
        afterIndexCheck: undefined as undefined | (() => void | Promise<void>),
        rootError: undefined as Error | undefined,
        onProgress: undefined as undefined | ((message: string) => void),
    };
    state.plan = new PreparedTransferPlan(state.storeIO);
    await state.plan.load();
    const tree = {
        tree_version: () => 1,
        root_hash_hex: () => state.commits === 0 ? BASE : digest(new TextEncoder().encode(JSON.stringify(committed))),
        root_bytes: () => new TextEncoder().encode(JSON.stringify(committed)),
        total_files: () => committed.length,
        begin_candidate: () => { check(candidate === null, "nested candidate"); candidate = clone(committed); },
        has_candidate: () => candidate !== null,
        candidate_root_hash_hex: () => candidate ? digest(new TextEncoder().encode(JSON.stringify(candidate))) : undefined,
        candidate_root_bytes: () => candidate ? new TextEncoder().encode(JSON.stringify(candidate)) : undefined,
        candidate_total_files: () => candidate?.length ?? 0,
        candidate_update_batch: (json: string) => {
            check(candidate !== null, "update without candidate");
            for (const row of JSON.parse(json)) {
                candidate = candidate.filter(([path]) => path !== row.path);
                candidate.push([row.path, { hash: row.hash, mtime: row.mtime_ms, size: row.size }]);
            }
        },
        candidate_delete_batch: () => { throw new Error("fixture has no deletions"); },
        commit_candidate: () => {
            check(candidate !== null, "commit without candidate");
            state.commits++; committed = candidate; candidate = null;
            return { before: 1, reachable: 1, removed: 0, after: 1 };
        },
        abort_candidate: () => {
            state.aborts++; candidate = null;
            return { before: 1, reachable: 1, removed: 0, after: 1 };
        },
    } as any;
    const syncBase = {
        entryCount: () => entries.size,
        getEntry: (path: string) => entries.get(path) ?? null,
        setEntry: (path: string, hash: string, mtime: number, size: number) => entries.set(path, { hash, mtime, size }),
        setLastSyncTimestamp: () => {},
        setTreeBaseRoot: (root: string) => { state.baseRoot = root; },
        bulkChangeApprovalCovers: () => false,
        save: async () => { state.saves++; },
    } as any;
    const wasm = {
        wasm_should_chunk: (size: number) => size >= MIB,
        wasm_root_hash_from_bytes: (bytes: Uint8Array) => digest(bytes),
        wasm_root_version_from_bytes: () => 1,
        wasm_tree_committed_chunk_hashes: () => [],
        wasm_tree_candidate_chunk_hashes: () => state.index ? [INDEX] : [],
        wasm_tree_new_candidate_chunk_hashes: () => state.index ? [INDEX] : [],
        wasm_tree_chunk_byte_length: () => 1,
        wasm_tree_get_chunk: () => new Uint8Array([7]),
        Hasher: class {
            private readonly hash = createHash("sha256");
            constructor() { state.rangeHashers++; }
            update(bytes: Uint8Array) { state.rangeHashBytes += bytes.length; this.hash.update(bytes); }
            finalize() { return this.hash.digest("hex"); }
            free() { state.freedRangeHashers++; }
        },
    } as any;
    const workers = {
        stats: () => ({ wasmMode: "simd" }),
        run: async (job: any) => {
            check(job.absolutePath === absolutePath && !("data" in job), "worker source crossed as a byte payload");
            state.workerModes.push(job.mode);
            events.push(`worker:${job.mode}`);
            const bytes = await readFile(absolutePath);
            state.workerBytes += bytes.length;
            const fingerprint = await fingerprintOf(absolutePath);
            check(bytes.length === job.expectedSize && Math.abs(fingerprint.mtime - job.expectedMtime) <= 1,
                "fixture worker metadata changed unexpectedly");
            const common = { type: "result", job_id: `job-${state.workerModes.length}`, size: bytes.length,
                mtime: fingerprint.mtime, fingerprint, read_ms: 11, hash_ms: job.mode === "hash" ? 17 : 29 };
            const result = job.mode === "hash" ? { ...common, mode: "hash", hash: digest(bytes) }
                : { ...common, mode: "manifest", manifest: manifestOf(bytes) };
            await state.afterWorker?.(job.mode);
            return result;
        },
    } as any;
    const io = {
        getAbsolutePath: () => absolutePath,
        stat: async () => { const info = await fingerprintOf(absolutePath); return { size: info.size, mtime: info.mtime }; },
        readFile: async () => { state.rendererReads++; throw new Error("unexpected whole-source renderer fallback"); },
    } as any;
    const api = {
        ensureTransportReady: async () => {},
        checkContentChunks: async (hashes: string[]) => {
            events.push("check:content"); state.chunkChecks.push([...hashes]);
            return hashes.filter(hash => !state.present.has(hash));
        },
        checkChunks: async (hashes: string[]) => {
            events.push("check:index"); await state.afterIndexCheck?.(); return [...hashes];
        },
        putObjects: async (records: readonly BulkUploadRecord[], _perf: unknown, memory: TransientWorkScope) => {
            check(memory.snapshot().totalBytes > 0 && !memory.snapshot().parentReleased,
                "upload escaped its admitted parent scope");
            for (const record of records) {
                events.push(`put:${record.kind}`);
                state.uploads.push({ kind: record.kind, hash: record.hash });
                if (record.kind === BulkObjectKind.ContentChunk) {
                    check(digest(record.data) === record.hash, "transport received mismatching content bytes");
                    state.present.add(record.hash);
                } else if (record.kind === BulkObjectKind.Manifest) {
                    const manifest = JSON.parse(new TextDecoder().decode(record.data)) as FileManifest;
                    check(manifest.chunks.every(chunk => state.present.has(chunk.hash)),
                        "manifest uploaded before every dependency was accepted");
                    state.manifests.set(record.hash, manifest);
                }
            }
            await state.afterPut?.(records);
        },
        putRoot: async (_vault: string, bytes: Uint8Array) => {
            state.rootCalls++; events.push("root");
            if (state.rootError) throw state.rootError;
            const root = JSON.parse(new TextDecoder().decode(bytes)) as typeof committed;
            state.roots.push(root);
            check(root.every(([, entry]) => state.manifests.has(entry.hash)), "root lacks its content manifest");
            return { root_hash: digest(bytes), conflicts: [] };
        },
    } as any;
    const prepared: PushPreparedContext = {
        scopeHash: SCOPE,
        journalThroughId: () => 41,
        assertApplicable: (path) => {
            if (path !== PATH || state.generation !== 41) throw new Error("stale prepared generation");
        },
        plan: {
            lookup: async (scope: string, path: string) => {
                events.push("store:load"); await state.beforeStore?.("load"); return state.plan.lookup(scope, path);
            },
            retain: async (input: any) => {
                events.push("store:retain:start"); await state.beforeStore?.("retain");
                const result = await state.plan.retain(input); events.push("store:retain:done"); return result;
            },
            discard: async (scope: string, path: string, mutation: number) => {
                events.push("store:discard"); await state.beforeStore?.("discard");
                return state.plan.discard(scope, path, mutation);
            },
        } as any,
    };
    return {
        state, events, entries, original, originalManifest, originalFingerprint, absolutePath, trace,
        parts: { api, io, syncBase, wasm, tree, workers }, prepared,
        seed: async (manifest = originalManifest, fingerprint = originalFingerprint, journalThroughId = 41) => {
            const result = await state.plan.retain({ scopeHash: SCOPE, path: PATH, journalThroughId,
                baseRoot: BASE, source: { size: manifest.total_size, mtime: fingerprint.mtime,
                    fingerprint: { kind: "desktop-v1", ...fingerprint } }, manifest });
            check(result.retained, "fixture prepared record was not retained");
        },
        restart: async () => {
            state.storeIO = new MemorySegmentedIO(state.storeIO.snapshot());
            state.plan = new PreparedTransferPlan(state.storeIO); await state.plan.load();
        },
        run: async (changes: FileChange[] = [{ action: "modified", path: PATH,
            mtime: originalFingerprint.mtime, size: original.length }]) => {
            const operation = trace.begin("push");
            let succeeded = false;
            try {
                const result = await push(api, io, syncBase, wasm, tree, "vault", changes, BASE,
                message => state.onProgress?.(message), operation, workers, undefined, undefined, prepared);
                succeeded = true; return result;
            } finally { operation.finish(succeeded ? "success" : "error"); }
        },
    };
}
type Fixture = Awaited<ReturnType<typeof fixture>>;

async function withFixture(work: (f: Fixture) => Promise<void>): Promise<void> {
    const directory = await mkdtemp(join(tmpdir(), "obsetync-push-prepared-"));
    try { await work(await fixture(directory)); }
    finally {
        if (dirname(directory) !== tmpdir() || !basename(directory).startsWith("obsetync-push-prepared-")) {
            throw new Error("invalid owned fixture cleanup target");
        }
        await rm(directory, { recursive: true, force: true });
    }
}
async function rejection(work: Promise<unknown>): Promise<unknown> {
    try { await work; } catch (error) { return error; }
    throw new Error("expected push failure");
}
function unchanged(f: Fixture) {
    check(f.state.rootCalls === 0 && f.state.commits === 0, "stale/failed preparation published a root");
    check(f.entries.get(PATH)?.hash === PREVIOUS && f.state.saves === 0, "failed preparation changed sync-base");
    check(f.state.aborts === 1, "failed preparation did not abort its candidate once");
}

async function sameMetadataOfflineEditCannotPublishOldManifest(): Promise<void> {
    await withFixture(async f => {
        await f.seed(); await f.restart();
        f.originalManifest.chunks.forEach(chunk => f.state.present.add(chunk.hash));
        f.state.manifests.set(f.originalManifest.file_hash, f.originalManifest);
        const changed = f.original.slice(); changed[MIB + 33] ^= 7;
        await writeFile(f.absolutePath, changed);
        const before = await stat(f.absolutePath);
        await utimes(f.absolutePath, before.atime, f.originalFingerprint.mtime / 1000);
        check(Math.abs((await stat(f.absolutePath)).mtimeMs - f.originalFingerprint.mtime) <= 1,
            "offline edit fixture failed to preserve size/mtime");
        await f.run();
        const latest = digest(changed);
        check(f.state.workerModes.join(",") === "hash,manifest", "changed same-metadata hint skipped verification/reprepare");
        check(f.state.workerBytes === 2 * changed.length, "mismatch did not read current content for both worker stages");
        check(f.state.roots[0][0][1].hash === latest && latest !== f.originalManifest.file_hash,
            "old available server manifest was published after offline content change");
        check(f.events.indexOf("store:retain:done") < f.events.indexOf("check:content"),
            "fresh manifest reached object checks before durable retention");
        check(f.state.plan.lookup(SCOPE, PATH)?.manifest.file_hash === latest, "replacement preparation was not durable");
        check(f.state.rangeHashBytes === 0 && f.state.rendererReads === 0,
            "fresh worker preparation unexpectedly used cached-range hashing or renderer fallback");
    });
}

async function coldHintAlwaysVerifiesAndUsesFreshFingerprint(): Promise<void> {
    for (const missing of [false, true]) await withFixture(async f => {
        await f.seed(f.originalManifest, { ...f.originalFingerprint, inode: f.originalFingerprint.inode + 100 });
        await f.restart();
        if (!missing) f.originalManifest.chunks.forEach(chunk => f.state.present.add(chunk.hash));
        await f.run();
        check(f.state.workerModes.join(",") === "hash", "accepted hint recomputed FastCDC or skipped hash verification");
        check(f.state.workerBytes === f.original.length, "accepted hint did not read every current source byte");
        check(f.state.rangeHashBytes === (missing ? f.original.length : 0), "missing ranges had wrong validation coverage");
        check(f.state.rangeHashers === f.state.freedRangeHashers, "cached range hashing leaked its WASM owner");
        check(f.state.chunkChecks.length === 1, "cached hint skipped fresh object presence checks");
        check(f.state.rootCalls === 1 && f.entries.get(PATH)?.hash === f.originalManifest.file_hash,
            "fresh fingerprint was not accepted by the real pathname/handle range reader");
        const record = f.trace.recent().at(-1)!;
        check((record.phases.fastcdc ?? 0) === 0, "reused preparation double-counted worker work as FastCDC");
        check((record.phases.hash ?? 0) >= 17, "full-file verification was missing from hash diagnostics");
        if (!missing) check(record.phases.hash === 17 && record.phases.read === 11,
            "zero-range reuse double-counted validation work");
        check(f.state.rendererReads === 0, "prepared desktop path read whole source in renderer");
    });
}

async function freshPreparationPublicationIsAwaited(): Promise<void> {
    await withFixture(async f => {
        const entered = deferred<void>(); const release = deferred<void>();
        f.state.beforeStore = async stage => { if (stage === "retain") { entered.resolve(); await release.promise; } };
        const running = f.run();
        await entered.promise;
        // Let a mistakenly unawaited publication escape through the pipeline
        // before inspecting the gate; do not only inspect the first microtask.
        await new Promise<void>(resolve => setImmediate(resolve));
        const earlyChecks = f.state.chunkChecks.length, earlyUploads = f.state.uploads.length;
        const earlyHint = f.state.plan.lookup(SCOPE, PATH);
        release.resolve(); await running;
        check(earlyChecks === 0 && earlyUploads === 0 && earlyHint === null,
            "unpublished fresh manifest was used before retain completed");
        await f.restart();
        check(f.state.plan.lookup(SCOPE, PATH)?.manifest.file_hash === f.originalManifest.file_hash,
            "accepted preparation was unavailable after cold load");
    });
}

async function newerMutationAtAsyncBoundariesNeverPublishes(): Promise<void> {
    for (const boundary of ["verify", "upload", "index-check", "index-upload", "before-root"]) {
        await withFixture(async f => {
            await f.seed();
            const invalidate = () => { f.state.generation++; };
            if (boundary === "verify") f.state.afterWorker = invalidate;
            if (boundary === "upload") f.state.afterPut = records => {
                if (records.some(row => row.kind === BulkObjectKind.ContentChunk)) invalidate();
            };
            if (boundary.startsWith("index")) {
                f.state.index = true;
                if (boundary === "index-check") f.state.afterIndexCheck = invalidate;
                else f.state.afterPut = records => {
                    if (records.some(row => row.kind === BulkObjectKind.IndexChunk)) invalidate();
                };
            }
            if (boundary === "before-root") f.state.onProgress = text => {
                if (text === "↑ pushing root...") invalidate();
            };
            await rejection(f.run());
            check(f.state.generation > 41, `${boundary}: fixture did not reach invalidation boundary`);
            unchanged(f);
        });
    }
}

async function storeFailuresNeverLicenseWorkerFallback(): Promise<void> {
    for (const stage of ["load", "discard", "retain"]) await withFixture(async f => {
        if (stage === "discard") await f.seed({ ...f.originalManifest, file_hash: "f".repeat(64) });
        const failure = new Error(`injected prepared ${stage} IO failure`);
        f.state.beforeStore = at => { if (at === stage) throw failure; };
        const error = await rejection(f.run());
        check(error instanceof PreparedDesktopManifestError && error.stage === stage && error.cause === failure,
            `${stage}: storage failure was hidden or converted to worker failure`);
        check(f.state.rendererReads === 0 && f.state.chunkChecks.length === 0,
            `${stage}: storage failure licensed renderer fallback or object checks`);
        unchanged(f);
    });
}

async function lostAckAndServerResetRequireFreshObjectChecks(): Promise<void> {
    await withFixture(async f => {
        await f.seed();
        const lost = new Error("accepted chunks but lost transport ACK");
        f.state.afterPut = records => {
            if (records.some(row => row.kind === BulkObjectKind.ContentChunk)) throw lost;
        };
        check(await rejection(f.run()) === lost, "lost chunk ACK classification changed");
        unchanged(f);
        const uploaded = f.state.uploads.filter(row => row.kind === BulkObjectKind.ContentChunk).length;
        check(uploaded === 2 && f.state.present.size === 2, "lost ACK fixture did not accept complete chunk pack");
        await f.restart(); f.state.afterPut = undefined;
        await f.run();
        check(f.state.uploads.filter(row => row.kind === BulkObjectKind.ContentChunk).length === uploaded,
            "cold retry resent already accepted chunks");
        check(f.state.chunkChecks.length === 2 && f.state.workerModes.join(",") === "hash,hash",
            "cold retry trusted durable object ACKs or skipped source validation");
        // Same scope/key, but server objects have disappeared after restoration.
        f.state.present.clear(); f.state.manifests.clear(); await f.restart();
        await f.run();
        check(Number(f.state.chunkChecks.length) === 3 && f.state.workerBytes === 3 * f.original.length,
            "server reset reused stale confirmations or source hash");
        check(f.state.uploads.filter(row => row.kind === BulkObjectKind.ContentChunk).length === 4,
            "reset server's missing content was not re-uploaded");
    });
}

async function durablePresenceSkipsChecksOnlyForExactServerGeneration(): Promise<void> {
    for (const matchingGeneration of [true, false]) await withFixture(async f => {
        await f.seed();
        const confirmations = new ObjectConfirmationStore(f.state.storeIO);
        await confirmations.load();
        const observed = "1".repeat(64), current = matchingGeneration ? observed : "2".repeat(64);
        check(await confirmations.retain(SCOPE, observed, [
            ...f.originalManifest.chunks.map(row => ({ kind: "content-chunk" as const, hash: row.hash })),
            { kind: "index-chunk", hash: INDEX },
        ]), "fixture confirmations were not durably retained");
        f.prepared.confirmations = confirmations;
        f.prepared.serverGeneration = current;
        f.state.index = true;
        if (matchingGeneration) f.originalManifest.chunks.forEach(row => f.state.present.add(row.hash));
        await f.run();
        check(f.state.workerModes.join(",") === "hash",
            "positive object confirmation skipped full current-source verification");
        if (matchingGeneration) {
            check(f.state.chunkChecks.length === 0,
                "exact durable server generation repeated an authoritative object check");
            check(f.state.uploads.every(row => row.kind !== BulkObjectKind.ContentChunk),
                "exact positive confirmations reuploaded content chunks");
            check(!f.events.includes("check:index") &&
                f.state.uploads.every(row => row.kind !== BulkObjectKind.IndexChunk),
            "exact positive confirmation rechecked or reuploaded a Merkle index chunk");
        } else {
            check(f.state.chunkChecks.length === 1 &&
                f.state.uploads.filter(row => row.kind === BulkObjectKind.ContentChunk).length === 2,
            "changed server generation trusted stale positive confirmations");
            check(f.events.includes("check:index") &&
                f.state.uploads.some(row => row.kind === BulkObjectKind.IndexChunk),
            "changed server generation trusted a stale Merkle index confirmation");
        }
    });
    await withFixture(async f => {
        await f.seed();
        const confirmations = new ObjectConfirmationStore(f.state.storeIO); await confirmations.load();
        const generation = "3".repeat(64);
        await confirmations.retain(SCOPE, generation, f.originalManifest.chunks.map(row => ({
            kind: "content-chunk" as const, hash: row.hash,
        })));
        f.prepared.confirmations = confirmations; f.prepared.serverGeneration = generation;
        f.originalManifest.chunks.forEach(row => f.state.present.add(row.hash));
        f.state.rootError = new Error("injected root refusal");
        check(await rejection(f.run()) === f.state.rootError, "root refusal fixture did not reach publication");
        check(confirmations.snapshot().records === 0,
            "failed publication retained positive hints for an infinite stale retry");
        const cold = new ObjectConfirmationStore(new MemorySegmentedIO(f.state.storeIO.snapshot()));
        await cold.load();
        check(cold.snapshot().records === 0, "failed-publication invalidation was not durable");
    });
}

async function incorrectCachedChunkIsRejectedBeforeTransport(): Promise<void> {
    await withFixture(async f => {
        // Structurally valid metadata with a correct whole-file hash, but an
        // incorrect expected chunk hash. This isolates actual range validation
        // from pathname stat guards without pretending to reproduce a native
        // filesystem that conceals ctime/inode changes.
        const bad = clone(f.originalManifest); bad.chunks[0].hash = "e".repeat(64);
        await f.seed(bad); await f.restart();
        const error = await rejection(f.run());
        check(error instanceof HashWorkerFileDriftError, "mismatching cached range did not report file drift");
        check(f.state.workerModes.join(",") === "hash" && f.state.rangeHashBytes === MIB,
            "bad-range fixture failed to reach the cached range validator");
        check(f.state.uploads.length === 0, "unverified range bytes or manifest reached transport");
        check(f.state.rangeHashers === 1 && f.state.freedRangeHashers === 1, "failed range hash leaked WASM state");
        unchanged(f);
    });
}

async function cleanupTokensCaptureOnlyPublishedMutations(): Promise<void> {
    for (const reuse of [false, true]) await withFixture(async f => {
        if (reuse) await f.seed();
        const result = await f.run();
        const hint = f.state.plan.lookup(SCOPE, PATH)!;
        check(result.published && JSON.stringify(result.preparedCleanup) === JSON.stringify([
            { scopeHash: SCOPE, path: PATH, expectedMutationId: hint.mutationId },
        ]), "successful push returned absent/incorrect prepared retirement tokens");
        check(f.state.plan.snapshot().records === 1, "push retired preparation before its caller's journal ACK");
    });
    await withFixture(async f => {
        await f.seed();
        const old = f.state.plan.lookup(SCOPE, PATH)!;
        f.state.afterPut = async records => {
            if (records.some(record => record.kind === BulkObjectKind.Manifest)) {
                // Another preparation owns this path while the older push is
                // still completing. Cleanup must retain the originally used ID.
                await f.seed(f.originalManifest, f.originalFingerprint, 42);
            }
        };
        const result = await f.run();
        const newer = f.state.plan.lookup(SCOPE, PATH)!;
        check(newer.mutationId > old.mutationId, "fixture did not install a newer prepared owner");
        check(result.preparedCleanup?.[0]?.expectedMutationId === old.mutationId,
            "older push captured the newer path owner's cleanup token");
        check(await f.state.plan.discardMany(result.preparedCleanup!) === 0,
            "old post-ACK token discarded newer prepared work");
        await f.restart();
        check(f.state.plan.lookup(SCOPE, PATH)?.mutationId === newer.mutationId,
            "CAS-skipped newer prepared work did not survive cold reload");
    });
    await withFixture(async f => {
        await f.seed();
        f.parts.io.getAbsolutePath = () => null;
        const result = await f.run([{ action: "modified", path: PATH, size: 512 * MIB, mtime: 1 }]);
        check(result.published === false && result.deferred?.length === 1 && !result.preparedCleanup?.length,
            "unadmitted/deferred file returned a prepared cleanup claim");
        check(f.state.rootCalls === 0 && f.state.plan.snapshot().records === 1,
            "deferred source published or discarded its previous hint");
    });
    await withFixture(async f => {
        await f.seed();
        f.parts.io.getAbsolutePath = (path: string) => path === PATH ? f.absolutePath : null;
        const result = await f.run([
            { action: "modified", path: PATH, size: f.original.length, mtime: f.originalFingerprint.mtime },
            { action: "created", path: "oversized.bin", size: 512 * MIB, mtime: 1 },
        ]);
        check(result.published && result.deferred?.[0]?.path === "oversized.bin" &&
            result.preparedCleanup?.length === 1 && result.preparedCleanup[0].path === PATH,
        "mixed published/deferred push returned cleanup for an uncommitted file");
    });
    await withFixture(async f => {
        f.state.plan = new PreparedTransferPlan(new MemorySegmentedIO(), { chunks: 1 });
        await f.state.plan.load();
        const result = await f.run();
        check(result.published && !result.preparedCleanup?.length && f.state.plan.snapshot().records === 0,
            "capacity-skipped optional retention invented a cleanup token");
    });
}

/** Actual pushPending/materialization/push/base adoption/journal ACK and
 * deferred settlement. Only renderer-host status/visibility and the remote
 * API/WASM ports use the surrounding fixture; no transaction step is replaced. */
function enableIncrementalTreeFixture(tree: any): void {
    type JobKind = "begin" | "chunks" | "update" | "delete" | "root-export";
    let nextToken = 1;
    let active: { token: number; kind: JobKind; payload?: string; retiring?: boolean;
        bytes?: Uint8Array; exportStage?: "plan" | "planned" | "build" | "built" } | null = null;
    let revision = 0;
    const beginCandidate = tree.begin_candidate.bind(tree);
    const updateCandidate = tree.candidate_update_batch.bind(tree);
    const deleteCandidate = tree.candidate_delete_batch.bind(tree);
    const commitCandidate = tree.commit_candidate.bind(tree);
    const abortCandidate = tree.abort_candidate.bind(tree);
    const begin = (kind: JobKind, payload?: string) => {
        check(active === null, "incremental fixture overlapped tree jobs");
        const token = nextToken++;
        active = { token, kind, payload };
        return token;
    };
    const owned = (token: number) => {
        check(active?.token === token, "incremental fixture lost its tree job token");
        return active!;
    };
    tree.candidate_revision = () => revision;
    tree.begin_candidate_job = () => begin("begin");
    tree.begin_candidate_chunks_job = () => begin("chunks");
    tree.begin_candidate_update_job = (payload: string) => begin("update", payload);
    tree.begin_candidate_delete_job = (payload: string) => begin("delete", payload);
    tree.step_tree_job = (token: number) => {
        const job = owned(token);
        check(job.kind === "update" || job.kind === "delete", "unexpected legacy tree-job step");
        return { done: true, units: 1, completed: 1, remaining: 0, reachable: 1 };
    };
    tree.finish_candidate_job = () => { throw new Error("deferred fixture used consuming candidate finish"); };
    tree.finish_candidate_chunks_job = () => { throw new Error("deferred fixture used consuming chunk finish"); };
    tree.finish_candidate_mutation_job = (token: number) => {
        const job = owned(token);
        check(job.kind === "update" || job.kind === "delete", "incremental fixture finished another job kind");
        (job.kind === "update" ? updateCandidate : deleteCandidate)(job.payload!);
        revision++;
        active = null;
    };
    tree.cancel_tree_job = (token: number) => { owned(token); active = null; };
    tree.step_reachability_job_deferred = (token: number) => {
        const job = owned(token);
        check(job.kind === "begin" || job.kind === "chunks", "incremental fixture traversed another job kind");
        const reachable = job.kind === "begin" ? tree.total_files() : 0;
        return { done: true, units: reachable, completed: reachable, remaining: 0, reachable };
    };
    tree.finish_candidate_job_deferred = (token: number) => {
        const job = owned(token);
        check(job.kind === "begin", "incremental fixture finished another reachability job");
        beginCandidate();
        revision++;
        job.retiring = true;
        return tree.candidate_total_files();
    };
    tree.finish_candidate_chunks_job_deferred = () => {
        throw new Error("paged fixture used detached chunk arrays");
    };
    tree.cancel_reachability_job_deferred = (token: number) => { owned(token).retiring = true; };
    tree.step_reachability_retirement = (token: number, completed: number) => {
        const job = owned(token);
        check(job.retiring === true && completed === 0, "incremental fixture retired an invalid cursor");
        return { done: true, units: 0, completed: 0 };
    };
    tree.finish_reachability_retirement = (token: number, completed: number) => {
        const job = owned(token);
        check(job.retiring === true && completed === 0, "incremental fixture finalized an invalid cursor");
        active = null;
    };
    tree.candidate_chunks_sort_memory_plan_v1_job = (token: number) => {
        const job = owned(token);
        check(job.kind === "chunks" && !job.retiring, "incremental fixture planned another cursor");
        return { schema: 1, scope: "candidate-chunk-plan-sort-workspace", hashCount: 0,
            hashSizeBytes: 32, sourceHashesRequestedBytes: 0, scratchHashesRequestedBytes: 0,
            peakAdmissionBytes: 0, reachableSetUnmeasured: true, pageOutputUnmeasured: true,
            sortStrategy: "stable-lsd-radix-v1", pageMaxHashes: 256 };
    };
    tree.resume_candidate_chunks_sort_memory_v1_job = (token: number, source: number, scratch: number) => {
        const job = owned(token);
        check(job.kind === "chunks" && source === 0 && scratch === 0,
            "incremental fixture resumed an invalid chunk sort");
    };
    tree.step_candidate_chunks_sort_v1_job = () => {
        throw new Error("empty incremental fixture unexpectedly sorted chunk hashes");
    };
    tree.candidate_chunks_plan_info_v1_job = (token: number) => {
        const job = owned(token);
        check(job.kind === "chunks", "incremental fixture read another chunk plan");
        return { schema: 1, scope: "candidate-chunk-plan-pages", allCount: 0, pageMaxHashes: 256 };
    };
    tree.read_candidate_chunks_page_v1_job = () => {
        throw new Error("empty incremental fixture unexpectedly read a chunk page");
    };
    tree.finish_candidate_chunks_plan_v1_job = (token: number) => {
        const job = owned(token);
        check(job.kind === "chunks", "incremental fixture sealed another chunk plan");
        job.retiring = true;
    };
    const beginRootExport = (bytes: Uint8Array | undefined, maxArenaBytes: number) => {
        check(bytes !== undefined && bytes.byteLength > 0 && bytes.byteLength <= maxArenaBytes,
            "incremental fixture received an invalid root export bound");
        const token = begin("root-export");
        active!.bytes = bytes;
        active!.exportStage = "plan";
        return token;
    };
    tree.begin_candidate_root_export_job = (maxArenaBytes: number) =>
        beginRootExport(tree.candidate_root_bytes(), maxArenaBytes);
    tree.begin_committed_root_export_job = (maxArenaBytes: number) =>
        beginRootExport(tree.root_bytes(), maxArenaBytes);
    tree.step_root_export_job = (token: number) => {
        const job = owned(token);
        check(job.kind === "root-export" && (job.exportStage === "plan" || job.exportStage === "build"),
            "incremental fixture stepped an invalid root export phase");
        job.exportStage = job.exportStage === "plan" ? "planned" : "built";
        return { done: true, units: 1, bytes: 256, completed: 1, processed: 256 };
    };
    tree.root_export_workset = (token: number) => {
        const job = owned(token);
        check(job.kind === "root-export" && job.exportStage === "planned",
            "incremental fixture read an unplanned root export");
        return { max_length: job.bytes!.byteLength, offset_bytes: 0 };
    };
    tree.start_root_export_build_job = (token: number) => {
        const job = owned(token);
        check(job.kind === "root-export" && job.exportStage === "planned",
            "incremental fixture built an unplanned root export");
        job.exportStage = "build";
    };
    tree.root_export_info = (token: number) => {
        const job = owned(token);
        check(job.kind === "root-export" && job.exportStage === "built",
            "incremental fixture inspected an unfinished root export");
        return { length: job.bytes!.byteLength, version: 1, hash: digest(job.bytes!) };
    };
    tree.read_root_export_job = (token: number, offset: number, maxBytes: number) => {
        const job = owned(token);
        check(job.kind === "root-export" && job.exportStage === "built",
            "incremental fixture read an unfinished root export");
        return job.bytes!.slice(offset, Math.min(job.bytes!.byteLength, offset + maxBytes));
    };
    tree.finish_root_export_job = (token: number) => {
        const job = owned(token);
        check(job.kind === "root-export" && job.exportStage === "built",
            "incremental fixture finished an incomplete root export");
        active = null;
    };
    tree.commit_candidate = () => { const result = commitCandidate(); revision++; return result; };
    tree.abort_candidate = () => { const result = abortCandidate(); revision++; return result; };
}

async function engineFixture(f: Fixture) {
    enableIncrementalTreeFixture(f.parts.tree);
    const journalIO = new MemorySegmentedIO();
    const app = { vault: { adapter: Object.assign(journalIO, {
        writeBinary: async (_path: string, _bytes: ArrayBuffer) => { f.events.push("root:cache"); },
    }) } };
    const journal = new ObsetyncJournal(app as any);
    await journal.load();
    const id = await journal.append({ action: "modified", path: PATH, ts: 1, synced: false });
    await f.seed(f.originalManifest, f.originalFingerprint, id);
    const pendingChanges = new DirtyPathSet();
    pendingChanges.add({ action: "modified", path: PATH }, id);
    const engine = Object.create(ObsetyncSyncEngine.prototype) as any;
    Object.assign(engine, {
        ...f.parts, hashWorkers: f.parts.workers, app, journal, pendingChanges,
        deferredChanges: new DeferredChangeTracker(), localEventsInFlight: new LocalEventGuards(),
        hashWorkerAbort: new AbortController(), preparedTransfers: { plan: f.state.plan }, preparedScopeHash: SCOPE,
        treeBaseRoot: BASE, localRootHash: BASE, vaultId: "vault", syncPriority: "oldest",
        state: "idle", syncing: false, stopped: false,
        onStatusUpdate: () => {}, isExcluded: () => false,
    });
    return { engine, journal, journalIO, id, pendingChanges, app };
}

function hasWalOperation(event: { method: string; phase: string; path: string; data?: string }, op: string) {
    if (event.method !== "write" || event.phase !== "before" || !event.path.includes("/wal-")) return false;
    return JSON.parse(JSON.parse(event.data!).payload).rows.some((row: any) => row.op === op);
}

async function actualEngineAckFailureRetainsPreparation(): Promise<void> {
    await withFixture(async f => {
        const e = await engineFixture(f);
        const mutation = f.state.plan.lookup(SCOPE, PATH)!.mutationId;
        const failure = new Error("injected journal ACK WAL failure");
        let attempted = false, cleanup = false;
        e.journalIO.onBoundary = event => {
            if (hasWalOperation(event, "ack")) { attempted = true; throw failure; }
        };
        f.state.storeIO.onBoundary = event => { if (hasWalOperation(event, "discard")) cleanup = true; };
        await e.engine.pushPending();
        check(attempted && e.engine.state === "error", "actual journal ACK did not fail at the injected boundary");
        check(f.state.rootCalls === 1 && f.state.commits === 1 && f.entries.get(PATH)?.hash === f.originalManifest.file_hash,
            "ACK failure fixture did not first accept root/base");
        check(!cleanup && f.state.plan.lookup(SCOPE, PATH)?.mutationId === mutation,
            "journal ACK failure retired the prepared source");
        check(e.pendingChanges.size === 1, "failed durable ACK lost its dirty retry hint");
        const restarted = new ObsetyncJournal({ vault: { adapter: new MemorySegmentedIO(e.journalIO.snapshot()) } } as any);
        await restarted.load();
        check(restarted.unsyncedCount() === 1 && restarted.unsynced()[0].id === e.id,
            "failed journal ACK erased the durable mutation on restart");
    });
}

async function actualEngineCleanupFollowsAckAndCannotUndoIt(): Promise<void> {
    for (const failCleanup of [false, true]) await withFixture(async f => {
        const e = await engineFixture(f);
        let cleanupCalls = 0, ackedAtCleanup = false;
        const failure = new Error("injected prepared cleanup WAL failure");
        e.journalIO.onBoundary = event => {
            if (hasWalOperation(event, "ack")) f.events.push("journal:ack");
        };
        f.state.storeIO.onBoundary = event => {
            if (!hasWalOperation(event, "discard")) return;
            cleanupCalls++; f.events.push("plan:cleanup");
            ackedAtCleanup = e.journal.unsyncedCount() === 0 && e.pendingChanges.size === 0;
            if (failCleanup && cleanupCalls === 1) throw failure;
        };
        await e.engine.pushPending();
        check(cleanupCalls === 1 && ackedAtCleanup, "actual engine cleanup preceded durable journal ACK/settlement");
        check(f.events.indexOf("root") < f.events.indexOf("journal:ack") &&
            f.events.indexOf("journal:ack") < f.events.indexOf("plan:cleanup"),
        "root → journal ACK → prepared cleanup order changed");
        check(f.state.rootCalls === 1 && f.state.commits === 1 && f.state.baseRoot !== BASE,
            "cleanup path lost accepted root or adopted base");
        check(e.engine.state === "idle" && e.pendingChanges.size === 0 && e.journal.unsyncedCount() === 0,
            "optional cleanup failure restored already acknowledged dirty work");
        const coldJournal = new ObsetyncJournal({ vault: { adapter: new MemorySegmentedIO(e.journalIO.snapshot()) } } as any);
        await coldJournal.load();
        check(coldJournal.unsyncedCount() === 0, "cleanup outcome undid durable journal ACK");
        const coldPlan = new PreparedTransferPlan(new MemorySegmentedIO(f.state.storeIO.snapshot()));
        await coldPlan.load();
        check(coldPlan.snapshot().records === (failCleanup ? 1 : 0), "cleanup publication cut did not survive cold recovery");
        if (!failCleanup) return;
        check(!f.state.plan.snapshot().ready, "failed cleanup did not poison its writer");
        let loadCalls = 0;
        const actualLoad = f.state.plan.load.bind(f.state.plan);
        f.state.plan.load = async () => {
            loadCalls++; f.events.push("plan:reload"); await actualLoad();
        };
        const nextId = await e.journal.append({ action: "modified", path: PATH, ts: 2, synced: false });
        e.pendingChanges.add({ action: "modified", path: PATH }, nextId);
        const after = f.events.length;
        await e.engine.pushPending();
        const nextEvents = f.events.slice(after);
        check(loadCalls === 1 && nextEvents.indexOf("plan:reload") < nextEvents.indexOf("worker:hash"),
            "next push reused poisoned prepared state before validated load");
        check(f.state.plan.snapshot().ready && f.state.plan.snapshot().records === 0 &&
            e.journal.unsyncedCount() === 0 && e.pendingChanges.size === 0,
        "validated retry failed to drain the next mutation and old optional hint");
    });
}

const deadline = setTimeout(() => {
    console.error("push-prepared.test: async suite did not finish"); process.exitCode = 1;
}, 60_000);
void sameMetadataOfflineEditCannotPublishOldManifest()
    .then(coldHintAlwaysVerifiesAndUsesFreshFingerprint)
    .then(freshPreparationPublicationIsAwaited)
    .then(newerMutationAtAsyncBoundariesNeverPublishes)
    .then(storeFailuresNeverLicenseWorkerFallback)
    .then(lostAckAndServerResetRequireFreshObjectChecks)
    .then(durablePresenceSkipsChecksOnlyForExactServerGeneration)
    .then(incorrectCachedChunkIsRejectedBeforeTransport)
    .then(cleanupTokensCaptureOnlyPublishedMutations)
    .then(actualEngineAckFailureRetainsPreparation)
    .then(actualEngineCleanupFollowsAckAndCannotUndoIt)
    .then(() => { clearTimeout(deadline); console.log(`push-prepared.test: ${assertions} assertions passed`); })
    .catch(error => { clearTimeout(deadline); console.error(error); process.exitCode = 1; });
