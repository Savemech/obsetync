import { pull } from "./pull";
import { ObsetyncSyncBase, SYNC_BASE_STORE_PATH } from "./sync-base";
import type { DiffPage, FileDelta } from "./api";
import { StoreRecoveryError } from "./segmented-store";
import { MemorySegmentedIO, readStoreTestFrame, sealStoreTestFrame } from "./segmented-store-test-io";
import { RootTreeResidentAdmission } from "./root-tree-resident-admission";
import { diffCursorToHex } from "./diff-page-codec";
import { TREE_CANDIDATE_MUTATION_STEP_UNITS } from "./tree-candidate-mutation-job";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

class MemoryVaultIO {
    files = new Map<string, { bytes: Uint8Array; mtime: number }>();
    writes: string[] = [];
    async readFile(path: string): Promise<Uint8Array> {
        const file = this.files.get(path);
        if (!file) throw new Error("missing");
        return file.bytes;
    }
    async writeFile(path: string, bytes: Uint8Array): Promise<void> {
        this.writes.push(path);
        this.files.set(path, { bytes: bytes.slice(), mtime: 100 + this.writes.length });
    }
    async deleteFile(path: string): Promise<void> { this.files.delete(path); }
    async renameFile(from: string, to: string): Promise<void> {
        const file = this.files.get(from);
        if (!file) throw new Error("missing");
        this.files.set(to, file);
        this.files.delete(from);
    }
    async stat(path: string): Promise<{ mtime: number; size: number } | null> {
        const file = this.files.get(path);
        return file ? { mtime: file.mtime, size: file.bytes.byteLength } : null;
    }
    async exists(path: string): Promise<boolean> { return this.files.has(path); }
    async mkdir(): Promise<void> {}
    getAbsolutePath(): null { return null; }
}

function makeTree(targetRoot: string): any {
    const entries = new Map<string, unknown>();
    let candidate: Map<string, unknown> | null = null;
    return {
        root_hash_hex: () => entries.size === 0 ? null : entries.size === 2 ? targetRoot : "f".repeat(64),
        build_from_entries: (json: string) => {
            entries.clear();
            for (const entry of JSON.parse(json)) entries.set(entry.path, entry);
        },
        begin_candidate: () => { candidate = new Map(entries); },
        has_candidate: () => candidate !== null,
        candidate_delete_batch: (json: string) => {
            for (const path of JSON.parse(json)) candidate!.delete(path);
        },
        candidate_update_batch: (json: string) => {
            for (const entry of JSON.parse(json)) candidate!.set(entry.path, entry);
        },
        commit_candidate: () => {
            entries.clear();
            for (const [path, entry] of candidate!) entries.set(path, entry);
            candidate = null;
        },
        abort_candidate: () => { candidate = null; },
    };
}

function addition(path: string, hash: string, byte: number): FileDelta {
    return { action: "added", path, hash, size: 1, mtime_ms: 1_800_000_000_000 + byte };
}

async function rendererKillResumesAfterDurablePage(): Promise<void> {
    const adapter = new MemorySegmentedIO();
    const app = { vault: { adapter } } as any;
    const io = new MemoryVaultIO();
    const sourceRoot = "1".repeat(64);
    const targetRoot = "2".repeat(64);
    const hashA = "a".repeat(64);
    const hashB = "b".repeat(64);
    const cursor = new Uint8Array([0x4f, 0x42, 0x43, 0x31, 1, 2, 3]);
    const firstPage: DiffPage = {
        fromRoot: sourceRoot,
        toRoot: targetRoot,
        deltas: [addition("a.md", hashA, 1)],
        nextCursor: cursor,
        wireBytes: 128,
    };
    const finalPage: DiffPage = {
        fromRoot: sourceRoot,
        toRoot: targetRoot,
        deltas: [addition("b.md", hashB, 2)],
        nextCursor: null,
        wireBytes: 128,
    };
    const downloads: string[] = [];
    const wasm = {
        wasm_hash: (bytes: Uint8Array) => bytes[0] === 1 ? hashA : hashB,
        wasm_root_hash_from_bytes: () => targetRoot,
    } as any;

    const firstBase = new ObsetyncSyncBase(app);
    await firstBase.load();
    let firstCalls = 0;
    const dyingApi = {
        supportsPagedDiff: async () => true,
        getDiffPage: async () => {
            firstCalls++;
            if (firstCalls === 1) return firstPage;
            throw new Error("simulated mobile renderer kill between pages");
        },
        getRootAt: async () => { throw new Error("incomplete pull fetched root"); },
        getContent: async (hash: string) => {
            downloads.push(hash);
            return new Uint8Array([hash === hashA ? 1 : 2]);
        },
    } as any;
    let killed = false;
    try {
        await pull(dyingApi, io as any, firstBase, "vault", sourceRoot, wasm, makeTree(targetRoot));
    } catch (error) {
        killed = String(error).includes("simulated mobile renderer kill");
    }
    check(killed, "first pull did not stop at injected renderer kill");
    check(firstBase.getHash("a.md") === hashA, "first page entry was not applied");
    check(firstBase.diffPageCheckpoint?.nextCursorHex !== null, "first page cursor was not durable");
    const headPath = `${SYNC_BASE_STORE_PATH}/head.json`;
    const head = readStoreTestFrame(adapter, headPath);
    const referencedRows: Array<{ path: string; row: any }> = [];
    for (let id = head.walStart; id <= head.walEnd; id++) {
        const path = `${SYNC_BASE_STORE_PATH}/wal-${head.epoch}-${id}.json`;
        const segment = readStoreTestFrame(adapter, path);
        for (const row of segment.rows) referencedRows.push({ path, row });
    }
    const setPosition = referencedRows.findIndex(({ row }) => row.op === "set" && row.path === "a.md");
    const cursorPosition = referencedRows.findIndex(({ row }) => row.op === "diff-page" && row.value !== null);
    check(setPosition >= 0 && cursorPosition > setPosition,
        "published head did not reference entry mutations before its page cursor");
    check(referencedRows[cursorPosition].row.value.nextCursorHex === firstBase.diffPageCheckpoint?.nextCursorHex,
        "published segment cursor differed from the completed page");

    // A corrupt REFERENCED frame is not an optional torn legacy tail. Skipping
    // it could adopt the later cursor without the entry it promises to cover.
    const corruptAdapter = adapter.clone();
    const damagedPath = referencedRows[setPosition].path;
    const intactFrame = corruptAdapter.files.get(damagedPath)!;
    corruptAdapter.files.set(damagedPath, intactFrame.slice(0, -1));
    const evidence = new Map(corruptAdapter.files);
    const corruptBase = new ObsetyncSyncBase({ vault: { adapter: corruptAdapter } } as any);
    let corruptError: unknown;
    try { await corruptBase.load(); } catch (error) { corruptError = error; }
    check(corruptError instanceof StoreRecoveryError && corruptError.code === "CORRUPT",
        "referenced sealed-segment corruption did not fail closed");
    check(corruptBase.getHash("a.md") === null && corruptBase.diffPageCheckpoint === null,
        "failed recovery exposed partially validated entries or cursor");
    check(corruptAdapter.files.size === evidence.size &&
        [...evidence].every(([path, raw]) => corruptAdapter.files.get(path) === raw),
        "failed recovery modified its authoritative evidence");

    // A fully written future segment is NOT published by a torn head stage.
    // Neither its entry nor its completed-page cursor may become visible.
    const stagedAdapter = adapter.clone();
    const orphanPath = `${SYNC_BASE_STORE_PATH}/wal-${head.epoch}-${head.nextSegment}.json`;
    stagedAdapter.files.set(orphanPath, sealStoreTestFrame({
        schema: 1, kind: "wal", epoch: head.epoch, id: head.nextSegment,
        from: head.sequence + 1, through: head.sequence + 2,
        rows: [
            { op: "set", path: "unpublished.md", entry: { hash: hashB, mtime: 200, size: 1 } },
            { op: "diff-page", value: { ...firstBase.diffPageCheckpoint!, complete: true, nextCursorHex: null } },
        ],
    }));
    stagedAdapter.files.set(`${headPath}.next`, sealStoreTestFrame({
        ...head, generation: head.generation + 1, sequence: head.sequence + 2,
        walEnd: head.nextSegment, nextSegment: head.nextSegment + 1,
    }).slice(0, 25));
    const originalHead = stagedAdapter.files.get(headPath);
    const stagedBase = new ObsetyncSyncBase({ vault: { adapter: stagedAdapter } } as any);
    await stagedBase.load();
    check(stagedBase.getHash("a.md") === hashA && stagedBase.getHash("unpublished.md") === null,
        "unreferenced staged segment replaced the published page state");
    check(stagedBase.diffPageCheckpoint?.complete === false &&
        stagedBase.diffPageCheckpoint.nextCursorHex === firstBase.diffPageCheckpoint?.nextCursorHex,
        "torn unreferenced head stage advanced the durable cursor");
    check(stagedAdapter.files.get(headPath) === originalHead,
        "unpublished segment changed the authoritative head during recovery");

    // New JS objects model a cold renderer restart. Only the adapter/vault
    // survive, exactly as they do across iOS Jetsam termination.
    const recoveredBase = new ObsetyncSyncBase(app);
    await recoveredBase.load();
    check(recoveredBase.getHash("a.md") === hashA, "published first page was lost on restart");
    let resumedRequest = false;
    const resumedApi = {
        supportsPagedDiff: async () => true,
        getDiffPage: async (
            _vault: string,
            from: string,
            to: string | null,
            receivedCursor: Uint8Array | null,
        ) => {
            resumedRequest = from === sourceRoot && to === targetRoot &&
                receivedCursor?.join() === cursor.join();
            return finalPage;
        },
        getRootAt: async (_vault: string, root: string) => {
            check(root === targetRoot, "historical snapshot root was not requested");
            return new Uint8Array([9]);
        },
        getContent: async (hash: string) => {
            downloads.push(hash);
            return new Uint8Array([hash === hashA ? 1 : 2]);
        },
    } as any;
    const result = await pull(
        resumedApi,
        io as any,
        recoveredBase,
        "vault",
        sourceRoot,
        wasm,
        makeTree(targetRoot),
    );
    check(resumedRequest, "restart did not continue from persisted snapshot cursor");
    check(result.requiresTreeRebuild === true,
        "partial checkpoint did not require a base-derived tree replacement");
    check(result.treeParity === null,
        "partial checkpoint compared a volatile tree without proving its persisted prefix");
    check(result.newRootHash === targetRoot, "resumed pull adopted moving instead of fixed root");
    check(recoveredBase.getHash("b.md") === hashB, "final page entry was not applied");
    check(recoveredBase.diffPageCheckpoint?.complete === true,
        "final page marker was not retained for atomic base adoption");
    check(downloads.join() === `${hashA},${hashB}`, "already-applied page downloaded twice");
    check(io.writes.join() === "a.md,b.md", "already-applied page rewrote disk after restart");

    // Second kill window: the final page/cursor is durable but the engine has
    // not yet atomically adopted treeBaseRoot. A cold renderer must rebuild
    // parity from sync-base without requesting or applying any page again.
    const completedBase = new ObsetyncSyncBase(app);
    await completedBase.load();
    let unexpectedPageRequest = false;
    const completedApi = {
        supportsPagedDiff: async () => true,
        getDiffPage: async () => {
            unexpectedPageRequest = true;
            throw new Error("completed cursor requested another page");
        },
        getRootAt: async () => new Uint8Array([9]),
    } as any;
    const staleCompletedTree = makeTree(targetRoot);
    staleCompletedTree.build_from_entries(JSON.stringify([{ path: "stale.md" }]));
    const completedResult = await pull(
        completedApi,
        io as any,
        completedBase,
        "vault",
        sourceRoot,
        wasm,
        staleCompletedTree,
    );
    check(!unexpectedPageRequest, "completed checkpoint fetched a duplicate page");
    check(completedResult.requiresTreeRebuild === true,
        "completed checkpoint did not require a base-derived tree replacement");
    check(completedResult.treeParity === null,
        "stale completed-checkpoint tree was compared before its required replacement");
    check(downloads.join() === `${hashA},${hashB}`, "completed checkpoint redownloaded content");
}

async function legacyTornCursorImportPreservesAppliedPrefix(): Promise<void> {
    const adapter = new MemorySegmentedIO();
    const walPath = ".obsidian/plugins/obsetync/sync-base.wal.ndjson";
    const hash = "a".repeat(64);
    // Explicit pre-segmented WAL-only fixture: without a snapshot there is no
    // ambiguous cut, so the valid prefix can be imported and the cursor replayed.
    const original = `${JSON.stringify({ op: "set", path: "legacy-applied.md",
        entry: { hash, mtime: 101, treeMtime: 99, size: 1 } })}\n{"op":"diff-pa`;
    adapter.files.set(walPath, original);
    const app = { vault: { adapter } } as any;
    const imported = new ObsetyncSyncBase(app);
    await imported.load();
    check(imported.getHash("legacy-applied.md") === hash, "legacy torn cursor discarded the applied entry prefix");
    check(imported.getTreeMtime("legacy-applied.md") === 99, "legacy import lost server-tree mtime");
    check(imported.diffPageCheckpoint === null, "legacy torn cursor advanced imported progress");
    check(adapter.files.get(walPath) === original, "legacy import overwrote the original WAL evidence");
    check(adapter.files.has(`${SYNC_BASE_STORE_PATH}/head.json`), "legacy prefix was not published into the versioned store");
    const restarted = new ObsetyncSyncBase(app);
    await restarted.load();
    check(restarted.getHash("legacy-applied.md") === hash && restarted.diffPageCheckpoint === null,
        "cold restart lost the imported prefix or resurrected its incomplete cursor");
    check(adapter.files.get(walPath) === original, "versioned restart changed the preserved legacy WAL");
}

async function invalidHistoricalRootFailsClosed(): Promise<void> {
    const adapter = new MemorySegmentedIO();
    const app = { vault: { adapter } } as any;
    const io = new MemoryVaultIO();
    const sourceRoot = "1".repeat(64);
    const targetRoot = "2".repeat(64);
    const hash = "a".repeat(64);
    const syncBase = new ObsetyncSyncBase(app);
    await syncBase.load();
    const api = {
        supportsPagedDiff: async () => true,
        getDiffPage: async () => ({
            fromRoot: sourceRoot,
            toRoot: targetRoot,
            deltas: [addition("doc.md", hash, 1)],
            nextCursor: null,
            wireBytes: 128,
        }),
        getRootAt: async () => new Uint8Array([9]),
        getContent: async () => new Uint8Array([1]),
    } as any;
    const wasm = {
        wasm_hash: () => hash,
        // A malformed RootNode is represented by the real WASM API as
        // undefined, and must never be accepted merely because it is falsy.
        wasm_root_hash_from_bytes: () => undefined,
    } as any;
    let rejected = false;
    try {
        await pull(api, io as any, syncBase, "vault", sourceRoot, wasm, makeTree(targetRoot));
    } catch (error) {
        rejected = String(error).includes("failed hash verification");
    }
    check(rejected, "malformed historical root bytes were accepted");
    check(syncBase.treeBaseRoot === null, "unverified historical root advanced the tree base");
}

async function pagedCursorWaitsForAdmittedMutationRetirement(): Promise<void> {
    const adapter = new MemorySegmentedIO();
    const app = { vault: { adapter } } as any;
    const io = new MemoryVaultIO();
    const sourceRoot = "3".repeat(64), targetRoot = "4".repeat(64);
    const hash = "c".repeat(64);
    const cursor = new Uint8Array([0x4f, 0x42, 0x43, 0x31, 9]);
    const syncBase = new ObsetyncSyncBase(app);
    await syncBase.load();
    syncBase.setEntry("existing.md", "d".repeat(64), 1, 1);
    await syncBase.save();

    type Entry = { path: string; hash: string; mtime_ms: number; size: number };
    const committed = new Map<string, Entry>([["existing.md", {
        path: "existing.md", hash: "d".repeat(64), mtime_ms: 1, size: 1,
    }]]);
    let candidate: Map<string, Entry> | null = null;
    let phase: "idle" | "begin" | "plan" | "planned" | "build" | "ready" | "retiring" = "idle";
    let token = 0, revision = 0, payload = "";
    const tree = {
        tree_version: () => 2,
        root_hash_hex: () => committed.size === 2 ? targetRoot : sourceRoot,
        candidate_revision: () => revision,
        has_candidate: () => candidate !== null,
        begin_candidate_job() { check(phase === "idle", "paged candidate begin overlapped work"); phase = "begin"; return ++token; },
        step_tree_job(value: number, units: number) {
            check(value === token && units === 1 && phase === "begin", "invalid paged candidate begin step");
            return { done: true, units: 1, completed: 1, remaining: 0, reachable: 1 };
        },
        finish_candidate_job(value: number) {
            check(value === token && phase === "begin", "invalid paged candidate begin finish");
            candidate = new Map(committed); revision++; phase = "idle"; return 1;
        },
        cancel_tree_job() { throw new Error("paged admitted candidate unexpectedly cancelled"); },
        begin_candidate_update_job(value: string) {
            check(phase === "idle" && candidate !== null, "paged update lacks candidate");
            payload = value; phase = "plan"; return ++token;
        },
        begin_candidate_delete_job() { throw new Error("unexpected paged delete"); },
        finish_candidate_mutation_job() { throw new Error("paged V2 mutation used consuming finish"); },
        step_candidate_mutation_output_memory_v1_job(value: number, units: number) {
            check(value === token && units === TREE_CANDIDATE_MUTATION_STEP_UNITS,
                "invalid paged mutation step");
            if (phase === "plan") {
                phase = "planned";
                return { done: false, units: 1, completed: 1, remaining: 1, reachable: 0, phase: "plan ready" };
            }
            check(phase === "build", "paged mutation crossed admission"); phase = "ready";
            return { done: true, units: 1, completed: 2, remaining: 0, reachable: 1, phase: "ready" };
        },
        candidate_mutation_output_memory_plan_v1_job() {
            check(phase === "planned", "paged mutation plan read in wrong phase");
            return { schema: 1, scope: "v2-candidate-mutation-output", nodePayloadBytes: 70,
                rangeEndpointPeakRequestedBytes: 20, rangeEndpointResidentRequestedBytes: 10,
                peakAdmissionBytes: 90, residentAdmissionBytes: 80 };
        },
        resume_candidate_mutation_output_memory_v1_job(value: number, node: number, peak: number, resident: number) {
            check(value === token && phase === "planned" && node === 70 && peak === 20 && resident === 10,
                "paged mutation resume witnesses changed"); phase = "build";
        },
        candidate_mutation_output_memory_ready_v1_job() {
            check(phase === "ready", "paged mutation Ready read in wrong phase");
            return { schema: 1, scope: "v2-candidate-mutation-output", stagedNodePayloadBytes: 40,
                rangeEndpointResidentRequestedBytes: 10, residentAdmissionBytes: 50 };
        },
        finish_candidate_mutation_job_deferred(value: number) {
            check(value === token && phase === "ready" && candidate !== null, "paged mutation finish lost owner");
            const target = candidate!;
            for (const entry of JSON.parse(payload)) target.set(entry.path, entry);
            revision++; phase = "retiring";
        },
        cancel_candidate_mutation_job_deferred() { throw new Error("paged admitted mutation unexpectedly cancelled"); },
        step_tree_retirement(value: number, units: number) {
            check(value === token && units === 256 && phase === "retiring", "invalid paged mutation retirement");
            phase = "idle"; return { done: true, units: 1, completed: 1 };
        },
        candidate_update_batch() { throw new Error("paged V2 mutation used legacy update"); },
        candidate_delete_batch() { throw new Error("paged V2 mutation used legacy delete"); },
        commit_candidate() {
            check(phase === "idle" && candidate !== null, "paged candidate committed before retirement");
            const target = candidate!; committed.clear();
            for (const [path, entry] of target) committed.set(path, entry);
            candidate = null; revision++;
        },
        abort_candidate() { candidate = null; revision++; },
    } as any;
    const admission = new RootTreeResidentAdmission({ capacityBytes: 90 });
    let pageCalls = 0, rootFetches = 0, retirementEnteredResolve!: () => void;
    let releaseRetirementResolve!: () => void;
    const retirementEntered = new Promise<void>(resolve => { retirementEnteredResolve = resolve; });
    const releaseRetirement = new Promise<void>(resolve => { releaseRetirementResolve = resolve; });
    const api = {
        supportsPagedDiff: async () => true,
        getDiffPage: async () => {
            pageCalls++;
            if (pageCalls === 1) return {
                fromRoot: sourceRoot, toRoot: targetRoot,
                deltas: [addition("new.md", hash, 3)], nextCursor: cursor, wireBytes: 128,
            } satisfies DiffPage;
            throw new Error("stop after admitted page cursor");
        },
        getRootAt: async () => { rootFetches++; return new Uint8Array([9]); },
        getContent: async () => new Uint8Array([3]),
    } as any;
    try {
        const work = pull(api, io as any, syncBase, "vault", sourceRoot,
            { wasm_hash: () => hash, wasm_root_hash_from_bytes: () => targetRoot } as any,
            tree, undefined, undefined, undefined, undefined, undefined, undefined, {
                residentAdmission: admission,
                cooperate: async () => { await Promise.resolve(); },
                cooperateRetirement: async () => {
                    retirementEnteredResolve();
                    await releaseRetirement;
                },
                assertCurrent: () => {},
            });
        await retirementEntered;
        check(syncBase.getHash("new.md") === hash,
            "held paged mutation lost the already durable base row");
        check(syncBase.diffPageCheckpoint === null,
            "paged cursor advanced before admitted mutation retirement completed");
        check(pageCalls === 1 && rootFetches === 0,
            "paged pull requested later transport work before mutation retirement");
        releaseRetirementResolve();
        let rejected = false;
        try { await work; } catch (error) { rejected = String(error).includes("stop after admitted page cursor"); }
        check(rejected, "paged barrier fixture did not stop after publishing the first cursor");
        const checkpoint = syncBase.diffPageCheckpoint;
        check(checkpoint !== null,
            "paged checkpoint disappeared after admitted mutation retirement");
        check(checkpoint?.nextCursorHex === diffCursorToHex(cursor) && checkpoint?.complete === false,
            "paged cursor was not published exactly after admitted mutation retirement");
        const restartedBase = new ObsetyncSyncBase(app);
        await restartedBase.load();
        check(restartedBase.diffPageCheckpoint?.nextCursorHex === diffCursorToHex(cursor) &&
            restartedBase.diffPageCheckpoint?.complete === false,
            "admitted mutation cursor was not durable across sync-base reload");
        check(pageCalls === 2 && rootFetches === 0 && committed.has("new.md"),
            "paged mutation barrier did not commit before requesting the next page");
        check(admission.snapshot().residentBytes === 50,
            "paged committed mutation lost its resident output cohort");
        admission.releaseResidentAfterFree(tree);
    } finally {
        releaseRetirementResolve();
        admission.close();
    }
}

void rendererKillResumesAfterDurablePage()
    .then(legacyTornCursorImportPreservesAppliedPrefix)
    .then(invalidHistoricalRootFailsClosed)
    .then(pagedCursorWaitsForAdmittedMutationRetirement)
    .then(() => console.log(`paged-pull.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
