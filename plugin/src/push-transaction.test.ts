import { push } from "./push";
import {
    isReenrollmentRequiredError,
    isUpgradeRequiredError,
    reenrollmentRequired,
    upgradeRequired,
} from "./transport-errors";
import { PerfTrace } from "./perf-trace";
import { HashWorkerFileDriftError } from "./desktop-hash-workers";
import { BulkObjectKind } from "./bulk-codec";
import { mkdtemp, open, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

(globalThis as any).window ??= globalThis;

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const keepHash = "a".repeat(64);
const goneHash = "b".repeat(64);

function fixture() {
    const entries = new Map<string, { hash: string; mtime: number; size: number }>([
        ["keep.md", { hash: keepHash, mtime: 1, size: 1 }],
        ["gone.md", { hash: goneHash, mtime: 2, size: 2 }],
    ]);
    let saved = 0;
    let allPathsCalls = 0;
    const syncBase = {
        allPaths: () => {
            allPathsCalls++;
            return [...entries.keys()];
        },
        entryCount: () => entries.size,
        getEntry: (path: string) => entries.get(path) ?? null,
        getTreeMtime: (path: string) => entries.get(path)?.mtime ?? null,
        removeEntry: (path: string) => { entries.delete(path); },
        setEntry: (path: string, hash: string, mtime: number, size: number) => {
            entries.set(path, { hash, mtime, size });
        },
        setLastSyncTimestamp: () => {},
        save: async () => { saved++; },
    } as any;

    let treeEntries = new Set(entries.keys());
    let candidateEntries: Set<string> | null = null;
    let deleteCalls = 0;
    let rebuildCalls = 0;
    let beginCalls = 0;
    let commitCalls = 0;
    let abortCalls = 0;
    const tree = {
        root_hash_hex: () => `root:${[...treeEntries].sort().join(",")}`,
        root_bytes: () => new Uint8Array([1]),
        total_files: () => treeEntries.size,
        begin_candidate: () => {
            beginCalls++;
            if (candidateEntries) throw new Error("candidate already active");
            candidateEntries = new Set(treeEntries);
        },
        has_candidate: () => candidateEntries !== null,
        candidate_root_hash_hex: () => candidateEntries
            ? `root:${[...candidateEntries].sort().join(",")}`
            : undefined,
        candidate_root_bytes: () => candidateEntries ? new Uint8Array([1]) : undefined,
        candidate_total_files: () => candidateEntries?.size ?? 0,
        candidate_delete_batch: (json: string) => {
            if (!candidateEntries) throw new Error("no candidate");
            for (const path of JSON.parse(json) as string[]) candidateEntries.delete(path);
        },
        candidate_update_batch: (json: string) => {
            if (!candidateEntries) throw new Error("no candidate");
            for (const row of JSON.parse(json) as Array<{ path: string }>) {
                candidateEntries.add(row.path);
            }
        },
        commit_candidate: () => {
            if (!candidateEntries) throw new Error("no candidate");
            commitCalls++;
            treeEntries = candidateEntries;
            candidateEntries = null;
            return { before: 2, reachable: treeEntries.size, removed: 1, after: treeEntries.size };
        },
        abort_candidate: () => {
            if (!candidateEntries) throw new Error("no candidate");
            abortCalls++;
            candidateEntries = null;
            return { before: 2, reachable: treeEntries.size, removed: 1, after: treeEntries.size };
        },
        // Legacy methods remain in the fixture so the assertions prove push
        // selected the transactional API rather than merely failing early.
        delete_batch: (json: string) => {
            deleteCalls++;
            for (const path of JSON.parse(json) as string[]) treeEntries.delete(path);
        },
        update_batch: () => {},
        build_from_entries: (json: string) => {
            rebuildCalls++;
            treeEntries = new Set((JSON.parse(json) as Array<{ path: string }>).map((row) => row.path));
        },
    } as any;
    const wasm = {
        wasm_should_chunk: () => false,
        wasm_tree_chunk_hashes: () => [],
        wasm_tree_committed_chunk_hashes: () => [],
        wasm_tree_candidate_chunk_hashes: () => [],
        wasm_tree_new_candidate_chunk_hashes: () => [],
        wasm_tree_get_chunk: () => null,
    } as any;
    const io = {} as any;

    return {
        entries,
        syncBase,
        tree,
        wasm,
        io,
        treePaths: () => [...treeEntries].sort(),
        deleteCalls: () => deleteCalls,
        rebuildCalls: () => rebuildCalls,
        beginCalls: () => beginCalls,
        commitCalls: () => commitCalls,
        abortCalls: () => abortCalls,
        allPathsCalls: () => allPathsCalls,
        saves: () => saved,
    };
}

async function terminalPreflightDoesNotMutate(): Promise<void> {
    const f = fixture();
    const terminal = reenrollmentRequired("server upgraded — re-enroll device");
    const api = {
        ensureTransportReady: async () => { throw terminal; },
    } as any;

    let caught: unknown;
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "deleted", path: "gone.md" },
        ], "base");
    } catch (error) {
        caught = error;
    }

    check(isReenrollmentRequiredError(caught), "terminal preflight error lost its classification");
    check(
        isReenrollmentRequiredError(new Error("missing key — re-enroll the device")),
        "plain legacy re-enroll error was not classified",
    );
    check(
        !isReenrollmentRequiredError(new Error("temporary network failure")),
        "transient error was classified as terminal",
    );
    const upgrade = upgradeRequired(
        "upgrade required: update/reload the Obsetync plugin; enrollment remains valid",
    );
    check(isUpgradeRequiredError(upgrade), "HTTP 426 upgrade lost its classification");
    check(
        !isReenrollmentRequiredError(upgrade),
        "HTTP 426 upgrade was incorrectly classified as re-enrollment",
    );
    check(f.deleteCalls() === 0, "terminal preflight mutated the tree");
    check(f.beginCalls() === 0, "terminal preflight opened a candidate");
    check(f.entries.has("gone.md"), "terminal preflight mutated sync-base");
}

async function failedRootRestoresCandidate(): Promise<void> {
    const f = fixture();
    let baseWasIntactAtRequest = false;
    const api = {
        ensureTransportReady: async () => {},
        putRoot: async () => {
            baseWasIntactAtRequest = f.entries.has("gone.md");
            throw new Error("network down");
        },
    } as any;

    let failed = false;
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            { action: "deleted", path: "gone.md" },
        ], "base");
    } catch {
        failed = true;
    }

    check(failed, "failed root request unexpectedly succeeded");
    check(baseWasIntactAtRequest, "sync-base changed before root acceptance");
    check(f.entries.has("gone.md"), "failed root request changed sync-base");
    check(f.treePaths().join(",") === "gone.md,keep.md", "failed root request left tree mutated");
    check(f.abortCalls() === 1, "failed candidate was not aborted exactly once");
    check(f.commitCalls() === 0, "failed candidate was committed");
    check(f.rebuildCalls() === 0, "failed push rebuilt the complete tree");
    check(f.allPathsCalls() === 0, "failed incremental push enumerated the full sync-base");
    check(f.saves() === 0, "failed root request saved sync-base");
}

async function acceptedRootCommitsMetadata(): Promise<void> {
    const f = fixture();
    const trace = new PerfTrace({ monitorEventLoop: false });
    const operation = trace.begin("push");
    let baseWasIntactAtRequest = false;
    const api = {
        ensureTransportReady: async () => {},
        putRoot: async () => {
            baseWasIntactAtRequest = f.entries.has("gone.md");
            return { root_hash: "accepted", conflicts: [] };
        },
    } as any;

    await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
        { action: "deleted", path: "gone.md" },
    ], "base", undefined, operation);
    operation.finish();

    check(baseWasIntactAtRequest, "sync-base committed before root acceptance");
    check(!f.entries.has("gone.md"), "accepted root did not commit sync-base deletion");
    check(f.treePaths().join(",") === "keep.md", "accepted candidate tree was rolled back");
    check(f.beginCalls() === 1, "accepted push did not open exactly one candidate");
    check(f.commitCalls() === 1, "accepted push did not commit exactly one candidate");
    check(f.abortCalls() === 0, "accepted push aborted its candidate");
    check(f.rebuildCalls() === 0, "accepted root unexpectedly rebuilt the tree");
    check(f.allPathsCalls() === 0, "accepted incremental push enumerated the full sync-base");
    check(f.saves() === 1, "accepted root did not save sync-base exactly once");
    const record = trace.recent()[0];
    check(record.filesTotal === 1, "push trace lost total path count");
    check(record.bytesTotal === 0, "push trace counted deletion bytes");
    check(record.filesCompleted === 1, "push trace lost completed deletion");
    check(record.phases.tree_update !== undefined, "push trace missed tree update");
    check(record.phases.root_commit !== undefined, "push trace missed root commit");
}

async function failedRootDoesNotCommitUpsert(): Promise<void> {
    const f = fixture();
    const replacementHash = "c".repeat(64);
    let putRootCalled = false;
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async () => [],
        putRoot: async () => {
            putRootCalled = true;
            throw new Error("root rejected");
        },
    } as any;

    let caught: unknown;
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [
            {
                action: "modified",
                path: "keep.md",
                hash: replacementHash,
                mtime: 3,
                size: 3,
            },
        ], "base");
    } catch (error) {
        caught = error;
    }

    check(putRootCalled, `upsert failed before putRoot: ${String(caught)}`);
    check(caught instanceof Error && caught.message === "root rejected", "wrong upsert failure surfaced");
    check(f.entries.get("keep.md")?.hash === keepHash, "failed upsert changed sync-base");
    check(f.saves() === 0, "failed upsert saved sync-base");
}

async function everyPostCandidateFailureAbortsWithoutFullSnapshot(): Promise<void> {
    const replacementHash = "d".repeat(64);
    const cases = ["content-check", "content-upload", "index-check", "index-upload"] as const;
    for (const failure of cases) {
        const f = fixture();
        const api: any = {
            ensureTransportReady: async () => {},
            checkContent: async () => [],
            checkChunks: async () => [],
            putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
        };
        if (failure === "content-check") {
            api.checkContent = async () => { throw new Error(failure); };
        } else if (failure === "content-upload") {
            api.checkContent = async () => [replacementHash];
            api.putObjects = async () => { throw new Error(failure); };
        } else {
            f.wasm.wasm_tree_candidate_chunk_hashes = () => ["index"];
            f.wasm.wasm_tree_new_candidate_chunk_hashes = () => ["index"];
            f.wasm.wasm_tree_get_chunk = () => new Uint8Array([1]);
            if (failure === "index-check") {
                api.checkChunks = async () => { throw new Error(failure); };
            } else {
                api.checkChunks = async () => ["index"];
                api.putObjects = async () => { throw new Error(failure); };
            }
        }

        let caught: unknown;
        try {
            await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
                action: "modified",
                path: "keep.md",
                hash: replacementHash,
                data: new Uint8Array([4, 5, 6]),
                mtime: 4,
                size: 3,
            }], "base");
        } catch (error) {
            caught = error;
        }

        check(caught instanceof Error && caught.message === failure, `${failure}: wrong error`);
        check(f.abortCalls() === 1, `${failure}: candidate was not aborted`);
        check(f.commitCalls() === 0, `${failure}: candidate was committed`);
        check(f.treePaths().join(",") === "gone.md,keep.md", `${failure}: root changed`);
        check(f.entries.get("keep.md")?.hash === keepHash, `${failure}: sync-base changed`);
        check(f.allPathsCalls() === 0, `${failure}: full sync-base snapshot was built`);
        check(f.saves() === 0, `${failure}: sync-base was saved`);
    }
}

async function incrementalPushChecksOnlyNewCandidateChunks(): Promise<void> {
    const f = fixture();
    const replacementHash = "e".repeat(64);
    f.wasm.wasm_tree_committed_chunk_hashes = () => ["old"];
    f.wasm.wasm_tree_candidate_chunk_hashes = () => ["new", "old"];
    f.wasm.wasm_tree_new_candidate_chunk_hashes = () => ["new"];
    let checked: string[] = [];
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async () => [],
        checkChunks: async (hashes: string[]) => {
            checked = hashes;
            return [];
        },
        putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
    } as any;

    await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
        action: "modified",
        path: "keep.md",
        hash: replacementHash,
        mtime: 5,
        size: 3,
    }], "base");

    check(checked.join(",") === "new", "incremental push checked historical index chunks");
    check(f.commitCalls() === 1, "incremental push did not commit its candidate");
    check(f.abortCalls() === 0, "incremental push aborted after success");
}

async function workerManifestAvoidsRendererFileBytes(): Promise<void> {
    const f = fixture();
    const fileHash = "f".repeat(64);
    const firstChunkHash = "1".repeat(64);
    const secondChunkHash = "2".repeat(64);
    const size = 2 * 1024 * 1024;
    const directory = await mkdtemp(join(tmpdir(), "obsetync-ranged-push-"));
    const absolutePath = join(directory, "large.bin");
    try {
        const handle = await open(absolutePath, "w");
        try {
            await handle.truncate(size);
            await handle.write(new Uint8Array([7, 8, 9]), 0, 3, 1024 * 1024);
        } finally {
            await handle.close();
        }
        const info = await stat(absolutePath);
        const fingerprint = {
            size: Number(info.size),
            mtime: Number(info.mtimeMs),
            ctime: Number(info.ctimeMs),
            device: Number(info.dev),
            inode: Number(info.ino),
        };
        f.wasm.wasm_should_chunk = (bytes: number) => bytes >= 1024 * 1024;
        let readCalls = 0;
        f.io.getAbsolutePath = () => absolutePath;
        f.io.readFile = async () => {
            readCalls++;
            throw new Error("renderer read should not happen");
        };
        let workerInput: any;
        const workers = {
            run: async (input: any) => {
                workerInput = input;
                return {
                    type: "result",
                    job_id: "test",
                    mode: "manifest",
                    manifest: {
                        file_hash: fileHash,
                        total_size: size,
                        chunks: [
                            { hash: firstChunkHash, offset: 0, size: 1024 * 1024 },
                            { hash: secondChunkHash, offset: 1024 * 1024, size: 1024 * 1024 },
                        ],
                    },
                    size,
                    mtime: fingerprint.mtime,
                    fingerprint,
                    read_ms: 1,
                    hash_ms: 2,
                };
            },
        } as any;
        let manifestUploads = 0;
        let contentChunkUploads = 0;
        let uploadedRangeLength = -1;
        let uploadedRangeFirst = -1;
        let uploadedRangeThird = -1;
        const api = {
            ensureTransportReady: async () => {},
            checkContentChunks: async () => [secondChunkHash],
            putObjects: async (records: Array<{
                kind: BulkObjectKind;
                data: Uint8Array;
            }>) => {
                manifestUploads += records.filter((record) =>
                    record.kind === BulkObjectKind.Manifest).length;
                for (const record of records) {
                    if (record.kind === BulkObjectKind.ContentChunk) {
                        contentChunkUploads++;
                        uploadedRangeLength = record.data.byteLength;
                        uploadedRangeFirst = record.data[0];
                        uploadedRangeThird = record.data[2];
                    }
                }
            },
            putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
        } as any;

        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
            action: "created",
            path: "large.bin",
            mtime: fingerprint.mtime,
            size,
        }], "base", undefined, undefined, workers);

        check(workerInput.absolutePath === absolutePath, "worker did not receive absolute path");
        check(workerInput.mode === "manifest", "large file did not request a manifest job");
        check(!("data" in workerInput), "file bytes crossed the worker boundary");
        check(readCalls === 0, "desktop ranged upload called renderer readFile");
        check(contentChunkUploads === 1, "missing bitmap did not select one range");
        check(uploadedRangeLength === 1024 * 1024, "ranged read returned the wrong size");
        check(uploadedRangeFirst === 7 && uploadedRangeThird === 9, "ranged read used the wrong offset");
        check(manifestUploads === 1, "worker manifest was not uploaded");
        check(f.entries.get("large.bin")?.hash === fileHash, "worker hash was not committed");
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}

async function interruptedRangedUploadResumesFromServerBitmap(): Promise<void> {
    const f = fixture();
    const mib = 1024 * 1024;
    const size = 12 * mib;
    const fileHash = "3".repeat(64);
    const chunkHashes = ["4".repeat(64), "5".repeat(64), "6".repeat(64)];
    const directory = await mkdtemp(join(tmpdir(), "obsetync-ranged-resume-"));
    const absolutePath = join(directory, "resume.bin");
    try {
        const handle = await open(absolutePath, "w");
        try {
            await handle.truncate(size);
        } finally {
            await handle.close();
        }
        const info = await stat(absolutePath);
        const fingerprint = {
            size: Number(info.size),
            mtime: Number(info.mtimeMs),
            ctime: Number(info.ctimeMs),
            device: Number(info.dev),
            inode: Number(info.ino),
        };
        f.wasm.wasm_should_chunk = (bytes: number) => bytes >= mib;
        f.io.getAbsolutePath = () => absolutePath;
        let rendererReads = 0;
        f.io.readFile = async () => {
            rendererReads++;
            throw new Error("renderer read should not happen");
        };
        const manifest = {
            file_hash: fileHash,
            total_size: size,
            chunks: chunkHashes.map((hash, index) => ({
                hash,
                offset: index * 4 * mib,
                size: 4 * mib,
            })),
        };
        const workers = {
            run: async () => ({
                type: "result",
                job_id: "resume",
                mode: "manifest",
                manifest,
                size,
                mtime: fingerprint.mtime,
                fingerprint,
                read_ms: 1,
                hash_ms: 2,
            }),
        } as any;
        const change = {
            action: "created" as const,
            path: "resume.bin",
            mtime: fingerprint.mtime,
            size,
        };

        const stored = new Set<string>();
        const firstAttempted: string[] = [];
        let chunkPutCalls = 0;
        const firstApi = {
            ensureTransportReady: async () => {},
            checkContentChunks: async () => chunkHashes,
            putObjects: async (records: Array<{
                kind: BulkObjectKind;
                hash: string;
            }>) => {
                const chunks = records.filter((record) =>
                    record.kind === BulkObjectKind.ContentChunk);
                if (chunks.length === 0) return;
                chunkPutCalls++;
                firstAttempted.push(...chunks.map((record) => record.hash));
                if (chunkPutCalls === 2) throw new Error("injected ranged disconnect");
                for (const record of chunks) stored.add(record.hash);
            },
        } as any;
        let interrupted = false;
        try {
            await push(
                firstApi,
                f.io,
                f.syncBase,
                f.wasm,
                f.tree,
                "vault",
                [change],
                "base",
                undefined,
                undefined,
                workers,
            );
        } catch (error) {
            interrupted = (error as Error).message === "injected ranged disconnect";
        }
        check(interrupted, "ranged interruption was hidden");
        check(f.abortCalls() === 1 && f.commitCalls() === 0, "interruption did not abort candidate");
        check(stored.size === 2, "first ACKed range pack differs");
        check(
            firstAttempted.join(",") === chunkHashes.join(","),
            "first attempt did not stop on the failing range pack",
        );
        check(!f.entries.has("resume.bin"), "interrupted range upload committed metadata");

        const retryContent: string[] = [];
        let retryManifest = 0;
        const retryApi = {
            ensureTransportReady: async () => {},
            checkContentChunks: async () => chunkHashes.filter((hash) => !stored.has(hash)),
            putObjects: async (records: Array<{
                kind: BulkObjectKind;
                hash: string;
            }>) => {
                for (const record of records) {
                    if (record.kind === BulkObjectKind.ContentChunk) {
                        retryContent.push(record.hash);
                        stored.add(record.hash);
                    } else if (record.kind === BulkObjectKind.Manifest) {
                        retryManifest++;
                    }
                }
            },
            putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
        } as any;
        await push(
            retryApi,
            f.io,
            f.syncBase,
            f.wasm,
            f.tree,
            "vault",
            [change],
            "base",
            undefined,
            undefined,
            workers,
        );
        check(retryContent.join(",") === chunkHashes[2], "retry re-uploaded an ACKed range");
        check(retryManifest === 1, "retry did not upload the dependent manifest once");
        check(stored.size === 3, "retry left content ranges missing");
        check(f.abortCalls() === 1 && f.commitCalls() === 1, "retry candidate outcome differs");
        check(f.entries.get("resume.bin")?.hash === fileHash, "retry did not commit metadata");
        check(rendererReads === 0, "ranged resume called renderer readFile");
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}

async function rangedDriftAfterTransferAbortsCandidate(): Promise<void> {
    const f = fixture();
    const mib = 1024 * 1024;
    const size = 2 * mib;
    const fileHash = "7".repeat(64);
    const chunkHash = "8".repeat(64);
    const directory = await mkdtemp(join(tmpdir(), "obsetync-ranged-drift-"));
    const absolutePath = join(directory, "drift.bin");
    try {
        const initial = await open(absolutePath, "w");
        try {
            await initial.truncate(size);
        } finally {
            await initial.close();
        }
        const info = await stat(absolutePath);
        const fingerprint = {
            size: Number(info.size),
            mtime: Number(info.mtimeMs),
            ctime: Number(info.ctimeMs),
            device: Number(info.dev),
            inode: Number(info.ino),
        };
        f.wasm.wasm_should_chunk = (bytes: number) => bytes >= mib;
        f.io.getAbsolutePath = () => absolutePath;
        const workers = {
            run: async () => ({
                type: "result",
                job_id: "drift",
                mode: "manifest",
                manifest: {
                    file_hash: fileHash,
                    total_size: size,
                    chunks: [{ hash: chunkHash, offset: 0, size }],
                },
                size,
                mtime: fingerprint.mtime,
                fingerprint,
                read_ms: 1,
                hash_ms: 2,
            }),
        } as any;
        let manifestUploads = 0;
        let putRootCalled = false;
        const api = {
            ensureTransportReady: async () => {},
            checkContentChunks: async () => [chunkHash],
            putObjects: async (records: Array<{ kind: BulkObjectKind }>) => {
                if (records.some((record) => record.kind === BulkObjectKind.ContentChunk)) {
                    const changed = await open(absolutePath, "r+");
                    try {
                        await changed.truncate(size - 1);
                    } finally {
                        await changed.close();
                    }
                }
                manifestUploads += records.filter((record) =>
                    record.kind === BulkObjectKind.Manifest).length;
            },
            putRoot: async () => {
                putRootCalled = true;
                return { root_hash: "accepted", conflicts: [] };
            },
        } as any;

        let drifted = false;
        try {
            await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
                action: "created",
                path: "drift.bin",
                mtime: fingerprint.mtime,
                size,
            }], "base", undefined, undefined, workers);
        } catch (error) {
            drifted = error instanceof HashWorkerFileDriftError;
        }
        check(drifted, "post-transfer file drift was hidden");
        check(manifestUploads === 1, "final drift check ran before transfer completion");
        check(!putRootCalled, "drifted ranged upload reached root commit");
        check(f.abortCalls() === 1 && f.commitCalls() === 0, "drift did not abort candidate");
        check(!f.entries.has("drift.bin"), "drifted range metadata reached sync-base");
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}

async function workerDriftAbortsCandidate(): Promise<void> {
    const f = fixture();
    f.io.getAbsolutePath = () => "/vault/drift.md";
    const workers = {
        run: async () => { throw new HashWorkerFileDriftError(); },
    } as any;
    const api = { ensureTransportReady: async () => {} } as any;
    let drifted = false;
    try {
        await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", [{
            action: "modified",
            path: "drift.md",
            mtime: 12,
            size: 12,
        }], "base", undefined, undefined, workers);
    } catch (error) {
        drifted = error instanceof HashWorkerFileDriftError;
    }
    check(drifted, "worker stat drift was hidden");
    check(f.abortCalls() === 1, "worker stat drift did not abort candidate");
}

async function smallFilesReachTransportAsPacksNotPerFilePuts(): Promise<void> {
    const f = fixture();
    const changes = Array.from({ length: 600 }, (_, index) => ({
        action: "created" as const,
        path: `bulk/${index}.md`,
        hash: index.toString(16).padStart(64, "0"),
        data: new Uint8Array([index & 0xff]),
        mtime: 100 + index,
        size: 1,
    }));
    let checkCalls = 0;
    let packedCalls = 0;
    let packedRecords = 0;
    let legacyPuts = 0;
    const api = {
        ensureTransportReady: async () => {},
        checkContent: async (hashes: string[]) => {
            checkCalls++;
            return hashes;
        },
        putObjects: async (records: unknown[]) => {
            packedCalls++;
            packedRecords += records.length;
        },
        putContent: async () => { legacyPuts++; },
        putRoot: async () => ({ root_hash: "accepted", conflicts: [] }),
    } as any;

    await push(api, f.io, f.syncBase, f.wasm, f.tree, "vault", changes, "base");
    check(checkCalls <= 10, `600 files expanded to ${checkCalls} check batches`);
    check(packedCalls === checkCalls, "each stream batch did not become one packed upload call");
    check(packedRecords === 600, "packed push lost content records");
    check(legacyPuts === 0, "packed push issued a per-file content PUT");
    check(f.commitCalls() === 1, "packed push did not commit its candidate");
}

void terminalPreflightDoesNotMutate()
    .then(failedRootRestoresCandidate)
    .then(failedRootDoesNotCommitUpsert)
    .then(everyPostCandidateFailureAbortsWithoutFullSnapshot)
    .then(incrementalPushChecksOnlyNewCandidateChunks)
    .then(workerManifestAvoidsRendererFileBytes)
    .then(interruptedRangedUploadResumesFromServerBitmap)
    .then(rangedDriftAfterTransferAbortsCandidate)
    .then(workerDriftAbortsCandidate)
    .then(smallFilesReachTransportAsPacksNotPerFilePuts)
    .then(acceptedRootCommitsMetadata)
    .then(() => console.log(`push-transaction.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
