import { push } from "./push";
import {
    isReenrollmentRequiredError,
    reenrollmentRequired,
} from "./transport-errors";
import { PerfTrace } from "./perf-trace";

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
            api.putContent = async () => { throw new Error(failure); };
        } else {
            f.wasm.wasm_tree_candidate_chunk_hashes = () => ["index"];
            f.wasm.wasm_tree_new_candidate_chunk_hashes = () => ["index"];
            f.wasm.wasm_tree_get_chunk = () => new Uint8Array([1]);
            if (failure === "index-check") {
                api.checkChunks = async () => { throw new Error(failure); };
            } else {
                api.checkChunks = async () => ["index"];
                api.putChunk = async () => { throw new Error(failure); };
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

void terminalPreflightDoesNotMutate()
    .then(failedRootRestoresCandidate)
    .then(failedRootDoesNotCommitUpsert)
    .then(everyPostCandidateFailureAbortsWithoutFullSnapshot)
    .then(incrementalPushChecksOnlyNewCandidateChunks)
    .then(acceptedRootCommitsMetadata)
    .then(() => console.log(`push-transaction.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
