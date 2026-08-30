import { push } from "./push";
import {
    isReenrollmentRequiredError,
    reenrollmentRequired,
} from "./transport-errors";

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
    const syncBase = {
        allPaths: () => [...entries.keys()],
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
    let deleteCalls = 0;
    let rebuildCalls = 0;
    const tree = {
        root_hash_hex: () => `root:${[...treeEntries].sort().join(",")}`,
        root_bytes: () => new Uint8Array([1]),
        total_files: () => treeEntries.size,
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
    check(f.rebuildCalls() === 1, "failed candidate tree was not rebuilt exactly once");
    check(f.saves() === 0, "failed root request saved sync-base");
}

async function acceptedRootCommitsMetadata(): Promise<void> {
    const f = fixture();
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
    ], "base");

    check(baseWasIntactAtRequest, "sync-base committed before root acceptance");
    check(!f.entries.has("gone.md"), "accepted root did not commit sync-base deletion");
    check(f.treePaths().join(",") === "keep.md", "accepted candidate tree was rolled back");
    check(f.rebuildCalls() === 0, "accepted root unexpectedly rebuilt the tree");
    check(f.saves() === 1, "accepted root did not save sync-base exactly once");
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

void terminalPreflightDoesNotMutate()
    .then(failedRootRestoresCandidate)
    .then(failedRootDoesNotCommitUpsert)
    .then(acceptedRootCommitsMetadata)
    .then(() => console.log(`push-transaction.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
