import { pull } from "./pull";
import type { FileDelta } from "./api";
import { PerfTrace } from "./perf-trace";

const check = (condition: unknown, message: string) => {
    if (!condition) throw new Error(message);
};

async function locallyEditedDeltaKeepsHonestBase(): Promise<void> {
    const remote: FileDelta = {
        action: "modified",
        path: "doc.md",
        hash: "remote-hash",
        size: 6,
        mtime_ms: 2,
    };
    const writes: unknown[] = [];
    const ioWrites: unknown[] = [];
    let saved = false;

    const api = {
        getDiff: async () => [remote],
        getRoot: async () => new Uint8Array([1]),
        getContent: async () => {
            throw new Error("locally edited content must not be downloaded");
        },
    } as any;
    const io = {
        renameFile: async (...args: unknown[]) => ioWrites.push(args),
        deleteFile: async (...args: unknown[]) => ioWrites.push(args),
        writeFile: async (...args: unknown[]) => ioWrites.push(args),
        stat: async () => null,
    } as any;
    const syncBase = {
        removeEntry: () => {},
        setEntry: () => {},
        setLastSyncTimestamp: () => {},
        save: async () => { saved = true; },
        allPaths: () => ["doc.md"],
        getEntry: () => ({ hash: "base-hash", mtime: 1, size: 4 }),
        getTreeMtime: () => 1,
    } as any;
    const tree = {
        root_hash_hex: () => "base-root",
        delete_batch: () => { throw new Error("deferred path reached tree delete"); },
        update_batch: () => { throw new Error("deferred path reached tree upsert"); },
    } as any;
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
    } as any;

    const result = await pull(
        api,
        io,
        syncBase,
        "vault",
        "base-root",
        wasm,
        tree,
        undefined,
        (planned) => writes.push(...planned),
        new Set(["doc.md"]),
    );

    check(ioWrites.length === 0, "locally edited delta touched disk");
    check(writes.length === 0, "locally edited delta registered a pull echo");
    check(result.deferredCount === 1, "local delta was not reported deferred");
    check(result.localDeferredCount === 1, "local deferral reason was lost");
    check(result.treeParity === false, "deferred tree incorrectly reached server parity");
    check(saved, "sync-base checkpoint was not saved");
}

async function renameAfterCrashIsIdempotent(): Promise<void> {
    let renameCalls = 0;
    let checkpoints = 0;
    const removed: string[] = [];
    const set: string[] = [];
    const api = {
        getDiff: async () => [{
            action: "renamed",
            old_path: "old.md",
            path: "new.md",
            hash: "same-hash",
            size: 4,
            mtime_ms: 3,
        }],
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        exists: async (path: string) => path === "new.md",
        renameFile: async () => { renameCalls++; },
        deleteFile: async () => {},
        stat: async () => ({ mtime: 3, size: 4 }),
    } as any;
    const syncBase = {
        getEntry: (path: string) => path === "old.md"
            ? { hash: "same-hash", mtime: 1, size: 4 }
            : path === "new.md"
                ? { hash: "same-hash", mtime: 3, size: 4 }
                : null,
        getTreeMtime: () => 1,
        removeEntry: (path: string) => removed.push(path),
        setEntry: (path: string) => set.push(path),
        checkpoint: async () => { checkpoints++; },
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = {
        root_hash_hex: () => "tree-root",
        delete_batch: () => {},
        update_batch: () => {},
    } as any;
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        wasm_hash: () => "remote-hash",
    } as any;

    await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    check(renameCalls === 0, "crash-resumed rename tried to move the missing source again");
    check(removed.includes("old.md"), "crash-resumed rename did not remove old base entry");
    check(set.includes("new.md"), "crash-resumed rename did not install target base entry");
    check(checkpoints === 1, "rename was not checkpointed immediately");
}

async function sameSizeUnverifiedRenameTargetIsDeferred(): Promise<void> {
    let removed = 0;
    let installed = 0;
    const api = {
        getDiff: async () => [{
            action: "renamed",
            old_path: "old.md",
            path: "new.md",
            hash: "expected-hash",
            size: 4,
            mtime_ms: 3,
        }],
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        exists: async (path: string) => path === "new.md",
        stat: async () => ({ mtime: 3, size: 4 }),
        readFile: async () => new Uint8Array([9, 9, 9, 9]),
        getAbsolutePath: () => null,
        deleteFile: async () => {},
    } as any;
    const syncBase = {
        getEntry: (path: string) => path === "old.md"
            ? { hash: "expected-hash", mtime: 1, size: 4 }
            : null,
        getTreeMtime: () => 1,
        removeEntry: () => { removed++; },
        setEntry: () => { installed++; },
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = {
        root_hash_hex: () => "tree-root",
        delete_batch: () => {},
        update_batch: () => {},
    } as any;
    class Hasher {
        update(): void {}
        finalize(): string { return "wrong-hash"; }
        free(): void {}
    }
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        Hasher,
    } as any;

    const result = await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    check(result.deferredCount === 1, "unverified rename target was accepted by size alone");
    check(removed === 0, "unverified rename removed the source base entry");
    check(installed === 0, "unverified rename installed a false target hash");
}

async function firstSyncAdoptsAnExistingEmptyServerRoot(): Promise<void> {
    let rootFetches = 0;
    let treeBuilds = 0;
    const api = {
        getDiff: async () => [],
        getRoot: async () => {
            rootFetches++;
            return new Uint8Array([1]);
        },
    } as any;
    const io = {} as any;
    const syncBase = {
        allPaths: () => [],
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    let treeRoot: string | null = null;
    const tree = {
        root_hash_hex: () => treeRoot,
        build_from_entries: (json: string) => {
            check(json === "[]", "empty first sync built a non-empty tree");
            treeBuilds++;
            treeRoot = "empty-server-root";
        },
    } as any;
    const wasm = {
        wasm_root_hash_from_bytes: () => "empty-server-root",
    } as any;

    const result = await pull(api, io, syncBase, "vault", null, wasm, tree);
    check(rootFetches === 1, "empty server root was not fetched on first sync");
    check(treeBuilds === 1, "empty local tree was not bootstrapped");
    check(result.newRootHash === "empty-server-root", "empty server root hash was not adopted");
    check(result.treeParity === true, "empty first sync did not establish exact parity");
}

async function editArrivingDuringDownloadDefersRemoteWrite(): Promise<void> {
    let dirty = false;
    let writes = 0;
    const api = {
        getDiff: async () => [{
            action: "added",
            path: "live.md",
            hash: "remote-hash",
            size: 4,
            mtime_ms: 7,
        }],
        getContent: async () => {
            dirty = true; // editor event landed while the request was in flight
            return new Uint8Array([1, 2, 3, 4]);
        },
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        stat: async () => null,
        writeFile: async () => { writes++; },
        exists: async () => false,
        deleteFile: async () => {},
        getAbsolutePath: () => null,
    } as any;
    const syncBase = {
        getEntry: () => null,
        getTreeMtime: () => null,
        setEntry: () => {},
        removeEntry: () => {},
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = {
        root_hash_hex: () => "local-root",
        delete_batch: () => {},
        update_batch: () => {},
    } as any;
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        wasm_hash: () => "remote-hash",
    } as any;

    const result = await pull(
        api,
        io,
        syncBase,
        "vault",
        "base-root",
        wasm,
        tree,
        undefined,
        undefined,
        { has: () => dirty },
    );
    check(writes === 0, "remote bytes overwrote an edit that arrived during download");
    check(result.deferredCount === 1, "mid-download local edit was not deferred");
    check(result.localDeferredCount === 1, "mid-download deferral lost its local reason");
}

async function untrackedLocalCollisionIsPreservedBeforePullWrite(): Promise<void> {
    const moves: Array<[string, string]> = [];
    let writes = 0;
    const api = {
        getDiff: async () => [{
            action: "added",
            path: "doc.md",
            hash: "remote-hash",
            size: 4,
            mtime_ms: 7,
        }],
        getContent: async () => new Uint8Array([1, 2, 3, 4]),
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        stat: async (path: string) => path === "doc.md" ? { mtime: 5, size: 4 } : null,
        readFile: async () => new Uint8Array([9, 9, 9, 9]),
        writeFile: async () => { writes++; },
        renameFile: async (from: string, to: string) => { moves.push([from, to]); },
        exists: async (path: string) => path === "doc.md",
        deleteFile: async () => {},
        getAbsolutePath: () => null,
    } as any;
    const syncBase = {
        getEntry: () => null,
        getTreeMtime: () => null,
        setEntry: () => {},
        removeEntry: () => {},
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = {
        root_hash_hex: () => "local-root",
        delete_batch: () => {},
        update_batch: () => {},
    } as any;
    class Hasher {
        update(): void {}
        finalize(): string { return "local-hash"; }
        free(): void {}
    }
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        wasm_hash: () => "remote-hash",
        Hasher,
    } as any;

    await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    check(moves.length === 1 && moves[0][0] === "doc.md", "untracked local file was not preserved");
    check(moves[0][1].includes("(conflict local-before-pull "), "preserved file used an invisible name");
    check(writes === 1, "remote file was not installed after preserving local bytes");
}

async function unchangedBaseIsReplacedWithoutConflictCopy(): Promise<void> {
    let moves = 0;
    let writes = 0;
    const trace = new PerfTrace({ monitorEventLoop: false });
    const operation = trace.begin("pull");
    const api = {
        getDiff: async () => [{
            action: "modified",
            path: "doc.md",
            hash: "remote-hash",
            size: 4,
            mtime_ms: 8,
        }],
        getContent: async () => new Uint8Array([1, 2, 3, 4]),
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        stat: async () => ({ mtime: 5, size: 4 }),
        readFile: async () => new Uint8Array([5, 5, 5, 5]),
        writeFile: async () => { writes++; },
        renameFile: async () => { moves++; },
        exists: async (path: string) => path === "doc.md",
        deleteFile: async () => {},
        getAbsolutePath: () => null,
    } as any;
    const syncBase = {
        getEntry: () => ({ hash: "base-hash", mtime: 5, size: 4 }),
        getTreeMtime: () => 5,
        setEntry: () => {},
        removeEntry: () => {},
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = {
        root_hash_hex: () => "local-root",
        delete_batch: () => {},
        update_batch: () => {},
    } as any;
    class Hasher {
        update(): void {}
        finalize(): string { return "base-hash"; }
        free(): void {}
    }
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        wasm_hash: () => "remote-hash",
        Hasher,
    } as any;

    await pull(
        api,
        io,
        syncBase,
        "vault",
        "base-root",
        wasm,
        tree,
        undefined,
        undefined,
        undefined,
        undefined,
        operation,
    );
    operation.finish();
    check(moves === 0, "unchanged sync base was preserved as a false conflict");
    check(writes === 1, "remote modification did not replace the unchanged base");
    const record = trace.recent()[0];
    check(record.filesTotal === 1, "pull trace lost delta count");
    check(record.bytesTotal === 4, "pull trace lost delta bytes");
    check(record.filesCompleted === 1, "pull trace lost applied file count");
    check(record.bytesTransferred === 4, "pull trace lost downloaded bytes");
    check(record.phases.download !== undefined, "pull trace missed download phase");
    check(record.phases.apply !== undefined, "pull trace missed apply phase");
}

async function offlineEditSurvivesRemoteDelete(): Promise<void> {
    let diskDeletes = 0;
    let baseRemovals = 0;
    let treeDeletes = 0;
    const api = {
        getDiff: async () => [{ action: "deleted", path: "doc.md" }],
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        stat: async () => ({ mtime: 2, size: 4 }),
        readFile: async () => new Uint8Array([9, 9, 9, 9]),
        getAbsolutePath: () => null,
        deleteFile: async () => { diskDeletes++; },
    } as any;
    const syncBase = {
        getEntry: () => ({ hash: "base-hash", mtime: 1, size: 4 }),
        getTreeMtime: () => 1,
        removeEntry: () => { baseRemovals++; },
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = {
        root_hash_hex: () => "base-root",
        delete_batch: () => { treeDeletes++; },
        update_batch: () => {},
    } as any;
    class Hasher {
        update(): void {}
        finalize(): string { return "local-hash"; }
        free(): void {}
    }
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
        Hasher,
    } as any;

    const result = await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    check(diskDeletes === 0, "remote delete erased an offline local edit");
    check(baseRemovals === 0, "deferred delete removed the honest sync base");
    check(treeDeletes === 0, "deferred delete advanced the Merkle tree");
    check(result.deferredCount === 1, "offline edit was not deferred from remote delete");
    check(result.localDeferredCount === 1, "offline delete deferral lost its local reason");
}

async function unchangedBaseAllowsRemoteDelete(): Promise<void> {
    let diskDeletes = 0;
    let baseRemovals = 0;
    let treeDeletes = 0;
    const api = {
        getDiff: async () => [{ action: "deleted", path: "doc.md" }],
        getRoot: async () => new Uint8Array([1]),
    } as any;
    const io = {
        stat: async () => ({ mtime: 1, size: 4 }),
        deleteFile: async () => { diskDeletes++; },
    } as any;
    const syncBase = {
        getEntry: () => ({ hash: "base-hash", mtime: 1, size: 4 }),
        getTreeMtime: () => 1,
        removeEntry: () => { baseRemovals++; },
        checkpoint: async () => {},
        setLastSyncTimestamp: () => {},
        save: async () => {},
    } as any;
    const tree = {
        root_hash_hex: () => "base-root",
        delete_batch: () => { treeDeletes++; },
        update_batch: () => {},
    } as any;
    const wasm = {
        wasm_root_hash_from_bytes: () => "server-root",
    } as any;

    const result = await pull(api, io, syncBase, "vault", "base-root", wasm, tree);
    check(diskDeletes === 1, "unchanged synced file was not deleted");
    check(baseRemovals === 1, "applied delete did not update sync-base");
    check(treeDeletes === 1, "applied delete did not update the Merkle tree");
    check(result.applied === 1, "applied delete was not counted");
    check(result.deferredCount === 0, "unchanged delete was falsely deferred");
}

void locallyEditedDeltaKeepsHonestBase()
    .then(renameAfterCrashIsIdempotent)
    .then(sameSizeUnverifiedRenameTargetIsDeferred)
    .then(firstSyncAdoptsAnExistingEmptyServerRoot)
    .then(editArrivingDuringDownloadDefersRemoteWrite)
    .then(untrackedLocalCollisionIsPreservedBeforePullWrite)
    .then(unchangedBaseIsReplacedWithoutConflictCopy)
    .then(offlineEditSurvivesRemoteDelete)
    .then(unchangedBaseAllowsRemoteDelete)
    .then(() => console.log("pull.test: 41 assertions passed"))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
