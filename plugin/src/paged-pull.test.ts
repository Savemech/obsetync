import { pull } from "./pull";
import { ObsetyncSyncBase } from "./sync-base";
import type { DiffPage, FileDelta } from "./api";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

class MemoryAdapter {
    files = new Map<string, string>();
    async read(path: string): Promise<string> {
        const value = this.files.get(path);
        if (value === undefined) throw new Error("missing");
        return value;
    }
    async write(path: string, value: string): Promise<void> { this.files.set(path, value); }
    async append(path: string, value: string): Promise<void> {
        this.files.set(path, (this.files.get(path) ?? "") + value);
    }
    async exists(path: string): Promise<boolean> { return this.files.has(path); }
    async mkdir(): Promise<void> {}
    async remove(path: string): Promise<void> { this.files.delete(path); }
    async rename(from: string, to: string): Promise<void> {
        const value = this.files.get(from);
        if (value === undefined) throw new Error("missing");
        this.files.set(to, value);
        this.files.delete(from);
    }
}

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
    const adapter = new MemoryAdapter();
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
    const walPath = ".obsidian/plugins/obsetync/sync-base.wal.ndjson";
    const wal = adapter.files.get(walPath) ?? "";
    check(wal.indexOf('"op":"set"') < wal.indexOf('"op":"diff-page"'),
        "page cursor entered the WAL before its entry mutations");

    // Model a storage tear immediately after the entry rows but inside the
    // cursor row. Recovery must keep useful applied state and forget progress,
    // which causes a safe idempotent page replay.
    const tornAdapter = new MemoryAdapter();
    tornAdapter.files = new Map(adapter.files);
    const cursorRow = wal.indexOf('{"op":"diff-page"');
    tornAdapter.files.set(walPath, `${wal.slice(0, cursorRow)}{"op":"diff-pa`);
    const tornBase = new ObsetyncSyncBase({ vault: { adapter: tornAdapter } } as any);
    await tornBase.load();
    check(tornBase.getHash("a.md") === hashA, "torn cursor tail discarded prior entry rows");
    check(tornBase.diffPageCheckpoint === null, "torn cursor tail advanced progress");

    // New JS objects model a cold renderer restart. Only the adapter/vault
    // survive, exactly as they do across iOS Jetsam termination.
    const recoveredBase = new ObsetyncSyncBase(app);
    await recoveredBase.load();
    check(recoveredBase.getHash("a.md") === hashA, "WAL-applied first page was lost on restart");
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
    check(result.treeParity === true, "resumed pages did not reconstruct target tree parity");
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
    const completedResult = await pull(
        completedApi,
        io as any,
        completedBase,
        "vault",
        sourceRoot,
        wasm,
        makeTree(targetRoot),
    );
    check(!unexpectedPageRequest, "completed checkpoint fetched a duplicate page");
    check(completedResult.treeParity === true, "completed checkpoint did not rebuild parity");
    check(downloads.join() === `${hashA},${hashB}`, "completed checkpoint redownloaded content");
}

async function invalidHistoricalRootFailsClosed(): Promise<void> {
    const adapter = new MemoryAdapter();
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

void rendererKillResumesAfterDurablePage()
    .then(invalidHistoricalRootFailsClosed)
    .then(() => console.log(`paged-pull.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
