import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { RootConflictPreserver, rootConflictCopyPath } from "./root-conflicts";
import { RootIntentStore, type StoredRootIntent } from "./root-intent";
import { RootSettlementCoordinator } from "./root-settlement";
import { ObsetyncSyncBase } from "./sync-base";
import { ObsetyncJournal } from "./journal";
import { createRootCommitIntent } from "./root-outcome";
import { BulkObjectKind } from "./bulk-codec";
import { MemorySegmentedIO, storeTestIOError } from "./segmented-store-test-io";
import { reserveTransientScope, type TransientWorkScope } from "./transient-memory";
import { ResourceBudget } from "./resource-budget";
import type { PlatformIO } from "./platform";
import { verifyConflictFile } from "./conflict-file-verification";

let assertions = 0;
const check = (v: unknown, message: string) => { assertions++; assert.ok(v, message); };
const same = (a: unknown, b: unknown, message: string) => { assertions++; assert.deepEqual(a, b, message); };
const h = (n: number) => n.toString(16).padStart(64, "0");
const hash = (v: Uint8Array) => createHash("sha256").update(v).digest("hex");
// Synthetic crypto for persistence/IO ordering. Real BLAKE3 is codec/WASM
// qualification, not the claim of these fixtures.
class Hasher {
    private hash = createHash("sha256");
    update(v: Uint8Array) { this.hash.update(v); }
    update_and_hash(v: Uint8Array) { this.update(v); return hash(v); }
    finalize() { return this.hash.digest("hex"); }
    free() {}
}
function gate() { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; }
class Files {
    readonly data = new Map<string, Uint8Array>();
    readonly directories = new Set<string>();
    readonly calls: Array<{ method: string; path: string; target?: string }> = [];
    copy?: (source: string, target: string) => Promise<void>;
    nativeAppend = true;
    io: PlatformIO = {
        readFile: async path => {
            this.calls.push({ method: "read", path });
            const data = this.data.get(path); if (!data) throw storeTestIOError("ENOENT");
            return Uint8Array.from(data);
        },
        writeFile: async (path, data) => {
            this.calls.push({ method: "write", path });
            check(path.startsWith(".obsidian/plugins/obsetync/conflict-staging-v1/"), "publisher overwrote a visible path");
            this.data.set(path, Uint8Array.from(data));
        },
        appendFile: async () => { throw new Error("naked append forbidden"); },
        appendFileOwned: async (path, data, memory) => {
            check(!!memory, "append lost the borrowed owner quota");
            this.calls.push({ method: "append", path });
            // Test-only full backing map simulates native disk append. The
            // production pipeline never allocates this concatenated array.
            await memory!.run(2 * data.length + 65536, async () => {
                const old = this.data.get(path)!; const next = new Uint8Array(old.length + data.length);
                next.set(old); next.set(data, old.length); this.data.set(path, next);
            });
        },
        copyFileExclusive: async (source, target) => {
            this.calls.push({ method: "copy", path: source, target });
            if (this.copy) return this.copy(source, target);
            if (this.data.has(target)) throw storeTestIOError("EEXIST");
            this.data.set(target, Uint8Array.from(this.data.get(source)!));
        },
        deleteFile: async path => {
            this.calls.push({ method: "delete", path });
            check(path.startsWith(".obsidian/plugins/obsetync/conflict-staging-v1/"), "publisher deleted visible content");
            this.data.delete(path);
        },
        replaceFile: async () => { throw new Error("replace forbidden"); },
        renameFile: async () => { throw new Error("rename forbidden"); },
        stat: async path => this.data.has(path) ? { size: this.data.get(path)!.length, mtime: 1 } : null,
        exists: async path => this.data.has(path) || this.directories.has(path),
        mkdir: async path => { this.directories.add(path); },
        listDirectory: async path => ({ files: [...this.data.keys()].filter(key => key.startsWith(`${path}/`)), folders: [] }),
        supportsNativeAppend: () => this.nativeAppend,
        statBulk: () => new Map(), listFiles: () => [], getAbsolutePath: () => null,
        listObsidianConfig: async () => new Map(),
    };
}
async function stores(disk: MemorySegmentedIO) {
    const app = { vault: { adapter: disk } } as any;
    const intents = new RootIntentStore(disk, h(1), hash), base = new ObsetyncSyncBase(app), journal = new ObsetyncJournal(app);
    await intents.load(); await base.load(); await journal.load(); return { intents, base, journal };
}
async function fixture(size = 10) {
    const disk = new MemorySegmentedIO(), files = new Files(), loaded = await stores(disk);
    const content = new Uint8Array(size); for (let i = 0; i < size; i++) content[i] = i % 251;
    const digest = hash(content);
    const throughId = await loaded.journal.append({ action: "modified", path: "note.md", ts: 1, synced: false });
    const request = await createRootCommitIntent("vault", "device", { protocol_version: 1, server_incarnation: h(2),
        sequence: 1, mutation_id: "a".repeat(32), parent_root: "", root: "AAAA" }, hash);
    const intent: StoredRootIntent = { vaultId: "vault", deviceId: "device", request,
        journalEpoch: loaded.journal.validatedEpoch!, journalCuts: [{ path: "note.md", throughId }],
        publication: { identity: { scopeHash: h(1), sequence: 1, mutationId: request.mutation_id, requestHash: request.request_hash },
            candidateRoot: h(3), committedAt: 10, entries: [{ action: "upsert", path: "note.md", hash: digest, size, mtime: 1 }] } };
    await loaded.intents.prepare(intent);
    await loaded.intents.recordTerminal({ protocol_version: 1, server_incarnation: h(9), sequence: 1,
        mutation_id: request.mutation_id, request_hash: request.request_hash, status: "accepted",
        result: { merged: true, root_hash: h(4), auto_resolved: 0, text_merged: 0,
            conflicts: [{ path: "note.md", base_hash: h(5), side_a_hash: h(6), side_b_hash: digest }] } });
    const objects = new Map<string, Uint8Array>();
    if (size < 1048576) objects.set(`${BulkObjectKind.Content}:${digest}`, content);
    else {
        const chunks: Array<{ hash: string; offset: number; size: number }> = [];
        for (let offset = 0; offset < size; offset += 700000) {
            const bytes = content.subarray(offset, offset + 700000), chunkHash = hash(bytes);
            chunks.push({ hash: chunkHash, offset, size: bytes.length });
            objects.set(`${BulkObjectKind.ContentChunk}:${chunkHash}`, bytes);
        }
        objects.set(`${BulkObjectKind.Manifest}:${digest}`, new TextEncoder().encode(JSON.stringify({ file_hash: digest, total_size: size, chunks })));
    }
    const budget = new ResourceBudget({ capacityBytes: 32 * 1024 * 1024 });
    let requests = 0, releases = 0;
    const api = { async getObjectsOwned(kind: BulkObjectKind, hashes: readonly string[]) {
        requests++;
        const values = new Map(hashes.map(key => {
            const bytes = objects.get(`${kind}:${key}`); if (!bytes) throw new Error("missing fixture object");
            return [key, bytes] as const;
        }));
        const bytes = [...values.values()].reduce((sum, value) => sum + value.buffer.byteLength, 0);
        const memory = await reserveTransientScope({ ownerBytes: bytes, workBytes: 16 * 1024 * 1024 }, { budget });
        let released = false;
        return { objects: values, memory, release: () => { if (!released) { released = true; releases++; values.clear(); memory.close(); } } };
    } };
    const preserver = new RootConflictPreserver(loaded.intents, api, files.io, { Hasher });
    const destination = await rootConflictCopyPath(intent.publication.identity, "note.md", digest);
    return { ...loaded, disk, files, intent, content, digest, destination, api, preserver,
        counters: () => ({ requests, releases, used: budget.snapshot().usedBytes }) };
}
async function rejects(work: Promise<unknown>, message: string) { let failed = false; try { await work; } catch { failed = true; } check(failed, message); }
async function freshAndRecorded() {
    for (const size of [0, 10, 2200000]) {
        const f = await fixture(size);
        f.files.data.set("note.md", new Uint8Array([99, 99])); // Already a newer, unrelated local generation.
        const late = await f.journal.append({ action: "modified", path: "note.md", ts: 2, synced: false });
        const coordinator = new RootSettlementCoordinator(f.intents, f.base, f.journal, f.preserver);
        same(await coordinator.settle(), { status: "accepted", candidateRoot: h(3), observedRoot: h(4) }, "conflicted acceptance did not settle");
        same(f.files.data.get(f.destination), f.content, "copy did not contain exact losing generation");
        check(!f.files.calls.some(call => call.method === "read" && call.path === "note.md"), "local fallback was used");
        check(f.files.calls.filter(call => call.method === "copy").length === 1, "fresh copy was published more than once");
        check(f.journal.unsynced()[0]?.id === late, "conflict settlement acknowledged a later local generation");
        check(f.counters().used === 0 && f.counters().requests === f.counters().releases, "download/sink owner leaked");
        if (size > 1048576) check(!f.files.calls.some(call => call.method === "read"), "large portable copy used a whole-file read");
    }
    const f = await fixture(); await f.preserver.preserve();
    const receipt = f.intents.pending()!.conflictCopies[0];
    check(!!receipt && f.intents.pending() !== null && f.journal.unsyncedCount() === 1, "copy itself retired or acknowledged the root");
    f.files.data.set(f.destination, new Uint8Array([100]));
    const restored = await stores(f.disk.clone()), before = f.files.calls.length;
    const again = new RootConflictPreserver(restored.intents, f.api, f.files.io, { Hasher }); await again.preserve();
    check(f.files.calls.length === before, "historically recorded copy was recreated after user edit");
    await new RootSettlementCoordinator(restored.intents, restored.base, restored.journal).settle();
    check(restored.intents.pending() === null, "already recorded conflicts required redundant preservation provider");
}
async function ambiguousCopiesAndRestart() {
    for (const size of [20, 2200000]) {
        const f = await fixture(size);
        f.files.copy = async (source, target) => {
            f.files.data.set(target, Uint8Array.from(f.files.data.get(source)!));
            throw storeTestIOError();
        };
        if (size < 1048576) {
            await f.preserver.preserve();
            check(f.intents.pending()!.conflictCopies.length === 1, "completed small copy error was not resolved by full hash");
        } else {
            await rejects(f.preserver.preserve(), "ambiguous large mobile copy used stat as proof");
            check(f.intents.pending()!.conflictCopies.length === 0, "ambiguous copy recorded a false completion");
            const restored = await stores(f.disk.clone()), before = f.files.calls.length;
            const again = new RootConflictPreserver(restored.intents, f.api, f.files.io, { Hasher });
            await rejects(again.preserve(), "large restart allocated a whole-file verifier");
            check(f.files.calls.length === before, "large ambiguous destination was read, copied or overwritten");
        }
    }
    const exact = await fixture(); exact.files.data.set(exact.destination, Uint8Array.from(exact.content));
    await exact.preserver.preserve();
    check(exact.counters().requests === 0 && !exact.files.calls.some(call => call.method === "copy"), "exact existing copy downloaded/published duplicate data");
    const edited = await fixture(); edited.files.data.set(edited.destination, new Uint8Array(edited.content.length).fill(33));
    const bytes = Uint8Array.from(edited.files.data.get(edited.destination)!);
    await rejects(edited.preserver.preserve(), "same-size edited destination was silently replaced");
    same(edited.files.data.get(edited.destination), bytes, "edited destination changed");
    check(edited.counters().requests === 0 && edited.intents.pending()!.conflictCopies.length === 0, "edited copy was accepted or redownloaded");
}
async function copyOwnerRacesAndFailure() {
    const f = await fixture(); const entered = gate(), release = gate();
    f.files.copy = async (source, target) => { entered.resolve(); await release.promise; f.files.data.set(target, Uint8Array.from(f.files.data.get(source)!)); };
    const controller = new AbortController(); const active = f.preserver.preserve(controller.signal); await entered.promise;
    await rejects(f.preserver.preserve(), "overlapping copy owner was admitted");
    controller.abort(); let drained = false;
    const closing = f.preserver.closeAndDrain().then(() => { drained = true; }); await Promise.resolve();
    check(!drained && f.intents.pending()!.conflictCopies.length === 0, "abort/close faked native copy completion");
    release.resolve(); await active; await closing;
    check(f.intents.pending()!.conflictCopies.length === 1, "accepted copy tail was discarded after close");
    const competing = await fixture();
    competing.files.copy = async (_source, target) => { competing.files.data.set(target, new Uint8Array([88])); throw storeTestIOError("EEXIST"); };
    await rejects(competing.preserver.preserve(), "competing create became false preservation");
    same(competing.files.data.get(competing.destination), new Uint8Array([88]), "competing destination was overwritten");
    check(competing.intents.pending()!.conflictCopies.length === 0 && competing.journal.unsyncedCount() === 1, "copy failure acknowledged user data");
    const partial = await fixture(2200000);
    partial.files.copy = async (_source, target) => { partial.files.data.set(target, new Uint8Array(partial.content.length)); throw storeTestIOError("ENOSPC"); };
    await rejects(partial.preserver.preserve(), "same-size partial large copy became acceptance");
    check(partial.intents.pending()!.conflictCopies.length === 0, "partial destination got a durable completion receipt");
}
async function pathsAndCapabilities() {
    const f = await fixture();
    const long = await rootConflictCopyPath(f.intent.publication.identity, `${"😀".repeat(100)}.md`, f.digest);
    check(new TextEncoder().encode(long).length <= 255 && long.endsWith(".md"), "new conflict basename exceeded portable bound");
    const changed = await rootConflictCopyPath({ ...f.intent.publication.identity, scopeHash: h(8) }, "note.md", f.digest);
    check(changed !== f.destination, "another owner reused deterministic destination");
    const large = await fixture(1100000); large.files.nativeAppend = false;
    await rejects(large.preserver.preserve(), "large conflict used prefix-copy append fallback");
    check(large.counters().requests === 0 && !large.files.calls.length, "unsupported large append admitted heavy work");
    const existing = await fixture(1100000); existing.files.data.set(existing.destination, existing.content);
    same(await verifyConflictFile(existing.files.io, { Hasher }, existing.destination, existing.digest, existing.content.length),
        "reader-unavailable", "large portable verifier bypassed whole read ceiling");
    check(!existing.files.calls.length, "large verifier allocated through DataAdapter readFile");
}
async function copyReceiptFailureAndFreshStaging() {
    for (const phase of ["before", "after"] as const) {
        const f = await fixture(); let injected = false;
        f.disk.onBoundary = event => {
            if (!injected && event.method === "write" && event.phase === phase && event.data?.includes("conflict-copy")) {
                injected = true; throw storeTestIOError("ENOSPC");
            }
        };
        await rejects(new RootSettlementCoordinator(f.intents, f.base, f.journal, f.preserver).settle(), "copy receipt write cut was hidden");
        check(injected && f.journal.unsyncedCount() === 1, "receipt failure lost the exact pending journal generation");
        same(f.files.data.get(f.destination), f.content, "receipt persistence failure changed the completed copy");
        const restored = await stores(f.disk.clone()), requests = f.counters().requests;
        const again = new RootConflictPreserver(restored.intents, f.api, f.files.io, { Hasher });
        await new RootSettlementCoordinator(restored.intents, restored.base, restored.journal, again).settle();
        check(restored.intents.pending() === null && restored.journal.unsyncedCount() === 0, "restart did not settle the actual preserved generation");
        check(f.counters().requests === requests && f.files.calls.filter(call => call.method === "copy").length === 1,
            "copy-before-receipt crash downloaded or copied a second visible destination");
    }
    const retry = await fixture();
    retry.files.copy = async () => { throw storeTestIOError("ENOSPC"); };
    await rejects(retry.preserver.preserve(), "failed staging fixture succeeded");
    const abandoned = retry.files.calls.find(call => call.method === "copy")!.path;
    const tampered = new Uint8Array(retry.content.length).fill(77); retry.files.data.set(abandoned, tampered);
    retry.files.copy = undefined;
    await retry.preserver.preserve();
    same(retry.files.data.get(retry.destination), retry.content, "retry trusted an old stat-only partial");
    same(retry.files.data.get(abandoned), tampered, "retry overwrote or deleted another attempt's partial");
    check(retry.files.calls.filter(call => call.method === "copy").every((call, i) => i === 0 || call.path !== abandoned), "retry reused an unverified staging prefix");
    const capped = await fixture(); capped.files.copy = async () => { throw storeTestIOError("ENOSPC"); };
    for (let attempt = 0; attempt < 4; attempt++) await rejects(capped.preserver.preserve(), "failed copy attempt succeeded");
    const requests = capped.counters().requests, calls = capped.files.calls.length;
    await rejects(capped.preserver.preserve(), "fifth retained staging attempt exceeded the per-conflict cap");
    check(capped.counters().requests === requests && capped.files.calls.length === calls, "staging cap was checked after heavy work or new writes");
    check(capped.files.data.size === 4 && capped.intents.pending()!.conflictCopies.length === 0,
        "staging exhaustion removed recovery data or falsely recorded success");
}
async function closeStopsUndispatchedWork() {
    const read = await fixture(), entered = gate(), release = gate();
    let nestedClose: Promise<void> | undefined;
    const preserver = new RootConflictPreserver(read.intents, read.api, read.files.io, { Hasher }, {
        verifyExisting: async (_path, _hash, _size, signal) => {
            signal?.addEventListener("abort", () => { nestedClose = preserver.closeAndDrain(); }, { once: true });
            entered.resolve(); await release.promise; return "missing";
        },
    });
    const active = preserver.preserve(), result = active.then(() => false, () => true);
    await entered.promise; let drained = false;
    const closing = preserver.closeAndDrain(); check(closing === preserver.closeAndDrain(), "close did not retain a stable actual drain");
    check(closing === nestedClose, "reentrant abort listener received a second drain promise");
    void closing.then(() => { drained = true; }); await Promise.resolve();
    check(!drained, "held verifier was abandoned during close");
    release.resolve(); check(await result, "closed verification admitted a new download"); await closing;
    check(read.counters().requests === 0 && read.files.calls.length === 0, "close during existing-copy verification admitted native staging/copy");

    const download = await fixture(), apiEntered = gate(), apiRelease = gate();
    const pendingApi = { async getObjectsOwned(...args: Parameters<typeof download.api.getObjectsOwned>) {
        apiEntered.resolve(); await apiRelease.promise; return download.api.getObjectsOwned(...args);
    } };
    const downloading = new RootConflictPreserver(download.intents, pendingApi, download.files.io, { Hasher });
    const owned = downloading.preserve().then(() => false, () => true);
    await apiEntered.promise; let apiDrained = false;
    const apiClosing = downloading.closeAndDrain().then(() => { apiDrained = true; });
    await Promise.resolve(); check(!apiDrained, "close discarded the real native download promise");
    apiRelease.resolve(); check(await owned, "late download started a copy after close"); await apiClosing;
    check(download.counters().requests === download.counters().releases && download.counters().used === 0,
        "late native download owner leaked on close");
    check(!download.files.calls.some(call => call.method === "copy" || call.method === "append"), "late download published/consumed after close");

    const reentrant = await fixture(), original = reentrant.intents.pending.bind(reentrant.intents);
    let fired = false, refused: Promise<boolean> | undefined, drain: Promise<void> | undefined;
    reentrant.intents.pending = () => {
        if (!fired) { fired = true;
            refused = reentrant.preserver.preserve().then(() => false, error => error.code === "BUSY");
            drain = reentrant.preserver.closeAndDrain();
        }
        return original();
    };
    await rejects(reentrant.preserver.preserve(), "reentrant close admitted heavy work");
    check(await refused, "storage accessor reentrancy admitted a second owner"); await drain;
    check(reentrant.counters().requests === 0 && reentrant.files.calls.length === 0, "reentrant close raced owner registration");
}
async function run() {
    await freshAndRecorded(); await ambiguousCopiesAndRestart(); await copyOwnerRacesAndFailure(); await pathsAndCapabilities();
    await copyReceiptFailureAndFreshStaging();
    await closeStopsUndispatchedWork();
    console.log(`root-conflicts: ${assertions} assertions passed`);
}
void run().catch(error => { console.error(error); process.exitCode = 1; });
