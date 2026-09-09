import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { RootIntentStore, ROOT_INTENT_PATH, ROOT_INTENT_LIMITS, type StoredRootIntent,
    type RootConflictCopyReceipt } from "./root-intent";
import { createRootCommitIntent, type RootTerminalOutcome } from "./root-outcome";
import { MemorySegmentedIO, readStoreTestFrame, sealStoreTestFrame, storeTestIOError,
    type StoreTestBoundary } from "./segmented-store-test-io";
import { STORE_LIMITS } from "./segmented-store";

let assertions = 0;
const check = (value: unknown, message: string) => { assertions++; assert.ok(value, message); };
const same = (a: unknown, b: unknown, message: string) => { assertions++; assert.deepEqual(a, b, message); };
const h = (n: number) => n.toString(16).padStart(64, "0");
const id = (n: number) => n.toString(16).padStart(32, "0");
const SCOPE = h(1), HEAD = `${ROOT_INTENT_PATH}/head.json`;
// Storage/replay tests inject a deterministic digest provider. Actual BLAKE3
// cross-language protocol parity is tested by root-outcome.test.ts separately.
const hashBytes = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const mutation = (e: StoreTestBoundary) => ["write", "rename", "remove", "mkdir"].includes(e.method);
function deferred() { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; }
async function input(sequence = 1, bytes = 30): Promise<StoredRootIntent> {
    const request = await createRootCommitIntent("vault", "device", {
        protocol_version: 1, server_incarnation: h(2), sequence, mutation_id: id(sequence),
        parent_root: h(3), root: Buffer.alloc(bytes, 7).toString("base64"),
    }, hashBytes);
    return { vaultId: "vault", deviceId: "device", request,
        publication: { identity: { scopeHash: SCOPE, sequence, mutationId: request.mutation_id,
            requestHash: request.request_hash }, candidateRoot: h(4), committedAt: 100,
            entries: [{ action: "upsert", path: "note.md", hash: h(5), mtime: 50, size: 5 }] },
        journalEpoch: id(8), journalCuts: [{ path: "note.md", throughId: 7 }] };
}
function terminal(intent: StoredRootIntent, cancelled = false): RootTerminalOutcome {
    const envelope = { protocol_version: 1 as const, server_incarnation: h(9), sequence: intent.request.sequence,
        mutation_id: intent.request.mutation_id, request_hash: intent.request.request_hash };
    return cancelled ? { ...envelope, status: "cancelled", result: { cancelled: true } } :
        { ...envelope, status: "accepted", result: { accepted: true, root_hash: h(99) } };
}
function mergedTerminal(intent: StoredRootIntent): RootTerminalOutcome {
    return { ...terminal(intent), status: "accepted", result: { merged: true, root_hash: h(99),
        conflicts: intent.publication.entries.map(entry => ({ path: entry.path, base_hash: h(6),
            side_a_hash: h(7), side_b_hash: entry.action === "upsert" ? entry.hash : h(5) })),
        auto_resolved: 0, text_merged: 0 } };
}
function copyReceipt(path = "note.md", size = 5): RootConflictCopyReceipt {
    return { path, copyPath: `copies/${path}`, hash: h(5), size };
}
async function fixture(io = new MemorySegmentedIO()) {
    const store = new RootIntentStore(io, SCOPE, hashBytes); await store.load(); return { io, store };
}
async function rejects(work: Promise<unknown> | (() => unknown), message: string) {
    let caught = false; try { if (typeof work === "function") work(); else await work; } catch { caught = true; }
    check(caught, message);
}
async function ownershipAndRestart() {
    const { io, store } = await fixture();
    const original = await input(1, 350 * 1024), caller = structuredClone(original);
    const submission = store.prepare(caller);
    caller.request.root = "AAAA"; caller.publication.entries[0].path = "later.md";
    (caller.journalCuts as any)[0].throughId = 800;
    same(await submission, "prepared", "intent was not published");
    same(store.pending()?.intent, original, "caller mutation escaped into durable intent");
    same(await store.prepare(original), "already-prepared", "exact intent retry was not idempotent");
    const copy = store.pending()!; copy.intent.request.parent_root = "";
    same(store.pending()?.intent.request.parent_root, original.request.parent_root, "lookup exposed mutable ownership");
    await rejects(store.prepare(await input(2)), "a second operation replaced pending work");
    await rejects(store.retire(original.publication.identity), "unresolved intent was retired");
    const changed = structuredClone(original);
    if (changed.publication.entries[0].action === "upsert") changed.publication.entries[0].size = 77;
    await rejects(store.prepare(changed), "same wire identity hid different local settlement payload");
    const badDigest = structuredClone(original); badDigest.request.root = "AAAA";
    await rejects(store.prepare(badDigest), "digest verification accepted different exact root bytes");
    check(store.snapshot().ready, "invalid caller input poisoned the current store");
    const receipt = terminal(original);
    same(await store.recordTerminal(receipt), "recorded", "accepted receipt was not saved");
    same(await store.recordTerminal({ ...receipt, server_incarnation: h(10) }), "already-recorded",
        "server restart changed the historical outcome");
    await rejects(store.recordTerminal(terminal(original, true)), "accepted outcome turned into cancellation");
    await store.compact();
    const restored = (await fixture(io)).store;
    same(restored.pending(), { intent: original, terminal: receipt, conflictCopies: [] }, "cold recovery lost exact intent or merged-independent root");
    check(restored.pending()!.intent.publication.candidateRoot !== receipt.result["root_hash" as keyof typeof receipt.result],
        "fixture failed to distinguish candidate and accepted root");
    await restored.retire(original.publication.identity); await restored.compact();
    const retired = (await fixture(io)).store;
    check(retired.pending() === null && retired.lastSequence === 1, "retirement lost the issued sequence");
    await rejects(retired.prepare(original), "retirement allowed sequence reuse");
    const next = await input(2); await retired.prepare(next); await retired.recordTerminal(terminal(next, true));
    await retired.retire(next.publication.identity);
    check(retired.lastSequence === 2, "cancellation did not consume sequence");
    for (const event of io.events) {
        if (event.method !== "write" || event.phase !== "after" || !/\/(wal|page)-/.test(event.path)) continue;
        const frame = JSON.parse(event.data!), page = JSON.parse(frame.payload);
        check(frame.bytes <= STORE_LIMITS.payloadBytes && page.rows.length <= STORE_LIMITS.rowsPerPage,
            "intent escaped portable frame bounds");
        check(page.rows.every((row: any) => row.op !== "root" || row.data.length <= ROOT_INTENT_LIMITS.rootPieceChars),
            "root was persisted in an unbounded row");
    }
}
async function faultsAtPublications() {
    for (const phase of ["prepare", "terminal", "conflict-copy", "copy-compact", "retire"] as const) {
        const { io: initialIO, store: initialStore } = await fixture();
        const intent = await input(1, 190 * 1024);
        if (phase !== "prepare") await initialStore.prepare(intent);
        if (phase === "retire") await initialStore.recordTerminal(terminal(intent));
        if (phase === "conflict-copy" || phase === "copy-compact") await initialStore.recordTerminal(mergedTerminal(intent));
        if (phase === "copy-compact") await initialStore.recordConflictCopy(intent.publication.identity, copyReceipt());
        const disk = initialIO.snapshot();
        const operation = (store: RootIntentStore) => phase === "prepare" ? store.prepare(intent) :
            phase === "terminal" ? store.recordTerminal(terminal(intent)) :
            phase === "conflict-copy" ? store.recordConflictCopy(intent.publication.identity, copyReceipt()) :
            phase === "copy-compact" ? store.compact() : store.retire(intent.publication.identity);
        const reference = await fixture(new MemorySegmentedIO(disk));
        const cut = reference.io.events.length;
        await operation(reference.store);
        const events = reference.io.events.slice(cut).filter(event => phase === "conflict-copy" || phase === "copy-compact" || mutation(event) ||
            (event.method === "read" && /head.json.next$/.test(event.path)));
        for (const target of events) {
            const { io, store } = await fixture(new MemorySegmentedIO(disk));
            let reached = false;
            io.onBoundary = event => {
                if (!reached && event.method === target.method && event.phase === target.phase &&
                    event.path === target.path && event.to === target.to) { reached = true; throw storeTestIOError(); }
            };
            let failed = false; try { await operation(store); } catch { failed = true; }
            check(reached, `fault boundary not reached: ${phase}/${target.method}/${target.phase}`);
            if (failed) check(!store.snapshot().ready, "ambiguous publication left the writer usable");
            io.onBoundary = undefined;
            const restored = (await fixture(io)).store;
            const pending = restored.pending();
            if (phase === "prepare") {
                check((pending === null && restored.lastSequence === 0) ||
                    (pending !== null && restored.lastSequence === 1 && pending.terminal === null), "partial prepared state was exposed");
                if (pending) same(pending.intent, intent, "prepare recovery lost bounded root pieces or journal cut");
            } else if (phase === "terminal") {
                same(pending?.intent, intent, "receipt fault lost the authoritative pending intent");
                check(pending?.terminal === null || JSON.stringify(pending?.terminal) === JSON.stringify(terminal(intent)),
                    "receipt fault invented a partial terminal result");
            } else if (phase === "conflict-copy" || phase === "copy-compact") {
                same(pending?.intent, intent, "conflict receipt fault lost its intent");
                same(pending?.terminal, mergedTerminal(intent), "conflict receipt fault lost its accepted terminal");
                const copies = pending!.conflictCopies;
                check((phase === "conflict-copy" && copies.length === 0) ||
                    JSON.stringify(copies) === JSON.stringify([copyReceipt()]), "partial conflict receipt was exposed or compacted away");
                if (!copies.length) await rejects(restored.retire(intent.publication.identity), "missing conflict receipt permitted retirement after recovery");
                else same(await restored.recordConflictCopy(intent.publication.identity, copyReceipt()), "already-recorded", "recovered completion was replayed");
            } else {
                check(restored.lastSequence === 1, "retirement fault reset stream sequence");
                check(pending === null || pending.terminal?.status === "accepted", "retirement exposed an unresolved intent");
            }
        }
    }
}

async function conflictCopyOwnershipAndRetirement() {
    const { io, store } = await fixture(), intent = await input();
    intent.publication.entries = [
        { action: "upsert", path: "a.md", hash: h(5), mtime: 50, size: 0 },
        ...intent.publication.entries,
    ];
    await store.prepare(intent);
    await rejects(store.recordConflictCopy(intent.publication.identity, copyReceipt()), "unresolved intent accepted a conflict copy");
    await store.recordTerminal(mergedTerminal(intent));
    await rejects(store.retire(intent.publication.identity), "accepted conflicts retired before copies");

    const identity = structuredClone(intent.publication.identity), receipt = copyReceipt();
    const recording = store.recordConflictCopy(identity, receipt);
    identity.scopeHash = h(88); receipt.copyPath = "changed.md"; receipt.hash = h(88); receipt.size = 88;
    same(await recording, "recorded", "conflict copy was not durably recorded");
    same(store.pending()!.conflictCopies, [copyReceipt()], "caller mutation escaped into conflict receipt");
    const detached = store.pending()!;
    (detached.conflictCopies as RootConflictCopyReceipt[])[0].copyPath = "aliased.md";
    (detached.conflictCopies as RootConflictCopyReceipt[]).push(copyReceipt("fake.md"));
    same(store.pending()!.conflictCopies, [copyReceipt()], "pending exposed mutable receipt ownership");
    const noIO = io.events.length;
    same(await store.recordConflictCopy(intent.publication.identity, copyReceipt()), "already-recorded", "exact duplicate was not idempotent");
    check(io.events.length === noIO, "exact conflict duplicate touched storage");
    await rejects(store.recordConflictCopy(intent.publication.identity, { ...copyReceipt(), copyPath: "another.md" }), "same conflict moved to a different receipt destination");
    await rejects(store.retire(intent.publication.identity), "one of two copies permitted premature retirement");
    same(await Promise.all([
        store.recordConflictCopy(intent.publication.identity, copyReceipt("a.md", 0)),
        store.recordConflictCopy(intent.publication.identity, copyReceipt("a.md", 0)),
    ]), ["recorded", "already-recorded"], "concurrent identical copies produced duplicate durable records");
    const expected = [copyReceipt("a.md", 0), copyReceipt()];
    same(store.pending()!.conflictCopies, expected, "out-of-order WAL arrivals were not canonically indexed");
    await store.compact();
    const pages = [...io.files.keys()].filter(path => /\/page-/.test(path));
    check(pages.some(path => readStoreTestFrame(io, path).rows.some((row: any) => row.op === "conflict-copy")),
        "compaction erased the old-reader conflict operation fence");
    const restored = (await fixture(io)).store;
    same(restored.pending()!.conflictCopies, expected, "restart/compaction lost conflict receipts");
    // The store owns historical completion, not copied-file bytes. An edit or
    // removal after completion must not turn reload into recreation or an ACK
    // of a newly inferred file generation.
    io.files.set(copyReceipt().copyPath, "user edited this after completion");
    io.files.delete(copyReceipt("a.md").copyPath);
    const changedFiles = new Map(io.files), start = io.events.length;
    const afterEdit = (await fixture(io)).store;
    same(afterEdit.pending()!.conflictCopies, expected, "later destination edit invalidated historical receipt");
    check(!io.events.slice(start).some(event => expected.some(copy => copy.copyPath === event.path)),
        "receipt recovery probed or rewrote user conflict copies");
    same(io.files.get(copyReceipt().copyPath), changedFiles.get(copyReceipt().copyPath), "receipt recovery changed edited conflict content");
    await afterEdit.retire(intent.publication.identity);
    check((await fixture(io)).store.pending() === null, "fully recorded conflict intent could not retire");
}

async function conflictCopyValidation() {
    const { io, store } = await fixture(), intent = await input();
    await store.prepare(intent); await store.recordTerminal(mergedTerminal(intent));
    const invalid: unknown[] = [
        { ...copyReceipt(), extra: true }, { ...copyReceipt(), path: "../note.md" },
        { ...copyReceipt(), path: "unrelated.md" }, { ...copyReceipt(), copyPath: "note.md" },
        { ...copyReceipt(), copyPath: "/outside.md" }, { ...copyReceipt(), copyPath: "a/../outside.md" },
        { ...copyReceipt(), copyPath: "x".repeat(4097) }, { ...copyReceipt(), hash: h(6) },
        { ...copyReceipt(), hash: "A".repeat(64) }, { ...copyReceipt(), size: 6 },
        { ...copyReceipt(), size: -1 }, { ...copyReceipt(), size: 1.5 },
        { ...copyReceipt(), size: Number.MAX_SAFE_INTEGER + 1 }, null,
    ];
    const before = io.events.length;
    for (const receipt of invalid) await rejects(store.recordConflictCopy(intent.publication.identity, receipt as RootConflictCopyReceipt), "invalid conflict receipt was retained");
    for (const identity of [
        { ...intent.publication.identity, scopeHash: h(99) },
        { ...intent.publication.identity, sequence: 2 },
        { ...intent.publication.identity, mutationId: id(99) },
        { ...intent.publication.identity, requestHash: h(99) },
    ]) await rejects(store.recordConflictCopy(identity, copyReceipt()), "foreign receipt owner was retained");
    check(io.events.length === before && store.snapshot().ready, "invalid conflict caller input wrote or poisoned storage");
    for (const mode of ["cancelled", "accepted", "delete", "wrong-publication-hash", "wrong-conflict-hash"] as const) {
        const owner = (await fixture()).store, candidate = await input();
        if (mode === "delete") candidate.publication.entries = [{ action: "delete", path: "note.md" }];
        if (mode === "wrong-publication-hash") candidate.publication.entries = [{ action: "upsert", path: "note.md", hash: h(9), size: 5, mtime: 50 }];
        await owner.prepare(candidate);
        const outcome = mode === "cancelled" ? terminal(candidate, true) : mode === "accepted" ? terminal(candidate) : mergedTerminal(candidate);
        if (outcome.status === "accepted" && "merged" in outcome.result) {
            outcome.result.conflicts[0].side_b_hash = mode === "wrong-conflict-hash" ? h(9) : h(5);
        }
        await owner.recordTerminal(outcome);
        await rejects(owner.recordConflictCopy(candidate.publication.identity, copyReceipt()), `unbound ${mode} copy was recorded`);
        if (mode === "cancelled" || mode === "accepted") await owner.retire(candidate.publication.identity);
    }
}

async function conflictCopyQueuedOwner() {
    const { io, store } = await fixture(), intent = await input(), next = await input(2);
    await store.prepare(intent); await store.recordTerminal(mergedTerminal(intent));
    const entered = deferred(), release = deferred(); let held = false;
    io.onBoundary = async event => {
        if (!held && event.method === "write" && event.phase === "after" && /\/wal-/.test(event.path)) {
            held = true; entered.resolve(); await release.promise;
        }
    };
    const active = store.recordConflictCopy(intent.publication.identity, copyReceipt()); await entered.promise;
    const retiring = store.retire(intent.publication.identity), preparing = store.prepare(next);
    // Captured while the old owner is still visible; checked again only after
    // the queued retirement and next intent have actually completed.
    const stale = store.recordConflictCopy(intent.publication.identity, copyReceipt()), observed = Promise.allSettled([stale]);
    release.resolve(); await active; await retiring; await preparing;
    check((await observed)[0].status === "rejected", "queued old conflict receipt crossed into the next intent");
    same(store.pending()?.intent, next, "stale receipt changed the newly prepared owner");
    same(store.pending()?.conflictCopies, [], "new intent inherited old conflict receipts");
    check(store.snapshot().ready && store.snapshot().queuedRequests === 0, "rejected queued owner poisoned or leaked the new stream");
}

async function conflictCopyCorruption() {
    const { io, store } = await fixture(), intent = await input();
    intent.publication.entries = [{ action: "upsert", path: "a.md", hash: h(5), mtime: 50, size: 5 }, ...intent.publication.entries];
    await store.prepare(intent); await store.recordTerminal(mergedTerminal(intent));
    await store.recordConflictCopy(intent.publication.identity, copyReceipt("a.md"));
    await store.recordConflictCopy(intent.publication.identity, copyReceipt());
    const original = io.snapshot();
    for (const edit of ["schema", "operation", "extra", "owner", "hash", "size", "unsafe", "original", "duplicate", "before-terminal"] as const) {
        const corrupt = new MemorySegmentedIO(original);
        const frames = [...corrupt.files.keys()].filter(path => /\/wal-/.test(path)).map(path => ({ path, page: readStoreTestFrame(corrupt, path) }));
        const target = frames.find(({ page }) => page.rows.some((row: any) => row.op === "conflict-copy" && row.receipt.path === "note.md"))!;
        const row = target.page.rows.find((row: any) => row.op === "conflict-copy");
        if (edit === "schema") row.schema = 99;
        else if (edit === "operation") row.op = "future-conflict-copy";
        else if (edit === "extra") row.receipt.extra = true;
        else if (edit === "owner") row.identity.scopeHash = h(999);
        else if (edit === "hash") row.receipt.hash = h(999);
        else if (edit === "size") row.receipt.size++;
        else if (edit === "unsafe") row.receipt.copyPath = "../outside.md";
        else if (edit === "original") row.receipt.copyPath = row.receipt.path;
        else if (edit === "duplicate") row.receipt = copyReceipt("a.md");
        else {
            const terminalFrame = frames.find(({ page }) => page.rows.some((value: any) => value.op === "terminal"))!;
            const previous = terminalFrame.page.rows[0]; terminalFrame.page.rows[0] = row; target.page.rows[0] = previous;
            corrupt.files.set(terminalFrame.path, sealStoreTestFrame(terminalFrame.page));
        }
        corrupt.files.set(target.path, sealStoreTestFrame(target.page));
        const before = corrupt.snapshot(); await rejects(fixture(corrupt), `corrupt conflict ${edit} was accepted`);
        same(corrupt.snapshot(), before, `corrupt conflict ${edit} changed recovery evidence`);
        check(!corrupt.events.some(mutation), `corrupt conflict ${edit} promoted before validation`);
    }
    // A forged retirement that skips an outstanding conflict is not a valid
    // lifecycle even when its outer WAL checksum/sequence are valid.
    const early = new MemorySegmentedIO(original);
    const last = [...early.files.keys()].filter(path => /\/wal-/.test(path)).find(path =>
        readStoreTestFrame(early, path).rows.some((row: any) => row.op === "conflict-copy" && row.receipt.path === "note.md"))!;
    const earlyPage = readStoreTestFrame(early, last);
    earlyPage.rows[0] = { schema: 1, op: "retire", identity: intent.publication.identity };
    early.files.set(last, sealStoreTestFrame(earlyPage));
    await rejects(fixture(early), "WAL retired a conflict lacking completion");
    check(!early.events.some(mutation), "premature retirement promoted before closure");

    await store.compact();
    const snapshotDisk = io.snapshot();
    for (const edit of ["reverse", "duplicate", "cancelled", "excess"] as const) {
        const corrupt = new MemorySegmentedIO(snapshotDisk);
        const path = [...corrupt.files.keys()].find(path => /\/page-/.test(path) && readStoreTestFrame(corrupt, path).rows.some((row: any) => row.op === "conflict-copy"))!;
        const page = readStoreTestFrame(corrupt, path), indices = page.rows.flatMap((row: any, index: number) => row.op === "conflict-copy" ? [index] : []);
        if (edit === "reverse") [page.rows[indices[0]], page.rows[indices[1]]] = [page.rows[indices[1]], page.rows[indices[0]]];
        else if (edit === "duplicate") page.rows[indices[1]] = page.rows[indices[0]];
        else if (edit === "cancelled") page.rows.find((row: any) => row.op === "terminal").terminal = terminal(intent, true);
        else page.rows.find((row: any) => row.op === "terminal").terminal.result.conflicts = Array.from({ length: 257 }, (_, index) =>
            ({ path: `c${index}`, base_hash: h(1), side_a_hash: h(2), side_b_hash: h(3) }));
        corrupt.files.set(path, sealStoreTestFrame(page));
        if (edit === "reverse") {
            // Recovery selected a complete-looking .next. Semantic closure
            // must reject it before the generic store can promote that head.
            corrupt.files.set(`${HEAD}.next`, corrupt.files.get(HEAD)!);
            corrupt.files.delete(HEAD); corrupt.files.delete(`${HEAD}.bak`);
        }
        const before = corrupt.snapshot(); await rejects(fixture(corrupt), `invalid conflict snapshot ${edit} was accepted`);
        same(corrupt.snapshot(), before, `invalid conflict snapshot ${edit} rewrote evidence`);
        check(!corrupt.events.some(mutation), `invalid conflict snapshot ${edit} promoted`);
    }
    const foreign = new RootIntentStore(io, h(999), hashBytes);
    await rejects(foreign.load(), "another scope adopted compacted conflict receipts");
}

async function conflictCopyLimitAndClose() {
    const { io, store } = await fixture(), intent = await input();
    // The independent 64 KiB terminal receipt cap also applies. Short unique
    // names let all 256 conflict records fit that pre-existing wire ceiling.
    const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    const paths = [...alphabet, ...Array.from({ length: ROOT_INTENT_LIMITS.conflictCopies - alphabet.length }, (_, index) =>
        alphabet[Math.floor(index / alphabet.length)] + alphabet[index % alphabet.length])].sort();
    intent.publication.entries = paths.map(path => ({ action: "upsert" as const, path, hash: h(5), mtime: 50, size: 5 }));
    intent.journalCuts = [];
    await store.prepare(intent); await store.recordTerminal(mergedTerminal(intent));
    for (const entry of intent.publication.entries.slice(0, -1)) await store.recordConflictCopy(intent.publication.identity, copyReceipt(entry.path));
    await rejects(store.retire(intent.publication.identity), "255 of 256 copies allowed retirement");
    const finalReceipt = copyReceipt(intent.publication.entries.at(-1)!.path);
    const entered = deferred(), release = deferred(); let held = false;
    io.onBoundary = async event => {
        if (!held && event.method === "write" && event.phase === "after" && /\/wal-/.test(event.path)) {
            held = true; entered.resolve(); await release.promise;
        }
    };
    const active = store.recordConflictCopy(intent.publication.identity, finalReceipt); await entered.promise;
    const queued = store.recordConflictCopy(intent.publication.identity, finalReceipt), observedQueued = Promise.allSettled([queued]);
    let drained = false; const closing = store.closeAndDrain().then(() => { drained = true; });
    await Promise.resolve(); check(!drained, "conflict receipt close escaped actual native publication");
    release.resolve(); await active; await closing;
    check((await observedQueued)[0].status === "rejected", "close dispatched a queued conflict receipt");
    check(store.snapshot().queuedRequests === 0 && store.snapshot().estimatedQueuedBytes === 0, "conflict receipt close leaked queue ownership");
    io.onBoundary = undefined;
    const restored = (await fixture(io)).store;
    check(restored.pending()!.conflictCopies.length === 256, "maximum receipt set lost rows on restart");
    await restored.compact();
    const compacted = (await fixture(io)).store;
    check(compacted.pending()!.conflictCopies.length === 256, "maximum receipt snapshot lost rows");
    await compacted.retire(intent.publication.identity);
}
async function corruptionAndScope() {
    const { io, store } = await fixture(); const intent = await input(); await store.prepare(intent);
    const original = io.snapshot();
    for (const edit of ["schema", "digest", "root-shadow", "cuts", "seal"] as const) {
        const corrupt = new MemorySegmentedIO(original);
        const wal = [...corrupt.files.keys()].find(path => /\/wal-/.test(path))!;
        const page = readStoreTestFrame(corrupt, wal);
        const start = page.rows.find((row: any) => row.op === "intent");
        if (edit === "schema") start.schema = 99;
        else if (edit === "digest") start.header.request.request_hash = h(999);
        else if (edit === "root-shadow") start.header.request.root = "AAAA";
        else if (edit === "cuts") page.rows.find((row: any) => row.op === "cut").cut.path = "unrelated.md";
        else page.rows.find((row: any) => row.op === "seal").identity.sequence++;
        corrupt.files.set(wal, sealStoreTestFrame(page));
        const before = corrupt.snapshot();
        await rejects(fixture(corrupt), `corrupt ${edit} was accepted`);
        same(corrupt.snapshot(), before, `corrupt ${edit} rewrote recovery evidence`);
        check(!corrupt.events.some(mutation), `corrupt ${edit} promoted before closure validation`);
    }
    const wrongScope = new RootIntentStore(io, h(999), hashBytes);
    const before = io.snapshot(); await rejects(wrongScope.load(), "different scope took ownership of pending work");
    same(io.snapshot(), before, "different scope rewrote existing intent");
    const lost = new MemorySegmentedIO(original);
    for (const suffix of ["", ".next", ".bak"]) lost.files.delete(HEAD + suffix);
    const lostBefore = lost.snapshot(); await rejects(fixture(lost), "advanced missing heads were recreated as empty");
    same(lost.snapshot(), lostBefore, "head loss erased migration evidence");
}
async function boundsAndClose() {
    const { io, store } = await fixture();
    const oversized = await input(); oversized.journalCuts = Array.from({ length: 257 }, () => ({ path: "note.md", throughId: 1 }));
    const before = io.events.length;
    await rejects(store.prepare(oversized), "oversized journal cut was retained");
    check(io.events.length === before, "oversized caller input wrote storage");
    const entered = deferred(), release = deferred(); let held = false;
    io.onBoundary = async event => {
        if (!held && event.method === "write" && event.phase === "after" && /\/wal-/.test(event.path)) {
            held = true; entered.resolve(); await release.promise;
        }
    };
    const intent = await input(); const active = store.prepare(intent); await entered.promise;
    const queued = [store.prepare(intent), store.prepare(intent), store.prepare(intent)];
    const settledQueued = Promise.allSettled(queued);
    await rejects(store.prepare(intent), "root-intent queue grew beyond request ceiling");
    check(store.snapshot().queuedRequests === 4, "active native write did not retain ownership");
    let drained = false; const closing = store.closeAndDrain().then(() => { drained = true; });
    await Promise.resolve(); check(!drained, "close completed before dispatched native IO");
    release.resolve(); await active; await closing;
    check((await settledQueued).every(result => result.status === "rejected"), "closed store dispatched queued publications");
    check(store.snapshot().queuedRequests === 0 && store.snapshot().estimatedQueuedBytes === 0, "drain leaked queue accounting");
    io.onBoundary = undefined;
    same((await fixture(io)).store.pending()?.intent, intent, "drain lost a publication accepted before close");
}

async function run() {
    await ownershipAndRestart(); await faultsAtPublications(); await corruptionAndScope(); await boundsAndClose();
    await conflictCopyOwnershipAndRetirement(); await conflictCopyValidation(); await conflictCopyQueuedOwner();
    await conflictCopyCorruption(); await conflictCopyLimitAndClose();
    console.log(`root-intent: ${assertions} assertions passed`);
}
void run().catch(error => { console.error(error); process.exitCode = 1; });
