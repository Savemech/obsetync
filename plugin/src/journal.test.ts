import { strict as assert } from "node:assert";
import { ObsetyncJournal, JOURNAL_PATHS, JOURNAL_STORE_PATH, JournalRecoveryError,
    type JournalEntry, type NewJournalEntry } from "./journal";
import { checksumJournalUtf8, encodeJournalCheckpoint, parseJournal } from "./journal-format";
import { STORE_LIMITS, segmentedMigrationPaths } from "./segmented-store";
import { MemorySegmentedIO, readStoreTestFrame, storeTestIOError, type StoreTestBoundary,
    type StoreTestDisk } from "./segmented-store-test-io";

let assertions = 0;
const check = (condition: unknown, message: string) => { assertions++; assert.ok(condition, message); };
const same = (actual: unknown, expected: unknown, message: string) => { assertions++; assert.deepEqual(actual, expected, message); };
const HEAD = `${JOURNAL_STORE_PATH}/head.json`;
const walFile = (path: string) => path.startsWith(`${JOURNAL_STORE_PATH}/wal-`);
const pageFile = (path: string) => path.startsWith(`${JOURNAL_STORE_PATH}/page-`);
const journalFor = (io: MemorySegmentedIO) => new ObsetyncJournal({ vault: { adapter: io } } as any);
const entry = (path: string, id: number): JournalEntry => ({ id, action: "modified", path, ts: id, synced: false });
const appendNote = (journal: ObsetyncJournal, path: string) =>
    journal.append({ action: "modified", path, ts: 1, synced: false });
const renameGroup: NewJournalEntry[] = [
    { action: "deleted", path: "old.md", ts: 2, synced: false },
    { action: "created", path: "new.md", ts: 2, synced: false },
];

function legacy(raw?: string): MemorySegmentedIO {
    const io = new MemorySegmentedIO();
    if (raw !== undefined) {
        io.directories.add(JOURNAL_PATHS.main.slice(0, JOURNAL_PATHS.main.lastIndexOf("/")));
        io.files.set(JOURNAL_PATHS.main, raw);
    }
    return io;
}
const originals = (io: MemorySegmentedIO) => Object.values(JOURNAL_PATHS).map(path => [path, io.files.get(path)]);
function unchangedOriginals(io: MemorySegmentedIO, before: ReturnType<typeof originals>): void {
    same(originals(io), before, "migration or later publication modified a legacy original");
    const paths = new Set<string>(Object.values(JOURNAL_PATHS));
    check(!io.events.some(event => ["write", "rename", "remove"].includes(event.method) &&
        (paths.has(event.path) || (event.to !== undefined && paths.has(event.to)))),
    "legacy originals were mutated and then restored instead of remaining untouched");
}
function unchangedDisk(io: MemorySegmentedIO, before: StoreTestDisk): void {
    same(io.snapshot(), before, "rejected recovery/poisoned operation mutated recovery evidence");
}
async function rejects(work: Promise<unknown>, code?: string): Promise<void> {
    let error: unknown;
    try { await work; } catch (failure) { error = failure; }
    check(error instanceof Error, "expected journal operation to reject");
    if (code) check(error instanceof JournalRecoveryError && error.code === code,
        `expected journal ${code}, got ${(error as any)?.code}`);
}
async function poisoned(journal: ObsetyncJournal, io: MemorySegmentedIO): Promise<void> {
    const before = io.snapshot();
    await rejects(appendNote(journal, "must-not-write.md"), "RECOVERY_REQUIRED");
    await rejects(journal.acknowledge([{ path: "seed.md", throughId: 1 }]), "RECOVERY_REQUIRED");
    await rejects(journal.compact(), "RECOVERY_REQUIRED");
    await rejects(journal.clear(), "RECOVERY_REQUIRED");
    unchangedDisk(io, before);
}
const writes = (events: readonly StoreTestBoundary[], predicate: (path: string) => boolean) =>
    events.filter(event => event.method === "write" && event.phase === "after" && predicate(event.path));
function boundedFrames(events: readonly StoreTestBoundary[]): void {
    for (const event of writes(events, path => walFile(path) || pageFile(path))) {
        const frame = JSON.parse(event.data!); const page = JSON.parse(frame.payload);
        check(checksumJournalUtf8(event.data!).bytes <= STORE_LIMITS.frameBytes, "individual stored frame exceeds its portable cap");
        check(frame.bytes <= STORE_LIMITS.payloadBytes && page.rows.length <= STORE_LIMITS.rowsPerPage,
            "individual transaction page exceeded its byte/row cap");
        same(checksumJournalUtf8(frame.payload), { bytes: frame.bytes, crc32: frame.crc32 }, "page checksum is invalid");
    }
}
async function restart(io: MemorySegmentedIO): Promise<ObsetyncJournal> {
    const journal = journalFor(io); await journal.load(); return journal;
}
function deferred(): { promise: Promise<void>; resolve(): void } {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
}

async function legacyTailCoalescesWithoutTouchingSource(): Promise<void> {
    check(checksumJournalUtf8("123456789").crc32 === "cbf43926", "CRC32 differs from its standard known vector");
    check(checksumJournalUtf8("éééééé😀").bytes === 16, "checksum counted UTF-16 rather than UTF-8 bytes");
    const raw = [
        JSON.stringify({ action: "modified", path: "same.md", ts: 1, synced: false }),
        JSON.stringify({ action: "modified", path: "same.md", ts: 2, synced: false }),
        '{"action":"modified"',
    ].join("\n");
    const io = legacy(raw); const before = originals(io); const journal = await restart(io);
    check(journal.unsyncedCount() === 1 && journal.unsynced()[0].id === 2,
        "legacy repeated path did not coalesce to its latest stable generation");
    check(parseJournal(raw).tornTail, "fixture does not contain an unterminated torn legacy tail");
    check(readStoreTestFrame(io, HEAD).snapshot.metadata.nextId === 3, "migration lost the watermark of coalesced rows");
    const events = io.events.length;
    check(await appendNote(journal, "other.md") === 3, "new append reused a coalesced legacy ID");
    check(writes(io.events.slice(events), walFile).length === 1 && writes(io.events.slice(events), pageFile).length === 0,
        "ordinary append rewrote the full snapshot instead of an immutable WAL segment");
    const headBeforeOldAck = io.files.get(HEAD);
    await journal.acknowledge([{ path: "same.md", throughId: 1 }]);
    check(journal.unsynced().some(row => row.path === "same.md" && row.id === 2), "old ACK erased a newer edit");
    check(io.files.get(HEAD) === headBeforeOldAck, "covered/no-op old ACK unnecessarily published a new head");
    await journal.acknowledge([{ path: "same.md", throughId: 2 }]);
    same((await restart(io)).unsynced().map(row => [row.path, row.id]), [["other.md", 3]], "durable ACK did not survive restart");
    unchangedOriginals(io, before); boundedFrames(io.events);
}

async function appendBurstPublishesOneCompleteBoundedCut(): Promise<void> {
    const io = legacy(); const journal = await restart(io); const beforeHead = readStoreTestFrame(io, HEAD);
    const start = io.events.length; const entered = deferred(); const release = deferred();
    let renamed = false; let gated = false; let completed = 0;
    io.onBoundary = async event => {
        if (event.method === "rename" && event.phase === "after" && event.to === HEAD) renamed = true;
        if (!gated && renamed && event.method === "read" && event.phase === "before" && event.path === HEAD) {
            gated = true; entered.resolve(); await release.promise;
        }
    };
    // Force byte-cap splitting inside one <=256-request batch, not extra heads.
    const paths = Array.from({ length: 200 }, (_, i) => `synthetic-${i}/${"é".repeat(700)}.md`);
    const done = Promise.all(paths.map(path => appendNote(journal, path).then(id => { completed++; return id; })));
    try {
        await entered.promise;
        check(completed === 0 && journal.unsyncedCount() === 0, "IDs/pending state escaped before final head readback");
        check(readStoreTestFrame(io, HEAD).sequence === 200, "head split or omitted the same-turn append burst");
    } finally { release.resolve(); }
    const ids = await done; io.onBoundary = undefined;
    same(ids, Array.from({ length: 200 }, (_, i) => i + 1), "batched IDs differ from invocation order");
    const events = io.events.slice(start); const segments = writes(events, walFile);
    check(segments.length > 1, "fixture did not exercise byte-bounded segment splitting");
    check(writes(events, path => path === `${HEAD}.next`).length === 1, "same-turn burst published more than one head");
    check(readStoreTestFrame(io, HEAD).generation === beforeHead.generation + 1, "burst skipped/split head generation");
    same(segments.flatMap(event => JSON.parse(JSON.parse(event.data!).payload).rows).map(row => [row.op, row.entry.id]),
        ids.map(id => ["append", id]), "WAL rows differ from the complete committed append cut");
    check(journal.unsyncedCount() === 200 && (await restart(io)).unsyncedCount() === 200, "burst cold recovery lost entries");
    boundedFrames(events);
}

async function explicitAppendBatchIsAtomicBoundedAndDetached(): Promise<void> {
    const io = legacy(); const journal = await restart(io);
    const inputs: NewJournalEntry[] = Array.from({ length: 256 }, (_, index) => ({
        action: "modified", path: `scan-${index}.md`, ts: index + 1, synced: false,
    }));
    const start = io.events.length;
    const accepted = journal.appendBatch(inputs);
    inputs[0].path = "caller-mutated.md";
    const ids = await accepted;
    same(ids, Array.from({ length: 256 }, (_, index) => index + 1),
        "explicit append batch did not receive one ordered ID cut");
    check(writes(io.events.slice(start), path => path === `${HEAD}.next`).length === 1,
        "explicit append batch published more than one authoritative head");
    check(journal.unsyncedCount() === 256 && journal.unsynced()[0].path === "scan-0.md" &&
        !journal.unsynced().some(row => row.path === "caller-mutated.md"),
    "explicit append batch retained caller mutation or lost rows");
    const restored = await restart(io);
    check(restored.unsyncedCount() === 256 && restored.unsynced().at(-1)?.id === 256,
        "cold recovery lost the explicit append batch");
    await rejects(journal.appendBatch(Array.from({ length: 257 }, (_, index) => ({
        action: "modified" as const, path: `overflow-${index}.md`, ts: index, synced: false,
    }))), "LIMIT");
    boundedFrames(io.events.slice(start));
}

async function scanSourceMetadataIsDurableStrictAndOptional(): Promise<void> {
    const io = legacy(); let journal = await restart(io);
    const hash = "ab".repeat(32);
    await journal.appendBatch([
        { action: "modified", path: "scanned.md", ts: 10, synced: false, hash, mtime: 20.5, size: 30 },
        { action: "created", path: "stat-only.md", ts: 11, synced: false, mtime: 21, size: 0 },
        { action: "deleted", path: "gone.md", ts: 12, synced: false },
    ]);
    journal = await restart(io);
    same(journal.unsynced().map(row => ({ path: row.path, hash: row.hash, mtime: row.mtime, size: row.size })), [
        { path: "scanned.md", hash, mtime: 20.5, size: 30 },
        { path: "stat-only.md", hash: undefined, mtime: 21, size: 0 },
        { path: "gone.md", hash: undefined, mtime: undefined, size: undefined },
    ], "cold recovery lost or invented optional scan source metadata");
    await journal.compact(); journal = await restart(io);
    const scanned = journal.unsynced().find(row => row.path === "scanned.md");
    check(scanned?.hash === hash && scanned.mtime === 20.5 && scanned.size === 30,
        "snapshot compaction discarded durable scan source metadata");

    const invalid: NewJournalEntry[] = [
        { action: "modified", path: "partial.md", ts: 1, synced: false, hash },
        { action: "modified", path: "uppercase.md", ts: 1, synced: false, hash: "AB".repeat(32), mtime: 1, size: 1 },
        { action: "modified", path: "fractional.md", ts: 1, synced: false, hash, mtime: 1, size: 1.5 },
        { action: "deleted", path: "deleted.md", ts: 1, synced: false, mtime: 1, size: 1 },
    ];
    for (const candidate of invalid) await rejects(journal.append(candidate), "CORRUPT");
    check(journal.unsyncedCount() === 3,
        "invalid source metadata entered the durable journal before validation");
}

async function sameAndDifferentBatchAcksRemainGenerationSafe(): Promise<void> {
    const io = legacy(); const journal = await restart(io); const first = await appendNote(journal, "same.md");
    const start = io.events.length;
    const [second, , other] = await Promise.all([appendNote(journal, "same.md"),
        journal.acknowledge([{ path: "same.md", throughId: first }]), appendNote(journal, "other.md")]);
    same([first, second, other], [1, 2, 3], "mixed batch reused/skipped an issued mutation ID");
    check(writes(io.events.slice(start), path => path === `${HEAD}.next`).length === 1, "mixed batch published per caller");
    same(journal.unsynced().map(row => [row.path, row.id]), [["same.md", 2], ["other.md", 3]], "same-batch old ACK consumed newer state");
    const [, newest] = await Promise.all([journal.acknowledge([{ path: "same.md", throughId: second }]), appendNote(journal, "same.md")]);
    check(newest === 4 && journal.unsynced().some(row => row.path === "same.md" && row.id === 4), "ACK erased later same-batch append");
    const watermarks = [{ path: "same.md", throughId: newest }]; const ack = journal.acknowledge(watermarks);
    watermarks[0].path = "other.md"; watermarks[0].throughId = 100; await ack;
    same((await restart(io)).unsynced().map(row => [row.path, row.id]), [["other.md", 3]], "caller mutation rewrote captured ACK");
    const candidate: NewJournalEntry = { action: "created", path: "captured.md", ts: 5, synced: false };
    const append = journal.append(candidate); candidate.path = "mutated.md";
    check(await append === 5 && journal.unsynced().some(row => row.path === "captured.md"), "caller mutation rewrote captured append");
    await journal.acknowledge([{ path: "other.md", throughId: 2 }, { path: "other.md", throughId: 3 }]);
    check(!journal.unsynced().some(row => row.path === "other.md"), "duplicate path ACKs did not use their maximum");
}

async function emptyCompactionAndClearRetainHighWatermarks(): Promise<void> {
    const io = legacy(); let journal = await restart(io);
    const ids = await Promise.all([appendNote(journal, "a.md"), appendNote(journal, "b.md")]);
    await journal.acknowledge(ids.map((throughId, i) => ({ path: i === 0 ? "a.md" : "b.md", throughId })));
    await journal.compact(); journal = await restart(io);
    check(journal.unsyncedCount() === 0, "empty compaction resurrected ACKed entries");
    check(await appendNote(journal, "new.md") === 3, "empty compaction reset IDs");
    await journal.appendGroup(renameGroup); const epoch = readStoreTestFrame(io, HEAD).epoch;
    await journal.clear(); journal = await restart(io);
    check(journal.unsyncedCount() === 0 && readStoreTestFrame(io, HEAD).epoch === epoch, "clear left a group/replaced its epoch");
    check(await appendNote(journal, "after-clear.md") === 5, "clear reused an earlier mutation ID");
    await journal.acknowledge([{ path: "absent-but-observed.md", throughId: 20 }]); await journal.clear(); journal = await restart(io);
    check(await appendNote(journal, "after-high-ack.md") === 21, "high ACK watermark did not survive clear/restart");
}

async function renameGroupsSurviveHalfAckAndCompaction(): Promise<void> {
    const io = legacy(); const journal = await restart(io); const start = io.events.length;
    const ids = await journal.appendGroup(renameGroup); const segments = writes(io.events.slice(start), walFile);
    check(ids[0] === ids[1] && segments.length === 1, "rename lost its shared mutation generation");
    const rows = JSON.parse(JSON.parse(segments[0].data!).payload).rows;
    check(rows.length === 1 && rows[0].op === "append" && rows[0].entry.action === "renamed", "rename split into independent physical rows");
    const newer = await appendNote(journal, "new.md");
    await journal.acknowledge([{ path: "old.md", throughId: ids[0] }]); await journal.compact();
    let restored = await restart(io);
    same(restored.unsynced().map(row => [row.path, row.id, row.action]),
        [["new.md", ids[0], "modified"], ["new.md", newer, "modified"]], "source-only ACK lost during snapshot/restart");
    await restored.acknowledge([{ path: "new.md", throughId: ids[1] }]); restored = await restart(io);
    same(restored.unsynced().map(row => row.id), [newer], "rename ACK erased newer destination generation");
    const reverse = await restored.appendGroup(renameGroup);
    await restored.acknowledge([{ path: "new.md", throughId: reverse[1] }]); await restored.compact(); restored = await restart(io);
    same(restored.unsynced().map(row => [row.path, row.id, row.action]), [["old.md", reverse[0], "deleted"]],
        "destination-only ACK erased its unacknowledged source");
}

async function strictLegacyRecoveryNeverInventsEmptyState(): Promise<void> {
    const first = JSON.stringify(entry("first.md", 1)); const last = JSON.stringify(entry("last.md", 2));
    for (const [raw, code] of [
        [`${first}\n{bad}\n${last}\n`, "CORRUPT"], [`${first}\n{bad}\n`, "CORRUPT"],
        [`${first}\n${JSON.stringify({ action: "modified", path: "bad.md", ts: null })}`, "CORRUPT"],
        [`${first}\n${JSON.stringify({ op: "future-ack", schema: 1 })}\n`, "UNKNOWN_SCHEMA"],
        ['{"op":"journal-meta","schema":2,"generation":1,"nextId":1}\n', "UNKNOWN_SCHEMA"],
        [JSON.stringify({ ...entry("new.md", 1), action: "renamed", oldPath: "new.md" }) + "\n", "CORRUPT"],
        [" ".repeat(128 * 1024 + 1), "LIMIT"],
    ]) {
        const io = legacy(raw); const before = io.snapshot(); const journal = journalFor(io);
        await rejects(journal.load(), code); check(journal.unsyncedCount() === 0, "invalid legacy recovery exposed provisional rows");
        await poisoned(journal, io); unchangedDisk(io, before);
    }
    for (const failure of [storeTestIOError("EIO"), storeTestIOError("ENOENT"), new Error("permission denied")]) {
        const io = legacy(first + "\n"); const before = io.snapshot(); const journal = journalFor(io);
        io.onBoundary = event => { if (event.method === "read" && event.phase === "before" && event.path === JOURNAL_PATHS.main) throw failure; };
        await rejects(journal.load(), "READ_FAILED"); io.onBoundary = undefined;
        await poisoned(journal, io); unchangedDisk(io, before);
    }
    const oversized = legacy(" ".repeat(8 * 1024 * 1024 + 1)); const before = oversized.snapshot();
    await rejects(journalFor(oversized).load(), "LIMIT");
    check(!oversized.events.some(event => event.method === "read" && event.path === JOURNAL_PATHS.main), "8 MiB stat cap was enforced after allocation");
    unchangedDisk(oversized, before);
    const empty = await restart(legacy()); check(await appendNote(empty, "first.md") === 1, "confirmed absence was not empty storage");
}

async function verifiedLegacyCopiesAreImportedWithoutPromotion(): Promise<void> {
    const old = encodeJournalCheckpoint([entry("old.md", 1)], 7, 1);
    const current = encodeJournalCheckpoint([entry("current.md", 3)], 7, 2);
    const next = encodeJournalCheckpoint([entry("newest.md", 5)], 7, 3);
    const io = legacy(current); io.files.set(JOURNAL_PATHS.backup, old); io.files.set(JOURNAL_PATHS.next, next);
    const before = originals(io); const journal = await restart(io);
    check(journal.unsynced()[0].path === "newest.md", "legacy import selected a filename instead of highest generation");
    check(await appendNote(journal, "next.md") === 7, "highest legacy checkpoint lost nextId"); await journal.compact(); unchangedOriginals(io, before);
    for (const [main, stage] of [
        [current, encodeJournalCheckpoint([entry("fork.md", 3)], 7, 2)],
        [current + JSON.stringify(entry("branch-a.md", 7)) + "\n", current + JSON.stringify(entry("branch-b.md", 7)) + "\n"],
    ]) {
        const fork = legacy(main); fork.files.set(JOURNAL_PATHS.next, stage); const disk = fork.snapshot();
        await rejects(journalFor(fork).load(), "CONTRADICTING_COPIES"); unchangedDisk(fork, disk);
    }
    const corrupt = legacy(current.replace("current.md", "corrupt.md")); corrupt.files.set(JOURNAL_PATHS.backup, old);
    const corruptBefore = corrupt.snapshot(); await rejects(journalFor(corrupt).load(), "CORRUPT"); unchangedDisk(corrupt, corruptBefore);
    const incomplete = legacy(current); incomplete.files.set(JOURNAL_PATHS.next, next.slice(0, 20)); const incompleteBefore = originals(incomplete);
    check((await restart(incomplete)).unsynced()[0].path === "current.md", "incomplete legacy stage replaced a valid main"); unchangedOriginals(incomplete, incompleteBefore);
    const orphan = legacy(); orphan.files.set(JOURNAL_PATHS.next, next.slice(0, 20)); const orphanBefore = orphan.snapshot();
    await rejects(journalFor(orphan).load(), "RECOVERY_REQUIRED"); unchangedDisk(orphan, orphanBefore);
    const impossibleTail = legacy(current); impossibleTail.files.set(JOURNAL_PATHS.next, next + '{"id":7'); const impossibleBefore = impossibleTail.snapshot();
    await rejects(journalFor(impossibleTail).load(), "CORRUPT"); unchangedDisk(impossibleTail, impossibleBefore);
    const badBackup = legacy(current); badBackup.files.set(JOURNAL_PATHS.backup, '{"schema":2,"op":"journal-meta"}\n'); const badBefore = badBackup.snapshot();
    await rejects(journalFor(badBackup).load(), "UNKNOWN_SCHEMA"); unchangedDisk(badBackup, badBefore);
    const prefix = legacy(current); prefix.files.set(JOURNAL_PATHS.backup, current + JSON.stringify(entry("tail.md", 7)) + "\n"); const prefixBefore = originals(prefix);
    check((await restart(prefix)).unsynced().some(row => row.id === 7), "longer same-generation legacy chain was ignored"); unchangedOriginals(prefix, prefixBefore);
}

async function legacyRenameHistoryCoalescesWithoutLosingGroups(): Promise<void> {
    const raw = [JSON.stringify({ id: 1, action: "renamed", path: "new.md", oldPath: "old.md", ts: 1 }),
        ...Array.from({ length: 9999 }, (_, i) => JSON.stringify(entry("new.md", i + 2)))].join("\n") + "\n";
    const io = legacy(raw); const before = originals(io); let journal = await restart(io);
    check(journal.unsyncedCount() === 2, "10k repeated legacy edits retained full history or erased their group");
    check(journal.unsynced()[0].action === "renamed" && journal.unsynced()[0].oldPath === "old.md" && journal.unsynced()[1].id === 10_000,
        "coalescing lost source link/latest destination generation");
    await journal.compact(); journal = await restart(io);
    check(journal.unsynced()[0].action === "renamed", "snapshot flattened an unacknowledged legacy group");
    check(await appendNote(journal, "after-import.md") === 10_001, "coalescing reset legacy watermark"); unchangedOriginals(io, before);
    const equal = legacy(encodeJournalCheckpoint([{ ...entry("old.md", 1), action: "deleted" }, entry("new.md", 1)], 2, 1));
    check((await restart(equal)).unsyncedCount() === 2, "flattened equal-ID distinct paths were rejected/collapsed");
    const half = legacy(JSON.stringify({ ...entry("new.md", 1), action: "renamed", oldPath: "old.md" }) +
        '\n{"op":"ack","path":"new.md","throughId":1}\n'); const halfBefore = originals(half);
    same((await restart(half)).unsynced().map(row => [row.action, row.path]), [["deleted", "old.md"]], "legacy destination ACK erased surviving source");
    unchangedOriginals(half, halfBefore);
}

async function walFailuresAndLostCompletionKeepAnExactCut(): Promise<void> {
    const seed = legacy(); const original = await restart(seed); await appendNote(original, "seed.md");
    for (const failure of ["before", "torn", "unheaded-complete", "verified-completion-lost"] as const) {
        const io = seed.clone(); const journal = await restart(io); const oldHead = io.files.get(HEAD); let hit = false;
        if (failure === "verified-completion-lost") {
            // Store has verified its complete cut; wrapper caller loses completion.
            const store = (journal as any).store; const commit = store.commit.bind(store);
            store.commit = async (rows: Iterable<unknown>) => { await commit(rows); hit = true; throw storeTestIOError(); };
        } else io.onBoundary = (event, target) => {
            if (!walFile(event.path) || event.method !== "write" || hit || event.phase !== (failure === "unheaded-complete" ? "after" : "before")) return;
            hit = true; if (failure === "torn") target.files.set(event.path, event.data!.slice(0, 25)); throw storeTestIOError("ENOSPC");
        };
        await rejects(journal.appendGroup(renameGroup)); io.onBoundary = undefined;
        check(hit, "failure fixture missed intended publication boundary");
        same(journal.unsynced().map(row => row.path), ["seed.md"], "failed caller observed provisional rename state");
        if (failure !== "verified-completion-lost") check(io.files.get(HEAD) === oldHead, "unpublished WAL advanced authoritative head");
        await poisoned(journal, io);
        const restored = await restart(io); const rows = restored.unsynced(); const committed = failure === "verified-completion-lost";
        check(rows.some(row => row.action === "renamed") === committed, "recovery guessed acceptance from WAL bytes rather than head cut");
        check(!rows.some(row => row.action === "deleted" && row.path === "old.md"), "failed rename persisted only deletion half");
        check(await appendNote(restored, "after-restart.md") === (committed ? 3 : 2), "recovery reused committed/skipped unaccepted ID");
    }
    const io = seed.clone(); const journal = await restart(io); const id = (await journal.appendGroup(renameGroup))[0];
    const newest = await appendNote(journal, "new.md"); const store = (journal as any).store; const commit = store.commit.bind(store);
    store.commit = async (rows: Iterable<unknown>) => { await commit(rows); throw storeTestIOError(); };
    await rejects(journal.acknowledge([{ path: "old.md", throughId: id }, { path: "new.md", throughId: id }])); await poisoned(journal, io);
    const restored = await restart(io);
    same(restored.unsynced().map(row => [row.path, row.id]), [["seed.md", 1], ["new.md", newest]], "lost ACK restored group/erased newer destination");
    check(await appendNote(restored, "after-lost-ack.md") === newest + 1, "lost ACK reset mutation watermark");
}

async function failedLoadsDoNotExposeProvisionalState(): Promise<void> {
    const seed = legacy(); const writer = await restart(seed);
    await Promise.all(Array.from({ length: 600 }, (_, i) => appendNote(writer, `page-note-${i}.md`))); await writer.compact();
    const head = readStoreTestFrame(seed, HEAD); check(head.snapshot.pages >= 3, "load fixture did not span multiple pages");
    const io = seed.clone(); const journal = journalFor(io);
    const lastPage = `${JOURNAL_STORE_PATH}/page-${head.epoch}-${head.snapshot.id}-${head.snapshot.pages - 1}.json`;
    io.files.set(lastPage, io.files.get(lastPage)!.slice(0, -1)); const before = io.snapshot(); let observed = false;
    io.onBoundary = event => {
        if (event.method === "read" && event.phase === "before" && event.path === lastPage) {
            observed = true; check(journal.unsyncedCount() === 0 && [...journal.iterateUnsynced()].length === 0,
                "earlier snapshot pages escaped before complete load validation");
        }
    };
    await rejects(journal.load(), "CORRUPT"); io.onBoundary = undefined;
    check(observed && journal.unsyncedCount() === 0, "failed multi-page recovery exposed provisional state");
    await poisoned(journal, io); unchangedDisk(io, before);
    const stageIO = legacy(); const stageWriter = await restart(stageIO); await appendNote(stageWriter, "seed.md");
    let staged: StoreTestDisk | undefined;
    stageIO.onBoundary = (event, target) => {
        if (event.method === "write" && event.phase === "after" && event.path === `${HEAD}.next`) { staged = target.snapshot(); throw storeTestIOError(); }
    };
    await rejects(appendNote(stageWriter, "newest.md")); check(staged !== undefined, "fixture missed complete staged head");
    const recoveryIO = new MemorySegmentedIO(staged); const recovering = journalFor(recoveryIO); let promoted = false;
    recoveryIO.onBoundary = event => {
        if (event.method === "rename" && event.phase === "before" && event.path === `${HEAD}.next`) {
            promoted = true; check(recovering.unsyncedCount() === 0, "rows escaped before required stage promotion"); throw storeTestIOError();
        }
    };
    await rejects(recovering.load()); recoveryIO.onBoundary = undefined;
    check(promoted && recovering.unsyncedCount() === 0, "failed selected-stage promotion exposed state");
    check((await restart(recoveryIO)).unsynced().some(row => row.path === "newest.md" && row.id === 2), "promotion failure destroyed newest cut");
}

async function initialImportRetriesRequireExactSourceProof(): Promise<void> {
    const raw = Array.from({ length: 600 }, (_, i) => JSON.stringify(entry(`legacy-${i}.md`, i + 1))).join("\n") + "\n";
    const io = legacy(raw); const before = originals(io); let hit = false;
    io.onBoundary = (event, target) => {
        if (!hit && event.method === "write" && event.phase === "before" && pageFile(event.path) && event.path.endsWith("-1.json")) {
            hit = true; target.files.set(event.path, event.data!.slice(0, 23)); throw storeTestIOError("ENOSPC");
        }
    };
    const interrupted = journalFor(io); await rejects(interrupted.load()); io.onBoundary = undefined;
    check(hit && interrupted.unsyncedCount() === 0 && !io.files.has(HEAD), "interrupted import exposed partial index/head");
    const partial = io.snapshot(); unchangedOriginals(io, before);
    const restored = await restart(io);
    check(restored.unsyncedCount() === 600 && await appendNote(restored, "after-import.md") === 601, "exact-source retry lost rows/watermark");
    unchangedOriginals(io, before);
    const changed = new MemorySegmentedIO(partial); changed.files.set(JOURNAL_PATHS.main, raw + JSON.stringify(entry("changed-source.md", 601)) + "\n");
    const changedBefore = changed.snapshot(); await rejects(journalFor(changed).load(), "CONTRADICTING_COPIES"); unchangedDisk(changed, changedBefore);
    const partialIntent = legacy(raw); const intent = segmentedMigrationPaths(JOURNAL_STORE_PATH).intent;
    partialIntent.onBoundary = (event, target) => {
        if (event.method === "write" && event.phase === "before" && event.path === intent) { target.files.set(event.path, event.data!.slice(0, 17)); throw storeTestIOError("ENOSPC"); }
    };
    await rejects(journalFor(partialIntent).load()); partialIntent.onBoundary = undefined;
    const intentBefore = partialIntent.snapshot(); await rejects(journalFor(partialIntent).load(), "CORRUPT"); unchangedDisk(partialIntent, intentBefore);
    check(!partialIntent.files.has(HEAD), "partial intent became permission for an empty epoch");
    const unheaded = legacy(raw); unheaded.directories.add(JOURNAL_STORE_PATH); unheaded.files.set(`${JOURNAL_STORE_PATH}/orphan.json`, "unknown prior state");
    const unheadedBefore = unheaded.snapshot(); await rejects(journalFor(unheaded).load(), "RECOVERY_REQUIRED"); unchangedDisk(unheaded, unheadedBefore);
}

async function run(): Promise<void> {
    const cases = [legacyTailCoalescesWithoutTouchingSource, appendBurstPublishesOneCompleteBoundedCut,
        explicitAppendBatchIsAtomicBoundedAndDetached, scanSourceMetadataIsDurableStrictAndOptional,
        sameAndDifferentBatchAcksRemainGenerationSafe, emptyCompactionAndClearRetainHighWatermarks,
        renameGroupsSurviveHalfAckAndCompaction, strictLegacyRecoveryNeverInventsEmptyState,
        verifiedLegacyCopiesAreImportedWithoutPromotion, legacyRenameHistoryCoalescesWithoutLosingGroups,
        walFailuresAndLostCompletionKeepAnExactCut, failedLoadsDoNotExposeProvisionalState,
        initialImportRetriesRequireExactSourceProof];
    for (const test of cases) {
        let timeout: ReturnType<typeof setTimeout> | undefined;
        try {
            await Promise.race([test(), new Promise<never>((_, reject) => {
                timeout = setTimeout(() => reject(new Error(`journal fixture stalled: ${test.name}`)), 30_000);
            })]);
        } finally { if (timeout) clearTimeout(timeout); }
    }
    console.log(`journal.test: ${assertions} assertions passed (segmented batches, strict legacy import, half ACKs, poisoned cuts and source-proof retry)`);
}
void run().catch(error => { console.error(error); process.exitCode = 1; });
