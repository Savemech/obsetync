import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { ObsetyncJournal } from "./journal";
import { ObsetyncSyncBase } from "./sync-base";
import { activateLegacyDowngrade, LEGACY_DOWNGRADE_PATHS, LegacyDowngradeError,
    authorizeLegacyDowngradeCurrentImport, completeLegacyDowngradeCurrentImport,
    inspectLegacyDowngradeState, resumeLegacyDowngrade } from "./legacy-downgrade";
import { MemorySegmentedIO, storeTestIOError } from "./segmented-store-test-io";
import { RootIntentStore, ROOT_INTENT_PATH, type StoredRootIntent } from "./root-intent";
import { createRootCommitIntent } from "./root-outcome";

let assertions = 0;
const check = (condition: unknown, message: string) => { assertions++; assert.ok(condition, message); };
const same = (actual: unknown, expected: unknown, message: string) => { assertions++; assert.deepEqual(actual, expected, message); };
const ROOT = ".obsidian/plugins/obsetync";
const JOURNAL = `${ROOT}/change-journal.ndjson`;
const BASE = `${ROOT}/sync-base.json`;
const RELEASED_1113_REV = "98b6aedf451528ab3bbe56c24b581bed8df483f1";
const SCOPE = "c".repeat(64);
const hashBytes = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");

/** Directory rename is the only extra operation used by the production
 * downgrade path. Obsidian adapters provide it; the generic segmented-store
 * fixture intentionally models files only. */
class DirectoryIO extends MemorySegmentedIO {
    afterDirectoryRename?: () => void;
    override async rename(from: string, to: string): Promise<void> {
        if (!this.directories.has(from)) return super.rename(from, to);
        if (this.files.has(to) || this.directories.has(to)) throw storeTestIOError("EEXIST");
        const files = [...this.files].filter(([path]) => path.startsWith(`${from}/`));
        const directories = [...this.directories].filter(path => path === from || path.startsWith(`${from}/`));
        for (const [path] of files) this.files.delete(path);
        for (const path of directories) this.directories.delete(path);
        for (const [path, value] of files) this.files.set(`${to}${path.slice(from.length)}`, value);
        for (const path of directories) this.directories.add(`${to}${path.slice(from.length)}`);
        this.afterDirectoryRename?.();
    }
}

interface ReleasedEntry { id: number; action: string; path: string; oldPath?: string; ts: number; synced: false }

/** Parser behavior copied from journal.ts at RELEASED_1113_REV. This is
 * deliberately not the current parser: the locked fixture proves old-bundle input. */
function released1113Journal(raw: string): { entries: ReleasedEntry[]; nextId: number } {
    const loaded: ReleasedEntry[] = [];
    const acknowledgedThrough = new Map<string, number>();
    let fallbackId = 1;
    for (const line of raw.split("\n")) {
        if (!line.trim()) continue;
        try {
            const parsed = JSON.parse(line);
            if (parsed.op === "ack") {
                if (typeof parsed.path === "string" && Number.isSafeInteger(parsed.throughId) && parsed.throughId > 0) {
                    acknowledgedThrough.set(parsed.path, Math.max(acknowledgedThrough.get(parsed.path) ?? 0, parsed.throughId));
                    fallbackId = Math.max(fallbackId, parsed.throughId + 1);
                }
                continue;
            }
            if (typeof parsed.path !== "string" || !["created", "modified", "deleted", "renamed"].includes(parsed.action) ||
                typeof parsed.ts !== "number") continue;
            const id = Number.isSafeInteger(parsed.id) && parsed.id > 0 ? parsed.id : fallbackId;
            fallbackId = Math.max(fallbackId + 1, id + 1);
            loaded.push({ id, action: parsed.action, path: parsed.path,
                ...(typeof parsed.oldPath === "string" ? { oldPath: parsed.oldPath } : {}), ts: parsed.ts, synced: false });
        } catch { /* released reader preserves every independently parseable row */ }
    }
    return { entries: loaded.filter(entry => entry.id > (acknowledgedThrough.get(entry.path) ?? 0)),
        nextId: Math.max(fallbackId, ...loaded.map(entry => entry.id + 1)) };
}

/** `readFirstValidSnapshot` copied from sync-base.ts at RELEASED_1113_REV. */
function released1113Base(raw: string): any {
    try {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed.lastSyncTimestamp === "number" && parsed.entries) return parsed;
    } catch { /* released reader tries the next snapshot copy */ }
    return null;
}

const lease = { assertHeld() { /* deterministic exclusive-owner fixture */ } };
const rootIntents = (lastSequence = 7, pending: unknown = null) => ({ lastSequence, pending: () => pending as any });
const appFor = (io: DirectoryIO) => ({ vault: { adapter: io } } as any);

async function settledRoot(io: DirectoryIO): Promise<RootIntentStore> {
    const store = new RootIntentStore(io, SCOPE, hashBytes); await store.load();
    const request = await createRootCommitIntent("vault", "device", {
        protocol_version: 1, server_incarnation: "1".repeat(64), sequence: 1,
        mutation_id: "2".repeat(32), parent_root: "3".repeat(64), root: Buffer.from("root").toString("base64"),
    }, hashBytes);
    const intent: StoredRootIntent = { vaultId: "vault", deviceId: "device", request,
        publication: { identity: { scopeHash: SCOPE, sequence: 1, mutationId: request.mutation_id,
            requestHash: request.request_hash }, candidateRoot: "4".repeat(64), committedAt: 1,
            entries: [{ action: "upsert", path: "offline.md", hash: "a".repeat(64), mtime: 10, size: 4 }] },
        journalEpoch: "5".repeat(32), journalCuts: [{ path: "offline.md", throughId: 1 }] };
    await store.prepare(intent);
    await store.recordTerminal({ protocol_version: 1, server_incarnation: "6".repeat(64), sequence: 1,
        mutation_id: request.mutation_id, request_hash: request.request_hash,
        status: "cancelled", result: { cancelled: true } });
    await store.retire(intent.publication.identity);
    return store;
}

function removePrefix(io: DirectoryIO, prefix: string): void {
    for (const path of [...io.files.keys()]) if (path === prefix || path.startsWith(`${prefix}/`)) io.files.delete(path);
    for (const path of [...io.directories]) if (path === prefix || path.startsWith(`${prefix}/`)) io.directories.delete(path);
}

async function rootLoadFails(io: DirectoryIO): Promise<boolean> {
    try { await new RootIntentStore(io, SCOPE, hashBytes).load(); return false; } catch { return true; }
}

async function populated(io: DirectoryIO) {
    const app = appFor(io);
    const journal = new ObsetyncJournal(app);
    const base = new ObsetyncSyncBase(app);
    await journal.load(); await base.load();
    await journal.append({ action: "modified", path: "offline.md", ts: 10, synced: false });
    await journal.appendGroup([
        { action: "deleted", path: "old.md", ts: 11, synced: false },
        { action: "created", path: "new.md", ts: 11, synced: false },
    ]);
    const half = await journal.appendGroup([
        { action: "deleted", path: "half-old.md", ts: 12, synced: false },
        { action: "created", path: "half-new.md", ts: 12, synced: false },
    ]);
    await journal.acknowledge([{ path: "half-new.md", throughId: half[0] }]);
    base.setEntry("offline.md", "a".repeat(64), 10, 4, 9);
    base.setLastSyncTimestamp(123);
    base.setTreeBaseRoot("b".repeat(64));
    await base.save();
    return { app, journal, base };
}

async function rejects(work: Promise<unknown>, code: LegacyDowngradeError["code"]): Promise<void> {
    let error: unknown;
    try { await work; } catch (failure) { error = failure; }
    check(error instanceof LegacyDowngradeError && error.code === code,
        `expected downgrade ${code}, got ${(error as any)?.code}`);
}

async function run(): Promise<void> {
    {
        check(RELEASED_1113_REV.length === 40, "released-reader fixture revision is not locked");
        const io = new DirectoryIO();
        const { journal, base } = await populated(io);
        const root = await settledRoot(io);
        const rootBefore = [...io.files].filter(([path]) => path.startsWith(`${ROOT_INTENT_PATH}/`));
        const downgradeEventCut = io.events.length;
        await activateLegacyDowngrade({ io, journal, syncBase: base, rootIntents: root, treeVersion: 1, lease });
        const oldJournal = released1113Journal(io.files.get(JOURNAL)!);
        same(oldJournal.entries.map(entry => [entry.id, entry.action, entry.path, entry.oldPath]), [
            [1, "modified", "offline.md", undefined],
            [2, "renamed", "new.md", "old.md"],
            [3, "deleted", "half-old.md", undefined],
        ], "1.11.3 projection collapsed a generation or pending rename endpoint");
        const oldBase = released1113Base(io.files.get(BASE)!);
        check(oldBase !== null, "released 1.11.3 base reader rejected projection");
        const allowed = new Set(["lastSyncTimestamp", "entries", "treeBaseRoot", "diffPageCheckpoint"]);
        check(Object.keys(oldBase).every(key => allowed.has(key)), "projection exposed a schema-2 authority field to 1.11.3");
        check(oldBase.entries["offline.md"].hash === "a".repeat(64), "1.11.3 base fixture lost an entry");
        check(oldBase.treeBaseRoot === "b".repeat(64), "1.11.3 base fixture lost verified ancestry");
        same([...io.files].filter(([path]) => path.startsWith(`${ROOT_INTENT_PATH}/`)), rootBefore,
            "root lastSequence store was archived or rewritten");
        check(JSON.parse(io.files.get(LEGACY_DOWNGRADE_PATHS.prepared)!).rootLastSequence === 1,
            "downgrade marker did not bind the preserved root sequence cut");
        check(io.files.size > 2 && [...io.files.keys()].some(path => path.includes("legacy-downgrade-archives")),
            "segmented authority was not retained in an archive");
        check(!io.events.slice(downgradeEventCut).some(event => event.method === "remove" &&
            event.path.includes("legacy-downgrade-archives")), "downgrade deleted an archive");
        const reopenedRoot = new RootIntentStore(io, SCOPE, hashBytes); await reopenedRoot.load();
        check(reopenedRoot.lastSequence === 1, "active downgrade did not validate the retained root sequence");

        const missingRoot = new DirectoryIO(io.snapshot()); removePrefix(missingRoot, ROOT_INTENT_PATH);
        check(await rootLoadFails(missingRoot), "missing root sequence store reset active downgrade authority");
        const corruptRoot = new DirectoryIO(io.snapshot());
        corruptRoot.files.set(`${ROOT_INTENT_PATH}/head.json`, "{corrupt");
        check(await rootLoadFails(corruptRoot), "corrupt root sequence store passed active downgrade startup");
        const emptyRootDisk = new DirectoryIO(); await new RootIntentStore(emptyRootDisk, SCOPE, hashBytes).load();
        const lowerRoot = new DirectoryIO(io.snapshot()); removePrefix(lowerRoot, ROOT_INTENT_PATH);
        for (const directory of emptyRootDisk.directories) if (directory === ROOT_INTENT_PATH || directory.startsWith(`${ROOT_INTENT_PATH}/`)) {
            lowerRoot.directories.add(directory);
        }
        for (const [path, value] of emptyRootDisk.files) if (path.startsWith(`${ROOT_INTENT_PATH}/`)) lowerRoot.files.set(path, value);
        check(await rootLoadFails(lowerRoot), "lower root sequence passed active downgrade startup floor");
        for (const missingPath of [JOURNAL, BASE]) {
            const missingProjection = new DirectoryIO(io.snapshot()); missingProjection.files.delete(missingPath);
            await rejects(authorizeLegacyDowngradeCurrentImport(missingProjection, 1, lease), "RECOVERY_REQUIRED");
        }
        const missingArchive = new DirectoryIO(io.snapshot());
        const archiveTarget = [...missingArchive.directories, ...missingArchive.files.keys()]
            .find(path => /\/authority\/\d{2}$/.test(path));
        check(archiveTarget !== undefined, "archive inventory fixture has no target");
        removePrefix(missingArchive, archiveTarget!);
        await rejects(authorizeLegacyDowngradeCurrentImport(missingArchive, 1, lease), "RECOVERY_REQUIRED");
        const crashBase = new DirectoryIO(io.snapshot());
        crashBase.files.set(`${BASE}.next`, JSON.stringify({ lastSyncTimestamp: 500,
            entries: { "next.md": { hash: "d".repeat(64), mtime: 5, size: 5 } } }));
        crashBase.files.set(`${BASE}.bak`, JSON.stringify({ lastSyncTimestamp: 300,
            entries: { "backup.md": { hash: "e".repeat(64), mtime: 3, size: 3 } } }));
        crashBase.files.set(`${ROOT}/sync-base.wal.ndjson`, `${JSON.stringify({ op: "set", path: "wal.md",
            entry: { hash: "f".repeat(64), mtime: 6, size: 6 } })}\n{torn\n`);
        await authorizeLegacyDowngradeCurrentImport(crashBase, 1, lease);
        const recoveredCrashBase = new ObsetyncSyncBase(appFor(crashBase)); await recoveredCrashBase.load();
        check(recoveredCrashBase.getHash("next.md") === "d".repeat(64) &&
            recoveredCrashBase.getHash("backup.md") === null,
        "active re-upgrade did not use released .next/main/.bak priority");
        check(recoveredCrashBase.getHash("wal.md") === "f".repeat(64),
            "active re-upgrade did not replay valid WAL rows around a torn row");
        const missingMainCrash = new DirectoryIO(io.snapshot());
        missingMainCrash.files.set(`${BASE}.next`, JSON.stringify({ lastSyncTimestamp: 600,
            entries: { "next-only.md": { hash: "7".repeat(64), mtime: 7, size: 7 } } }));
        missingMainCrash.files.set(`${BASE}.bak`, io.files.get(BASE)!);
        missingMainCrash.files.delete(BASE);
        await authorizeLegacyDowngradeCurrentImport(missingMainCrash, 1, lease);
        const recoveredMissingMain = new ObsetyncSyncBase(appFor(missingMainCrash)); await recoveredMissingMain.load();
        check(recoveredMissingMain.getHash("next-only.md") === "7".repeat(64),
            "valid released crash between main-to-backup and next-to-main was blocked");

        const corruptLower = new DirectoryIO(io.snapshot());
        corruptLower.files.set(`${BASE}.next`, JSON.stringify({ lastSyncTimestamp: 700,
            entries: { "selected.md": { hash: "8".repeat(64), mtime: 8, size: 8 } } }));
        corruptLower.files.set(BASE, "{corrupt lower main");
        corruptLower.files.set(`${BASE}.bak`, JSON.stringify({ schema: 99, lastSyncTimestamp: 1,
            entries: { "unsafe/../path": { hash: "9".repeat(64), mtime: 9, size: 9 } } }));
        await authorizeLegacyDowngradeCurrentImport(corruptLower, 1, lease);
        const recoveredCorruptLower = new ObsetyncSyncBase(appFor(corruptLower)); await recoveredCorruptLower.load();
        check(recoveredCorruptLower.getHash("selected.md") === "8".repeat(64),
            "irrelevant lower-priority legacy copy blocked selected .next authority");

        // Released client continues offline after downgrade. A current client
        // then imports the exact legacy files while root sequence state stays.
        io.files.set(JOURNAL, `${io.files.get(JOURNAL)}${JSON.stringify({ id: oldJournal.nextId, action: "modified",
            path: "written-by-old.md", ts: 20, synced: false })}\n`);
        check(await inspectLegacyDowngradeState(io) === "legacy-active", "active legacy state collapsed to pristine none");
        await authorizeLegacyDowngradeCurrentImport(io, reopenedRoot.lastSequence, lease);
        check(await inspectLegacyDowngradeState(io) === "current-import-authorized", "current import was not durably authorized");
        const upgradedJournal = new ObsetyncJournal(appFor(io));
        const upgradedBase = new ObsetyncSyncBase(appFor(io));
        await upgradedJournal.load(); await upgradedBase.load();
        check(upgradedJournal.unsynced().some(entry => entry.path === "written-by-old.md"),
            "old-client offline write was not importable after re-upgrade");
        check(upgradedBase.getHash("offline.md") === "a".repeat(64), "re-upgrade lost projected sync-base authority");
        const upgradedRoot = new RootIntentStore(io, SCOPE, hashBytes); await upgradedRoot.load();
        check(upgradedRoot.lastSequence === 1, "re-upgrade replaced root sequence authority");
        await completeLegacyDowngradeCurrentImport(io, lease);
        check(await inspectLegacyDowngradeState(io) === "current-import-complete",
            "completed current import collapsed ambiguously to pristine none");
        for (const predecessor of [LEGACY_DOWNGRADE_PATHS.prepared, LEGACY_DOWNGRADE_PATHS.archived,
            LEGACY_DOWNGRADE_PATHS.active, LEGACY_DOWNGRADE_PATHS.currentImport]) {
            const orphan = new DirectoryIO(io.snapshot()); orphan.files.delete(predecessor);
            let rejected = false;
            try { await inspectLegacyDowngradeState(orphan); } catch { rejected = true; }
            check(rejected, `completed phase ignored missing predecessor ${predecessor}`);
        }
        await rejects(activateLegacyDowngrade({ io, journal: upgradedJournal, syncBase: upgradedBase,
            rootIntents: rootIntents(), treeVersion: 1, lease }), "UNSAFE");
    }

    {
        const io = new DirectoryIO(), app = appFor(io);
        const journal = new ObsetyncJournal(app), base = new ObsetyncSyncBase(app);
        const root = new RootIntentStore(io, SCOPE, hashBytes);
        await journal.load(); await base.load(); await root.load();
        const oldEpoch = journal.validatedEpoch;
        const high = await journal.append({ action: "modified", path: "already-synced.md", ts: 1, synced: false });
        await journal.acknowledge([{ path: "already-synced.md", throughId: high }]);
        check(journal.unsyncedCount() === 0 && high === 1, "fresh-epoch fixture did not retire its prior ID");
        await activateLegacyDowngrade({ io, journal, syncBase: base, rootIntents: root, treeVersion: 1, lease });
        const old = released1113Journal(io.files.get(JOURNAL)!);
        check(old.entries.length === 0 && old.nextId === 1,
            "released reader fixture did not model compaction dropping an ACK watermark");
        await authorizeLegacyDowngradeCurrentImport(io, 0, lease);
        const upgraded = new ObsetyncJournal(app); await upgraded.load();
        check(upgraded.validatedEpoch !== oldEpoch, "re-upgrade reused the pre-downgrade journal epoch");
        const reused = await upgraded.append({ action: "modified", path: "new-epoch.md", ts: 2, synced: false });
        check(reused === 1 && upgraded.unsynced().some(entry => entry.path === "new-epoch.md"),
            "fresh-epoch ID reuse acknowledged or lost a new pending row");
    }

    {
        const io = new DirectoryIO();
        const { journal, base } = await populated(io);
        await rejects(activateLegacyDowngrade({ io, journal, syncBase: base, rootIntents: rootIntents(7, {}),
            treeVersion: 1, lease }), "BUSY");
        await rejects(activateLegacyDowngrade({ io, journal, syncBase: base, rootIntents: rootIntents(),
            treeVersion: 2, lease }), "UNSAFE");
        base.setVerifiedBaseRequired(true);
        await rejects(activateLegacyDowngrade({ io, journal, syncBase: base, rootIntents: rootIntents(),
            treeVersion: 1, lease }), "BUSY");
        check(!(await io.exists(LEGACY_DOWNGRADE_PATHS.prepared)), "refused downgrade wrote a phase marker");
    }

    {
        const io = new DirectoryIO();
        const { journal, base } = await populated(io);
        let failed = false;
        io.onBoundary = event => {
            if (!failed && event.method === "write" && event.phase === "before" && event.path === BASE &&
                io.files.has(LEGACY_DOWNGRADE_PATHS.archived)) {
                failed = true; throw storeTestIOError();
            }
        };
        let interruption: unknown;
        try { await activateLegacyDowngrade({ io, journal, syncBase: base, rootIntents: rootIntents(), treeVersion: 1, lease }); }
        catch (error) { interruption = error; }
        check(interruption instanceof Error && failed, "promotion interruption was not observed");
        check(!(await io.exists(LEGACY_DOWNGRADE_PATHS.active)), "interrupted promotion became active");
        let fenced = false;
        try { await new ObsetyncJournal(appFor(io)).load(); } catch { fenced = true; }
        check(fenced, "current reader ignored an interrupted downgrade fence");
        io.onBoundary = undefined;
        io.files.set(`${BASE}.next`, "unexpected concurrent legacy source");
        await rejects(resumeLegacyDowngrade(io, lease), "RECOVERY_REQUIRED");
        io.files.delete(`${BASE}.next`);
        await resumeLegacyDowngrade(io, lease);
        check((await io.exists(LEGACY_DOWNGRADE_PATHS.active)), "restart did not resume archived promotion");
        released1113Journal(io.files.get(JOURNAL)!);
        released1113Base(io.files.get(BASE)!);
    }

    {
        const io = new DirectoryIO();
        const { journal, base } = await populated(io);
        let stopped = false;
        io.onBoundary = event => {
            if (!stopped && event.method === "mkdir" && event.phase === "before" && event.path.endsWith("/authority")) {
                stopped = true; throw storeTestIOError();
            }
        };
        try { await activateLegacyDowngrade({ io, journal, syncBase: base, rootIntents: rootIntents(), treeVersion: 1, lease }); }
        catch { /* expected prepared-only crash */ }
        const prepared = JSON.parse(io.files.get(LEGACY_DOWNGRADE_PATHS.prepared)!);
        const projection = `${ROOT}/legacy-downgrade-archives/${prepared.bundleId}/projection/change-journal.ndjson`;
        io.files.set(projection, `${io.files.get(projection)}corrupt`);
        io.onBoundary = undefined;
        await rejects(resumeLegacyDowngrade(io, lease), "CORRUPT");
        check(!(await io.exists(LEGACY_DOWNGRADE_PATHS.active)), "corrupt projection was activated");
    }

    {
        const io = new DirectoryIO();
        const { journal, base } = await populated(io);
        let owner: object = {}, captured = owner;
        const revocable = { assertHeld() { if (owner !== captured) throw new Error("lease generation changed"); } };
        io.afterDirectoryRename = () => { owner = {}; };
        let revoked = false;
        try { await activateLegacyDowngrade({ io, journal, syncBase: base, rootIntents: rootIntents(0),
            treeVersion: 1, lease: revocable }); } catch { revoked = true; }
        check(revoked && !(await io.exists(LEGACY_DOWNGRADE_PATHS.active)),
            "revoked/ABA-changed lease crossed another irreversible phase");
        io.afterDirectoryRename = undefined; captured = owner;
        await resumeLegacyDowngrade(io, revocable);
        check((await io.exists(LEGACY_DOWNGRADE_PATHS.active)), "new lease generation could not resume exact phase state");
    }

    {
        const io = new DirectoryIO();
        io.directories.add(ROOT); io.files.set(LEGACY_DOWNGRADE_PATHS.prepared, "{torn");
        let fenced = false;
        try { await new ObsetyncJournal(appFor(io)).load(); } catch { fenced = true; }
        check(fenced, "torn downgrade marker was interpreted as an empty journal");
    }

    console.log(`legacy downgrade tests passed (${assertions} assertions)`);
}

run().catch(error => { console.error(error); process.exit(1); });
