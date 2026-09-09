import type { ObsetyncJournal, JournalEntry } from "./journal";
import type { ObsetyncSyncBase, DiffPageCheckpoint } from "./sync-base";
import type { RootIntentStore } from "./root-intent";
import { isSafeVaultPath } from "./delta-validation";
import { checksumJournalUtf8 } from "./journal-format";
import { StoreRecoveryError, segmentedMigrationPaths, type SegmentedStoreIO } from "./segmented-store";

const ROOT = ".obsidian/plugins/obsetync";
const JOURNAL_STORE = `${ROOT}/change-journal.store-v1`;
const BASE_STORE = `${ROOT}/sync-base.store-v1`;
const JOURNAL_LEGACY = `${ROOT}/change-journal.ndjson`;
const BASE_LEGACY = `${ROOT}/sync-base.json`;
const BASE_WAL = `${ROOT}/sync-base.wal.ndjson`;
const PREPARED = `${ROOT}/legacy-downgrade-v1.prepared.json`;
const ARCHIVED = `${ROOT}/legacy-downgrade-v1.archived.json`;
const ACTIVE = `${ROOT}/legacy-downgrade-v1.active.json`;
const CURRENT_IMPORT = `${ROOT}/legacy-downgrade-v1.current-import.json`;
const CURRENT_COMPLETE = `${ROOT}/legacy-downgrade-v1.current-active.json`;
const ARCHIVE_ROOT = `${ROOT}/legacy-downgrade-archives`;
const MAX_FILE_BYTES = 8 * 1024 * 1024;
const MAX_ENTRIES = 65_536;
const MAX_MARKER_BYTES = 32 * 1024;

export const LEGACY_DOWNGRADE_PATHS = Object.freeze({ prepared: PREPARED, archived: ARCHIVED, active: ACTIVE,
    currentImport: CURRENT_IMPORT, currentActive: CURRENT_COMPLETE });

export class LegacyDowngradeError extends Error {
    constructor(readonly code: "BUSY" | "CORRUPT" | "LIMIT" | "RECOVERY_REQUIRED" | "UNSAFE", message: string) {
        super(message); this.name = "LegacyDowngradeError";
    }
}

export interface DowngradeAuthorityLease {
    /** The caller must keep vault listeners and sync publication stopped until
     * activateLegacyDowngrade returns or throws. */
    assertHeld(): void;
}

export interface LegacyDowngradeRequest {
    io: SegmentedStoreIO;
    journal: ObsetyncJournal;
    syncBase: ObsetyncSyncBase;
    rootIntents: Pick<RootIntentStore, "pending" | "lastSequence">;
    treeVersion: number;
    lease: DowngradeAuthorityLease;
}

interface MarkerCore {
    schema: 1;
    kind: "legacy-downgrade";
    sourceVersion: "segmented-v1";
    targetVersion: "1.11.3";
    treeVersion: 1;
    bundleId: string;
    journalSha256: string;
    baseSha256: string;
    rootLastSequence: number;
    archiveSources: string[];
}
interface Marker extends MarkerCore {
    phase: "prepared" | "archived" | "active" | "current-import" | "current-active";
}

const fixedSources = [
    JOURNAL_STORE, segmentedMigrationPaths(JOURNAL_STORE).intent, segmentedMigrationPaths(JOURNAL_STORE).advanced,
    BASE_STORE, segmentedMigrationPaths(BASE_STORE).intent, segmentedMigrationPaths(BASE_STORE).advanced,
    JOURNAL_LEGACY, `${JOURNAL_LEGACY}.next`, `${JOURNAL_LEGACY}.bak`,
    BASE_LEGACY, `${BASE_LEGACY}.next`, `${BASE_LEGACY}.bak`, BASE_WAL,
] as const;

function fail(code: LegacyDowngradeError["code"], message: string): never {
    throw new LegacyDowngradeError(code, message);
}
function exact(value: unknown, keys: readonly string[]): asserts value is Record<string, any> {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).length !== keys.length || Object.keys(value).some(key => !keys.includes(key))) {
        fail("CORRUPT", "legacy downgrade marker fields differ");
    }
}
function hex(value: unknown): value is string { return typeof value === "string" && /^[0-9a-f]{64}$/.test(value); }
function uint(value: unknown): value is number {
    return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 && value < Number.MAX_SAFE_INTEGER;
}
function markerCore(marker: Marker): MarkerCore {
    const { phase: _phase, ...core } = marker;
    return core;
}
function sameCore(left: Marker, right: Marker): boolean {
    return JSON.stringify(markerCore(left)) === JSON.stringify(markerCore(right));
}
function validateMarker(raw: string, expectedPhase: Marker["phase"]): Marker {
    if (checksumJournalUtf8(raw).bytes > MAX_MARKER_BYTES) fail("LIMIT", "legacy downgrade marker exceeds ceiling");
    let value: unknown;
    try { value = JSON.parse(raw); } catch { fail("CORRUPT", "legacy downgrade marker is not JSON"); }
    exact(value, ["schema", "kind", "sourceVersion", "targetVersion", "treeVersion", "bundleId",
        "journalSha256", "baseSha256", "rootLastSequence", "archiveSources", "phase"]);
    if (value.schema !== 1 || value.kind !== "legacy-downgrade" || value.sourceVersion !== "segmented-v1" ||
        value.targetVersion !== "1.11.3" || value.treeVersion !== 1 || value.phase !== expectedPhase ||
        !hex(value.bundleId) || !hex(value.journalSha256) || !hex(value.baseSha256) ||
        !uint(value.rootLastSequence) || !Array.isArray(value.archiveSources) ||
        value.archiveSources.length > fixedSources.length) fail("CORRUPT", "invalid legacy downgrade marker");
    const allowed = new Set<string>(fixedSources);
    let previous: string | null = null;
    for (const path of value.archiveSources) {
        if (typeof path !== "string" || !allowed.has(path) || (previous !== null && path <= previous)) {
            fail("CORRUPT", "legacy downgrade archive inventory differs");
        }
        previous = path;
    }
    if (!value.archiveSources.includes(JOURNAL_STORE) || !value.archiveSources.includes(BASE_STORE)) {
        fail("CORRUPT", "legacy downgrade lacks authoritative stores");
    }
    return value as Marker;
}

async function digest(raw: string): Promise<string> {
    try {
        const value = new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256",
            new TextEncoder().encode(raw) as BufferSource));
        return [...value].map(byte => byte.toString(16).padStart(2, "0")).join("");
    } catch { fail("RECOVERY_REQUIRED", "legacy downgrade hashing unavailable"); }
}
function bundleIdentity(value: Pick<MarkerCore, "journalSha256" | "baseSha256" | "rootLastSequence" | "archiveSources">): string {
    return JSON.stringify({ schema: 1, sourceVersion: "segmented-v1", targetVersion: "1.11.3", treeVersion: 1,
        journalSha256: value.journalSha256, baseSha256: value.baseSha256,
        rootLastSequence: value.rootLastSequence, archiveSources: value.archiveSources });
}
async function readOptional(io: SegmentedStoreIO, path: string, limit: number): Promise<string | null> {
    if (!(await io.exists(path))) return null;
    if (io.stat) {
        const stat = await io.stat(path);
        if (!stat || (stat.type !== undefined && stat.type !== "file") || !uint(stat.size) || stat.size > limit) {
            fail("LIMIT", "legacy downgrade file metadata exceeds ceiling");
        }
    }
    const raw = await io.read(path);
    if (checksumJournalUtf8(raw).bytes > limit) fail("LIMIT", "legacy downgrade file exceeds ceiling");
    return raw;
}
async function readMarker(io: SegmentedStoreIO, path: string, phase: Marker["phase"]): Promise<Marker | null> {
    const raw = await readOptional(io, path, MAX_MARKER_BYTES);
    if (raw === null) return null;
    const marker = validateMarker(raw, phase);
    if (await digest(bundleIdentity(marker)) !== marker.bundleId) fail("CORRUPT", "legacy downgrade marker identity differs");
    return marker;
}

async function hasReleasedBaseSnapshot(io: SegmentedStoreIO): Promise<boolean> {
    for (const path of [`${BASE_LEGACY}.next`, BASE_LEGACY, `${BASE_LEGACY}.bak`]) {
        const raw = await readOptional(io, path, MAX_FILE_BYTES);
        if (raw === null) continue;
        try {
            const parsed = JSON.parse(raw);
            if (parsed && typeof parsed.lastSyncTimestamp === "number" && parsed.entries) return true;
        } catch { /* released reader tries the next priority copy */ }
    }
    return false;
}

/** Called before either current authoritative store is opened. A prepared or
 * archived transition cannot be interpreted as an empty store after a crash. */
export async function assertLegacyDowngradeReadable(io: SegmentedStoreIO): Promise<boolean> {
    try {
        const [prepared, archived, active, currentImport, currentActive] = await Promise.all([
            readMarker(io, PREPARED, "prepared"), readMarker(io, ARCHIVED, "archived"), readMarker(io, ACTIVE, "active"),
            readMarker(io, CURRENT_IMPORT, "current-import"), readMarker(io, CURRENT_COMPLETE, "current-active"),
        ]);
        if (!prepared && !archived && !active && !currentImport && !currentActive) return false;
        if (!prepared || (active && !archived) || (archived && !sameCore(prepared, archived)) ||
            (active && !sameCore(prepared, active)) || (currentImport && !sameCore(prepared, currentImport)) ||
            (currentActive && (!currentImport || !sameCore(prepared, currentActive)))) {
            throw new StoreRecoveryError("RECOVERY_REQUIRED", "legacy downgrade phase markers differ");
        }
        if (!active) throw new StoreRecoveryError("RECOVERY_REQUIRED", "legacy downgrade requires explicit resume");
        const journal = await readOptional(io, JOURNAL_LEGACY, MAX_FILE_BYTES);
        if (journal === null || !(await hasReleasedBaseSnapshot(io))) {
            throw new StoreRecoveryError("RECOVERY_REQUIRED", "active legacy downgrade projection is unavailable");
        }
        await validateArchivedInventory(io, active, "legacy-active");
        if (!currentImport) {
            throw new StoreRecoveryError("RECOVERY_REQUIRED", "legacy downgrade current import is not authorized");
        }
        return true;
    } catch (error) {
        if (error instanceof StoreRecoveryError) throw error;
        throw new StoreRecoveryError("RECOVERY_REQUIRED", "legacy downgrade marker requires explicit recovery");
    }
}

/** Root-intent startup floor retained across the legacy interval. A legacy
 * endpoint does not consume this stream, so equal or greater is safe; lower
 * means the new client lost/reset sequence authority and must not publish. */
export async function legacyDowngradeRootSequenceFloor(io: SegmentedStoreIO): Promise<number | null> {
    const [prepared, archived, active, currentImport, currentActive] = await Promise.all([
        readMarker(io, PREPARED, "prepared"), readMarker(io, ARCHIVED, "archived"), readMarker(io, ACTIVE, "active"),
        readMarker(io, CURRENT_IMPORT, "current-import"), readMarker(io, CURRENT_COMPLETE, "current-active"),
    ]);
    if (!active) {
        if (prepared || archived || currentImport || currentActive) {
            fail("RECOVERY_REQUIRED", "legacy downgrade root sequence awaits recovery");
        }
        return null;
    }
    if (!prepared || !archived || !sameCore(prepared, archived) || !sameCore(prepared, active) ||
        (currentImport && !sameCore(prepared, currentImport)) ||
        (currentActive && (!currentImport || !sameCore(prepared, currentActive)))) {
        fail("CORRUPT", "legacy downgrade root sequence markers differ");
    }
    return active.rootLastSequence;
}

export type LegacyDowngradeState = "none" | "interrupted" | "legacy-active" |
    "current-import-authorized" | "current-import-complete";

export async function inspectLegacyDowngradeState(io: SegmentedStoreIO): Promise<LegacyDowngradeState> {
    const [prepared, archived, active, currentImport, currentActive] = await Promise.all([
        readMarker(io, PREPARED, "prepared"), readMarker(io, ARCHIVED, "archived"), readMarker(io, ACTIVE, "active"),
        readMarker(io, CURRENT_IMPORT, "current-import"), readMarker(io, CURRENT_COMPLETE, "current-active"),
    ]);
    if (!prepared && !archived && !active && !currentImport && !currentActive) return "none";
    if (!prepared || (active && !archived) || (archived && !sameCore(prepared, archived)) ||
        (active && !sameCore(prepared, active)) ||
        (currentImport && (!active || !sameCore(prepared, currentImport))) ||
        (currentActive && (!currentImport || !sameCore(prepared, currentActive)))) fail("CORRUPT", "legacy downgrade phases differ");
    if (currentActive) return "current-import-complete";
    if (currentImport) return "current-import-authorized";
    if (active) return "legacy-active";
    return "interrupted";
}

/** Host calls this only after the old client is stopped, root-intent load has
 * validated its durable state, and one revocable exclusion lease is held. */
export async function authorizeLegacyDowngradeCurrentImport(io: SegmentedStoreIO,
    rootLastSequence: number, lease: DowngradeAuthorityLease): Promise<void> {
    lease.assertHeld();
    const [prepared, archived, active, existing] = await Promise.all([
        readMarker(io, PREPARED, "prepared"), readMarker(io, ARCHIVED, "archived"), readMarker(io, ACTIVE, "active"),
        readMarker(io, CURRENT_IMPORT, "current-import"),
    ]);
    if (!prepared || !archived || !active || !sameCore(prepared, archived) || !sameCore(prepared, active)) {
        fail("RECOVERY_REQUIRED", "legacy downgrade is not active");
    }
    if (!uint(rootLastSequence) || rootLastSequence < active.rootLastSequence) {
        fail("RECOVERY_REQUIRED", "legacy downgrade root sequence moved backwards");
    }
    if (await readOptional(io, JOURNAL_LEGACY, MAX_FILE_BYTES) === null || !(await hasReleasedBaseSnapshot(io))) {
        fail("RECOVERY_REQUIRED", "legacy downgrade authority is unavailable");
    }
    await validateArchivedInventory(io, active, "legacy-active");
    if (existing) {
        if (!sameCore(active, existing)) fail("CORRUPT", "legacy downgrade current import differs");
        return;
    }
    await writeImmutable(io, CURRENT_IMPORT, JSON.stringify({ ...markerCore(active), phase: "current-import" }), lease);
}

/** Host calls after both current stores have loaded their active legacy cut.
 * Completion remains explicit and never deletes legacy files or archives. */
export async function completeLegacyDowngradeCurrentImport(io: SegmentedStoreIO,
    lease: DowngradeAuthorityLease): Promise<void> {
    lease.assertHeld();
    const active = await readMarker(io, ACTIVE, "active");
    const currentImport = await readMarker(io, CURRENT_IMPORT, "current-import");
    if (!active || !currentImport || !sameCore(active, currentImport)) fail("RECOVERY_REQUIRED", "current import is not authorized");
    if (!(await io.exists(JOURNAL_STORE)) || !(await io.exists(BASE_STORE))) {
        fail("RECOVERY_REQUIRED", "current authoritative stores are not both present");
    }
    const existing = await readMarker(io, CURRENT_COMPLETE, "current-active");
    if (existing) {
        if (!sameCore(active, existing)) fail("CORRUPT", "completed current import differs");
        return;
    }
    await writeImmutable(io, CURRENT_COMPLETE, JSON.stringify({ ...markerCore(active), phase: "current-active" }), lease);
}

function detachedJournal(entryCount: number, entries: Iterable<JournalEntry>): string {
    if (entryCount > MAX_ENTRIES) fail("LIMIT", "legacy journal entry ceiling exceeded");
    let previousId = 0;
    const rows: string[] = [];
    let bytes = 0;
    for (const entry of entries) {
        if (!uint(entry.id) || entry.id === 0 || entry.id < previousId || !isSafeVaultPath(entry.path) ||
            (entry.action === "renamed" && (!entry.oldPath || !isSafeVaultPath(entry.oldPath)))) {
            fail("CORRUPT", "legacy journal projection is invalid");
        }
        previousId = entry.id;
        const row = JSON.stringify({ id: entry.id, action: entry.action, path: entry.path,
            ...(entry.oldPath === undefined ? {} : { oldPath: entry.oldPath }), ts: entry.ts, synced: false });
        bytes += checksumJournalUtf8(row).bytes + 1;
        if (bytes > MAX_FILE_BYTES) fail("LIMIT", "legacy journal projection exceeds ceiling");
        rows.push(row);
    }
    if (rows.length !== entryCount) fail("CORRUPT", "legacy journal capture changed");
    return rows.length ? `${rows.join("\n")}\n` : "";
}

function detachedBase(syncBase: ObsetyncSyncBase): string {
    const capture = syncBase.captureTreeEntries();
    if (capture.entryCount > MAX_ENTRIES) fail("LIMIT", "legacy sync-base entry ceiling exceeded");
    const entries: Record<string, { hash: string; mtime: number; size: number; treeMtime?: number }> = Object.create(null);
    let previous: string | null = null;
    let count = 0;
    let retainedBytes = 256;
    for (const captured of capture.entries()) {
        const path = captured.path;
        if (!isSafeVaultPath(path) || (previous !== null && path <= previous)) fail("CORRUPT", "legacy sync-base paths differ");
        previous = path;
        const entry = syncBase.getEntry(path);
        if (!entry) fail("CORRUPT", "legacy sync-base capture changed");
        retainedBytes += checksumJournalUtf8(`${JSON.stringify(path)}:${JSON.stringify(entry)},`).bytes;
        if (retainedBytes > MAX_FILE_BYTES) fail("LIMIT", "legacy sync-base projection exceeds ceiling");
        entries[path] = entry;
        count++;
    }
    if (count !== capture.entryCount || !capture.isCurrent()) fail("CORRUPT", "legacy sync-base capture changed");
    const checkpoint: DiffPageCheckpoint | null = syncBase.diffPageCheckpoint;
    const raw = JSON.stringify({ lastSyncTimestamp: syncBase.lastSyncTimestamp, entries,
        ...(syncBase.treeBaseRoot === null ? {} : { treeBaseRoot: syncBase.treeBaseRoot }),
        ...(checkpoint === null ? {} : { diffPageCheckpoint: checkpoint }) });
    if (checksumJournalUtf8(raw).bytes > MAX_FILE_BYTES) fail("LIMIT", "legacy sync-base projection exceeds ceiling");
    return raw;
}

async function ensureDir(io: SegmentedStoreIO, path: string): Promise<void> {
    if (!(await io.exists(path))) await io.mkdir(path);
}
async function writeImmutable(io: SegmentedStoreIO, path: string, raw: string, lease: DowngradeAuthorityLease): Promise<void> {
    const existing = await readOptional(io, path, Math.max(MAX_FILE_BYTES, MAX_MARKER_BYTES));
    if (existing !== null) {
        if (existing !== raw) fail("CORRUPT", "legacy downgrade immutable file differs");
        return;
    }
    lease.assertHeld();
    await io.write(path, raw);
    if (await readOptional(io, path, Math.max(MAX_FILE_BYTES, MAX_MARKER_BYTES)) !== raw) {
        fail("RECOVERY_REQUIRED", "legacy downgrade immutable write was not verified");
    }
}
function archivePath(marker: Marker, source: string): string {
    const index = fixedSources.indexOf(source as any);
    if (index < 0) fail("CORRUPT", "legacy downgrade source path is not fixed");
    return `${ARCHIVE_ROOT}/${marker.bundleId}/authority/${index.toString().padStart(2, "0")}`;
}
async function moveOnce(io: SegmentedStoreIO, marker: Marker, source: string, lease: DowngradeAuthorityLease): Promise<void> {
    const target = archivePath(marker, source);
    const [hasSource, hasTarget] = await Promise.all([io.exists(source), io.exists(target)]);
    if (hasSource && hasTarget) fail("CORRUPT", "legacy downgrade source and archive both exist");
    if (!hasSource && hasTarget) return;
    if (!hasSource) fail("RECOVERY_REQUIRED", "legacy downgrade source vanished before archive");
    lease.assertHeld();
    await io.rename(source, target);
    if (await io.exists(source) || !(await io.exists(target))) fail("RECOVERY_REQUIRED", "legacy downgrade archive was not verified");
}

async function validateArchivedInventory(io: SegmentedStoreIO, marker: Marker,
    allowedSources: "none" | "promoted-main" | "legacy-active"): Promise<void> {
    for (const source of marker.archiveSources) {
        const target = archivePath(marker, source);
        if (!(await io.exists(target))) fail("RECOVERY_REQUIRED", "legacy downgrade archive inventory is incomplete");
    }
    for (const source of fixedSources) {
        const promoted = source === JOURNAL_LEGACY || source === BASE_LEGACY;
        const allowed = (allowedSources === "promoted-main" && promoted) ||
            allowedSources === "legacy-active";
        if (await io.exists(source) && !allowed) {
            fail("RECOVERY_REQUIRED", "legacy downgrade source remained outside its archive");
        }
    }
}

async function resumePrepared(io: SegmentedStoreIO, prepared: Marker, lease: DowngradeAuthorityLease): Promise<Marker> {
    const bundle = `${ARCHIVE_ROOT}/${prepared.bundleId}`;
    const projectionDir = `${bundle}/projection`;
    const journalPath = `${projectionDir}/change-journal.ndjson`;
    const basePath = `${projectionDir}/sync-base.json`;
    const journal = await readOptional(io, journalPath, MAX_FILE_BYTES);
    const base = await readOptional(io, basePath, MAX_FILE_BYTES);
    if (journal === null || base === null || await digest(journal) !== prepared.journalSha256 ||
        await digest(base) !== prepared.baseSha256) fail("CORRUPT", "legacy downgrade projection digest differs");
    const archived: Marker = { ...markerCore(prepared), phase: "archived" };
    const existingArchived = await readMarker(io, ARCHIVED, "archived");
    if (existingArchived) {
        if (!sameCore(prepared, existingArchived)) fail("CORRUPT", "legacy downgrade archived marker differs");
        await validateArchivedInventory(io, prepared, "promoted-main");
    } else {
        await ensureDir(io, `${bundle}/authority`);
        for (const source of prepared.archiveSources) await moveOnce(io, prepared, source, lease);
        await validateArchivedInventory(io, prepared, "none");
        await writeImmutable(io, ARCHIVED, JSON.stringify(archived), lease);
    }
    lease.assertHeld();
    await io.write(JOURNAL_LEGACY, journal);
    lease.assertHeld();
    await io.write(BASE_LEGACY, base);
    if (await readOptional(io, JOURNAL_LEGACY, MAX_FILE_BYTES) !== journal ||
        await readOptional(io, BASE_LEGACY, MAX_FILE_BYTES) !== base) {
        fail("RECOVERY_REQUIRED", "legacy downgrade promotion was not verified");
    }
    await validateArchivedInventory(io, prepared, "promoted-main");
    const active: Marker = { ...markerCore(prepared), phase: "active" };
    lease.assertHeld();
    await writeImmutable(io, ACTIVE, JSON.stringify(active), lease);
    return active;
}

/** Complete a crash-interrupted downgrade. It never deletes an archive and
 * never guesses a projection from filenames. Current stores remain fenced
 * until the active marker and both exact legacy projections are durable. */
export async function resumeLegacyDowngrade(io: SegmentedStoreIO, lease: DowngradeAuthorityLease): Promise<void> {
    lease.assertHeld();
    const prepared = await readMarker(io, PREPARED, "prepared");
    if (!prepared) fail("RECOVERY_REQUIRED", "no prepared legacy downgrade exists");
    const archived = await readMarker(io, ARCHIVED, "archived");
    if (archived && !sameCore(prepared, archived)) fail("CORRUPT", "legacy downgrade archived marker differs");
    const active = await readMarker(io, ACTIVE, "active");
    if (active) {
        if (!archived || !sameCore(prepared, active)) fail("CORRUPT", "legacy downgrade active phase is incomplete");
        await validateArchivedInventory(io, active, "legacy-active");
        if (await readOptional(io, JOURNAL_LEGACY, MAX_FILE_BYTES) === null || !(await hasReleasedBaseSnapshot(io))) {
            fail("RECOVERY_REQUIRED", "active legacy downgrade projection is unavailable");
        }
        return;
    }
    await resumePrepared(io, prepared, lease);
    lease.assertHeld();
}

/** Project one quiescent, fully settled v1 authority cut into files consumed by
 * the released 1.11.3 readers. The root-intent store is deliberately retained:
 * it has no pending intent and preserves lastSequence for a future upgrade. */
export async function activateLegacyDowngrade(request: LegacyDowngradeRequest): Promise<void> {
    const { io, journal, syncBase, rootIntents, lease } = request;
    lease.assertHeld();
    if (request.treeVersion !== 1) fail("UNSAFE", "legacy downgrade requires tree format v1");
    if (rootIntents.pending() !== null) fail("BUSY", "root publication intent is pending or uncertain");
    if (syncBase.verifiedBaseRequired) fail("BUSY", "sync-base ancestry is not verified");
    if (await readMarker(io, ACTIVE, "active")) fail("UNSAFE", "legacy downgrade is already active");
    if (await readMarker(io, PREPARED, "prepared")) {
        await resumeLegacyDowngrade(io, lease); return;
    }
    await journal.compact();
    await syncBase.save();
    lease.assertHeld();
    const journalRaw = detachedJournal(journal.unsyncedCount(), journal.iterateUnsynced());
    const baseRaw = detachedBase(syncBase);
    const journalSha256 = await digest(journalRaw);
    const baseSha256 = await digest(baseRaw);
    lease.assertHeld();
    const archiveSources: string[] = [];
    for (const path of fixedSources) if (await io.exists(path)) archiveSources.push(path);
    archiveSources.sort();
    if (!archiveSources.includes(JOURNAL_STORE) || !archiveSources.includes(BASE_STORE)) {
        fail("RECOVERY_REQUIRED", "authoritative segmented stores are unavailable");
    }
    const rootLastSequence = rootIntents.lastSequence;
    const bundleId = await digest(bundleIdentity({ journalSha256, baseSha256, rootLastSequence, archiveSources }));
    const core: MarkerCore = { schema: 1, kind: "legacy-downgrade", sourceVersion: "segmented-v1",
        targetVersion: "1.11.3", treeVersion: 1, bundleId, journalSha256, baseSha256,
        rootLastSequence, archiveSources };
    const bundle = `${ARCHIVE_ROOT}/${bundleId}`;
    await ensureDir(io, ARCHIVE_ROOT); await ensureDir(io, bundle); await ensureDir(io, `${bundle}/projection`);
    await writeImmutable(io, `${bundle}/projection/change-journal.ndjson`, journalRaw, lease);
    await writeImmutable(io, `${bundle}/projection/sync-base.json`, baseRaw, lease);
    const prepared: Marker = { ...core, phase: "prepared" };
    await writeImmutable(io, PREPARED, JSON.stringify(prepared), lease);
    await resumePrepared(io, prepared, lease);
    lease.assertHeld();
}
