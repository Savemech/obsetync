import type { JournalEntry } from "./journal";
import { isSafeVaultPath } from "./delta-validation";
import { yieldWork } from "./work-scheduler";

export class JournalRecoveryError extends Error {
    constructor(readonly code: "CORRUPT" | "UNKNOWN_SCHEMA" | "INCOMPLETE_CHECKPOINT" |
        "CONTRADICTING_COPIES" | "READ_FAILED" | "RECOVERY_REQUIRED" | "LIMIT", message: string) {
        super(message);
        this.name = "JournalRecoveryError";
    }
}

export interface ParsedJournal {
    entries: JournalEntry[];
    /** Physical legacy mutations and durable ACKs, before the compatibility
     * projection. Migration needs both rename endpoints' independent state. */
    mutations: JournalEntry[];
    acknowledgements: Array<{ path: string; throughId: number }>;
    nextId: number;
    generation: number;
    recordsRead: number;
    tornTail: boolean;
    checkpointIdentity: string | null;
}

const integer = (value: unknown, minimum = 1): value is number =>
    typeof value === "number" && Number.isSafeInteger(value) && value >= minimum;

function corrupt(message: string): never {
    throw new JournalRecoveryError("CORRUPT", message);
}

const CRC32_TABLE = Uint32Array.from({ length: 256 }, (_, value) => {
    let crc = value;
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (0xedb8_8320 & -(crc & 1));
    return crc >>> 0;
});

/** Per-line UTF-8 CRC avoids another full encoded copy of a legacy WAL.
 * Accidental-corruption detection only; this is not an authentication tag. */
class JournalChecksum {
    private crc = 0xffff_ffff;
    bytes = 0;
    add(value: string): void {
        const encoded = new TextEncoder().encode(value);
        this.bytes += encoded.byteLength;
        for (const byte of encoded) {
            this.crc = (this.crc >>> 8) ^ CRC32_TABLE[(this.crc ^ byte) & 0xff];
        }
    }
    hex(): string { return ((this.crc ^ 0xffff_ffff) >>> 0).toString(16).padStart(8, "0"); }
}

export function checksumJournalUtf8(value: string): { bytes: number; crc32: string } {
    const checksum = new JournalChecksum();
    checksum.add(value);
    return { bytes: checksum.bytes, crc32: checksum.hex() };
}

export function validateJournalEntry(value: unknown, fallbackId?: number): JournalEntry {
    if (!value || typeof value !== "object" || Array.isArray(value)) corrupt("invalid journal mutation");
    const row = value as Record<string, unknown>;
    if (row.schema !== undefined && row.schema !== 1) throw new JournalRecoveryError("UNKNOWN_SCHEMA", "unsupported journal mutation schema");
    if (row.op !== undefined || !isSafeVaultPath(row.path) ||
        (row.action !== "created" && row.action !== "modified" && row.action !== "deleted" && row.action !== "renamed") ||
        typeof row.ts !== "number" || !Number.isFinite(row.ts) || row.ts < 0 ||
        (row.synced !== undefined && typeof row.synced !== "boolean")) corrupt("invalid journal mutation fields");
    if (row.action === "renamed" && (!isSafeVaultPath(row.oldPath) || row.oldPath === row.path)) corrupt("invalid linked rename journal mutation");
    if (row.oldPath !== undefined && !isSafeVaultPath(row.oldPath)) corrupt("invalid old journal path");
    const hasSourceMetadata = row.hash !== undefined || row.mtime !== undefined || row.size !== undefined;
    if (hasSourceMetadata) {
        if ((row.action !== "created" && row.action !== "modified") ||
            typeof row.mtime !== "number" || !Number.isFinite(row.mtime) || row.mtime < 0 ||
            typeof row.size !== "number" || !Number.isSafeInteger(row.size) || row.size < 0 ||
            (row.hash !== undefined && (typeof row.hash !== "string" || !/^[0-9a-f]{64}$/.test(row.hash)))) {
            corrupt("invalid journal source metadata");
        }
    }
    const id = row.id === undefined ? fallbackId : row.id;
    if (!integer(id) || id >= Number.MAX_SAFE_INTEGER) corrupt("invalid journal generation");
    return { id, action: row.action, path: row.path,
        ...(typeof row.oldPath === "string" ? { oldPath: row.oldPath } : {}), ts: row.ts, synced: false,
        ...(typeof row.hash === "string" ? { hash: row.hash } : {}),
        ...(typeof row.mtime === "number" ? { mtime: row.mtime } : {}),
        ...(typeof row.size === "number" ? { size: row.size } : {}) };
}

/** Strict forward-only replay. Only malformed JSON at an unterminated EOF
 * may be an incomplete append; invalid complete rows are never skipped. */
function* parseJournalSteps(raw: string): Generator<void, ParsedJournal> {
    const loaded: JournalEntry[] = [];
    const acknowledgements = new Map<string, number>();
    let nextId = 1;
    let generation = 0;
    let recordsRead = 0;
    let snapshotRows = 0;
    let sawHeader = false;
    let sawSeal = false;
    let tornTail = false;
    let checkpointIdentity: string | null = null;
    let linesRead = 0;
    const checksum = new JournalChecksum();
    for (let offset = 0; offset < raw.length;) {
        const newline = raw.indexOf("\n", offset);
        const terminated = newline !== -1;
        const end = terminated ? newline : raw.length;
        const line = raw.slice(offset, end);
        const rawLine = terminated ? `${line}\n` : line;
        offset = terminated ? end + 1 : end;
        if (++linesRead % 256 === 0) yield;
        // The whole legacy input is separately capped by its reader. Yielding
        // only between rows still needs a bound on one JSON.parse operation.
        if (line.length > 128 * 1024) throw new JournalRecoveryError("LIMIT", "legacy journal row exceeds portable parse limit");
        if (!line.trim()) {
            if (sawHeader && !sawSeal) checksum.add(rawLine);
            continue;
        }
        let row: Record<string, unknown>;
        try { row = JSON.parse(line); }
        catch {
            if (!terminated) { tornTail = true; break; }
            corrupt("journal contains an invalid complete or middle row");
        }
        if (!row || typeof row !== "object" || Array.isArray(row)) corrupt("invalid journal record");
        if (row.schema !== undefined && row.schema !== 1) throw new JournalRecoveryError("UNKNOWN_SCHEMA", "unsupported journal schema");
        if (row.op === "journal-meta") {
            if (recordsRead !== 0 || sawHeader || row.schema !== 1 || !integer(row.generation) || !integer(row.nextId)) corrupt("invalid journal checkpoint header");
            generation = row.generation;
            nextId = row.nextId;
            sawHeader = true;
            checksum.add(rawLine);
            recordsRead++;
            continue;
        }
        if (row.op === "journal-seal") {
            if (!sawHeader || sawSeal || row.schema !== 1 || row.generation !== generation || row.records !== snapshotRows ||
                row.bytes !== checksum.bytes || row.crc32 !== checksum.hex()) corrupt("journal checkpoint checksum or boundary mismatch");
            sawSeal = true;
            checkpointIdentity = `${generation}:${row.records}:${row.bytes}:${row.crc32}`;
            recordsRead++;
            continue;
        }
        if (row.op === "ack") {
            if ((sawHeader && !sawSeal) || !isSafeVaultPath(row.path) || !integer(row.throughId) || row.throughId >= Number.MAX_SAFE_INTEGER) corrupt("invalid journal acknowledgement");
            acknowledgements.set(row.path, Math.max(acknowledgements.get(row.path) ?? 0, row.throughId));
            nextId = Math.max(nextId, row.throughId + 1);
        } else {
            if (row.op !== undefined) throw new JournalRecoveryError("UNKNOWN_SCHEMA", "unsupported journal operation");
            const entry = validateJournalEntry(row, sawHeader ? undefined : nextId);
            if (sawSeal && entry.id < nextId) corrupt("appended journal generation moved backwards");
            loaded.push(entry);
            nextId = Math.max(nextId, entry.id + 1);
            if (sawHeader && !sawSeal) { snapshotRows++; checksum.add(rawLine); }
        }
        recordsRead++;
    }
    if (sawHeader && !sawSeal) throw new JournalRecoveryError("INCOMPLETE_CHECKPOINT", "journal checkpoint is incomplete");
    const entries: JournalEntry[] = [];
    let projected = 0;
    for (const entry of loaded) {
        const destinationPending = entry.id > (acknowledgements.get(entry.path) ?? 0);
        if (entry.action === "renamed" && entry.oldPath) {
            const sourcePending = entry.id > (acknowledgements.get(entry.oldPath) ?? 0);
            if (sourcePending && destinationPending) entries.push(entry);
            else if (sourcePending) entries.push({ id: entry.id, action: "deleted", path: entry.oldPath, ts: entry.ts, synced: false });
            else if (destinationPending) entries.push({ id: entry.id, action: "modified", path: entry.path, ts: entry.ts, synced: false });
        } else if (destinationPending) entries.push(entry);
        if (++projected % 256 === 0) yield;
    }
    return { entries, mutations: loaded,
        acknowledgements: [...acknowledgements].map(([path, throughId]) => ({ path, throughId })),
        nextId, generation, recordsRead, tornTail, checkpointIdentity };
}

/** Small/pure compatibility entry point. Production migration uses the
 * cooperative decoder; neither API claims streaming native legacy reads. */
export function parseJournal(raw: string): ParsedJournal {
    const steps = parseJournalSteps(raw);
    for (;;) { const step = steps.next(); if (step.done) return step.value; }
}

export async function parseJournalCooperatively(raw: string): Promise<ParsedJournal> {
    const steps = parseJournalSteps(raw);
    for (;;) {
        const step = steps.next();
        if (step.done) return step.value;
        await yieldWork();
    }
}

export function encodeJournalCheckpoint(entries: readonly JournalEntry[], nextId: number, generation: number): string {
    if (!integer(nextId) || !integer(generation)) corrupt("invalid journal checkpoint identity");
    const checksum = new JournalChecksum();
    const header = `${JSON.stringify({ op: "journal-meta", schema: 1, generation, nextId })}\n`;
    checksum.add(header);
    const lines = [header];
    for (const candidate of entries) {
        const entry = validateJournalEntry(candidate);
        if (entry.id >= nextId) corrupt("journal checkpoint watermark trails a mutation");
        const line = `${JSON.stringify(entry)}\n`;
        checksum.add(line);
        lines.push(line);
    }
    lines.push(`${JSON.stringify({ op: "journal-seal", schema: 1, generation,
        records: entries.length, bytes: checksum.bytes, crc32: checksum.hex() })}\n`);
    return lines.join("");
}
