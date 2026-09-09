import { isMissingPathError } from "./file-safety";
import { checksumJournalUtf8, JournalRecoveryError, parseJournalCooperatively, type ParsedJournal } from "./journal-format";
import { fingerprintLegacyCopies } from "./legacy-source";
import type { SegmentedStoreIO } from "./segmented-store";

const main = ".obsidian/plugins/obsetync/change-journal.ndjson";
export const JOURNAL_PATHS = { main, next: `${main}.next`, backup: `${main}.bak` } as const;
const LEGACY_READ_LIMIT = 8 * 1024 * 1024;
interface Copy { path: string; raw: string; parsed: ParsedJournal }

/** A bounded, cooperative one-time import, with originals left untouched.
 * This is not a native streaming importer or a repair-by-filename heuristic. */
export async function importLegacyJournal(io: SegmentedStoreIO): Promise<{
    parsed: ParsedJournal; sourceSha256: string;
}> {
    const copies: Copy[] = [];
    const sources: Array<{ role: string; raw: string | null }> = [];
    let incompleteStage = false;
    for (const [role, path] of Object.entries(JOURNAL_PATHS)) {
        const raw = await readLegacy(io, path);
        sources.push({ role, raw });
        if (raw === null) continue;
        try {
            const parsed = await parseJournalCooperatively(raw);
            if (path === JOURNAL_PATHS.next && !parsed.checkpointIdentity) {
                incompleteStage = true; continue;
            }
            // A checkpoint stage is immutable before promotion. An appended
            // torn suffix there cannot be explained as a normal journal tail.
            if (path === JOURNAL_PATHS.next && parsed.tornTail) {
                throw new JournalRecoveryError("CORRUPT", "legacy checkpoint stage has an impossible appended tail");
            }
            copies.push({ path, raw, parsed });
        } catch (error) {
            if (path === JOURNAL_PATHS.next && error instanceof JournalRecoveryError &&
                error.code === "INCOMPLETE_CHECKPOINT") { incompleteStage = true; continue; }
            throw error;
        }
    }
    let selected: Copy | undefined;
    for (let index = 0; index < copies.length; index++) {
        const copy = copies[index];
        for (let previous = 0; previous < index; previous++) {
            const peer = copies[previous];
            if (copy.parsed.generation !== peer.parsed.generation) continue;
            if (copy.parsed.checkpointIdentity !== peer.parsed.checkpointIdentity ||
                (!copy.raw.startsWith(peer.raw) && !peer.raw.startsWith(copy.raw))) {
                throw new JournalRecoveryError("CONTRADICTING_COPIES", "legacy journal copies contradict one checkpoint generation");
            }
        }
        if (!selected || copy.parsed.generation > selected.parsed.generation ||
            (copy.parsed.generation === selected.parsed.generation && copy.raw.length > selected.raw.length)) selected = copy;
    }
    if (!selected && incompleteStage) {
        throw new JournalRecoveryError("RECOVERY_REQUIRED", "incomplete legacy stage has no authoritative recovery copy");
    }
    return { parsed: selected?.parsed ?? await parseJournalCooperatively(""),
        sourceSha256: await fingerprintLegacyCopies(sources) };
}

async function readLegacy(io: SegmentedStoreIO, path: string): Promise<string | null> {
    try {
        if (!(await io.exists(path))) return null;
        if (io.stat) {
            const stat = await io.stat(path);
            if (!stat || (stat.type !== undefined && stat.type !== "file") ||
                !Number.isSafeInteger(stat.size) || stat.size < 0) {
                throw new JournalRecoveryError("READ_FAILED", "legacy journal metadata is unavailable");
            }
            if (stat.size > LEGACY_READ_LIMIT) throw new JournalRecoveryError("LIMIT", "legacy journal requires bounded external migration");
        }
        const raw = await io.read(path);
        if (checksumJournalUtf8(raw).bytes > LEGACY_READ_LIMIT) throw new JournalRecoveryError("LIMIT", "legacy journal exceeds portable read limit");
        return raw;
    } catch (error) {
        if (error instanceof JournalRecoveryError) throw error;
        if (isMissingPathError(error)) {
            try { if (!(await io.exists(path))) return null; } catch { /* unavailable */ }
        }
        throw new JournalRecoveryError("READ_FAILED", "legacy journal unavailable; refusing empty recovery");
    }
}
