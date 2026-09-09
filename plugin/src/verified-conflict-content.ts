import { BulkObjectKind } from "./bulk-codec";
import type { ObsetyncApi, OwnedObjects } from "./api";
import { getHashTuning } from "./hash-runtime";
import type { PerfOperation } from "./perf-trace";
import type { WasmHasher } from "./push";
import type { TransientWorkScope } from "./transient-memory";
import { throwIfWorkAborted, yieldWork } from "./work-scheduler";

// Matches FastCDC FILE_CHUNK_THRESHOLD and the existing push/pull protocols.
export const CONFLICT_CHUNK_THRESHOLD = 1_048_576;
export const CONFLICT_MAX_CHUNK_BYTES = 4 * 1024 * 1024;
export const CONFLICT_MAX_MANIFEST_BYTES = 2 * 1024 * 1024;
export const CONFLICT_MAX_CHUNKS = 16_384;
const FEED_CEILING = 64 * 1024;
const WORK_OVERHEAD = 64 * 1024;
const HASH = /^[0-9a-f]{64}$/;

export interface VerifiedConflictProof { hash: string; size: number }
export interface VerifyConflictContentOptions {
    api: Pick<ObsetyncApi, "getObjectsOwned">;
    wasm: { Hasher: new () => Pick<WasmHasher, "update" | "finalize" | "free"> };
    expectedHash: string;
    /** Exact persisted publication-row size, required even when zero. */
    expectedSize: number;
    /** Borrowed bytes remain owned until this ACTUAL IO promise settles.
     * Use memory.run for consumer copies; do not reserve the global pool again
     * or mutate/retain the bytes after returning. Treat the view as read-only
     * throughout consumption. A timeout/abort race is not native
     * completion. Partial output is untrusted until the final proof returns. */
    consume(data: Uint8Array, offset: number, memory: TransientWorkScope): Promise<void>;
    signal?: AbortSignal;
    perf?: PerfOperation;
    beforeBatch?: () => Promise<void>;
    /** Synthetic scheduler seam; production defaults to a real task boundary. */
    yieldControl?: () => Promise<void>;
}

interface ManifestChunk { hash: string; offset: number; size: number }
function invalid(message: string): never { throw new Error(`Conflict content verification: ${message}`); }
function canonicalHash(value: unknown): value is string { return typeof value === "string" && HASH.test(value); }
function single(owned: OwnedObjects, hash: string): Uint8Array {
    if (owned.objects.size !== 1 || !owned.objects.has(hash)) invalid("requested object missing or foreign objects returned");
    const data = owned.objects.get(hash)!;
    if (!(data instanceof Uint8Array)) invalid("object bytes unavailable");
    return data;
}

/** Narrow schema parser: never JSON.parse an unbounded chunk array/unknown
 * object graph. Count/layout checks precede each retained metadata row; only
 * short bounded JSON strings use JSON.parse (to support ordinary escaping).
 * Keys may be reordered, but duplicates/unknown fields and nested data fail. */
async function parseManifest(data: Uint8Array, expectedHash: string, expectedSize: number,
    cooperate: () => Promise<void>, abort: () => void): Promise<ManifestChunk[]> {
    let text: string;
    try { text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(data); }
    catch { return invalid("invalid manifest UTF-8"); }
    let position = 0;
    const space = () => {
        while (position < text.length) {
            const code = text.charCodeAt(position);
            if (code !== 32 && code !== 9 && code !== 10 && code !== 13) break;
            position++;
        }
    };
    const expect = (character: string) => { space(); if (text[position++] !== character) invalid("invalid manifest JSON"); };
    const string = (maximum: number): string => {
        space(); const start = position;
        if (text[position++] !== '"') invalid("invalid manifest string");
        while (position < text.length && position - start <= maximum * 6 + 2) {
            const character = text[position++];
            if (character === '"') {
                let value: unknown;
                try { value = JSON.parse(text.slice(start, position)); } catch { return invalid("invalid manifest string"); }
                if (typeof value !== "string" || value.length > maximum) invalid("manifest string exceeds bound");
                return value;
            }
            if (character === "\\") position++;
        }
        return invalid("manifest string exceeds bound");
    };
    const integer = (): number => {
        space(); const start = position;
        while (position < text.length && text.charCodeAt(position) >= 48 && text.charCodeAt(position) <= 57) position++;
        if (position === start || position - start > 16 || (text[start] === "0" && position - start !== 1)) {
            invalid("invalid manifest integer");
        }
        const value = Number(text.slice(start, position));
        if (!Number.isSafeInteger(value)) invalid("invalid manifest integer");
        return value;
    };
    const next = (closing: string): boolean => {
        space();
        if (text[position] === closing) { position++; return false; }
        expect(","); return true;
    };
    const chunks: ManifestChunk[] = [];
    const keys = new Set<string>();
    let fileHash: string | undefined, totalSize: number | undefined, offset = 0;
    expect("{");
    do {
        abort();
        const key = string(10);
        if (keys.has(key)) invalid("duplicate manifest field"); keys.add(key); expect(":");
        if (key === "file_hash") fileHash = string(64);
        else if (key === "total_size") totalSize = integer();
        else if (key === "chunks") {
            expect("["); space();
            if (text[position] === "]") { position++; continue; }
            do {
                abort();
                if (chunks.length >= CONFLICT_MAX_CHUNKS) invalid("manifest chunk limit exceeded");
                const fields = new Set<string>();
                let hash: string | undefined, start: number | undefined, size: number | undefined;
                expect("{");
                do {
                    const field = string(6);
                    if (fields.has(field)) invalid("duplicate manifest chunk field"); fields.add(field); expect(":");
                    if (field === "hash") hash = string(64);
                    else if (field === "offset") start = integer();
                    else if (field === "size") size = integer();
                    else invalid("unexpected manifest chunk field");
                } while (next("}"));
                if (fields.size !== 3 || !canonicalHash(hash) || start !== offset || size === undefined ||
                    size < 1 || size > CONFLICT_MAX_CHUNK_BYTES || offset + size > expectedSize) {
                    invalid("invalid manifest chunk layout");
                }
                chunks.push({ hash, offset, size }); offset += size;
                if (chunks.length % 256 === 0) await cooperate();
            } while (next("]"));
        } else invalid("unexpected manifest field");
    } while (next("}"));
    space();
    if (position !== text.length || keys.size !== 3 || fileHash !== expectedHash || totalSize !== expectedSize ||
        !chunks.length || offset !== expectedSize) invalid("manifest identity or exact file size mismatch");
    return chunks;
}

/** Fetches/verifies only the immutable losing side, never current local data.
 * Sequential one-object packs keep ownership bounded even for legal 4 MiB
 * chunks. There is no resume-by-size/stat shortcut and no destination publish.
 * The caller must join this promise on shutdown: getObjectsOwned presently
 * has no cancellation/per-request-size parameter, so its actual native request
 * must settle before we can release the returned owner or WASM state.
 *
 * API receive allocation is bounded by that API (single manifest/chunk up to
 * 4 MiB), not by our tighter manifest parse cap. Initial native requestUrl
 * allocation, fixed WASM heap and retained parsed manifest metadata remain
 * outside the transient buffer ledger. No full-file buffer is assembled. */
export async function verifyConflictContent(options: VerifyConflictContentOptions): Promise<VerifiedConflictProof> {
    const { api, wasm, expectedHash, expectedSize, consume, signal, perf } = options;
    if (!canonicalHash(expectedHash) || !Number.isSafeInteger(expectedSize) || Object.is(expectedSize, -0) ||
        expectedSize < 0 || expectedSize > CONFLICT_MAX_CHUNKS * CONFLICT_MAX_CHUNK_BYTES) {
        invalid("expected hash and exact size are required and bounded");
    }
    if (typeof api?.getObjectsOwned !== "function" || typeof wasm?.Hasher !== "function" || typeof consume !== "function") {
        invalid("owned downloader, streaming verifier and consumer are required");
    }
    const abort = () => throwIfWorkAborted(signal);
    const cooperate = async () => {
        abort();
        if (options.yieldControl) await options.yieldControl();
        else await yieldWork({ signal, perf });
        abort();
    };
    const download = async (kind: BulkObjectKind, hash: string): Promise<OwnedObjects> => {
        abort();
        await options.beforeBatch?.();
        abort();
        const end = perf?.phase("download");
        try { return await api.getObjectsOwned(kind, [hash], perf); }
        finally { end?.(); }
    };
    abort();
    const whole = new wasm.Hasher();
    try {
        const feed = async (data: Uint8Array, memory: TransientWorkScope,
            individual?: Pick<WasmHasher, "update">): Promise<void> => {
            // Source bytes already belong to the owned API parent. This child
            // covers bounded WASM feed copies, not another global reservation.
            const tuningFeed = getHashTuning().feedBytes;
            if (!Number.isSafeInteger(tuningFeed) || tuningFeed < 1) invalid("invalid feed limit");
            const feedBytes = Math.min(FEED_CEILING, tuningFeed);
            await memory.run(2 * feedBytes + WORK_OVERHEAD, async () => {
                for (let offset = 0; offset < data.byteLength; offset += feedBytes) {
                    abort();
                    const view = data.subarray(offset, Math.min(data.byteLength, offset + feedBytes));
                    const end = perf?.phase("hash");
                    try { individual?.update(view); whole.update(view); }
                    finally { end?.(); }
                    // A 4 MiB chunk must not become one uninterruptible WASM
                    // call. Yield between feeds, including on a fast machine.
                    if (offset + feedBytes < data.byteLength) await cooperate();
                }
                abort();
            }, { signal });
        };
        if (expectedSize < CONFLICT_CHUNK_THRESHOLD) {
            const owned = await download(BulkObjectKind.Content, expectedHash);
            try {
                abort();
                const data = single(owned, expectedHash);
                if (data.byteLength !== expectedSize) invalid("small object size mismatch");
                await feed(data, owned.memory);
                if (whole.finalize() !== expectedHash) invalid("whole file hash mismatch");
                abort();
                // Includes zero length: empty content still needs actual hash
                // verification and an explicitly awaited consumer operation.
                await consume(data, 0, owned.memory);
                abort();
            } finally { owned.release(); }
        } else {
            let chunks: ManifestChunk[];
            const owned = await download(BulkObjectKind.Manifest, expectedHash);
            try {
                abort();
                const data = single(owned, expectedHash);
                if (!data.byteLength || data.byteLength > CONFLICT_MAX_MANIFEST_BYTES) invalid("manifest byte limit exceeded");
                chunks = await owned.memory.run(4 * data.byteLength + WORK_OVERHEAD,
                    () => parseManifest(data, expectedHash, expectedSize, cooperate, abort), { signal });
            } finally { owned.release(); }
            // Release the manifest's global owner before another download.
            // Only its explicitly bounded metadata graph remains retained.
            let consumed = 0;
            for (const chunk of chunks) {
                const downloaded = await download(BulkObjectKind.ContentChunk, chunk.hash);
                try {
                    abort();
                    const data = single(downloaded, chunk.hash);
                    if (data.byteLength !== chunk.size) invalid("chunk size mismatch");
                    const individual = new wasm.Hasher();
                    try {
                        await feed(data, downloaded.memory, individual);
                        if (individual.finalize() !== chunk.hash) invalid("individual chunk hash mismatch");
                    } finally { individual.free(); }
                    abort();
                    await consume(data, chunk.offset, downloaded.memory);
                    consumed += data.byteLength;
                    abort();
                } finally { downloaded.release(); }
                await cooperate();
            }
            if (consumed !== expectedSize || whole.finalize() !== expectedHash) invalid("whole concatenation hash/size mismatch");
            abort();
        }
        return { hash: expectedHash, size: expectedSize };
    } finally { whole.free(); }
}
