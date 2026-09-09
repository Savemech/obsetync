import {
    MAX_FASTCDC_CHUNK_BYTES,
    type DesktopFileFingerprint,
    type HashWorkerHashResult,
    type HashWorkerManifestResult,
    type HashWorkerResult,
} from "./hash-worker-protocol";
import { throwIfWorkAborted } from "./work-scheduler";
import type { SyncMemoryArbiter, SyncMemoryLease } from "./sync-memory-arbiter";

type Manifest = HashWorkerManifestResult["manifest"];
// Versioned local producer invariant, not a general manifest wire restriction:
// blake3-fastcdc-256k-1m-4m-manifest-v1 (sync-core/fastcdc_chunker.rs).
const MIN_PREPARED_FASTCDC_CHUNK_BYTES = 256 * 1024;
export type PreparedDesktopManifestStage = "load" | "discard" | "retain" | "applicability" | "validation";

/** These are not worker runtime failures and MUST NOT license renderer
 * fallback. Native run() failures retain their original error identity. */
export class PreparedDesktopManifestError extends Error {
    readonly name = "PreparedDesktopManifestError";
    constructor(readonly stage: PreparedDesktopManifestStage, readonly cause?: unknown) {
        super(`Prepared desktop manifest failed during ${stage}`);
    }
}

export interface PreparedDesktopManifestOptions {
    expectedSize: number;
    expectedMtime: number;
    loadHint(): Promise<{ mutationId: number; manifest: Manifest } | null>;
    /** CAS: remove only this prepared mutation, never a newer replacement. */
    discardHint(mutationId: number): Promise<unknown>;
    /** Bound to the caller's original journal generation. A completed native
     * save after invalidation remains only an old-generation hint. A capacity
     * skip may return normally; this helper makes no durable-retention claim.
     * Persistence errors must reject, not masquerade as a cache miss. */
    retain(result: HashWorkerManifestResult): Promise<unknown>;
    run(mode: "hash" | "manifest"): Promise<HashWorkerResult>;
    /** Check the captured pending generation, not just size/mtime. */
    assertApplicable(): void;
    signal?: AbortSignal;
    /** Phase-A injection seam. Omission preserves the legacy unwired caller. */
    memoryArbiter?: SyncMemoryArbiter;
}

export interface PreparedManifestMemoryOwner {
    readonly bytes: number;
    /** Idempotent; release only after the detached manifest is unreachable. */
    release(): void;
}

export interface PreparedDesktopManifestResolution {
    result: HashWorkerManifestResult;
    reusedPrepared: boolean;
    /** Full-file verification costs, separate from fresh FastCDC service.
     * On reuse result.read_ms/hash_ms are zero; these fields own all work. */
    validationReadMs: number;
    validationHashMs: number;
    /** Present only when a shared arbiter was injected. Caller owns release. */
    memoryOwner?: PreparedManifestMemoryOwner;
}

/** Deterministic detached-clone workset, not measured JS heap/RSS. Canonical
 * hashes are exactly 64 ASCII code units, so their UTF16 charge is exact. */
export const PREPARED_MANIFEST_ADMISSION = {
    baseObjectBytes: 512,
    arraySlotBytes: 8,
    chunkObjectBytes: 192,
    hashUtf16Bytes: 128,
} as const;
export function preparedManifestCloneBytes(chunkCount: number): number {
    if (!Number.isSafeInteger(chunkCount) || chunkCount < 0) invalid();
    const perChunk = PREPARED_MANIFEST_ADMISSION.arraySlotBytes + PREPARED_MANIFEST_ADMISSION.chunkObjectBytes +
        PREPARED_MANIFEST_ADMISSION.hashUtf16Bytes;
    if (chunkCount > Math.floor((Number.MAX_SAFE_INTEGER - PREPARED_MANIFEST_ADMISSION.baseObjectBytes) / perChunk)) invalid();
    return PREPARED_MANIFEST_ADMISSION.baseObjectBytes + chunkCount * perChunk;
}

function invalid(): never { throw new PreparedDesktopManifestError("validation"); }
function object(value: unknown): value is Record<string, any> {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function nonnegative(value: unknown): value is number {
    return typeof value === "number" && Number.isFinite(value) && value >= 0;
}
function integer(value: unknown): value is number {
    return nonnegative(value) && Number.isSafeInteger(value);
}
function hash(value: unknown): value is string {
    return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

function data(value: Record<string, any> | any[], key: string): unknown {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !("value" in descriptor)) invalid();
    return descriptor.value;
}
interface ManifestHeader { fileHash: string; totalSize: number; chunks: any[]; count: number; bytes: number }
function manifestHeader(value: unknown): ManifestHeader {
    if (!object(value)) invalid();
    const fileHash = data(value, "file_hash"), totalSize = data(value, "total_size"), chunks = data(value, "chunks");
    if (!hash(fileHash) || !integer(totalSize) || !Array.isArray(chunks)) invalid();
    const count = data(chunks, "length");
    if (!integer(count) || count > Math.ceil(totalSize / MIN_PREPARED_FASTCDC_CHUNK_BYTES)) invalid();
    return { fileHash, totalSize, chunks, count, bytes: preparedManifestCloneBytes(count) };
}

/** Validate even a size-mismatching hint before retiring it: corruption is
 * not permission to silently discard unknown persisted evidence. */
function detachManifest(header: ManifestHeader): Manifest {
    let offset = 0;
    const chunks: Manifest["chunks"] = [];
    for (let index = 0; index < header.count; index++) {
        const chunk = data(header.chunks, String(index));
        if (!object(chunk)) invalid();
        const chunkHash = data(chunk, "hash"), chunkOffset = data(chunk, "offset"), chunkSize = data(chunk, "size");
        if (!hash(chunkHash) || !integer(chunkOffset) || chunkOffset !== offset ||
            !integer(chunkSize) || chunkSize === 0 || chunkSize > MAX_FASTCDC_CHUNK_BYTES ||
            (index < header.count - 1 && chunkSize < MIN_PREPARED_FASTCDC_CHUNK_BYTES) ||
            chunkOffset > header.totalSize - chunkSize) invalid();
        chunks.push({ hash: chunkHash, offset: chunkOffset, size: chunkSize });
        offset += chunkSize;
    }
    if (offset !== header.totalSize) invalid();
    return { file_hash: header.fileHash, total_size: header.totalSize, chunks };
}

async function admitManifest(value: unknown, arbiter: SyncMemoryArbiter | undefined,
    signal: AbortSignal | undefined, wait = true): Promise<{ manifest: Manifest; owner?: SyncMemoryLease }> {
    // Count and scalar header validation precede admission; no chunk row/index
    // is read and no O(chunks) array/object/string clone exists yet.
    const header = manifestHeader(value);
    let owner: SyncMemoryLease | undefined;
    try {
        if (arbiter) owner = await arbiter.reserve("prepared-manifest", header.bytes, { signal, wait });
        throwIfWorkAborted(signal);
        return { manifest: detachManifest(header), owner };
    } catch (error) { owner?.release(); throw error; }
}

function detachFingerprint(value: unknown, size: number, mtime: number): DesktopFileFingerprint {
    if (!object(value)) invalid();
    const actualSize = data(value, "size"), actualMtime = data(value, "mtime"), ctime = data(value, "ctime"),
        device = data(value, "device"), inode = data(value, "inode");
    if (actualSize !== size || !nonnegative(actualMtime) || Math.abs(actualMtime - mtime) > 1 ||
        !nonnegative(ctime) || !integer(device) || !integer(inode)) invalid();
    return { size, mtime: actualMtime, ctime, device, inode };
}

function detachResult(value: unknown, mode: "hash", size: number, mtime: number): HashWorkerHashResult;
function detachResult(value: unknown, mode: "manifest", size: number, mtime: number, manifest: Manifest): HashWorkerManifestResult;
function detachResult(value: unknown, mode: "hash" | "manifest", size: number, mtime: number, manifest?: Manifest): HashWorkerResult {
    if (!object(value)) invalid();
    const type = data(value, "type"), actualMode = data(value, "mode"), jobId = data(value, "job_id"),
        actualSize = data(value, "size"), actualMtime = data(value, "mtime"), readMs = data(value, "read_ms"),
        hashMs = data(value, "hash_ms"), rawFingerprint = data(value, "fingerprint");
    if (type !== "result" || actualMode !== mode || typeof jobId !== "string" || jobId.length === 0 || actualSize !== size ||
        !nonnegative(actualMtime) || Math.abs(actualMtime - mtime) > 1 || !nonnegative(readMs) || !nonnegative(hashMs)) invalid();
    const fingerprint = detachFingerprint(rawFingerprint, size, mtime);
    if (Math.abs(fingerprint.mtime - actualMtime) > 1) invalid();
    const common = { type: "result" as const, job_id: jobId, size, mtime: actualMtime,
        fingerprint, read_ms: readMs, hash_ms: hashMs };
    if (mode === "hash") {
        const resultHash = data(value, "hash");
        if (!hash(resultHash)) invalid();
        return { ...common, mode, hash: resultHash };
    }
    if (!manifest || manifest.total_size !== size) invalid();
    return { ...common, mode, manifest };
}

/** Prepared chunks are an optimization hint, never source/hash authority.
 * Even unchanged size/mtime/fingerprint requires a full current-file hash.
 * There is deliberately no remote "all objects present" shortcut here.
 *
 * Each started callback promise is awaited before cancellation is reported;
 * no Promise.race abandons pending IO. A worker may reject before native
 * termination: its caller still owns the separately tracked cleanup lifetime.
 * Applicability guards cannot roll back native writes: retain must bind to
 * the captured journal generation; upload must keep its own source checks.
 *
 * When an arbiter is injected, every detached O(chunks) clone is admitted
 * before row access/allocation. The returned clone keeps its lease until the
 * caller releases memoryOwner; the retain clone owns a separate lease only
 * through retain settlement. This remains a deterministic managed workset,
 * not a claim about worker memory, JS heap, or total process RSS. */
export async function resolvePreparedDesktopManifest(options: PreparedDesktopManifestOptions): Promise<PreparedDesktopManifestResolution> {
    const { expectedSize, expectedMtime, loadHint, discardHint, retain, run, assertApplicable, signal, memoryArbiter } = options;
    function check(): void {
        throwIfWorkAborted(signal);
        try { assertApplicable(); }
        catch (error) { throw new PreparedDesktopManifestError("applicability", error); }
        throwIfWorkAborted(signal);
    }
    async function guarded<T>(stage: "load" | "discard" | "retain" | "worker", work: () => Promise<T>): Promise<T> {
        check();
        try {
            try { return await work(); }
            catch (error) {
                if (stage === "worker" || (error as Error)?.name === "AbortError") throw error;
                throw new PreparedDesktopManifestError(stage, error);
            }
        } finally { check(); }
    }
    check();
    if (!integer(expectedSize) || !nonnegative(expectedMtime)) invalid();
    let hintOwner: SyncMemoryLease | undefined;
    let resultOwner: SyncMemoryLease | undefined;
    try {
        let loaded = await guarded("load", loadHint);
        let hint: { mutationId: number; manifest: Manifest } | null = null;
        if (loaded !== null) {
            if (!object(loaded)) invalid();
            const mutationId = data(loaded, "mutationId"), rawManifest = data(loaded, "manifest");
            if (!integer(mutationId) || mutationId === 0) invalid();
            const detached = await admitManifest(rawManifest, memoryArbiter, signal);
            hintOwner = detached.owner;
            hint = { mutationId, manifest: detached.manifest };
        }
        loaded = null;
        let validationReadMs = 0;
        let validationHashMs = 0;
        if (hint?.manifest.total_size === expectedSize) {
            const verified = detachResult(await guarded("worker", () => run("hash")), "hash", expectedSize, expectedMtime);
            validationReadMs = verified.read_ms;
            validationHashMs = verified.hash_ms;
            if (verified.hash === hint.manifest.file_hash) {
                check();
                const memoryOwner = hintOwner; hintOwner = undefined;
                return { result: { type: "result", mode: "manifest", job_id: verified.job_id,
                    manifest: hint.manifest, size: verified.size, mtime: verified.mtime,
                    fingerprint: verified.fingerprint, read_ms: 0, hash_ms: 0 },
                    reusedPrepared: true, validationReadMs, validationHashMs,
                    ...(memoryOwner ? { memoryOwner } : {}) };
            }
        }
        if (hint) {
            const mutationId = hint.mutationId;
            hint = null;
            hintOwner?.release(); hintOwner = undefined;
            await guarded("discard", () => discardHint(mutationId));
        }
        const rawResult = await guarded("worker", () => run("manifest"));
        if (!object(rawResult)) invalid();
        const detached = await admitManifest(data(rawResult, "manifest"), memoryArbiter, signal);
        resultOwner = detached.owner;
        const result = detachResult(rawResult, "manifest", expectedSize, expectedMtime, detached.manifest);
        // Keep the returned owner separate from a cache implementation retaining
        // or mutating its argument during an asynchronous native publication.
        const retained = await admitManifest(result.manifest, memoryArbiter, signal, false);
        try {
            const retainedResult = detachResult(result, "manifest", expectedSize, expectedMtime, retained.manifest);
            await guarded("retain", () => retain(retainedResult));
        } finally { retained.owner?.release(); }
        check();
        const memoryOwner = resultOwner; resultOwner = undefined;
        return { result, reusedPrepared: false, validationReadMs, validationHashMs,
            ...(memoryOwner ? { memoryOwner } : {}) };
    } finally {
        hintOwner?.release();
        resultOwner?.release();
    }
}
