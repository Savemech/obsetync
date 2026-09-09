import { throwIfWorkAborted } from "./work-scheduler";
import type { MobilePreparedTransferPlan, PreparedManifest, PreparedMobileSource } from "./transfer-plan";
import type { SyncMemoryLease } from "./sync-memory-arbiter";

type ChunkManifest = PreparedManifest["manifest"];

export class PreparedMobileManifestError extends Error {
    constructor(readonly stage: "validation" | "applicability" | "retention", readonly cause?: unknown) {
        super(`Prepared mobile manifest failed during ${stage}`);
        this.name = "PreparedMobileManifestError";
    }
}

export interface PreparedMobileManifestOptions {
    plan: MobilePreparedTransferPlan;
    scopeHash: string;
    path: string;
    journalThroughId: number;
    baseRoot?: string | null;
    source: PreparedMobileSource;
    /** MUST hash every current source byte; metadata/resource identity is not authority. */
    hashCurrent(): Promise<string>;
    /** Transfers ownership of a freshly chunked current-source manifest. */
    prepareCurrent(): Promise<ChunkManifest>;
    assertApplicable(): void;
    signal?: AbortSignal;
}

export interface PreparedMobileManifestResolution {
    manifest: ChunkManifest;
    mutationId: number;
    reusedPrepared: boolean;
    /** Owns the detached O(chunks) result until upload/consumer settlement. */
    memoryOwner?: SyncMemoryLease;
}

const hash = (value: unknown): value is string => typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
function sameSource(left: PreparedManifest["source"], right: PreparedMobileSource): boolean {
    return left.fingerprint.kind === "mobile-resource-v1" &&
        left.size === right.size && left.mtime === right.mtime &&
        left.fingerprint.size === right.fingerprint.size && left.fingerprint.mtime === right.fingerprint.mtime &&
        left.fingerprint.resourceVersion === right.fingerprint.resourceVersion;
}

/**
 * Restart-safe prepared-manifest resolver. A persisted record is only a
 * FastCDC layout hint: exact scope/generation/source fences are checked first,
 * then the entire current source is hashed before any reuse. Hash equality
 * skips rechunking, not source reads. Mismatch leaves the old durable record as
 * forensic evidence until an atomic CAS replacement commits.
 */
export async function resolvePreparedMobileManifest(
    options: PreparedMobileManifestOptions,
): Promise<PreparedMobileManifestResolution> {
    const { plan, scopeHash, path, journalThroughId, baseRoot, source, signal } = options;
    const check = () => {
        throwIfWorkAborted(signal);
        try { options.assertApplicable(); }
        catch (error) { throw new PreparedMobileManifestError("applicability", error); }
        throwIfWorkAborted(signal);
    };
    const retention = async <T>(work: () => Promise<T>): Promise<T> => {
        try { return await work(); }
        catch (error) {
            if ((error as Error)?.name === "AbortError") throw error;
            throw new PreparedMobileManifestError("retention", error);
        }
    };
    if (!Number.isSafeInteger(journalThroughId) || journalThroughId < 1 ||
        source.fingerprint.kind !== "mobile-resource-v1" || !hash(source.fingerprint.resourceVersion)) {
        throw new PreparedMobileManifestError("validation");
    }
    if (plan.snapshot().managedResidentBytes === 0) {
        throw new PreparedMobileManifestError("retention", new Error("prepared store is not memory-arbitrated"));
    }
    check();
    let candidate = await retention(() => plan.lookupAdmitted(scopeHash, path, signal));
    try {
        check();
        const reusable = candidate && candidate.value.journalThroughId === journalThroughId &&
            candidate.value.baseRoot === baseRoot && sameSource(candidate.value.source, source);
        if (reusable) {
            const actualHash = await options.hashCurrent();
            check();
            if (!hash(actualHash)) throw new PreparedMobileManifestError("validation");
            if (actualHash === candidate!.value.manifest.file_hash) {
                const memoryOwner = candidate!.memoryOwner; candidate!.memoryOwner = undefined;
                return { manifest: candidate!.value.manifest, mutationId: candidate!.value.mutationId,
                    reusedPrepared: true, ...(memoryOwner ? { memoryOwner } : {}) };
            }
        }
        const expectedMutationId = candidate?.value.mutationId ?? null;
        candidate?.memoryOwner?.release(); candidate = null;
        const manifest = await options.prepareCurrent();
        check();
        const retained = await retention(() => plan.retain({ scopeHash, path, journalThroughId, baseRoot, source, manifest,
            expectedMutationId }));
        check();
        if (!retained.retained) throw new PreparedMobileManifestError("retention");
        const installed = await retention(() => plan.lookupAdmitted(scopeHash, path, signal));
        if (!installed || installed.value.mutationId !== retained.mutationId) {
            installed?.memoryOwner?.release();
            throw new PreparedMobileManifestError("retention");
        }
        try {
            check();
            const memoryOwner = installed.memoryOwner; installed.memoryOwner = undefined;
            return { manifest: installed.value.manifest, mutationId: retained.mutationId,
                reusedPrepared: false, ...(memoryOwner ? { memoryOwner } : {}) };
        } finally { installed.memoryOwner?.release(); }
    } finally { candidate?.memoryOwner?.release(); }
}
