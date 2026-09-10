import type { ObsetyncApi } from "./api";
import { isSafeVaultPath } from "./delta-validation";
import type { PlatformIO } from "./platform";
import type { WasmModule } from "./push";
import type { PerfOperation } from "./perf-trace";
import type { RootIntentStore } from "./root-intent";
import type { RootPublicationIdentity } from "./sync-base";
import { verifyConflictContent, CONFLICT_CHUNK_THRESHOLD } from "./verified-conflict-content";
import { verifyConflictFile, type ConflictFileVerification } from "./conflict-file-verification";
import { throwIfWorkAborted } from "./work-scheduler";

const STAGING_ROOT = ".obsidian/plugins/obsetync/conflict-staging-v1";
const MAX_STAGE_ATTEMPTS = 4;
export class RootConflictError extends Error {
    constructor(readonly code: "BUSY" | "CLOSED" | "IDENTITY" | "DESTINATION_CHANGED" |
        "READER_UNAVAILABLE" | "CAPABILITY" | "STAGING_LIMIT", message: string) {
        super(message); this.name = "RootConflictError";
    }
}

/** Stable sibling basename, independent of wall clock/device display name.
 * Full digest binds the complete operation identity, path and losing content.
 * A short conservative stem keeps this new basename below common 255-byte
 * limits without merging Unicode-normalization/case-sensitive source paths. */
export async function rootConflictCopyPath(identity: RootPublicationIdentity, path: string, hash: string): Promise<string> {
    if (!isSafeVaultPath(path) || path.length > 4096 || !/^[0-9a-f]{64}$/.test(hash) ||
        !/^[0-9a-f]{64}$/.test(identity.scopeHash) || !/^[0-9a-f]{64}$/.test(identity.requestHash) ||
        !/^[0-9a-f]{32}$/.test(identity.mutationId) || !Number.isSafeInteger(identity.sequence) || identity.sequence < 1) {
        throw new RootConflictError("IDENTITY", "invalid root conflict path identity");
    }
    const source = new TextEncoder().encode(JSON.stringify({ domain: "obsetync-conflict-copy-v1", scope: identity.scopeHash,
        sequence: identity.sequence, mutation: identity.mutationId, request: identity.requestHash, path, hash }));
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", source));
    const suffix = [...digest].map(byte => byte.toString(16).padStart(2, "0")).join("");
    const slash = path.lastIndexOf("/"), parent = path.slice(0, slash + 1), name = path.slice(slash + 1);
    const dot = name.lastIndexOf(".");
    const extension = dot > 0 && /^\.[a-zA-Z0-9]{1,16}$/.test(name.slice(dot)) ? name.slice(dot) : "";
    const sourceStem = extension ? name.slice(0, dot) : name;
    let stem = "";
    for (const character of sourceStem) {
        if (new TextEncoder().encode(stem + character).byteLength > 80) break;
        stem += character;
    }
    stem = stem.replace(/[. ]+$/, "") || "file";
    const result = `${parent}${stem} (conflict sync ${suffix})${extension}`;
    if (result.length > 4096 || !isSafeVaultPath(result)) throw new RootConflictError("IDENTITY", "conflict destination exceeds safe path bound");
    return result;
}

/** Stable local staging owner for the legacy conflict path. It is not a
 * publication identity or receipt; it only prevents unrelated paths with the
 * same content hash from sharing the retained-attempt ceiling. */
export async function legacyConflictStagingKey(path: string, hash: string): Promise<string> {
    if (!isSafeVaultPath(path) || path.length > 4096 || !/^[0-9a-f]{64}$/.test(hash)) {
        throw new RootConflictError("IDENTITY", "invalid legacy conflict staging identity");
    }
    const source = new TextEncoder().encode(JSON.stringify({
        domain: "obsetync-legacy-conflict-staging-v1",
        path,
        hash,
    }));
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", source));
    return [...digest].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

export interface RootConflictOptions {
    perf?: PerfOperation;
    /** Optional independently qualified full-content reader. Never implement
     * this using only size/mtime or a remembered partial-download checkpoint. */
    verifyExisting?: (path: string, hash: string, size: number, signal?: AbortSignal) => Promise<ConflictFileVerification>;
}

export interface VerifiedConflictCopyInput {
    path: string;
    copyPath: string;
    hash: string;
    size: number;
    /** Stable lowercase hex owner for a bounded set of retained attempts. */
    stagingKey: string;
}

export interface VerifiedConflictCopyOptions extends RootConflictOptions {
    signal?: AbortSignal;
}

/** Publish one conflict copy exclusively from immutable server objects. The
 * current local path is never a source. Large content is reconstructed one
 * verified chunk at a time through native append; partial staging remains
 * private and bounded when publication fails. */
export async function preserveVerifiedConflictCopy(
    api: Pick<ObsetyncApi, "getObjectsOwned">,
    io: PlatformIO,
    wasm: Pick<WasmModule, "Hasher">,
    input: VerifiedConflictCopyInput,
    options: VerifiedConflictCopyOptions = {},
): Promise<"existing" | "created"> {
    const { path, copyPath, hash, size, stagingKey } = input;
    const signal = options.signal;
    if (!isSafeVaultPath(path) || !isSafeVaultPath(copyPath) || !/^[0-9a-f]{64}$/.test(hash) ||
        !Number.isSafeInteger(size) || size < 0 || !/^[0-9a-f]{64}$/.test(stagingKey)) {
        throw new RootConflictError("IDENTITY", "invalid verified conflict copy input");
    }
    const verify = options.verifyExisting ?? ((target, expectedHash, expectedSize, abort) =>
        verifyConflictFile(io, wasm, target, expectedHash, expectedSize, abort));
    const existing = await verify(copyPath, hash, size, signal);
    throwIfWorkAborted(signal);
    if (existing === "verified") return "existing";
    if (existing !== "missing") {
        throw existing === "reader-unavailable"
            ? new RootConflictError("READER_UNAVAILABLE", "existing large conflict copy needs a qualified bounded verifier")
            : new RootConflictError("DESTINATION_CHANGED", "existing conflict destination differs; preserving it unchanged");
    }
    if (!io.copyFileExclusive || !io.listDirectory) {
        throw new RootConflictError("CAPABILITY", "exclusive copy or bounded staging inventory unavailable");
    }
    if (size >= CONFLICT_CHUNK_THRESHOLD && (!io.supportsNativeAppend?.() || !io.appendFileOwned)) {
        throw new RootConflictError("CAPABILITY", "large conflict staging requires native bounded append");
    }
    const directory = `${STAGING_ROOT}/${stagingKey}`;
    if (await io.exists(directory)) {
        const listing = await io.listDirectory(directory);
        if (listing.folders.length || listing.files.length >= MAX_STAGE_ATTEMPTS) {
            throw new RootConflictError("STAGING_LIMIT", "conflict staging requires explicit recovery/cleanup");
        }
    } else {
        await io.mkdir(STAGING_ROOT);
        await io.mkdir(directory);
    }
    const random = new Uint8Array(16);
    crypto.getRandomValues(random);
    const token = [...random].map(byte => byte.toString(16).padStart(2, "0")).join("");
    const stagingPath = `${directory}/${token}.part`;
    if (await io.exists(stagingPath)) {
        throw new RootConflictError("STAGING_LIMIT", "new conflict staging name is occupied");
    }
    throwIfWorkAborted(signal);
    await io.writeFile(stagingPath, new Uint8Array());
    let written = 0;
    const proof = await verifyConflictContent({
        api,
        wasm,
        expectedHash: hash,
        expectedSize: size,
        signal,
        perf: options.perf,
        consume: async (data, offset, memory) => {
            if (offset !== written) throw new Error("conflict staging write order differs");
            if (size < CONFLICT_CHUNK_THRESHOLD) {
                if (offset !== 0 || data.byteLength !== size) {
                    throw new Error("small conflict source was not one complete blob");
                }
                await memory.run(2 * data.byteLength + 64 * 1024, () => io.writeFile(stagingPath, data));
            } else {
                await io.appendFileOwned!(stagingPath, data, memory);
            }
            written += data.byteLength;
        },
    });
    if (proof.hash !== hash || proof.size !== size || written !== size) {
        throw new Error("conflict staging lacks complete content proof");
    }
    const complete = await io.stat(stagingPath);
    if (!complete || complete.size !== size) throw new Error("conflict staging write size differs");
    throwIfWorkAborted(signal);
    try {
        await io.copyFileExclusive(stagingPath, copyPath);
    } catch (error) {
        const after = await verify(copyPath, hash, size);
        if (after !== "verified") throw error;
    }
    try { await io.deleteFile(stagingPath); } catch { /* retained for explicit bounded maintenance */ }
    return "created";
}

/** Preserve only a durable accepted intent's actual losing generation. This
 * never reads the original local file, replaces a visible destination, or
 * acknowledges a journal. Receipts are persisted after real copy completion;
 * restart reuses those historical receipts without re-creating edited copies.
 * SDK/native copy behavior still needs device conformance (see ADR). */
export class RootConflictPreserver {
    private active: Promise<void> | null = null;
    private activeController: AbortController | null = null;
    private closed = false;
    private closing: Promise<void> | null = null;
    private readonly options: RootConflictOptions;
    constructor(private readonly intents: RootIntentStore, private readonly api: Pick<ObsetyncApi, "getObjectsOwned">,
        private readonly io: PlatformIO, private readonly wasm: Pick<WasmModule, "Hasher">,
        options: RootConflictOptions = {}) { this.options = { ...options }; }

    preserve(signal?: AbortSignal): Promise<void> {
        if (this.closed) return Promise.reject(new RootConflictError("CLOSED", "conflict preserver is closed"));
        if (this.active) return Promise.reject(new RootConflictError("BUSY", "conflict preservation already owns an operation"));
        const controller = new AbortController();
        const abort = () => controller.abort(signal?.reason);
        if (signal?.aborted) abort();
        else signal?.addEventListener("abort", abort, { once: true });
        let resolve!: () => void, reject!: (error: unknown) => void;
        const operation = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
        // Register before invoking a storage accessor or other injected port.
        this.active = operation;
        this.activeController = controller;
        const finish = () => {
            signal?.removeEventListener("abort", abort);
            this.active = null; this.activeController = null;
        };
        try {
            this.preserveInside(controller.signal).then(() => { finish(); resolve(); }, error => { finish(); reject(error); });
        } catch (error) { finish(); reject(error); }
        return operation;
    }
    closeAndDrain(): Promise<void> {
        if (!this.closing) {
            this.closed = true;
            this.closing = (this.active ?? Promise.resolve()).then(() => undefined, () => undefined);
            // Abort listeners run synchronously and may call close again.
            this.activeController?.abort(new RootConflictError("CLOSED", "conflict preserver is closing"));
        }
        return this.closing;
    }
    private async preserveInside(signal?: AbortSignal): Promise<void> {
        const pending = this.intents.pending();
        if (!pending?.terminal || pending.terminal.status !== "accepted") throw new RootConflictError("IDENTITY", "no accepted root owns conflict preservation");
        if (!("merged" in pending.terminal.result)) return;
        const identity = pending.intent.publication.identity;
        for (const conflict of pending.terminal.result.conflicts) {
            throwIfWorkAborted(signal);
            const row = pending.intent.publication.entries.find(entry => entry.path === conflict.path);
            if (!row || row.action !== "upsert" || row.hash !== conflict.side_b_hash) {
                throw new RootConflictError("IDENTITY", "server conflict differs from persisted local generation");
            }
            const saved = pending.conflictCopies.find(copy => copy.path === conflict.path);
            if (saved) continue;
            const copyPath = await rootConflictCopyPath(identity, conflict.path, conflict.side_b_hash);
            const receipt = { path: conflict.path, copyPath, hash: row.hash, size: row.size };
            const key = copyPath.match(/ \(conflict sync ([a-f0-9]{64})\)(?:\.[a-zA-Z0-9]{1,16})?$/)![1];
            await preserveVerifiedConflictCopy(this.api, this.io, this.wasm, {
                path: conflict.path,
                copyPath,
                hash: row.hash,
                size: row.size,
                stagingKey: key,
            }, { ...this.options, signal });
            await this.intents.recordConflictCopy(identity, receipt);
        }
    }
}
