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

export interface RootConflictOptions {
    perf?: PerfOperation;
    /** Optional independently qualified full-content reader. Never implement
     * this using only size/mtime or a remembered partial-download checkpoint. */
    verifyExisting?: (path: string, hash: string, size: number, signal?: AbortSignal) => Promise<ConflictFileVerification>;
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
            const verify = this.options.verifyExisting ?? ((path, hash, size, abort) => verifyConflictFile(this.io, this.wasm, path, hash, size, abort));
            const existing = await verify(copyPath, row.hash, row.size, signal);
            throwIfWorkAborted(signal);
            if (existing === "verified") {
                await this.intents.recordConflictCopy(identity, receipt);
                continue;
            }
            if (existing !== "missing") this.destinationError(existing);
            if (!this.io.copyFileExclusive || !this.io.listDirectory) throw new RootConflictError("CAPABILITY", "exclusive copy or bounded staging inventory unavailable");
            if (row.size >= CONFLICT_CHUNK_THRESHOLD && (!this.io.supportsNativeAppend?.() || !this.io.appendFileOwned)) {
                throw new RootConflictError("CAPABILITY", "large conflict staging requires native bounded append");
            }
            // Never reuse a stat-only partial after restart. Retain ambiguous
            // attempts for recovery, bounded per conflict before new writes.
            const key = copyPath.match(/ \(conflict sync ([a-f0-9]{64})\)(?:\.[a-zA-Z0-9]{1,16})?$/)![1];
            const directory = `${STAGING_ROOT}/${key}`;
            if (await this.io.exists(directory)) {
                const listing = await this.io.listDirectory(directory);
                if (listing.folders.length || listing.files.length >= MAX_STAGE_ATTEMPTS) throw new RootConflictError("STAGING_LIMIT", "conflict staging requires explicit recovery/cleanup");
            } else {
                await this.io.mkdir(STAGING_ROOT);
                await this.io.mkdir(directory);
            }
            const random = new Uint8Array(16); crypto.getRandomValues(random);
            const token = [...random].map(byte => byte.toString(16).padStart(2, "0")).join("");
            const stagingPath = `${directory}/${token}.part`;
            if (await this.io.exists(stagingPath)) throw new RootConflictError("STAGING_LIMIT", "new conflict staging name is occupied");
            throwIfWorkAborted(signal);
            await this.io.writeFile(stagingPath, new Uint8Array());
            let written = 0;
            const proof = await verifyConflictContent({ api: this.api, wasm: this.wasm, expectedHash: row.hash,
                expectedSize: row.size, signal, perf: this.options.perf,
                consume: async (data, offset, memory) => {
                    if (offset !== written) throw new Error("conflict staging write order differs");
                    if (row.size < CONFLICT_CHUNK_THRESHOLD) {
                        if (offset !== 0 || data.byteLength !== row.size) throw new Error("small conflict source was not one complete blob");
                        await memory.run(2 * data.byteLength + 64 * 1024, () => this.io.writeFile(stagingPath, data));
                    } else await this.io.appendFileOwned!(stagingPath, data, memory);
                    written += data.byteLength;
                } });
            if (proof.hash !== row.hash || proof.size !== row.size || written !== row.size) throw new Error("conflict staging lacks complete content proof");
            const complete = await this.io.stat(stagingPath);
            if (!complete || complete.size !== row.size) throw new Error("conflict staging write size differs");
            throwIfWorkAborted(signal);
            // Once native copy starts, settle the actual outcome even if an
            // abort/close arrives; no replacement owner may race this tail.
            try { await this.io.copyFileExclusive(stagingPath, copyPath); }
            catch (error) {
                // A rejected copy may nevertheless have completed. Only full
                // content verification can convert that ambiguity to success.
                const after = await verify(copyPath, row.hash, row.size);
                if (after !== "verified") throw error;
            }
            await this.intents.recordConflictCopy(identity, receipt);
            // Delete only the fresh internal name owned by this completed call,
            // never a visible destination or an old ambiguous staging attempt.
            try { await this.io.deleteFile(stagingPath); } catch { /* retained for explicit bounded maintenance */ }
        }
    }
    private destinationError(result: ConflictFileVerification): never {
        throw result === "reader-unavailable"
            ? new RootConflictError("READER_UNAVAILABLE", "existing large conflict copy needs a qualified bounded verifier")
            : new RootConflictError("DESTINATION_CHANGED", "existing conflict destination differs; preserving it and the pending intent");
    }
}
