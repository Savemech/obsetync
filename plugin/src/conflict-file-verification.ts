import type { PlatformIO } from "./platform";
import type { WasmModule } from "./push";
import { openDesktopRangeReader } from "./desktop-ranged-upload";
import { withHashSource, withStreamingHashSource } from "./hash-source-budget";
import { throwIfWorkAborted, yieldWork } from "./work-scheduler";
import type { TransientReservationBudget } from "./transient-memory";

export const CONFLICT_WHOLE_READ_LIMIT = 1024 * 1024;
const FEED = 64 * 1024;
export type ConflictFileVerification = "missing" | "verified" | "different" | "reader-unavailable";
export interface ConflictFileVerificationOptions {
    /** Explicit isolated admission for tests/embedding; production uses the
     * shared source/transfer pool. It must retain leases through actual IO. */
    budget?: TransientReservationBudget;
}

/** A stat never proves existing conflict contents. Whole reads are permitted
 * only for admitted small files; desktop ranges verify their open descriptor
 * and pathname identity before/after hashing, excluding symlink/hardlink
 * aliases. A portable adapter does not expose inode/link evidence. No atomic
 * exclusion of external writers or native/host allocation cap is claimed for
 * a small DataAdapter file that grows between its fresh stat and read. */
export async function verifyConflictFile(io: PlatformIO, wasm: Pick<WasmModule, "Hasher">,
    path: string, hash: string, size: number, signal?: AbortSignal,
    options: ConflictFileVerificationOptions = {}): Promise<ConflictFileVerification> {
    if (!/^[0-9a-f]{64}$/.test(hash) || !Number.isSafeInteger(size) || size < 0) throw new Error("invalid conflict verification identity");
    throwIfWorkAborted(signal);
    const observed = await io.stat(path);
    if (!observed) return "missing";
    const before = { size: observed.size, mtime: observed.mtime };
    if (before.size !== size) return "different";
    const absolute = io.getAbsolutePath(path);
    const requireNode = typeof require === "function" ? require : (globalThis as any).require;
    let fs: typeof import("node:fs/promises") | undefined;
    if (absolute) { try { fs = requireNode?.("node:fs/promises"); } catch { /* no qualified native reader */ } }
    let actual: string;
    if (absolute) {
        // A desktop path without a qualified lstat must not silently use an
        // adapter read that follows links and calls an alias a preserved copy.
        if (!fs?.lstat) return "reader-unavailable";
        const native = await fs.lstat(absolute);
        if (!native.isFile() || native.nlink !== 1 || native.size !== size) return "different";
        const verifyIndependentPath = async (): Promise<void> => {
            const current = await fs!.lstat(absolute);
            if (!current.isFile() || current.nlink !== 1 || current.dev !== native.dev ||
                current.ino !== native.ino || current.size !== native.size ||
                current.mtimeMs !== native.mtimeMs || current.ctimeMs !== native.ctimeMs) {
                throw new Error("conflict verification pathname changed or became linked");
            }
        };
        actual = await withStreamingHashSource({ feedBytes: FEED, signal, budget: options.budget, consume: async () => {
            // Admission can wait. Detect a changed/linked pathname before
            // opening even the bounded native reader, and again before proof.
            await verifyIndependentPath();
            throwIfWorkAborted(signal);
            const reader = await openDesktopRangeReader({ absolutePath: absolute,
                fingerprint: { size: native.size, mtime: native.mtimeMs, ctime: native.ctimeMs,
                    device: native.dev, inode: native.ino } });
            let hasher: InstanceType<WasmModule["Hasher"]> | undefined;
            try {
                await reader.verify();
                hasher = new wasm.Hasher();
                for (let offset = 0; offset < size; offset += FEED) {
                    throwIfWorkAborted(signal);
                    const data = await reader.read(offset, Math.min(FEED, size - offset));
                    throwIfWorkAborted(signal);
                    hasher.update(data);
                    await yieldWork({ signal });
                }
                await reader.verify();
                await verifyIndependentPath();
                throwIfWorkAborted(signal);
                return hasher.finalize();
            } finally {
                try { await reader.close(); } finally { hasher?.free(); }
            }
        } });
    } else {
        if (size > CONFLICT_WHOLE_READ_LIMIT) return "reader-unavailable";
        actual = await withHashSource({ sourceBytes: size, feedBytes: FEED, signal, budget: options.budget,
            read: async () => {
                // This callback runs only after admission. Do not perform a
                // whole native read using metadata captured before queueing.
                const fresh = await io.stat(path);
                throwIfWorkAborted(signal);
                if (!fresh || fresh.size !== before.size || fresh.mtime !== before.mtime) {
                    throw new Error("conflict verification source changed before admitted read");
                }
                return io.readFile(path);
            }, consume: async data => {
                if (data.byteLength !== size) throw new Error("conflict verification source size changed");
                const hasher = new wasm.Hasher();
                try {
                    for (let offset = 0; offset < data.length; offset += FEED) {
                        throwIfWorkAborted(signal);
                        hasher.update(data.subarray(offset, offset + FEED));
                        await yieldWork({ signal });
                    }
                    return hasher.finalize();
                } finally { hasher.free(); }
            } });
    }
    const after = await io.stat(path);
    throwIfWorkAborted(signal);
    if (!after || after.size !== before.size || after.mtime !== before.mtime) return "different";
    return actual === hash ? "verified" : "different";
}
