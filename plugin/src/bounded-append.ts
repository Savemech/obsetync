import { exactArrayBuffer } from "./binary";
import type { FileStat } from "./file-safety";
import { reserveTransientWorkset, type TransientWorkScope } from "./transient-memory";
import { ResourceBudgetOversizedError } from "./resource-budget";
import { throwIfWorkAborted } from "./work-scheduler";

const LEGACY_APPEND_LIMIT = 32 * 1024 * 1024;
const OVERHEAD = 64 * 1024;

export interface BinaryAppendAdapter {
    readBinary(path: string): Promise<ArrayBuffer>;
    writeBinary(path: string, data: ArrayBuffer): Promise<unknown>;
    appendBinary?: (path: string, data: ArrayBuffer) => Promise<unknown>;
}

export class AppendPrefixChangedError extends Error {
    constructor() {
        super("staging prefix changed during bounded append; existing bytes preserved");
        this.name = "AppendPrefixChangedError";
    }
}

/** Additional JS/bridge workset; the input is already owned by the caller.
 * Legacy adapters read the full prefix and construct a replacement. This is
 * deliberately not a claim that their native allocator or process RSS is capped. */
export function estimateAppendWorkset(dataBytes: number, prefixBytes?: number): number {
    if (!Number.isSafeInteger(dataBytes) || dataBytes < 0 ||
        (prefixBytes !== undefined && (!Number.isSafeInteger(prefixBytes) || prefixBytes < 0))) {
        throw new RangeError("append sizes must be non-negative safe integers");
    }
    const bytes = prefixBytes === undefined
        ? 2 * dataBytes + OVERHEAD
        : 3 * prefixBytes + 2 * (prefixBytes + dataBytes) + OVERHEAD;
    if (!Number.isSafeInteger(bytes)) throw new RangeError("append workset exceeds safe integer range");
    return bytes;
}

/** Reserve before any prefix read/copy. Native append is selected by the real
 * adapter capability, never inferred from the platform name. A failed admission
 * or read leaves the checkpointed prefix untouched for a later safe retry. */
export async function appendBinaryBounded(
    adapter: BinaryAppendAdapter,
    statFile: (path: string) => Promise<FileStat | null>,
    path: string,
    data: Uint8Array,
    options: { memory?: TransientWorkScope; signal?: AbortSignal } = {},
): Promise<void> {
    throwIfWorkAborted(options.signal);
    const nativeAppend = typeof adapter.appendBinary === "function";
    const initialStat = nativeAppend ? undefined : await statFile(path);
    const stat = initialStat ? { ...initialStat } : initialStat;
    const prefixBytes = stat?.size ?? 0;
    if (!nativeAppend && prefixBytes + data.byteLength > LEGACY_APPEND_LIMIT) {
        throw new ResourceBudgetOversizedError(prefixBytes + data.byteLength, LEGACY_APPEND_LIMIT);
    }
    const workBytes = estimateAppendWorkset(data.byteLength, nativeAppend ? undefined : prefixBytes);
    const verifyPrefixMetadata = async (): Promise<void> => {
        const current = await statFile(path);
        if ((!stat) !== (current === null) ||
            (stat && current && (stat.size !== current.size || stat.mtime !== current.mtime))) {
            throw new AppendPrefixChangedError();
        }
    };
    const append = async (): Promise<void> => {
        throwIfWorkAborted(options.signal);
        if (nativeAppend) {
            // Await the real native promise even when the signal fires meanwhile.
            await adapter.appendBinary!(path, exactArrayBuffer(data));
            return;
        }
        // Admission may have waited behind other work. Reject stale/missing-
        // then-created prefixes before a read, and recheck after its real native
        // completion before allocating the replacement. Public adapters cannot
        // make the final stat/write pair atomic against an external writer.
        await verifyPrefixMetadata();
        throwIfWorkAborted(options.signal);
        const previous = stat ? new Uint8Array(await adapter.readBinary(path)) : new Uint8Array();
        throwIfWorkAborted(options.signal);
        // stat/read cannot cap a racing native allocation. Detect growth before
        // allocating the replacement, and never overwrite a changed prefix.
        if (previous.byteLength !== prefixBytes || previous.buffer.byteLength > prefixBytes) {
            throw new AppendPrefixChangedError();
        }
        await verifyPrefixMetadata();
        throwIfWorkAborted(options.signal);
        const combined = new Uint8Array(prefixBytes + data.byteLength);
        combined.set(previous);
        combined.set(data, prefixBytes);
        await adapter.writeBinary(path, combined.buffer);
    };
    if (options.memory) {
        // Do not call from inside another run on the same quota: append owns
        // this stage independently from the preceding chunk hash stage.
        await options.memory.run(workBytes, append, { signal: options.signal });
    } else {
        // Compatibility callers do not supply an owned input scope. Include
        // their retained backing buffer in the same complete admission.
        const lease = await reserveTransientWorkset(workBytes + data.buffer.byteLength, options);
        try { await append(); }
        finally { lease.release(); }
    }
}
