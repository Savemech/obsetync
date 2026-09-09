import type { ObsetyncApi } from "./api";
import { BulkObjectKind, type BulkUploadRecord } from "./bulk-codec";
import { getHashTuning } from "./hash-runtime";
import type { PerfOperation } from "./perf-trace";
import type { WasmModule } from "./push";
import { pushGroupingByteLimit } from "./push-memory";
import { ResourceBudgetOversizedError } from "./resource-budget";
import { exportTreeChunk, type TreeChunkExportJobTree } from "./tree-chunk-export-job";
import { estimateTransportWorkset, reserveTransientScope, reserveTransientWorkset,
    transientMemorySnapshot } from "./transient-memory";
import { throwIfWorkAborted, yieldWork } from "./work-scheduler";

const INDEX_HASH_BYTES = 32;
const LOWER_HEX = "0123456789abcdef";

export interface IndexChunkHashSource {
    readonly length: number;
    hashAt(index: number): string;
}

export class CompactIndexHashSpool implements IndexChunkHashSource {
    private storage: Uint8Array | undefined;
    private count = 0;
    private sealed = false;
    private closed = false;

    constructor(readonly capacityHashes: number, private readonly onClose: () => void) {
        if (!Number.isSafeInteger(capacityHashes) || capacityHashes < 0 ||
            !Number.isSafeInteger(capacityHashes * INDEX_HASH_BYTES)) {
            throw new RangeError("index hash spool capacity must be a safe non-negative count");
        }
        this.storage = new Uint8Array(capacityHashes * INDEX_HASH_BYTES);
    }

    get length(): number { return this.count; }
    get capacityBytes(): number { return this.capacityHashes * INDEX_HASH_BYTES; }

    append(hash: string): void {
        if (this.closed || this.sealed) throw new Error("index hash spool is not writable");
        if (typeof hash !== "string" || hash.length !== INDEX_HASH_BYTES * 2 ||
            this.count >= this.capacityHashes) {
            throw new Error("invalid index hash spool append");
        }
        const storage = this.storage!;
        const offset = this.count * INDEX_HASH_BYTES;
        for (let index = 0; index < INDEX_HASH_BYTES; index++) {
            const high = LOWER_HEX.indexOf(hash[index * 2]);
            const low = LOWER_HEX.indexOf(hash[index * 2 + 1]);
            if (high < 0 || low < 0) throw new Error("index hash spool requires lowercase hexadecimal hashes");
            storage[offset + index] = high * 16 + low;
        }
        if (this.count > 0) {
            const previous = offset - INDEX_HASH_BYTES;
            let order = 0;
            for (let index = 0; index < INDEX_HASH_BYTES; index++) {
                order = storage[offset + index] - storage[previous + index];
                if (order !== 0) break;
            }
            if (order <= 0) throw new Error("index hash spool hashes must be globally increasing");
        }
        this.count++;
    }

    seal(): void {
        if (this.closed) throw new Error("index hash spool is closed");
        this.sealed = true;
    }

    hashAt(index: number): string {
        if (this.closed || !this.sealed || !Number.isSafeInteger(index) || index < 0 || index >= this.count) {
            throw new RangeError("index hash spool read is outside the sealed range");
        }
        const storage = this.storage!;
        const offset = index * INDEX_HASH_BYTES;
        let hash = "";
        for (let cursor = 0; cursor < INDEX_HASH_BYTES; cursor++) {
            const byte = storage[offset + cursor];
            hash += LOWER_HEX[byte >>> 4] + LOWER_HEX[byte & 0x0f];
        }
        return hash;
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;
        this.storage = undefined;
        this.count = 0;
        this.onClose();
    }
}

export interface CompactIndexHashSpoolAdmission {
    spool: CompactIndexHashSpool;
    /** Releases the co-admitted native sort workspace. The compact backing
     * remains charged independently until spool.close(). */
    coOwner: { release(): void };
    spoolBytes: number;
    peakAdmissionBytes: number;
}

/** Atomically admit the native sort and compact JS backing before allocating
 * either. The single reservation shrinks to the still-live owner as soon as
 * sort retirement or spool disposal completes, avoiding a release/reacquire
 * gap and avoiding upload admission waiting behind its own retired workspace. */
export async function reserveCompactIndexHashSpool(
    capacityHashes: number,
    coOwnedBytes: number,
    options: { signal?: AbortSignal } = {},
): Promise<CompactIndexHashSpoolAdmission> {
    if (!Number.isSafeInteger(capacityHashes) || capacityHashes < 0 ||
        !Number.isSafeInteger(coOwnedBytes) || coOwnedBytes < 0) {
        throw new RangeError("compact index hash admission requires safe non-negative counts");
    }
    const spoolBytes = capacityHashes * INDEX_HASH_BYTES;
    const peakAdmissionBytes = spoolBytes + coOwnedBytes;
    if (!Number.isSafeInteger(spoolBytes) || !Number.isSafeInteger(peakAdmissionBytes)) {
        throw new RangeError("compact index hash admission overflow");
    }
    const lease = peakAdmissionBytes > 0
        ? await reserveTransientWorkset(peakAdmissionBytes, options) : undefined;
    let spoolLive = spoolBytes > 0;
    let coOwnerLive = coOwnedBytes > 0;
    const reconcile = () => {
        if (!lease) return;
        const retained = (spoolLive ? spoolBytes : 0) + (coOwnerLive ? coOwnedBytes : 0);
        if (retained === 0) lease.release();
        else lease.shrinkTo(retained);
    };
    let spool: CompactIndexHashSpool;
    try {
        spool = new CompactIndexHashSpool(capacityHashes, () => {
            if (!spoolLive) return;
            spoolLive = false;
            reconcile();
        });
    } catch (error) {
        lease?.release();
        throw error;
    }
    let coReleased = !coOwnerLive;
    const coOwner = Object.freeze({ release: () => {
        if (coReleased) return;
        coReleased = true;
        coOwnerLive = false;
        reconcile();
    } });
    return { spool, coOwner, spoolBytes, peakAdmissionBytes };
}

type IndexChunkHashes = readonly string[] | IndexChunkHashSource;

function indexChunkHashAt(hashes: IndexChunkHashes, index: number): string {
    return Array.isArray(hashes)
        ? hashes[index]
        : (hashes as IndexChunkHashSource).hashAt(index);
}

/** The tree store itself is not covered here. Its immutable bytes are copied
 * into JS only after their exact lengths have admitted a bounded upload pack.
 * Never gather every needed index object into one unbounded records array. */
export async function uploadIndexChunks(
    api: Pick<ObsetyncApi, "putObjects">,
    wasm: Pick<WasmModule, "wasm_tree_chunk_byte_length" | "wasm_tree_get_chunk">,
    tree: TreeChunkExportJobTree,
    hashes: IndexChunkHashes,
    perf?: PerfOperation,
    beforeHeavyBatch?: () => Promise<void>,
    signal?: AbortSignal,
    assertCurrent?: () => void,
    /** Capacity available in the shared pool after a caller-owned compact
     * hash source. Pack estimates above this headroom must fail instead of
     * queueing behind that source forever. */
    availableCapacityBytes?: number,
): Promise<void> {
    if (!Number.isSafeInteger(hashes.length) || hashes.length < 0) {
        throw new RangeError("index hash source length must be a safe non-negative count");
    }
    if (availableCapacityBytes !== undefined &&
        (!Number.isSafeInteger(availableCapacityBytes) || availableCapacityBytes < 1)) {
        throw new RangeError("index upload available capacity must be a positive safe integer");
    }
    const assertOwner = () => {
        throwIfWorkAborted(signal);
        assertCurrent?.();
        // A caller ownership hook may itself cancel the operation.
        throwIfWorkAborted(signal);
    };
    const cooperate = async () => {
        assertOwner();
        await beforeHeavyBatch?.();
        assertOwner();
        await yieldWork({ perf, signal });
        assertOwner();
    };
    for (let cursor = 0; cursor < hashes.length;) {
        await cooperate();
        assertOwner();
        const tuning = getHashTuning();
        const capacity = availableCapacityBytes ?? transientMemorySnapshot().capacityBytes;
        const maxBytes = pushGroupingByteLimit(tuning, capacity);
        const lengths: number[] = [];
        // Only one bounded pack is detached. Never reread a caller-owned hash
        // slot after an export yield and attach it to another object's bytes.
        const packHashes: string[] = [];
        let sourceBytes = 0;
        let end = cursor;
        while (end < hashes.length && lengths.length < tuning.maxBatchFiles) {
            const hash = indexChunkHashAt(hashes, end);
            const bytes = wasm.wasm_tree_chunk_byte_length(tree, hash);
            if (bytes === undefined || !Number.isSafeInteger(bytes) || bytes < 0) {
                throw new Error("needed index object is absent from the candidate store");
            }
            if (lengths.length > 0 && sourceBytes + bytes > maxBytes) break;
            lengths.push(bytes);
            packHashes.push(hash);
            sourceBytes += bytes;
            end++;
            if (sourceBytes >= maxBytes) break;
        }
        const estimate = {
            // Rust export clone + JS bridge copy, not a second tree snapshot.
            ownerBytes: 2 * sourceBytes + 64 * 1024,
            workBytes: estimateTransportWorkset(sourceBytes + lengths.length * 42 + 10),
        };
        const estimatedBytes = estimate.ownerBytes + estimate.workBytes;
        if (!Number.isSafeInteger(estimatedBytes) || estimatedBytes > capacity) {
            throw new ResourceBudgetOversizedError(estimatedBytes, capacity);
        }
        let memory;
        try { memory = await reserveTransientScope(estimate, { signal }); }
        catch (error) {
            if (error instanceof ResourceBudgetOversizedError &&
                transientMemorySnapshot().capacityBytes < capacity) continue;
            // An oversized immutable index object cannot be redefined/split.
            // Keep the candidate unpublished; durable work remains pending.
            throw error;
        }
        const records: BulkUploadRecord[] = [];
        try {
            assertOwner();
            for (let i = 0; i < lengths.length; i++) {
                // The export driver yields between pages, not before its first
                // page. Small objects also need a host turn between exports.
                if (i > 0) {
                    await cooperate();
                    assertOwner();
                }
                assertOwner();
                const hash = packHashes[i];
                const data = await exportTreeChunk(tree, hash, lengths[i], {
                    cooperate, signal, assertCurrent: assertOwner,
                    legacy: () => wasm.wasm_tree_get_chunk(tree, hash),
                });
                assertOwner();
                if (!data || data.byteLength !== lengths[i] || data.buffer.byteLength > lengths[i]) {
                    throw new Error("candidate index object changed after memory admission");
                }
                records.push({ kind: BulkObjectKind.IndexChunk, hash, data });
            }
            assertOwner();
            await api.putObjects(records, perf, memory);
            // Do not race the ACK with cancellation: the actual request owns
            // these bytes until it settles, even when this fence then fails.
            assertOwner();
            cursor = end;
        } finally {
            records.length = 0;
            memory.close();
        }
    }
}
