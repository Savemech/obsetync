import { throwIfWorkAborted } from "./work-scheduler";

export const TREE_CHUNK_EXPORT_PAGE_BYTES = 64 * 1024;

export interface TreeChunkExportJobTree {
    begin_tree_chunk_export_job?: (hash: string) => unknown;
    read_tree_chunk_export_job?: (token: number, expectedOffset: number, maxBytes: number) => unknown;
    finish_tree_chunk_export_job?: (token: number) => void;
    cancel_tree_job?: (token: number) => void;
}

export interface ExportTreeChunkOptions {
    /** A real host yield, including the caller's heavy-work/visibility gate. */
    cooperate(): Promise<void>;
    signal?: AbortSignal;
    /** Revalidates the caller's captured committed/candidate tree and policy owner. */
    assertCurrent?(): void;
    /** One synchronous legacy getter, only when all export-specific APIs are
     * absent. It MUST return the old WASM glue's detached JS-owned `.slice()`;
     * an arbitrary borrowed callback is not supported by this contract. */
    legacy(): unknown;
}

const isToken = (value: unknown): value is number =>
    Number.isSafeInteger(value) && (value as number) >= 1 && (value as number) <= 0xffff_ffff;
const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
const byteLengthOf = Object.getOwnPropertyDescriptor(typedArrayPrototype, "byteLength")!.get!;
const byteOffsetOf = Object.getOwnPropertyDescriptor(typedArrayPrototype, "byteOffset")!.get!;
const bufferOf = Object.getOwnPropertyDescriptor(typedArrayPrototype, "buffer")!.get!;
const bufferLengthOf = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "byteLength")!.get!;

function exactBytes(value: unknown, expected: number): Uint8Array {
    if (!(value instanceof Uint8Array)) throw new TypeError("tree chunk export returned invalid bytes");
    // Use intrinsic lengths: shadowed properties must not conceal an oversized
    // backing allocation. Shared buffers are not immutable native return values.
    const buffer: unknown = bufferOf.call(value);
    if (!(buffer instanceof ArrayBuffer) || byteOffsetOf.call(value) !== 0 ||
        byteLengthOf.call(value) !== expected || bufferLengthOf.call(buffer) !== expected) {
        throw new TypeError("tree chunk export returned invalid byte length or backing");
    }
    return value;
}

function reportCleanupFailure(primary: unknown, cleanup: unknown): void {
    if ((typeof primary === "object" && primary !== null) || typeof primary === "function") {
        try {
            const target = primary as { treeCandidateCleanupErrors?: unknown[] };
            const errors = target.treeCandidateCleanupErrors ?? [];
            errors.push(cleanup);
            Object.defineProperty(target, "treeCandidateCleanupErrors", { configurable: true, value: errors });
        } catch { /* Frozen/foreign errors retain the original failure identity. */ }
    }
    try { console.error("[obsetync] tree chunk export cleanup failed:", cleanup); }
    catch { /* A diagnostic failure must not mask the operation failure. */ }
}

/** Copy one already-admitted immutable index object. The caller must reserve
 * the final buffer plus native/page/transport work BEFORE calling this helper;
 * this is not a memory allocator/admission mechanism. There is no initial or
 * inter-chunk yield here: the existing pack owner supplies that preflight.
 * Only page boundaries yield, after copying and dropping the native page view.
 * The caller may own either a committed graph or a candidate. This helper
 * never opens, requires, aborts or commits a candidate; assertCurrent owns that
 * policy. Neither cancellation nor failure changes the caller-owned graph.
 * Native read and finish errors RETAIN their job and require cancel cleanup. */
export async function exportTreeChunk(
    tree: TreeChunkExportJobTree,
    hash: string,
    expectedLength: number,
    options: ExportTreeChunkOptions,
): Promise<Uint8Array> {
    if (typeof hash !== "string" || !/^[0-9a-f]{64}$/.test(hash) ||
        !Number.isSafeInteger(expectedLength) || expectedLength < 0 || expectedLength > 0xffff_ffff) {
        throw new TypeError("tree chunk export requires a canonical hash and admitted u32 length");
    }
    if (typeof options?.cooperate !== "function" || typeof options.legacy !== "function" ||
        (options.assertCurrent !== undefined && typeof options.assertCurrent !== "function")) {
        throw new TypeError("tree chunk export requires host cooperation and a fallback");
    }
    const { cooperate, signal, assertCurrent: assertOwner, legacy } = options;
    const assertCurrent = () => {
        throwIfWorkAborted(signal);
        assertOwner?.();
        throwIfWorkAborted(signal);
    };
    assertCurrent();
    const begin = tree.begin_tree_chunk_export_job, read = tree.read_tree_chunk_export_job;
    const finish = tree.finish_tree_chunk_export_job, cancel = tree.cancel_tree_job;
    const claimed = [begin, read, finish].some(method => method !== undefined);
    if (claimed && ![begin, read, finish, cancel].every(method => typeof method === "function")) {
        throw new TypeError("WASM tree exposes an incomplete tree chunk export job API");
    }
    assertCurrent();
    if (!claimed) {
        // Old wasm-bindgen glue already copies its Vec into JS and frees the
        // Rust temporary. Keep that exact owned result, with no second copy.
        const result = exactBytes(legacy(), expectedLength);
        assertCurrent();
        return result;
    }

    let token: number | undefined, jobOpen = false, failed = false, primary: unknown;
    try {
        const raw = begin!.call(tree, hash);
        if (!raw || typeof raw !== "object") throw new TypeError("tree chunk export returned an invalid start");
        const fields = Object.getOwnPropertyDescriptors(raw);
        // Capture a usable owner token before any remaining start validation.
        // A malformed response with no valid token cannot be safely cancelled.
        if (isToken(fields.token?.value)) { token = fields.token.value; jobOpen = true; }
        if (!jobOpen || Reflect.ownKeys(fields).length !== 2 || !fields.length ||
            fields.length.get || fields.length.set || fields.length.value !== expectedLength) {
            throw new TypeError("tree chunk export returned an invalid start token or length");
        }
        assertCurrent();
        const result = new Uint8Array(expectedLength);
        let offset = 0;
        while (offset < expectedLength) {
            assertCurrent();
            const count = Math.min(TREE_CHUNK_EXPORT_PAGE_BYTES, expectedLength - offset);
            // Deliberate block: no native page reference is retained across await.
            {
                const page = exactBytes(read!.call(tree, token!, offset,
                    TREE_CHUNK_EXPORT_PAGE_BYTES), count);
                result.set(page, offset);
            }
            offset += count;
            assertCurrent();
            if (offset < expectedLength) { await cooperate(); assertCurrent(); }
        }
        assertCurrent();
        finish!.call(tree, token!);
        jobOpen = false;
        assertCurrent();
        return result;
    } catch (error) {
        failed = true; primary = error; throw error;
    } finally {
        if (jobOpen) {
            try { cancel!.call(tree, token!); }
            catch (cleanup) {
                if (failed) reportCleanupFailure(primary, cleanup); else throw cleanup;
            }
        }
    }
}
