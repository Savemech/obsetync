import { reserveTransientScope, type TransientReservationBudget, type TransientWorkScope } from "./transient-memory";
import { throwIfWorkAborted } from "./work-scheduler";

export const TREE_ROOT_EXPORT_PAGE_BYTES = 64 * 1024;
export const TREE_ROOT_EXPORT_MAX_STEP_UNITS = 4096;
export const TREE_ROOT_EXPORT_MAX_STEP_BYTES = 16 * 1024 * 1024;
export const TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES = TREE_ROOT_EXPORT_PAGE_BYTES + 128;
const TREE_ROOT_EXPORT_MIN_ATOMIC_BYTES = 256;
export type TreeRootExportMode = "candidate" | "committed";

export interface TreeRootExportJobTree {
    begin_candidate_root_export_job?: (maxArenaBytes: number) => unknown;
    begin_committed_root_export_job?: (maxArenaBytes: number) => unknown;
    /** Old shared jobs do not establish root byte-step support. Never called. */
    step_tree_job?: (token: number, maxUnits: number) => unknown;
    step_root_export_job?: (token: number, maxUnits: number, maxBytes: number) => unknown;
    cancel_tree_job?: (token: number) => void;
    root_export_workset?: (token: number) => unknown;
    start_root_export_build_job?: (token: number) => void;
    root_export_info?: (token: number) => unknown;
    read_root_export_job?: (token: number, offset: number, maxBytes: number) => unknown;
    finish_root_export_job?: (token: number) => void;
}

export interface ExportTreeRootOptions {
    /** A real host task yield, including the caller's current visibility gate. */
    cooperate(): Promise<void>;
    signal?: AbortSignal;
    assertCurrent?(): void;
    /** Native primitive limit per host turn; integer 1..4096, default 4096. */
    stepUnits?: number;
    /** Scheduled work/CPU charge per host turn, NOT native bytes touched;
     * integer 1..16 MiB, default 64 KiB. A first indivisible primitive may
     * exceed this only within MAX_ATOMIC_BYTES. */
    stepBytes?: number;
    expectedHash: string;
    expectedVersion: 1 | 2;
    /** Native Plan must enforce this arena ceiling incrementally; positive
     * u32, default u32 max. This is distinct from the sealed-output limit. */
    maxArenaBytes?: number;
    /** Optional actual serialized-output cap, checked at seal or legacy return. */
    maxOutputBytes?: number;
    /** Optional already-admitted consumer quota; no transport/digest allowance
     * is inferred. Defaults to the scope API's minimum positive byte. */
    workBytes?: number | ((maxLength: number) => number);
    /** Semantic root parser, NOT a content hash of serialized root bytes. */
    parse(bytes: Uint8Array): { hash: string; version: number };
    /** The selected old getter must return its already detached JS-owned copy.
     * Its allocation necessarily precedes admission; no extra copy is made. */
    legacy(mode: TreeRootExportMode): unknown;
    budget?: TransientReservationBudget;
}

export interface OwnedTreeRootExport {
    /** Borrow until release. The caller must drop all escaped references too. */
    readonly bytes: Uint8Array;
    /** Export owners plus the caller-selected reusable work quota are covered;
     * no digest/transport/conflict allowance is inferred automatically. */
    readonly memory: TransientWorkScope;
    /** Idempotent; call after all borrowers are done. */
    release(): void;
}

const u32 = (value: unknown, minimum = 0): value is number =>
    Number.isSafeInteger(value) && (value as number) >= minimum && (value as number) <= 0xffff_ffff;
const hash = (value: unknown): value is string => typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
const byteLengthOf = Object.getOwnPropertyDescriptor(typedArrayPrototype, "byteLength")!.get!;
const byteOffsetOf = Object.getOwnPropertyDescriptor(typedArrayPrototype, "byteOffset")!.get!;
const bufferOf = Object.getOwnPropertyDescriptor(typedArrayPrototype, "buffer")!.get!;
const bufferLengthOf = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "byteLength")!.get!;

function exactRecord(value: unknown, keys: readonly string[]): Record<string, unknown> {
    if (!value || typeof value !== "object") throw new TypeError("root export returned an invalid descriptor");
    const fields = Object.getOwnPropertyDescriptors(value);
    if (Reflect.ownKeys(fields).length !== keys.length || keys.some(key => !fields[key] || !("value" in fields[key]))) {
        throw new TypeError("root export returned an invalid descriptor");
    }
    return Object.fromEntries(keys.map(key => [key, fields[key].value]));
}

function exactBytes(value: unknown, expected?: number): Uint8Array {
    if (!(value instanceof Uint8Array)) throw new TypeError("root export returned invalid bytes");
    const buffer: unknown = bufferOf.call(value), length: number = byteLengthOf.call(value);
    if (!(buffer instanceof ArrayBuffer) || byteOffsetOf.call(value) !== 0 ||
        bufferLengthOf.call(buffer) !== length || (expected !== undefined && length !== expected)) {
        throw new TypeError("root export returned invalid byte length or backing");
    }
    return value;
}

interface RootExportProgress { done: boolean; units: number; bytes: number; completed: number; processed: number }

function progress(value: unknown, previous: RootExportProgress | undefined, stepUnits: number, stepBytes: number): RootExportProgress {
    const row = exactRecord(value, ["done", "units", "bytes", "completed", "processed"]);
    for (const field of ["units", "bytes", "completed", "processed"] as const) {
        if (!Number.isSafeInteger(row[field]) || (row[field] as number) < 0) throw new TypeError("root export returned invalid progress");
    }
    const result = row as unknown as RootExportProgress;
    if (typeof result.done !== "boolean" || result.units > stepUnits ||
        (!result.done && result.units === 0) || ((result.units === 0) !== (result.bytes === 0)) ||
        result.bytes < result.units * TREE_ROOT_EXPORT_MIN_ATOMIC_BYTES ||
        result.bytes > result.units * TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES ||
        (result.bytes > stepBytes && (result.units !== 1 || result.bytes > TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES)) ||
        result.completed !== (previous?.completed ?? 0) + result.units ||
        result.processed !== (previous?.processed ?? 0) + result.bytes) {
        throw new TypeError("root export progress is inconsistent");
    }
    return result;
}

/** Native max + offset metadata + final JS max + both bridge page copies.
 * The 1-byte unused child is required by the existing positive-only scope API.
 * After finish, the native allowance is reusable for the parser input copy.
 * This is controlled byte accounting, not parsed-metadata/allocator/heap/RSS
 * measurement. */
export function estimateTreeRootExportWorkset(maxLength: number, offsetBytes: number, workBytes = 1): { ownerBytes: number; workBytes: number } {
    if (!u32(maxLength, 1) || !u32(offsetBytes) || !Number.isSafeInteger(workBytes) || workBytes < 1) throw new RangeError("invalid root export workset");
    const ownerBytes = 2 * maxLength + offsetBytes + 2 * TREE_ROOT_EXPORT_PAGE_BYTES;
    if (!Number.isSafeInteger(ownerBytes + workBytes)) throw new RangeError("root export workset exceeds safe byte accounting");
    return { ownerBytes, workBytes };
}

function cleanupFailure(primary: unknown, cleanup: unknown): void {
    if ((typeof primary === "object" && primary !== null) || typeof primary === "function") {
        try {
            const target = primary as { treeCandidateCleanupErrors?: unknown[] };
            const failures = target.treeCandidateCleanupErrors ?? [];
            failures.push(cleanup);
            Object.defineProperty(target, "treeCandidateCleanupErrors", { configurable: true, value: failures });
        } catch { /* Preserve even a frozen primary failure. */ }
    }
    try { console.error("[obsetync] root export cleanup failed; native ownership requires retirement:", cleanup); }
    catch { /* Diagnostics cannot replace the primary failure. */ }
}

/** Export the explicitly selected root without changing history or tree state.
 * Plan is metadata-only. Build and all page copies start only after admission.
 * No nested global reservation is made by the helper. Returned memory does NOT
 * authorize allocating a root digest, intent, transport or conflict workset.
 * Matching-token native cancellation is allocation-free. A broken/synthetic
 * cancel is a poisoned-tree residual: report it without masking the primary
 * error or permanently stranding the shared accounting pool.
 */
export async function exportTreeRoot(
    tree: TreeRootExportJobTree, mode: TreeRootExportMode, options: ExportTreeRootOptions,
): Promise<OwnedTreeRootExport> {
    if ((mode !== "candidate" && mode !== "committed") || !options ||
        typeof options.cooperate !== "function" || typeof options.parse !== "function" || typeof options.legacy !== "function" ||
        (options.assertCurrent !== undefined && typeof options.assertCurrent !== "function") ||
        !hash(options.expectedHash) || ![1, 2].includes(options.expectedVersion) ||
        (options.maxArenaBytes !== undefined && !u32(options.maxArenaBytes, 1)) ||
        (options.maxOutputBytes !== undefined && !u32(options.maxOutputBytes, 1)) ||
        (options.workBytes !== undefined && typeof options.workBytes !== "function" &&
            (!Number.isSafeInteger(options.workBytes) || options.workBytes < 1))) {
        throw new TypeError("root export requires a mode, identity, bound and host callbacks");
    }
    const requestedStepUnits = options.stepUnits;
    const stepUnits = requestedStepUnits === undefined ? TREE_ROOT_EXPORT_MAX_STEP_UNITS : requestedStepUnits;
    if (!Number.isSafeInteger(stepUnits) || stepUnits < 1 || stepUnits > TREE_ROOT_EXPORT_MAX_STEP_UNITS) {
        throw new TypeError("root export stepUnits must be an integer from 1 to 4096");
    }
    const requestedStepBytes = options.stepBytes;
    const stepBytes = requestedStepBytes === undefined ? TREE_ROOT_EXPORT_PAGE_BYTES : requestedStepBytes;
    if (!Number.isSafeInteger(stepBytes) || stepBytes < 1 || stepBytes > TREE_ROOT_EXPORT_MAX_STEP_BYTES) {
        throw new TypeError("root export stepBytes must be an integer from 1 to 16 MiB");
    }
    const { cooperate, signal, assertCurrent: assertOwner, parse, legacy, expectedHash, expectedVersion, budget } = options;
    const maxLength = options.maxOutputBytes ?? 0xffff_ffff, maxArenaBytes = options.maxArenaBytes ?? 0xffff_ffff;
    const workOption = options.workBytes ?? 1;
    const check = () => { throwIfWorkAborted(signal); assertOwner?.(); throwIfWorkAborted(signal); };
    check();
    const candidate = tree.begin_candidate_root_export_job, committed = tree.begin_committed_root_export_job;
    const workset = tree.root_export_workset, start = tree.start_root_export_build_job, info = tree.root_export_info;
    const read = tree.read_root_export_job, finish = tree.finish_root_export_job;
    const step = tree.step_root_export_job, cancel = tree.cancel_tree_job;
    const specific = [candidate, committed, step, workset, start, info, read, finish];
    const claimed = specific.some(method => method !== undefined);
    if (claimed && ![...specific, cancel].every(method => typeof method === "function")) {
        throw new TypeError("WASM tree exposes an incomplete root export job API");
    }
    const validateIdentity = (bytes: Uint8Array) => {
        const identity = exactRecord(parse(bytes), ["hash", "version"]);
        if (identity.hash !== expectedHash || identity.version !== expectedVersion) throw new Error("exported root identity differs from its captured owner");
        check();
    };
    const cooperateAndCheck = async () => { check(); await cooperate(); check(); };
    const consumerQuota = (length: number) => {
        check();
        const quota = typeof workOption === "function" ? workOption(length) : workOption;
        check();
        if (!Number.isSafeInteger(quota) || quota < 1) throw new RangeError("invalid root export consumer quota");
        return quota;
    };
    let memory: TransientWorkScope | undefined, token: number | undefined;
    let jobOpen = false, transferred = false;
    let primary: unknown, bytes: Uint8Array | undefined;
    try {
        await cooperateAndCheck(); check();
        if (!claimed) {
            bytes = exactBytes(legacy(mode)); check();
            const length = byteLengthOf.call(bytes) as number;
            if (!u32(length, 1) || length > maxLength) throw new RangeError("legacy root exceeds its byte bound");
            // The legacy native temporary has already been freed by glue;
            // reserve the returned JS buffer plus the semantic parser input.
            memory = await reserveTransientScope({ ownerBytes: 2 * length, workBytes: consumerQuota(length) }, { signal, budget }); check();
            validateIdentity(bytes);
        } else {
            const rawToken = (mode === "candidate" ? candidate! : committed!).call(tree, maxArenaBytes);
            if (!u32(rawToken, 1)) throw new TypeError("root export returned an invalid token");
            token = rawToken; jobOpen = true; check();
            const completePhase = async () => {
                let previous: RootExportProgress | undefined;
                do {
                    await cooperateAndCheck(); check();
                    let raw: unknown;
                    try { raw = step!.call(tree, token!, stepUnits, stepBytes); }
                    catch (error) {
                        // The native dispatcher drops a provisional job when
                        // its Plan/Build step fails. Do not issue a misleading
                        // cancel against an owner that no longer exists.
                        jobOpen = false;
                        throw error;
                    }
                    previous = progress(raw, previous, stepUnits, stepBytes);
                    check();
                } while (!previous.done);
            };
            await completePhase(); check();
            const plan = exactRecord(workset!.call(tree, token), ["max_length", "offset_bytes"]); check();
            if (!u32(plan.max_length, 1) || plan.max_length > maxArenaBytes || !u32(plan.offset_bytes)) throw new RangeError("root export plan exceeds its byte bound");
            memory = await reserveTransientScope(estimateTreeRootExportWorkset(plan.max_length, plan.offset_bytes, consumerQuota(plan.max_length)), { signal, budget }); check();
            start!.call(tree, token); check();
            await completePhase(); check(); // Build counters start at zero, independently of Plan.
            const sealed = exactRecord(info!.call(tree, token), ["length", "version", "hash"]); check();
            if (!u32(sealed.length, 1) || sealed.length > plan.max_length || sealed.length > maxLength ||
                sealed.version !== expectedVersion || sealed.hash !== expectedHash) {
                throw new Error("root export sealed length or identity differs from its captured owner");
            }
            bytes = new Uint8Array(sealed.length);
            for (let offset = 0; offset < bytes.length;) {
                check();
                const count = Math.min(TREE_ROOT_EXPORT_PAGE_BYTES, bytes.length - offset);
                { const page = exactBytes(read!.call(tree, token, offset, TREE_ROOT_EXPORT_PAGE_BYTES), count); bytes.set(page, offset); }
                offset += count; check();
                if (offset < bytes.length) { await cooperateAndCheck(); check(); }
            }
            finish!.call(tree, token); jobOpen = false; check();
            // Release native serialized output BEFORE a parser copies the
            // complete input into WASM; both stages reuse the native allowance.
            validateIdentity(bytes);
        }
        const ownedMemory = memory!;
        let ownedBytes: Uint8Array | undefined = bytes;
        const owned: OwnedTreeRootExport = Object.freeze({
            get bytes() { if (!ownedBytes) throw new Error("root export has been released"); return ownedBytes; },
            memory: ownedMemory,
            release() { ownedBytes = undefined; ownedMemory.close(); },
        });
        bytes = undefined; transferred = true; return owned;
    } catch (error) {
        primary = error; throw error;
    } finally {
        if (jobOpen) {
            try { cancel!.call(tree, token!); }
            catch (cleanup) { cleanupFailure(primary, cleanup); }
        }
        bytes = undefined;
        if (!transferred) memory?.close();
    }
}
