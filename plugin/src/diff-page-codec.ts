import type { FileDelta } from "./api";
import { isSafeVaultPath } from "./delta-validation";

const REQUEST_MAGIC = [0x4f, 0x42, 0x51, 0x31] as const; // OBQ1
const PAGE_MAGIC = [0x4f, 0x42, 0x44, 0x31] as const; // OBD1
const CURSOR_MAGIC = [0x4f, 0x42, 0x43, 0x31] as const; // OBC1
const CURSOR_VERSION = 1;
const REQUEST_HEADER_BYTES = 78;
const PAGE_HEADER_BYTES = 74;
const HARD_MIN_PAGE_BYTES = 64 * 1024;
const HARD_MAX_PAGE_BYTES = 2 * 1024 * 1024;
const HARD_MAX_PAGE_RECORDS = 8_192;
const HARD_MAX_PATH_BYTES = 4_096;
const HARD_MAX_CURSOR_BYTES = 4 + 1 + 32 + 32 + 1 + 2 + 2 + 2 * HARD_MAX_PATH_BYTES;
const MOBILE_PAGE_BYTES = 512 * 1024;
const HASH_HEX = /^[0-9a-f]{64}$/i;
const UTF8 = new TextDecoder("utf-8", { fatal: true });

export interface DiffPageLimits {
    maxBytes: number;
    maxRecords: number;
    maxPathBytes: number;
}

export interface DiffPage {
    fromRoot: string;
    toRoot: string;
    deltas: FileDelta[];
    /** Opaque, snapshot-bound continuation. Null means this was the final page. */
    nextCursor: Uint8Array | null;
    wireBytes: number;
}

interface CursorKey {
    fromRoot: Uint8Array;
    toRoot: Uint8Array;
    pathBytes: Uint8Array;
    action: number;
    oldPathBytes: Uint8Array;
}

export function negotiateDiffPageLimits(
    bundle: unknown,
    runtime: "desktop" | "mobile",
): DiffPageLimits | null {
    if (!bundle || typeof bundle !== "object") return null;
    const object = bundle as Record<string, unknown>;
    if (!Array.isArray(object.capabilities) || !object.capabilities.includes("paged-diff-v1")) {
        return null;
    }
    const raw = object.limits;
    if (!raw || typeof raw !== "object") return null;
    const limits = raw as Record<string, unknown>;
    const bytes = limits.diff_page_bytes;
    const records = limits.diff_page_records;
    const pathBytes = limits.diff_path_bytes;
    if (
        !Number.isSafeInteger(bytes) || (bytes as number) < HARD_MIN_PAGE_BYTES ||
        (bytes as number) > HARD_MAX_PAGE_BYTES ||
        !Number.isSafeInteger(records) || (records as number) < 1 ||
        (records as number) > HARD_MAX_PAGE_RECORDS ||
        !Number.isSafeInteger(pathBytes) || (pathBytes as number) < 1 ||
        (pathBytes as number) > HARD_MAX_PATH_BYTES
    ) {
        return null;
    }
    return {
        maxBytes: Math.min(bytes as number, runtime === "mobile" ? MOBILE_PAGE_BYTES : HARD_MAX_PAGE_BYTES),
        maxRecords: records as number,
        maxPathBytes: pathBytes as number,
    };
}

export function encodeDiffPageRequest(
    fromRootHex: string,
    toRootHex: string | null,
    cursor: Uint8Array | null,
    limits: DiffPageLimits,
): Uint8Array {
    validateLimits(limits);
    const fromRoot = decodeHash(fromRootHex, "source root");
    const toRoot = toRootHex === null ? new Uint8Array(32) : decodeHash(toRootHex, "target root");
    const cursorBytes = cursor ?? new Uint8Array();
    if (cursorBytes.byteLength > HARD_MAX_CURSOR_BYTES || cursorBytes.byteLength > 0xffff) {
        throw new Error("diff cursor exceeds hard limit");
    }
    if (cursorBytes.byteLength > 0 && toRootHex === null) {
        throw new Error("diff continuation omitted target root");
    }

    const output = new Uint8Array(REQUEST_HEADER_BYTES + cursorBytes.byteLength);
    output.set(REQUEST_MAGIC, 0);
    output.set(fromRoot, 4);
    output.set(toRoot, 36);
    const view = new DataView(output.buffer);
    view.setUint32(68, limits.maxRecords, true);
    view.setUint32(72, limits.maxBytes, true);
    view.setUint16(76, cursorBytes.byteLength, true);
    output.set(cursorBytes, REQUEST_HEADER_BYTES);
    return output;
}

export function decodeDiffPage(
    body: Uint8Array,
    expectedFromRoot: string,
    expectedToRoot: string | null,
    requestCursor: Uint8Array | null,
    limits: DiffPageLimits,
): DiffPage {
    validateLimits(limits);
    if (body.byteLength > limits.maxBytes) throw new Error("diff page exceeds negotiated byte cap");
    if (body.byteLength < PAGE_HEADER_BYTES) throw new Error("diff page is truncated");
    requireMagic(body, 0, PAGE_MAGIC, "diff page");

    const fromRootBytes = body.slice(4, 36);
    const toRootBytes = body.slice(36, 68);
    const fromRoot = encodeHex(fromRootBytes);
    const toRoot = encodeHex(toRootBytes);
    if (fromRoot !== expectedFromRoot.toLowerCase()) throw new Error("diff page source root changed");
    if (expectedToRoot !== null && toRoot !== expectedToRoot.toLowerCase()) {
        throw new Error("diff page target root changed");
    }
    if (toRootBytes.every((byte) => byte === 0)) throw new Error("diff page target root is zero");

    const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
    const recordCount = view.getUint32(68, true);
    const nextLength = view.getUint16(72, true);
    if (recordCount > limits.maxRecords) throw new Error("diff page exceeds record cap");
    if (nextLength > HARD_MAX_CURSOR_BYTES) throw new Error("diff page cursor exceeds hard limit");
    let offset = checkedAdd(PAGE_HEADER_BYTES, nextLength, body.byteLength, "diff cursor");
    const nextCursor = nextLength === 0
        ? null
        : body.slice(PAGE_HEADER_BYTES, PAGE_HEADER_BYTES + nextLength);
    const previousCursor = requestCursor === null ? null : decodeCursor(requestCursor, limits);
    if (previousCursor) {
        requireEqualBytes(previousCursor.fromRoot, fromRootBytes, "request cursor source root");
        requireEqualBytes(previousCursor.toRoot, toRootBytes, "request cursor target root");
    }

    const deltas: FileDelta[] = [];
    let previousKey: CursorKey | null = previousCursor;
    let pathBytesSeen = 0;
    for (let index = 0; index < recordCount; index++) {
        if (offset >= body.byteLength) throw new Error(`diff record ${index} is truncated`);
        const action = body[offset++];
        if (action < 1 || action > 4) throw new Error(`diff record ${index} has invalid action`);

        const pathLength = decodeVarint(body, offset);
        offset = pathLength.next;
        if (pathLength.value < 1 || pathLength.value > limits.maxPathBytes) {
            throw new Error(`diff record ${index} path length is invalid`);
        }
        const pathEnd = checkedAdd(offset, pathLength.value, body.byteLength, `diff record ${index} path`);
        const pathBytes = body.slice(offset, pathEnd);
        offset = pathEnd;
        const path = decodePath(pathBytes, `diff record ${index} path`);

        let oldPathBytes = new Uint8Array();
        let oldPath: string | undefined;
        if (action === 4) {
            const oldLength = decodeVarint(body, offset);
            offset = oldLength.next;
            if (oldLength.value < 1 || oldLength.value > limits.maxPathBytes) {
                throw new Error(`diff record ${index} rename source length is invalid`);
            }
            const oldEnd = checkedAdd(offset, oldLength.value, body.byteLength, `diff record ${index} rename source`);
            oldPathBytes = body.slice(offset, oldEnd);
            offset = oldEnd;
            oldPath = decodePath(oldPathBytes, `diff record ${index} rename source`);
            if (oldPath === path) throw new Error(`diff record ${index} renames a path onto itself`);
        }
        pathBytesSeen = checkedAdd(pathBytesSeen, pathBytes.byteLength + oldPathBytes.byteLength,
            limits.maxBytes, "diff path byte sum");

        const key: CursorKey = { fromRoot: fromRootBytes, toRoot: toRootBytes, pathBytes, action, oldPathBytes };
        if (previousKey) {
            const pathOrder = compareBytes(previousKey.pathBytes, pathBytes);
            if (pathOrder === 0) throw new Error("diff page touches one target path more than once");
            if (compareKeys(previousKey, key) >= 0) throw new Error("diff records are not strictly ordered");
        }
        previousKey = key;

        if (action === 3) {
            deltas.push({ action: "deleted", path });
            continue;
        }
        const payloadEnd = checkedAdd(offset, 48, body.byteLength, `diff record ${index} payload`);
        const hash = encodeHex(body.subarray(offset, offset + 32));
        const size = readSafeU64(view, offset + 32, `diff record ${index} size`);
        const mtime_ms = readSafeU64(view, offset + 40, `diff record ${index} mtime`);
        offset = payloadEnd;
        if (action === 1) deltas.push({ action: "added", path, hash, size, mtime_ms });
        else if (action === 2) deltas.push({ action: "modified", path, hash, size, mtime_ms });
        else deltas.push({ action: "renamed", path, old_path: oldPath!, hash, size, mtime_ms });
    }
    if (offset !== body.byteLength) throw new Error("diff page has trailing bytes");
    if (nextCursor && recordCount === 0) throw new Error("empty diff page carries a continuation");
    if (nextCursor) {
        const decodedNext = decodeCursor(nextCursor, limits);
        requireEqualBytes(decodedNext.fromRoot, fromRootBytes, "next cursor source root");
        requireEqualBytes(decodedNext.toRoot, toRootBytes, "next cursor target root");
        if (!previousKey || compareKeys(decodedNext, previousKey) !== 0) {
            throw new Error("next cursor does not name the final diff record");
        }
    }

    return { fromRoot, toRoot, deltas, nextCursor, wireBytes: body.byteLength };
}

export function diffCursorToHex(cursor: Uint8Array | null): string | null {
    return cursor === null ? null : encodeHex(cursor);
}

export function diffCursorFromHex(value: string | null): Uint8Array | null {
    if (value === null) return null;
    if (typeof value !== "string" || value.length % 2 !== 0 || value.length > HARD_MAX_CURSOR_BYTES * 2 ||
        !/^[0-9a-f]*$/i.test(value)) {
        throw new Error("persisted diff cursor is invalid");
    }
    const bytes = new Uint8Array(value.length / 2);
    for (let index = 0; index < bytes.length; index++) {
        bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
    }
    return bytes;
}

function validateLimits(limits: DiffPageLimits): void {
    if (
        !Number.isSafeInteger(limits.maxBytes) || limits.maxBytes < HARD_MIN_PAGE_BYTES ||
        limits.maxBytes > HARD_MAX_PAGE_BYTES ||
        !Number.isSafeInteger(limits.maxRecords) || limits.maxRecords < 1 ||
        limits.maxRecords > HARD_MAX_PAGE_RECORDS ||
        !Number.isSafeInteger(limits.maxPathBytes) || limits.maxPathBytes < 1 ||
        limits.maxPathBytes > HARD_MAX_PATH_BYTES
    ) {
        throw new Error("invalid paged diff limits");
    }
}

function decodeCursor(bytes: Uint8Array, limits: DiffPageLimits): CursorKey {
    const fixed = 74;
    if (bytes.byteLength < fixed || bytes.byteLength > HARD_MAX_CURSOR_BYTES) {
        throw new Error("diff cursor length is invalid");
    }
    requireMagic(bytes, 0, CURSOR_MAGIC, "diff cursor");
    if (bytes[4] !== CURSOR_VERSION) throw new Error("unsupported diff cursor version");
    const fromRoot = bytes.slice(5, 37);
    const toRoot = bytes.slice(37, 69);
    const action = bytes[69];
    if (action < 1 || action > 4) throw new Error("diff cursor action is invalid");
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const pathLength = view.getUint16(70, true);
    const oldPathLength = view.getUint16(72, true);
    if (pathLength < 1 || pathLength > limits.maxPathBytes || oldPathLength > limits.maxPathBytes) {
        throw new Error("diff cursor path length is invalid");
    }
    if ((action === 4) !== (oldPathLength > 0)) throw new Error("diff cursor fields do not match action");
    const pathEnd = checkedAdd(fixed, pathLength, bytes.byteLength, "diff cursor path");
    const expectedEnd = checkedAdd(pathEnd, oldPathLength, bytes.byteLength, "diff cursor old path");
    if (expectedEnd !== bytes.byteLength) throw new Error("diff cursor has trailing bytes");
    const pathBytes = bytes.slice(fixed, pathEnd);
    const oldPathBytes = bytes.slice(pathEnd, expectedEnd);
    const path = decodePath(pathBytes, "diff cursor path");
    if (oldPathBytes.byteLength > 0) {
        const oldPath = decodePath(oldPathBytes, "diff cursor old path");
        if (oldPath === path) throw new Error("diff cursor rename source equals target");
    }
    return { fromRoot, toRoot, pathBytes, action, oldPathBytes };
}

function decodePath(bytes: Uint8Array, description: string): string {
    let path: string;
    try {
        path = UTF8.decode(bytes);
    } catch {
        throw new Error(`${description} is not UTF-8`);
    }
    if (!isSafeVaultPath(path)) throw new Error(`${description} is unsafe`);
    return path;
}

function decodeVarint(bytes: Uint8Array, start: number): { value: number; next: number } {
    let value = 0;
    let shift = 0;
    for (let count = 0; count < 5; count++) {
        const offset = start + count;
        if (offset >= bytes.byteLength) throw new Error("diff varint is truncated");
        const byte = bytes[offset];
        if (count === 4 && (byte & 0xf0) !== 0) throw new Error("diff varint overflows u32");
        value += (byte & 0x7f) * 2 ** shift;
        if ((byte & 0x80) === 0) {
            if (count > 0 && value < 2 ** (count * 7)) throw new Error("diff varint is not canonical");
            return { value, next: offset + 1 };
        }
        shift += 7;
    }
    throw new Error("diff varint is too long");
}

function readSafeU64(view: DataView, offset: number, description: string): number {
    const low = view.getUint32(offset, true);
    const high = view.getUint32(offset + 4, true);
    if (high > 0x1fffff) throw new Error(`${description} exceeds JavaScript safe integer range`);
    const value = high * 0x1_0000_0000 + low;
    if (!Number.isSafeInteger(value)) throw new Error(`${description} is invalid`);
    return value;
}

function checkedAdd(offset: number, length: number, cap: number, description: string): number {
    if (!Number.isSafeInteger(offset) || !Number.isSafeInteger(length) || length < 0) {
        throw new Error(`${description} length is invalid`);
    }
    const end = offset + length;
    if (!Number.isSafeInteger(end) || end < offset || end > cap) {
        throw new Error(`${description} exceeds page bounds`);
    }
    return end;
}

function compareKeys(left: CursorKey, right: CursorKey): number {
    return compareBytes(left.pathBytes, right.pathBytes) || left.action - right.action ||
        compareBytes(left.oldPathBytes, right.oldPathBytes);
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
    const common = Math.min(left.byteLength, right.byteLength);
    for (let index = 0; index < common; index++) {
        if (left[index] !== right[index]) return left[index] - right[index];
    }
    return left.byteLength - right.byteLength;
}

function requireEqualBytes(left: Uint8Array, right: Uint8Array, description: string): void {
    if (compareBytes(left, right) !== 0) throw new Error(`${description} changed`);
}

function requireMagic(
    bytes: Uint8Array,
    offset: number,
    magic: readonly number[],
    description: string,
): void {
    for (let index = 0; index < magic.length; index++) {
        if (bytes[offset + index] !== magic[index]) throw new Error(`${description} magic is invalid`);
    }
}

function decodeHash(value: string, description: string): Uint8Array {
    if (!HASH_HEX.test(value)) throw new Error(`${description} is not a BLAKE3 hash`);
    const bytes = new Uint8Array(32);
    for (let index = 0; index < 32; index++) {
        bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
    }
    return bytes;
}

function encodeHex(bytes: Uint8Array): string {
    let output = "";
    for (const byte of bytes) output += byte.toString(16).padStart(2, "0");
    return output;
}
