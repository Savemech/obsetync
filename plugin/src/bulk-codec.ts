/** Strict codecs for the bounded bulk HTTP protocol carried inside wire v2. */

export const BULK_CHECK_MAGIC = "OBC1";
export const BULK_CHECK_ACK_MAGIC = "OBA1";
export const BULK_PACK_MAGIC = "OBP1";
export const BULK_PACK_ACK_MAGIC = "OBK1";
export const BULK_GET_MAGIC = "OBG1";
export const BULK_DOWNLOAD_MAGIC = "OBD1";

export const BULK_CHECK_HEADER_BYTES = 9;
export const BULK_PACK_HEADER_BYTES = 10;
export const BULK_RECORD_HEADER_BYTES = 42;
export const BULK_GET_HEADER_BYTES = 17;
export const BULK_DOWNLOAD_HEADER_BYTES = 12;

export enum BulkObjectKind {
    Content = 0,
    ContentChunk = 1,
    IndexChunk = 2,
    Manifest = 3,
}

export enum BulkUploadStatus {
    Stored = 0,
    AlreadyPresent = 1,
    BadHash = 2,
    RejectedLimit = 3,
    RetryableStorageError = 4,
}

export interface BulkUploadRecord {
    kind: BulkObjectKind;
    hash: string;
    data: Uint8Array;
    flags?: number;
}

export interface BulkDecodedRecord extends BulkUploadRecord {
    plainLength: number;
    storedLength: number;
}

export interface BulkDownloadPage {
    requestCount: number;
    nextCursor: number;
    remaining: boolean[];
    records: BulkDecodedRecord[];
}

export interface BulkCodecLimits {
    maxObjects: number;
    maxBytes: number;
    maxObjectBytes: number;
}

export type BulkUploadStep =
    | { kind: "bulk"; records: BulkUploadRecord[] }
    | { kind: "single"; record: BulkUploadRecord };

export type BulkRuntime = "desktop" | "mobile";

export const BULK_SERVER_MAX_BYTES = 8 * 1024 * 1024;
export const BULK_MOBILE_MAX_BYTES = 2 * 1024 * 1024;
export const BULK_MAX_OBJECTS = 256;
export const BULK_MAX_OBJECT_BYTES = 1024 * 1024 - 1;

export const BULK_RECORD_WIRE_OVERHEAD = BULK_RECORD_HEADER_BYTES;

const encoder = new TextEncoder();

/** Accept only an authenticated, internally consistent capability bundle.
 *  Peer values can lower local budgets but can never raise compiled safety
 *  ceilings. Missing/old-server bundles cleanly disable the fast path. */
export function negotiateBulkLimits(value: unknown, runtime: BulkRuntime): BulkCodecLimits | null {
    if (!value || typeof value !== "object") return null;
    const bundle = value as {
        capabilities?: unknown;
        limits?: { bulk_request_bytes?: unknown; bulk_objects?: unknown };
    };
    if (
        !Array.isArray(bundle.capabilities) ||
        !bundle.capabilities.includes("bulk-http-v1")
    ) {
        return null;
    }
    const advertisedBytes = bundle.limits?.bulk_request_bytes;
    const advertisedObjects = bundle.limits?.bulk_objects;
    if (
        !Number.isSafeInteger(advertisedBytes) ||
        (advertisedBytes as number) <= BULK_PACK_HEADER_BYTES + BULK_RECORD_HEADER_BYTES ||
        !Number.isSafeInteger(advertisedObjects) ||
        (advertisedObjects as number) <= 0
    ) {
        return null;
    }
    const runtimeBytes = runtime === "mobile" ? BULK_MOBILE_MAX_BYTES : BULK_SERVER_MAX_BYTES;
    return {
        maxBytes: Math.min(advertisedBytes as number, runtimeBytes, BULK_SERVER_MAX_BYTES),
        maxObjects: Math.min(advertisedObjects as number, BULK_MAX_OBJECTS),
        maxObjectBytes: BULK_MAX_OBJECT_BYTES,
    };
}

export function bulkBitmapBytes(count: number): number {
    requireSafeUint(count, "bitmap count");
    return Math.floor((count + 7) / 8);
}

export function bulkPackEncodedLength(records: readonly BulkUploadRecord[]): number {
    let total = BULK_PACK_HEADER_BYTES;
    for (const record of records) {
        assertRecordShape(record);
        total = checkedAdd(total, BULK_RECORD_HEADER_BYTES, "bulk pack length");
        total = checkedAdd(total, record.data.byteLength, "bulk pack length");
    }
    return total;
}

/** Pure, deterministic planner used by the transport and W1 request-count
 *  gate. It preserves dependency order across bulk and oversized legacy
 *  records while never constructing the aggregate body. */
export function planBulkUploadSteps(
    records: readonly BulkUploadRecord[],
    limits: BulkCodecLimits,
): BulkUploadStep[] {
    validateLimits(limits);
    const steps: BulkUploadStep[] = [];
    let pending: BulkUploadRecord[] = [];
    let pendingBytes = BULK_PACK_HEADER_BYTES;
    const flush = (): void => {
        if (pending.length === 0) return;
        steps.push({ kind: "bulk", records: pending });
        pending = [];
        pendingBytes = BULK_PACK_HEADER_BYTES;
    };
    for (const record of records) {
        assertRecordShape(record);
        const recordBytes = checkedAdd(
            BULK_RECORD_HEADER_BYTES,
            record.data.byteLength,
            "bulk record step length",
        );
        const eligible =
            record.data.byteLength <= limits.maxObjectBytes &&
            BULK_PACK_HEADER_BYTES + recordBytes <= limits.maxBytes;
        if (!eligible) {
            flush();
            steps.push({ kind: "single", record });
            continue;
        }
        if (
            pending.length >= limits.maxObjects ||
            pendingBytes + recordBytes > limits.maxBytes
        ) {
            flush();
        }
        pending.push(record);
        pendingBytes += recordBytes;
    }
    flush();
    return steps;
}

/** Split a rejected upload pack into two byte-balanced, ordered retries.
 *  This is deliberately independent of the advertised limit: a reverse
 *  proxy or stale server configuration can impose a smaller effective
 *  ceiling than capability negotiation reported. */
export function splitBulkUploadRecords(
    records: readonly BulkUploadRecord[],
): [BulkUploadRecord[], BulkUploadRecord[]] {
    if (records.length < 2) {
        throw new RangeError("bulk upload pack cannot be split");
    }
    const payloadBytes = records.reduce(
        (total, record) => checkedAdd(
            total,
            checkedAdd(BULK_RECORD_HEADER_BYTES, record.data.byteLength, "bulk split record"),
            "bulk split length",
        ),
        0,
    );
    const target = Math.ceil(payloadBytes / 2);
    let consumed = 0;
    let splitAt = 1;
    for (let index = 0; index < records.length - 1; index++) {
        consumed = checkedAdd(
            consumed,
            checkedAdd(
                BULK_RECORD_HEADER_BYTES,
                records[index].data.byteLength,
                "bulk split record",
            ),
            "bulk split length",
        );
        splitAt = index + 1;
        if (consumed >= target) break;
    }
    return [records.slice(0, splitAt), records.slice(splitAt)];
}

export function encodeBulkCheckRequest(
    kind: BulkObjectKind,
    hashes: readonly string[],
    maxObjects: number,
): Uint8Array {
    assertKind(kind);
    requireCount(hashes.length, maxObjects, "bulk check");
    const length = checkedAdd(
        BULK_CHECK_HEADER_BYTES,
        checkedMultiply(hashes.length, 32, "bulk check hashes"),
        "bulk check length",
    );
    const output = new Uint8Array(length);
    writeMagic(output, 0, BULK_CHECK_MAGIC);
    output[4] = kind;
    writeU32(output, 5, hashes.length);
    let cursor = BULK_CHECK_HEADER_BYTES;
    for (const hash of hashes) {
        output.set(hexToHash(hash), cursor);
        cursor += 32;
    }
    return output;
}

export function decodeBulkCheckResponse(
    body: Uint8Array,
    requestedHashes: readonly string[],
): string[] {
    requireMagic(body, BULK_CHECK_ACK_MAGIC);
    if (body.byteLength < 8) throw new Error("truncated bulk check response");
    const count = readU32(body, 4);
    if (count !== requestedHashes.length) {
        throw new Error("bulk check response count mismatch");
    }
    const bitmapLength = bulkBitmapBytes(count);
    if (body.byteLength !== 8 + bitmapLength) {
        throw new Error("bulk check response length mismatch");
    }
    assertUnusedBitmapBitsClear(body.subarray(8), count);
    const needed: string[] = [];
    for (let index = 0; index < count; index++) {
        if (bitmapGet(body.subarray(8), index)) needed.push(requestedHashes[index]);
    }
    return needed;
}

export function encodeBulkUploadPack(
    records: readonly BulkUploadRecord[],
    limits: BulkCodecLimits,
): Uint8Array {
    validateLimits(limits);
    requireCount(records.length, limits.maxObjects, "bulk upload");
    const length = bulkPackEncodedLength(records);
    if (length > limits.maxBytes) throw new RangeError("bulk upload exceeds byte limit");
    const output = new Uint8Array(length);
    writeMagic(output, 0, BULK_PACK_MAGIC);
    writeU16(output, 4, 0); // Compression/reserved flags are off in v1.
    writeU32(output, 6, records.length);
    let cursor = BULK_PACK_HEADER_BYTES;
    for (const record of records) {
        assertRecordShape(record);
        if (record.data.byteLength > limits.maxObjectBytes) {
            throw new RangeError("bulk upload object exceeds byte limit");
        }
        output[cursor] = record.kind;
        output[cursor + 1] = record.flags ?? 0;
        output.set(hexToHash(record.hash), cursor + 2);
        writeU32(output, cursor + 34, record.data.byteLength);
        writeU32(output, cursor + 38, record.data.byteLength);
        output.set(record.data, cursor + BULK_RECORD_HEADER_BYTES);
        cursor += BULK_RECORD_HEADER_BYTES + record.data.byteLength;
    }
    return output;
}

export function decodeBulkUploadPack(
    body: Uint8Array,
    limits: BulkCodecLimits,
): BulkDecodedRecord[] {
    validateLimits(limits);
    if (body.byteLength > limits.maxBytes) throw new RangeError("bulk pack exceeds byte limit");
    requireMagic(body, BULK_PACK_MAGIC);
    if (body.byteLength < BULK_PACK_HEADER_BYTES) throw new Error("truncated bulk pack header");
    if (readU16(body, 4) !== 0) throw new Error("unsupported bulk pack flags");
    const count = readU32(body, 6);
    requireCount(count, limits.maxObjects, "bulk pack");
    const minimum = checkedAdd(
        BULK_PACK_HEADER_BYTES,
        checkedMultiply(count, BULK_RECORD_HEADER_BYTES, "bulk record headers"),
        "bulk pack minimum",
    );
    if (body.byteLength < minimum) throw new Error("truncated bulk record headers");

    const records: BulkDecodedRecord[] = [];
    let cursor = BULK_PACK_HEADER_BYTES;
    for (let index = 0; index < count; index++) {
        const headerEnd = checkedAdd(cursor, BULK_RECORD_HEADER_BYTES, "bulk record header");
        if (headerEnd > body.byteLength) throw new Error("truncated bulk record header");
        const kind = body[cursor];
        assertKind(kind);
        const flags = body[cursor + 1];
        if (flags !== 0) throw new Error("unsupported bulk record flags");
        const plainLength = readU32(body, cursor + 34);
        const storedLength = readU32(body, cursor + 38);
        if (plainLength !== storedLength) {
            throw new Error("compressed bulk records are not supported in v1");
        }
        if (plainLength > limits.maxObjectBytes) {
            throw new RangeError("bulk record exceeds object byte limit");
        }
        const dataEnd = checkedAdd(headerEnd, storedLength, "bulk record bytes");
        if (dataEnd > body.byteLength) throw new Error("truncated bulk record bytes");
        records.push({
            kind,
            flags,
            hash: hashToHex(body.subarray(cursor + 2, cursor + 34)),
            plainLength,
            storedLength,
            data: body.subarray(headerEnd, dataEnd),
        });
        cursor = dataEnd;
    }
    if (cursor !== body.byteLength) throw new Error("trailing bytes after bulk pack");
    return records;
}

export function decodeBulkUploadAck(body: Uint8Array, expectedCount: number): BulkUploadStatus[] {
    requireMagic(body, BULK_PACK_ACK_MAGIC);
    if (body.byteLength < 8) throw new Error("truncated bulk upload ACK");
    const count = readU32(body, 4);
    if (count !== expectedCount || body.byteLength !== 8 + count) {
        throw new Error("bulk upload ACK count/length mismatch");
    }
    const statuses: BulkUploadStatus[] = [];
    for (const status of body.subarray(8)) {
        if (status > BulkUploadStatus.RetryableStorageError) {
            throw new Error("unknown bulk upload ACK status");
        }
        statuses.push(status as BulkUploadStatus);
    }
    return statuses;
}

export function encodeBulkGetRequest(
    kind: BulkObjectKind,
    hashes: readonly string[],
    cursor: number,
    maxResponseBytes: number,
    maxObjects: number,
): Uint8Array {
    assertKind(kind);
    requireCount(hashes.length, maxObjects, "bulk get");
    requireSafeUint(cursor, "bulk get cursor");
    if (cursor > hashes.length) throw new RangeError("bulk get cursor exceeds count");
    requirePositiveUint(maxResponseBytes, "bulk get response budget");
    const length = checkedAdd(
        BULK_GET_HEADER_BYTES,
        checkedMultiply(hashes.length, 32, "bulk get hashes"),
        "bulk get length",
    );
    const output = new Uint8Array(length);
    writeMagic(output, 0, BULK_GET_MAGIC);
    output[4] = kind;
    writeU32(output, 5, hashes.length);
    writeU32(output, 9, cursor);
    writeU32(output, 13, maxResponseBytes);
    let offset = BULK_GET_HEADER_BYTES;
    for (const hash of hashes) {
        output.set(hexToHash(hash), offset);
        offset += 32;
    }
    return output;
}

export function decodeBulkDownloadResponse(
    body: Uint8Array,
    expectedCount: number,
    previousCursor: number,
    limits: BulkCodecLimits,
): BulkDownloadPage {
    validateLimits(limits);
    if (body.byteLength > limits.maxBytes) throw new RangeError("bulk download exceeds byte limit");
    requireMagic(body, BULK_DOWNLOAD_MAGIC);
    if (body.byteLength < BULK_DOWNLOAD_HEADER_BYTES) {
        throw new Error("truncated bulk download header");
    }
    const requestCount = readU32(body, 4);
    const nextCursor = readU32(body, 8);
    if (requestCount !== expectedCount) throw new Error("bulk download request count mismatch");
    if (nextCursor < previousCursor || nextCursor > requestCount) {
        throw new Error("bulk download cursor did not advance monotonically");
    }
    const bitmapLength = bulkBitmapBytes(requestCount);
    const packOffset = checkedAdd(BULK_DOWNLOAD_HEADER_BYTES, bitmapLength, "download pack offset");
    if (packOffset > body.byteLength) throw new Error("truncated bulk download bitmap");
    const bitmap = body.subarray(BULK_DOWNLOAD_HEADER_BYTES, packOffset);
    assertUnusedBitmapBitsClear(bitmap, requestCount);
    const remaining = Array.from({ length: requestCount }, (_, index) => bitmapGet(bitmap, index));
    for (let index = 0; index < requestCount; index++) {
        if (remaining[index] !== (index >= nextCursor)) {
            throw new Error("bulk download remaining bitmap disagrees with cursor");
        }
    }
    const records = decodeBulkUploadPack(body.subarray(packOffset), limits);
    return { requestCount, nextCursor, remaining, records };
}

export function hashToHex(hash: Uint8Array): string {
    if (hash.byteLength !== 32) throw new Error("bulk hash must be 32 bytes");
    let output = "";
    for (const byte of hash) output += byte.toString(16).padStart(2, "0");
    return output;
}

export function hexToHash(hex: string): Uint8Array {
    if (!/^[0-9a-fA-F]{64}$/.test(hex)) throw new Error("bulk hash must be 64 hex characters");
    const output = new Uint8Array(32);
    for (let index = 0; index < 32; index++) {
        output[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
    }
    return output;
}

function assertRecordShape(record: BulkUploadRecord): void {
    assertKind(record.kind);
    hexToHash(record.hash);
    if (!(record.data instanceof Uint8Array)) throw new TypeError("bulk record data must be bytes");
    const flags = record.flags ?? 0;
    if (flags !== 0) throw new Error("bulk record flags are reserved in v1");
    requireSafeUint(record.data.byteLength, "bulk record length");
}

function assertKind(value: number): asserts value is BulkObjectKind {
    if (!Number.isInteger(value) || value < BulkObjectKind.Content || value > BulkObjectKind.Manifest) {
        throw new Error("unknown bulk object kind");
    }
}

function validateLimits(limits: BulkCodecLimits): void {
    requirePositiveUint(limits.maxObjects, "bulk object limit");
    requirePositiveUint(limits.maxBytes, "bulk byte limit");
    requirePositiveUint(limits.maxObjectBytes, "bulk per-object limit");
}

function requireCount(count: number, maximum: number, label: string): void {
    requireSafeUint(count, `${label} count`);
    requirePositiveUint(maximum, `${label} maximum`);
    if (count > maximum) throw new RangeError(`${label} object count exceeds limit`);
}

function requireSafeUint(value: number, label: string): void {
    if (!Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff) {
        throw new RangeError(`${label} is not a safe u32`);
    }
}

function requirePositiveUint(value: number, label: string): void {
    requireSafeUint(value, label);
    if (value === 0) throw new RangeError(`${label} must be positive`);
}

function checkedAdd(left: number, right: number, label: string): number {
    const value = left + right;
    if (!Number.isSafeInteger(value) || value > 0xffff_ffff) {
        throw new RangeError(`${label} overflow`);
    }
    return value;
}

function checkedMultiply(left: number, right: number, label: string): number {
    const value = left * right;
    if (!Number.isSafeInteger(value) || value > 0xffff_ffff) {
        throw new RangeError(`${label} overflow`);
    }
    return value;
}

function writeMagic(output: Uint8Array, offset: number, magic: string): void {
    output.set(encoder.encode(magic), offset);
}

function requireMagic(body: Uint8Array, magic: string): void {
    if (body.byteLength < 4) throw new Error("truncated bulk message magic");
    for (let index = 0; index < 4; index++) {
        if (body[index] !== magic.charCodeAt(index)) throw new Error("invalid bulk message magic");
    }
}

function writeU16(output: Uint8Array, offset: number, value: number): void {
    requireSafeUint(value, "u16");
    if (value > 0xffff) throw new RangeError("u16 overflow");
    new DataView(output.buffer, output.byteOffset, output.byteLength).setUint16(offset, value, true);
}

function writeU32(output: Uint8Array, offset: number, value: number): void {
    requireSafeUint(value, "u32");
    new DataView(output.buffer, output.byteOffset, output.byteLength).setUint32(offset, value, true);
}

function readU16(body: Uint8Array, offset: number): number {
    if (offset + 2 > body.byteLength) throw new Error("truncated bulk u16");
    return new DataView(body.buffer, body.byteOffset, body.byteLength).getUint16(offset, true);
}

function readU32(body: Uint8Array, offset: number): number {
    if (offset + 4 > body.byteLength) throw new Error("truncated bulk u32");
    return new DataView(body.buffer, body.byteOffset, body.byteLength).getUint32(offset, true);
}

function bitmapGet(bitmap: Uint8Array, index: number): boolean {
    return (bitmap[index >>> 3] & (1 << (index & 7))) !== 0;
}

function assertUnusedBitmapBitsClear(bitmap: Uint8Array, count: number): void {
    const used = count & 7;
    if (used === 0 || bitmap.byteLength === 0) return;
    const unusedMask = (0xff << used) & 0xff;
    if ((bitmap[bitmap.byteLength - 1] & unusedMask) !== 0) {
        throw new Error("bulk bitmap has non-zero padding bits");
    }
}
