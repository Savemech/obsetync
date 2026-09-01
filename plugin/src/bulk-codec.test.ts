import {
    BULK_DOWNLOAD_MAGIC,
    BULK_PACK_MAGIC,
    BULK_PACK_ACK_MAGIC,
    BulkObjectKind,
    BulkUploadStatus,
    bulkPackEncodedLength,
    decodeBulkCheckResponse,
    decodeBulkDownloadResponse,
    decodeBulkUploadAck,
    decodeBulkUploadPack,
    encodeBulkCheckRequest,
    encodeBulkGetRequest,
    encodeBulkUploadPack,
    hashToHex,
    negotiateBulkLimits,
    planBulkUploadSteps,
    type BulkCodecLimits,
    type BulkUploadRecord,
} from "./bulk-codec";

const check = (condition: unknown, message: string) => {
    if (!condition) throw new Error(message);
};

const limits: BulkCodecLimits = {
    maxObjects: 256,
    maxBytes: 8 * 1024 * 1024,
    maxObjectBytes: 1024 * 1024 - 1,
};

const hash = (seed: number): string =>
    Array.from({ length: 32 }, (_, index) => (seed + index) & 0xff)
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");

function checkCodecPreservesOrderingAndBitmap(): void {
    const hashes = [hash(0), hash(1), hash(2), hash(3), hash(4), hash(5), hash(6), hash(7), hash(8)];
    const request = encodeBulkCheckRequest(BulkObjectKind.Content, hashes, 256);
    check(new TextDecoder().decode(request.subarray(0, 4)) === "OBC1", "check magic differs");
    check(request[4] === BulkObjectKind.Content, "check kind differs");
    check(new DataView(request.buffer).getUint32(5, true) === hashes.length, "check count differs");

    const response = new Uint8Array([79, 66, 65, 49, 9, 0, 0, 0, 0b1000_1001, 1]);
    const needed = decodeBulkCheckResponse(response, hashes);
    check(JSON.stringify(needed) === JSON.stringify([hashes[0], hashes[3], hashes[7], hashes[8]]),
        "needed bitmap lost request ordering");
}

function uploadPackAndAckAreStrict(): void {
    const records: BulkUploadRecord[] = [
        { kind: BulkObjectKind.Content, hash: hash(10), data: new Uint8Array([1, 2, 3]) },
        { kind: BulkObjectKind.IndexChunk, hash: hash(20), data: new Uint8Array([4, 5]) },
    ];
    const encoded = encodeBulkUploadPack(records, limits);
    check(new TextDecoder().decode(encoded.subarray(0, 4)) === BULK_PACK_MAGIC, "pack magic differs");
    check(encoded.byteLength === bulkPackEncodedLength(records), "pack length planner differs");
    const decoded = decodeBulkUploadPack(encoded, limits);
    check(decoded.length === 2, "pack record count differs");
    check(decoded[0].hash === records[0].hash, "raw hash changed");
    check(decoded[0].data.join(",") === "1,2,3", "record bytes changed");
    check(decoded[1].kind === BulkObjectKind.IndexChunk, "record kind changed");

    const ack = new Uint8Array([79, 66, 75, 49, 5, 0, 0, 0, 0, 1, 2, 3, 4]);
    const statuses = decodeBulkUploadAck(ack, 5);
    check(statuses[0] === BulkUploadStatus.Stored, "stored ACK changed");
    check(statuses[4] === BulkUploadStatus.RetryableStorageError, "retryable ACK changed");

    const malformed = encoded.slice();
    new DataView(malformed.buffer).setUint32(48, 0xffff_ffff, true);
    let rejected = false;
    try { decodeBulkUploadPack(malformed, limits); } catch { rejected = true; }
    check(rejected, "oversized declared record was sliced");

    const trailing = new Uint8Array(encoded.byteLength + 1);
    trailing.set(encoded);
    rejected = false;
    try { decodeBulkUploadPack(trailing, limits); } catch { rejected = true; }
    check(rejected, "trailing pack bytes were accepted");
}

function downloadPagesCarryCursorBitmapAndPack(): void {
    const hashes = [hash(30), hash(31), hash(32)];
    const get = encodeBulkGetRequest(BulkObjectKind.Content, hashes, 0, 2048, 256);
    check(new TextDecoder().decode(get.subarray(0, 4)) === "OBG1", "get magic differs");
    check(new DataView(get.buffer).getUint32(9, true) === 0, "get cursor differs");
    check(new DataView(get.buffer).getUint32(13, true) === 2048, "get budget differs");

    const nested = encodeBulkUploadPack([
        { kind: BulkObjectKind.Content, hash: hashes[0], data: new Uint8Array([7, 8]) },
    ], limits);
    const response = new Uint8Array(12 + 1 + nested.byteLength);
    response.set(new TextEncoder().encode(BULK_DOWNLOAD_MAGIC));
    new DataView(response.buffer).setUint32(4, 3, true);
    new DataView(response.buffer).setUint32(8, 1, true);
    response[12] = 0b0000_0110;
    response.set(nested, 13);
    const page = decodeBulkDownloadResponse(response, 3, 0, limits);
    check(page.nextCursor === 1, "download cursor differs");
    check(JSON.stringify(page.remaining) === "[false,true,true]", "remaining bitmap differs");
    check(page.records.length === 1 && page.records[0].data[1] === 8, "nested pack differs");
}

function malformedAndLimitCasesFailClosed(): void {
    const h = hash(1);
    let rejected = false;
    try { encodeBulkCheckRequest(99 as BulkObjectKind, [h], 256); } catch { rejected = true; }
    check(rejected, "unknown kind accepted");
    rejected = false;
    try { encodeBulkCheckRequest(BulkObjectKind.Content, [h, h], 1); } catch { rejected = true; }
    check(rejected, "check count cap ignored");
    rejected = false;
    try {
        encodeBulkUploadPack([
            { kind: BulkObjectKind.Content, hash: h, data: new Uint8Array(limits.maxObjectBytes + 1) },
        ], limits);
    } catch { rejected = true; }
    check(rejected, "per-object byte cap ignored");

    const badAck = new Uint8Array([79, 66, 75, 49, 1, 0, 0, 0, 9]);
    rejected = false;
    try { decodeBulkUploadAck(badAck, 1); } catch { rejected = true; }
    check(rejected, "unknown ACK status accepted");

    const badBitmap = new Uint8Array([79, 66, 65, 49, 1, 0, 0, 0, 0b1000_0001]);
    rejected = false;
    try { decodeBulkCheckResponse(badBitmap, [h]); } catch { rejected = true; }
    check(rejected, "non-zero bitmap padding accepted");

    check(hashToHex(new Uint8Array(32).fill(0xab)) === "ab".repeat(32), "hash hex codec differs");
    check(new TextDecoder().decode(new TextEncoder().encode(BULK_PACK_ACK_MAGIC)) === "OBK1",
        "ACK magic differs");

    const advertised = {
        capabilities: ["bulk-http-v1"],
        limits: { bulk_request_bytes: 8 * 1024 * 1024, bulk_objects: 999 },
    };
    const desktop = negotiateBulkLimits(advertised, "desktop");
    const mobile = negotiateBulkLimits(advertised, "mobile");
    check(desktop?.maxBytes === 8 * 1024 * 1024, "desktop negotiation differs");
    check(mobile?.maxBytes === 2 * 1024 * 1024, "mobile negotiation exceeds local cap");
    check(desktop?.maxObjects === 256, "server object count raised compiled cap");
    check(negotiateBulkLimits({ capabilities: [] }, "desktop") === null,
        "missing capability enabled fast path");
}

function w1RequestCountCollapsesToBoundedPacks(): void {
    const sharedPayload = new Uint8Array(8 * 1024);
    const records = Array.from({ length: 100_000 }, () => ({
        kind: BulkObjectKind.Content,
        hash: hash(40),
        data: sharedPayload,
    }));
    const steps = planBulkUploadSteps(records, limits);
    check(steps.length === 391, `W1 planned ${steps.length} uploads instead of 391`);
    check(steps.every((step) => step.kind === "bulk"), "W1 unexpectedly used a single PUT");
    check(steps.every((step) => step.kind !== "bulk" || step.records.length <= 256),
        "W1 pack exceeded object cap");
    check(steps.every((step) => step.kind !== "bulk" || bulkPackEncodedLength(step.records) <= limits.maxBytes),
        "W1 pack exceeded byte cap");
    check(steps.length < records.length / 250, "W1 request count did not collapse by ~256x");

    const oversized = new Uint8Array(limits.maxObjectBytes + 1);
    const ordered = planBulkUploadSteps([
        { kind: BulkObjectKind.ContentChunk, hash: hash(50), data: sharedPayload },
        { kind: BulkObjectKind.ContentChunk, hash: hash(51), data: oversized },
        { kind: BulkObjectKind.Manifest, hash: hash(52), data: sharedPayload },
    ], limits);
    check(ordered.map((step) => step.kind).join(",") === "bulk,single,bulk",
        "oversized fallback reordered dependent records");
    check(ordered[1].kind === "single" && ordered[1].record.hash === hash(51),
        "oversized fallback selected the wrong record");
    check(ordered[2].kind === "bulk" && ordered[2].records[0].kind === BulkObjectKind.Manifest,
        "manifest did not remain after its chunk fallback");
}

checkCodecPreservesOrderingAndBitmap();
uploadPackAndAckAreStrict();
downloadPagesCarryCursorBitmapAndPack();
malformedAndLimitCasesFailClosed();
w1RequestCountCollapsesToBoundedPacks();
console.log("bulk-codec.test: 41 assertions passed");
