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
    splitBulkUploadRecords,
    type BulkCodecLimits,
    type BulkUploadRecord,
} from "./bulk-codec";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

function bulkRejected(operation: () => unknown, expected: RegExp, message: string): void {
    try {
        operation();
    } catch (error) {
        check(error instanceof Error && expected.test(error.message),
            `${message}: non-protocol failure ${String(error)}`);
        return;
    }
    check(false, `${message}: input was accepted`);
}

function deterministicWords(seed: number): () => number {
    let state = seed >>> 0;
    return () => {
        state ^= state << 13;
        state ^= state >>> 17;
        state ^= state << 5;
        return state >>> 0;
    };
}

function deterministicBytes(length: number, word: () => number): Uint8Array {
    const bytes = new Uint8Array(length);
    for (let index = 0; index < length; index++) bytes[index] = word() & 0xff;
    return bytes;
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
    if (left.byteLength !== right.byteLength) return false;
    for (let index = 0; index < left.byteLength; index++) {
        if (left[index] !== right[index]) return false;
    }
    return true;
}

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

function payloadTooLargeRetrySplitIsBalancedAndOrdered(): void {
    const records = [
        { kind: BulkObjectKind.Content, hash: hash(60), data: new Uint8Array(700_000) },
        { kind: BulkObjectKind.Content, hash: hash(61), data: new Uint8Array(600_000) },
        { kind: BulkObjectKind.Content, hash: hash(62), data: new Uint8Array(500_000) },
        { kind: BulkObjectKind.Manifest, hash: hash(63), data: new Uint8Array(400_000) },
    ];
    const [first, second] = splitBulkUploadRecords(records);
    check(first.length > 0 && second.length > 0, "413 split produced an empty pack");
    check(
        [...first, ...second].map((record) => record.hash).join(",") ===
            records.map((record) => record.hash).join(","),
        "413 split reordered records",
    );
    check(
        bulkPackEncodedLength(first) < bulkPackEncodedLength(records) &&
            bulkPackEncodedLength(second) < bulkPackEncodedLength(records),
        "413 split did not reduce both retry packs",
    );

    let rejected = false;
    try { splitBulkUploadRecords(records.slice(0, 1)); } catch { rejected = true; }
    check(rejected, "413 split accepted a pack that cannot be divided");
}

function seededBoundedRoundTrips(): void {
    const word = deterministicWords(0x4f425031);
    const propertyLimits: BulkCodecLimits = {
        maxObjects: 32,
        maxBytes: 128 * 1024,
        maxObjectBytes: 4096,
    };
    const countEdges = [0, 1, 2, 7, 8, 9, 16, 31, 32];
    const lengthEdges = [0, 1, 31, 32, 255, 256, 1023, 4096];
    for (let iteration = 0; iteration < 80; iteration++) {
        const count = iteration < countEdges.length ? countEdges[iteration] : word() % 17;
        const records: BulkUploadRecord[] = [];
        for (let index = 0; index < count; index++) {
            const length = index < lengthEdges.length
                ? lengthEdges[(iteration + index) % lengthEdges.length]
                : word() % 1025;
            const rawHash = hashToHex(deterministicBytes(32, word));
            records.push({
                kind: word() % 4 as BulkObjectKind,
                hash: (iteration + index) % 2 === 0 ? rawHash : rawHash.toUpperCase(),
                data: deterministicBytes(length, word),
                flags: index % 3 === 0 ? 0 : undefined,
            });
        }
        const encoded = encodeBulkUploadPack(records, propertyLimits);
        check(encoded.byteLength === bulkPackEncodedLength(records),
            `seeded bulk pack length changed at ${iteration}`);
        const padded = new Uint8Array(encoded.byteLength + 9);
        padded.set(encoded, 4);
        const input = iteration % 2 === 0 ? encoded : padded.subarray(4, 4 + encoded.byteLength);
        const decoded = decodeBulkUploadPack(input, propertyLimits);
        check(decoded.length === records.length, `seeded bulk count changed at ${iteration}`);
        for (let index = 0; index < records.length; index++) {
            check(decoded[index].kind === records[index].kind &&
                decoded[index].hash === records[index].hash.toLowerCase(),
            `seeded bulk identity changed at ${iteration}/${index}`);
            check(decoded[index].plainLength === records[index].data.byteLength &&
                decoded[index].storedLength === records[index].data.byteLength &&
                sameBytes(decoded[index].data, records[index].data),
            `seeded bulk payload changed at ${iteration}/${index}`);
        }

        const hashes = records.map(record => record.hash);
        const checkRequest = encodeBulkCheckRequest(
            iteration % 4 as BulkObjectKind,
            hashes,
            propertyLimits.maxObjects,
        );
        check(checkRequest.byteLength === 9 + 32 * hashes.length &&
            new DataView(checkRequest.buffer).getUint32(5, true) === hashes.length,
        `seeded bulk check layout changed at ${iteration}`);
        for (let index = 0; index < hashes.length; index++) {
            check(hashToHex(checkRequest.subarray(9 + index * 32, 41 + index * 32)) ===
                hashes[index].toLowerCase(), `seeded bulk check hash changed at ${iteration}/${index}`);
        }

        const bitmap = new Uint8Array(8 + Math.floor((hashes.length + 7) / 8));
        bitmap.set(new TextEncoder().encode("OBA1"));
        new DataView(bitmap.buffer).setUint32(4, hashes.length, true);
        const expectedNeeded: string[] = [];
        for (let index = 0; index < hashes.length; index++) {
            if ((word() & 1) !== 0) {
                bitmap[8 + (index >>> 3)] |= 1 << (index & 7);
                expectedNeeded.push(hashes[index]);
            }
        }
        check(JSON.stringify(decodeBulkCheckResponse(bitmap, hashes)) === JSON.stringify(expectedNeeded),
            `seeded bulk bitmap changed at ${iteration}`);

        const ack = new Uint8Array(8 + count);
        ack.set(new TextEncoder().encode(BULK_PACK_ACK_MAGIC));
        new DataView(ack.buffer).setUint32(4, count, true);
        const expectedStatuses: number[] = [];
        for (let index = 0; index < count; index++) {
            const status = word() % 5;
            ack[8 + index] = status;
            expectedStatuses.push(status);
        }
        check(decodeBulkUploadAck(ack, count).join() === expectedStatuses.join(),
            `seeded bulk ACK changed at ${iteration}`);

        const cursor = count === 0 ? 0 : word() % (count + 1);
        const get = encodeBulkGetRequest(
            iteration % 4 as BulkObjectKind,
            hashes,
            cursor,
            1 + word() % 0xffff_ffff,
            propertyLimits.maxObjects,
        );
        const getView = new DataView(get.buffer);
        check(get.byteLength === 17 + 32 * hashes.length &&
            getView.getUint32(5, true) === count && getView.getUint32(9, true) === cursor,
        `seeded bulk GET layout changed at ${iteration}`);
    }
}

function seededMalformedPacksFailClosed(): void {
    const word = deterministicWords(0x42414431);
    const propertyLimits: BulkCodecLimits = { maxObjects: 8, maxBytes: 4096, maxObjectBytes: 512 };
    for (let iteration = 0; iteration < 96; iteration++) {
        const records: BulkUploadRecord[] = [
            { kind: BulkObjectKind.Content, hash: hash(iteration), data: deterministicBytes(3, word) },
            { kind: BulkObjectKind.Manifest, hash: hash(iteration + 1), data: deterministicBytes(5, word) },
        ];
        const encoded = encodeBulkUploadPack(records, propertyLimits);
        let malformed: Uint8Array;
        let expected: RegExp;
        switch (iteration % 12) {
            case 0:
                malformed = encoded.slice(); malformed[0] ^= 0xff; expected = /bulk message magic/; break;
            case 1:
                malformed = encoded.slice(); malformed[4] = 1; expected = /bulk pack flags/; break;
            case 2:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setUint32(6, propertyLimits.maxObjects + 1, true);
                expected = /bulk pack object count/; break;
            case 3:
                malformed = encoded.slice(); new DataView(malformed.buffer).setUint32(6, 3, true);
                expected = /bulk record headers/; break;
            case 4:
                malformed = encoded.slice(); malformed[10] = 4; expected = /bulk object kind/; break;
            case 5:
                malformed = encoded.slice(); malformed[11] = 1; expected = /bulk record flags/; break;
            case 6:
                malformed = encoded.slice(); new DataView(malformed.buffer).setUint32(44, 4, true);
                expected = /compressed bulk records/; break;
            case 7:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setUint32(44, propertyLimits.maxObjectBytes + 1, true);
                new DataView(malformed.buffer).setUint32(48, propertyLimits.maxObjectBytes + 1, true);
                expected = /bulk record exceeds object byte limit/; break;
            case 8:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setUint32(44, 30, true);
                new DataView(malformed.buffer).setUint32(48, 30, true);
                expected = /truncated bulk record (bytes|header)/; break;
            case 9:
                malformed = encoded.subarray(0, word() % encoded.byteLength); expected = /bulk|truncated/; break;
            case 10:
                malformed = new Uint8Array(encoded.byteLength + 1); malformed.set(encoded);
                expected = /trailing bytes after bulk pack/; break;
            default:
                malformed = encoded.slice(); new DataView(malformed.buffer).setUint32(6, 0, true);
                expected = /trailing bytes after bulk pack/; break;
        }
        bulkRejected(
            () => decodeBulkUploadPack(malformed, propertyLimits),
            expected,
            `seeded malformed bulk pack ${iteration}`,
        );
    }

    const headerOnlyPack = encodeBulkUploadPack([
        { kind: BulkObjectKind.Content, hash: hash(200), data: new Uint8Array() },
    ], propertyLimits);
    const headerView = new DataView(headerOnlyPack.buffer);
    headerView.setUint32(44, 0xffff_ffff, true);
    headerView.setUint32(48, 0xffff_ffff, true);
    bulkRejected(
        () => decodeBulkUploadPack(headerOnlyPack,
            { maxObjects: 0xffff_ffff, maxBytes: 0xffff_ffff, maxObjectBytes: 0xffff_ffff }),
        /bulk record bytes overflow/,
        "bulk pack with a 4-GiB header-only record declaration",
    );

    const countOnlyPack = new Uint8Array(10);
    countOnlyPack.set(new TextEncoder().encode(BULK_PACK_MAGIC));
    new DataView(countOnlyPack.buffer).setUint32(6, 0xffff_ffff, true);
    bulkRejected(
        () => decodeBulkUploadPack(countOnlyPack,
            { maxObjects: 0xffff_ffff, maxBytes: 0xffff_ffff, maxObjectBytes: 1 }),
        /bulk record headers overflow/,
        "bulk pack with a 4-billion record declaration",
    );

    const hugeAck = new Uint8Array(8);
    hugeAck.set(new TextEncoder().encode(BULK_PACK_ACK_MAGIC));
    new DataView(hugeAck.buffer).setUint32(4, 0xffff_ffff, true);
    bulkRejected(
        () => decodeBulkUploadAck(hugeAck, 0xffff_ffff),
        /bulk upload ACK count\/length mismatch/,
        "bulk ACK with a 4-billion status declaration",
    );

    const hugeDownload = new Uint8Array(12);
    hugeDownload.set(new TextEncoder().encode(BULK_DOWNLOAD_MAGIC));
    const downloadView = new DataView(hugeDownload.buffer);
    downloadView.setUint32(4, 0xffff_ffff, true);
    downloadView.setUint32(8, 0, true);
    bulkRejected(
        () => decodeBulkDownloadResponse(hugeDownload, 0xffff_ffff, 0,
            { maxObjects: 1, maxBytes: 0xffff_ffff, maxObjectBytes: 1 }),
        /truncated bulk download bitmap/,
        "bulk download with a 4-billion bitmap declaration",
    );
}

checkCodecPreservesOrderingAndBitmap();
uploadPackAndAckAreStrict();
downloadPagesCarryCursorBitmapAndPack();
malformedAndLimitCasesFailClosed();
w1RequestCountCollapsesToBoundedPacks();
payloadTooLargeRetrySplitIsBalancedAndOrdered();
seededBoundedRoundTrips();
seededMalformedPacksFailClosed();
console.log(`bulk-codec.test: ${assertions} assertions passed`);
