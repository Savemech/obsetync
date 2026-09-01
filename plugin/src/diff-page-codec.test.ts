import {
    decodeDiffPage,
    diffCursorFromHex,
    diffCursorToHex,
    encodeDiffPageRequest,
    negotiateDiffPageLimits,
    type DiffPageLimits,
} from "./diff-page-codec";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const limits: DiffPageLimits = {
    maxBytes: 64 * 1024,
    maxRecords: 100,
    maxPathBytes: 4096,
};

function root(byte: number): string { return byte.toString(16).padStart(2, "0").repeat(32); }

function makeCursor(path: string, action: number): Uint8Array {
    const pathBytes = new TextEncoder().encode(path);
    const bytes = new Uint8Array(74 + pathBytes.length);
    const view = new DataView(bytes.buffer);
    bytes.set([0x4f, 0x42, 0x43, 0x31, 1], 0);
    bytes.fill(1, 5, 37);
    bytes.fill(2, 37, 69);
    bytes[69] = action;
    view.setUint16(70, pathBytes.length, true);
    view.setUint16(72, 0, true);
    bytes.set(pathBytes, 74);
    return bytes;
}

function checkRejected(bytes: Uint8Array, message: string): void {
    let rejected = false;
    try { decodeDiffPage(bytes, root(1), null, null, limits); }
    catch { rejected = true; }
    check(rejected, message);
}

function fixture(): Uint8Array {
    const path = new TextEncoder().encode("notes/a.md");
    const deleted = new TextEncoder().encode("old.md");
    const bytes = new Uint8Array(74 + 1 + 1 + path.length + 32 + 8 + 8 + 1 + 1 + deleted.length);
    const view = new DataView(bytes.buffer);
    bytes.set([0x4f, 0x42, 0x44, 0x31], 0);
    bytes.fill(1, 4, 36);
    bytes.fill(2, 36, 68);
    view.setUint32(68, 2, true);
    view.setUint16(72, 0, true);
    let offset = 74;
    bytes[offset++] = 1;
    bytes[offset++] = path.length;
    bytes.set(path, offset); offset += path.length;
    bytes.fill(9, offset, offset + 32); offset += 32;
    view.setUint32(offset, 9, true); offset += 8;
    const mtime = 1_700_000_000_009;
    view.setUint32(offset, mtime >>> 0, true);
    view.setUint32(offset + 4, Math.floor(mtime / 0x1_0000_0000), true);
    offset += 8;
    bytes[offset++] = 3;
    bytes[offset++] = deleted.length;
    bytes.set(deleted, offset);
    return bytes;
}

function run(): void {
    const negotiated = negotiateDiffPageLimits({
        capabilities: ["paged-diff-v1"],
        limits: { diff_page_bytes: 2 * 1024 * 1024, diff_page_records: 8192, diff_path_bytes: 4096 },
    }, "mobile");
    check(negotiated?.maxBytes === 512 * 1024, "mobile page budget was not clamped");
    check(negotiateDiffPageLimits({ capabilities: [], limits: {} }, "desktop") === null,
        "missing capability negotiated");

    const request = encodeDiffPageRequest(root(1), null, null, limits);
    check(request.byteLength === 78, "first request header length differs");
    check(request.slice(0, 4).join() === "79,66,81,49", "request magic differs");

    const pageBytes = fixture();
    let fixtureDigest = 2_166_136_261;
    for (const byte of pageBytes) fixtureDigest = Math.imul(fixtureDigest ^ byte, 16_777_619) >>> 0;
    check(fixtureDigest === 0x44b5_235b, "Rust/TypeScript binary fixture drifted");
    const page = decodeDiffPage(pageBytes, root(1), null, null, limits);
    check(page.toRoot === root(2), "target root decoded incorrectly");
    check(page.deltas.length === 2, "record count decoded incorrectly");
    check(page.deltas[0].action === "added" && page.deltas[0].path === "notes/a.md",
        "added record decoded incorrectly");
    check(page.deltas[0].mtime_ms === 1_700_000_000_009, "u64 mtime decoded incorrectly");
    check(page.deltas[1].action === "deleted" && page.deltas[1].path === "old.md",
        "deleted record decoded incorrectly");

    for (let end = 0; end < pageBytes.byteLength; end++) {
        let rejected = false;
        try { decodeDiffPage(pageBytes.slice(0, end), root(1), null, null, limits); }
        catch { rejected = true; }
        check(rejected, `decoder accepted truncated prefix ${end}`);
    }

    const trailing = new Uint8Array(pageBytes.length + 1); trailing.set(pageBytes);
    checkRejected(trailing, "decoder accepted trailing bytes");

    const unsafe = fixture();
    const unsafePath = new TextEncoder().encode("../secretx");
    unsafe.set(unsafePath, 76);
    checkRejected(unsafe, "decoder accepted traversal path");

    const invalidUtf8 = fixture();
    invalidUtf8[76] = 0xff;
    checkRejected(invalidUtf8, "decoder accepted invalid UTF-8");

    const wrongRoot = fixture();
    wrongRoot[4] = 3;
    checkRejected(wrongRoot, "decoder accepted source-root substitution");

    const tooManyRecords = fixture();
    new DataView(tooManyRecords.buffer).setUint32(68, limits.maxRecords + 1, true);
    checkRejected(tooManyRecords, "decoder accepted record count above negotiated cap");

    const invalidAction = fixture();
    invalidAction[74] = 0;
    checkRejected(invalidAction, "decoder accepted invalid action");

    const unsorted = fixture();
    const secondPathOffset = 74 + 1 + 1 + "notes/a.md".length + 32 + 8 + 8 + 1 + 1;
    unsorted[secondPathOffset] = "a".charCodeAt(0);
    checkRejected(unsorted, "decoder accepted records outside canonical order");

    const nonCanonical = new Uint8Array(pageBytes.length + 1);
    nonCanonical.set(pageBytes.slice(0, 76));
    nonCanonical[75] = 0x8a;
    nonCanonical[76] = 0;
    nonCanonical.set(pageBytes.slice(76), 77);
    checkRejected(nonCanonical, "decoder accepted a non-canonical varint");

    const overCap = new Uint8Array(limits.maxBytes + 1);
    checkRejected(overCap, "decoder retained a response above the negotiated byte cap");

    const overflow = fixture();
    const overflowView = new DataView(overflow.buffer);
    // high 32 bits above 2^21 cannot be represented exactly in JS.
    overflowView.setUint32(74 + 1 + 1 + "notes/a.md".length + 32 + 4, 0x20_0000, true);
    checkRejected(overflow, "decoder accepted unsafe u64");

    const finalCursor = makeCursor("old.md", 3);
    const withCursor = new Uint8Array(pageBytes.length + finalCursor.length);
    withCursor.set(pageBytes.slice(0, 74));
    new DataView(withCursor.buffer).setUint16(72, finalCursor.length, true);
    withCursor.set(finalCursor, 74);
    withCursor.set(pageBytes.slice(74), 74 + finalCursor.length);
    check(decodeDiffPage(withCursor, root(1), null, null, limits).nextCursor !== null,
        "valid exact-key continuation was rejected");
    const forgedCursor = withCursor.slice();
    forgedCursor[74 + 74] = "z".charCodeAt(0);
    checkRejected(forgedCursor, "decoder accepted a cursor not naming the final record");

    let missingTargetRejected = false;
    try { encodeDiffPageRequest(root(1), null, makeCursor("old.md", 3), limits); }
    catch { missingTargetRejected = true; }
    check(missingTargetRejected, "continuation request accepted without a fixed target root");

    const persistedCursor = new Uint8Array([1, 2, 3, 255]);
    check(diffCursorFromHex(diffCursorToHex(persistedCursor))?.join() === persistedCursor.join(),
        "cursor hex persistence round-trip differs");
    let oddHexRejected = false;
    try { diffCursorFromHex("abc"); } catch { oddHexRejected = true; }
    check(oddHexRejected, "persisted odd-length cursor hex was accepted");
}

run();
console.log(`diff-page-codec.test: ${assertions} assertions passed`);
