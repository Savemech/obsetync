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

function diffRejected(operation: () => unknown, expected: RegExp, message: string): void {
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

function concatBytes(parts: readonly Uint8Array[]): Uint8Array {
    const length = parts.reduce((total, part) => total + part.byteLength, 0);
    const output = new Uint8Array(length);
    let offset = 0;
    for (const part of parts) {
        output.set(part, offset);
        offset += part.byteLength;
    }
    return output;
}

function varint(value: number): Uint8Array {
    const bytes: number[] = [];
    do {
        let byte = value & 0x7f;
        value = Math.floor(value / 128);
        if (value > 0) byte |= 0x80;
        bytes.push(byte);
    } while (value > 0);
    return new Uint8Array(bytes);
}

function hex(bytes: Uint8Array): string {
    return Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
}

function writeSafeU64(output: Uint8Array, offset: number, value: number): void {
    const view = new DataView(output.buffer, output.byteOffset, output.byteLength);
    view.setUint32(offset, value >>> 0, true);
    view.setUint32(offset + 4, Math.floor(value / 0x1_0000_0000), true);
}

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

interface SeededDelta {
    action: "added" | "modified" | "deleted" | "renamed";
    path: string;
    old_path?: string;
    hash?: string;
    size?: number;
    mtime_ms?: number;
}

function seededPage(word: () => number, count: number): { bytes: Uint8Array; deltas: SeededDelta[] } {
    const records: Uint8Array[] = [];
    const deltas: SeededDelta[] = [];
    for (let index = 0; index < count; index++) {
        const action = 1 + word() % 4;
        const path = `notes/p-${String(index).padStart(3, "0")}-${(word() & 0xffff).toString(16).padStart(4, "0")}.md`;
        const pathBytes = new TextEncoder().encode(path);
        const parts = [new Uint8Array([action]), varint(pathBytes.byteLength), pathBytes];
        let old_path: string | undefined;
        if (action === 4) {
            old_path = `archive/old-${String(index).padStart(3, "0")}.md`;
            const oldBytes = new TextEncoder().encode(old_path);
            parts.push(varint(oldBytes.byteLength), oldBytes);
        }
        if (action === 3) {
            deltas.push({ action: "deleted", path });
        } else {
            const hashBytes = deterministicBytes(32, word);
            const size = index % 7 === 0 ? Number.MAX_SAFE_INTEGER :
                (word() & 0x1f_ffff) * 0x1_0000_0000 + word();
            const mtime_ms = index % 5 === 0 ? 0 :
                (word() & 0x0f_ffff) * 0x1_0000_0000 + word();
            const payload = new Uint8Array(48);
            payload.set(hashBytes);
            writeSafeU64(payload, 32, size);
            writeSafeU64(payload, 40, mtime_ms);
            parts.push(payload);
            const common = { path, hash: hex(hashBytes), size, mtime_ms };
            if (action === 1) deltas.push({ action: "added", ...common });
            else if (action === 2) deltas.push({ action: "modified", ...common });
            else deltas.push({ action: "renamed", old_path, ...common });
        }
        records.push(concatBytes(parts));
    }
    const header = new Uint8Array(74);
    header.set([0x4f, 0x42, 0x44, 0x31]);
    header.fill(1, 4, 36);
    header.fill(2, 36, 68);
    new DataView(header.buffer).setUint32(68, count, true);
    return { bytes: concatBytes([header, ...records]), deltas };
}

function seededBoundedPagesRoundTrip(): void {
    const word = deterministicWords(0x44494631);
    const countEdges = [0, 1, 2, 3, 7, 8, 15, 24];
    for (let iteration = 0; iteration < 72; iteration++) {
        const count = iteration < countEdges.length ? countEdges[iteration] : word() % 17;
        const generated = seededPage(word, count);
        const padded = new Uint8Array(generated.bytes.byteLength + 11);
        padded.set(generated.bytes, 5);
        const input = iteration % 2 === 0
            ? generated.bytes
            : padded.subarray(5, 5 + generated.bytes.byteLength);
        const decoded = decodeDiffPage(input, root(1), root(2), null, limits);
        check(decoded.fromRoot === root(1) && decoded.toRoot === root(2),
            `seeded diff roots changed at ${iteration}`);
        check(decoded.wireBytes === generated.bytes.byteLength && decoded.nextCursor === null,
            `seeded diff framing changed at ${iteration}`);
        check(decoded.deltas.length === generated.deltas.length,
            `seeded diff count changed at ${iteration}`);
        for (let index = 0; index < generated.deltas.length; index++) {
            const actual = decoded.deltas[index];
            const expected = generated.deltas[index];
            check(actual.action === expected.action && actual.path === expected.path,
                `seeded diff identity changed at ${iteration}/${index}`);
            if (actual.action !== "deleted" && expected.action !== "deleted") {
                check(actual.hash === expected.hash && actual.size === expected.size &&
                    actual.mtime_ms === expected.mtime_ms,
                `seeded diff payload changed at ${iteration}/${index}`);
                if (actual.action === "renamed" && expected.action === "renamed") {
                    check(actual.old_path === expected.old_path,
                        `seeded diff rename changed at ${iteration}/${index}`);
                }
            }
        }

        const from = hex(deterministicBytes(32, word));
        const to = iteration % 3 === 0 ? null : hex(deterministicBytes(32, word));
        const request = encodeDiffPageRequest(from, to, null, limits);
        const requestView = new DataView(request.buffer);
        check(request.byteLength === 78 && hex(request.subarray(4, 36)) === from &&
            hex(request.subarray(36, 68)) === (to ?? "00".repeat(32)) &&
            requestView.getUint32(68, true) === limits.maxRecords &&
            requestView.getUint32(72, true) === limits.maxBytes &&
            requestView.getUint16(76, true) === 0,
        `seeded diff request changed at ${iteration}`);

        const persisted = deterministicBytes(word() % 257, word);
        const restored = diffCursorFromHex(diffCursorToHex(persisted));
        check(restored !== null && hex(restored) === hex(persisted),
            `seeded persisted cursor changed at ${iteration}`);
    }
}

function seededMalformedPagesFailClosed(): void {
    const word = deterministicWords(0x42414432);
    for (let iteration = 0; iteration < 112; iteration++) {
        const valid = fixture();
        let malformed: Uint8Array;
        let expected: RegExp;
        switch (iteration % 14) {
            case 0:
                malformed = valid.slice(); malformed[0] ^= 0xff; expected = /diff page magic/; break;
            case 1:
                malformed = valid.slice(); malformed.fill(0, 36, 68);
                expected = /diff page target root (changed|is zero)/; break;
            case 2:
                malformed = valid.slice();
                new DataView(malformed.buffer).setUint32(68, limits.maxRecords + 1, true);
                expected = /diff page exceeds record cap/; break;
            case 3:
                malformed = valid.slice(); new DataView(malformed.buffer).setUint16(72, 0xffff, true);
                expected = /diff page cursor exceeds hard limit/; break;
            case 4:
                malformed = valid.slice(); new DataView(malformed.buffer).setUint32(68, 1, true);
                expected = /diff page has trailing bytes/; break;
            case 5:
                malformed = valid.slice(); new DataView(malformed.buffer).setUint32(68, 3, true);
                expected = /diff record 2 is truncated/; break;
            case 6:
                malformed = valid.slice(); malformed[74] = 0; expected = /invalid action/; break;
            case 7:
                malformed = valid.slice(); malformed[75] = 0; expected = /path length is invalid/; break;
            case 8:
                malformed = valid.slice(); malformed[75] = 0x7f; expected = /diff record 0 path exceeds page bounds/; break;
            case 9:
                malformed = valid.slice(); malformed[76] = 0xff; expected = /path is not UTF-8/; break;
            case 10:
                malformed = new Uint8Array(valid.byteLength + 1); malformed.set(valid);
                expected = /diff page has trailing bytes/; break;
            case 11:
                malformed = valid.subarray(0, word() % valid.byteLength); expected = /diff|record|page/; break;
            case 12:
                malformed = valid.slice(); new DataView(malformed.buffer).setUint32(68, 0, true);
                expected = /diff page has trailing bytes/; break;
            default:
                malformed = valid.slice(); malformed[75] = 0x80; malformed[76] = 0;
                expected = /diff varint is not canonical/; break;
        }
        diffRejected(
            () => decodeDiffPage(malformed, root(1), root(2), null, limits),
            expected,
            `seeded malformed diff page ${iteration}`,
        );
    }

    const countOnly = new Uint8Array(74);
    countOnly.set([0x4f, 0x42, 0x44, 0x31]);
    countOnly.fill(1, 4, 36);
    countOnly.fill(2, 36, 68);
    new DataView(countOnly.buffer).setUint32(68, 0xffff_ffff, true);
    diffRejected(
        () => decodeDiffPage(countOnly, root(1), root(2), null, limits),
        /diff page exceeds record cap/,
        "header-only diff page with a 4-billion record declaration",
    );

    const cursorOnly = countOnly.slice();
    new DataView(cursorOnly.buffer).setUint32(68, 0, true);
    new DataView(cursorOnly.buffer).setUint16(72, 0xffff, true);
    diffRejected(
        () => decodeDiffPage(cursorOnly, root(1), root(2), null, limits),
        /diff page cursor exceeds hard limit/,
        "header-only diff page with an oversized cursor declaration",
    );

    const pathOnly = new Uint8Array(80);
    pathOnly.set([0x4f, 0x42, 0x44, 0x31]);
    pathOnly.fill(1, 4, 36);
    pathOnly.fill(2, 36, 68);
    new DataView(pathOnly.buffer).setUint32(68, 1, true);
    pathOnly[74] = 1;
    pathOnly.set([0xff, 0xff, 0xff, 0xff, 0x0f], 75);
    diffRejected(
        () => decodeDiffPage(pathOnly, root(1), root(2), null, limits),
        /diff record 0 path length is invalid/,
        "header-only diff record with a 4-GiB path declaration",
    );
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

    seededBoundedPagesRoundTrip();
    seededMalformedPagesFailClosed();
}

run();
console.log(`diff-page-codec.test: ${assertions} assertions passed`);
