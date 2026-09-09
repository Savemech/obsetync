import {
    WsDataErrorCode,
    WsDataFrameType,
    WsDataProtocolError,
    WS_DATA_FRAGMENT_HEADER_BYTES,
    WS_DATA_FRAME_HEADER_BYTES,
    WS_DATA_V2_FRAGMENT_BYTES,
    WS_DATA_V2_LOGICAL_BYTES,
    decodeWsDataError,
    decodeWsDataFrame,
    decodeWsDataFragment,
    decodeWsDataHelloAck,
    decodeWsDataHelloAckV2,
    encodeWsDataFrame,
    encodeWsDataFragment,
    encodeWsDataHello,
    encodeWsDataHelloV2,
    initialWsDataLimits,
    initialWsDataV2Limits,
    negotiateWsDataCapability,
    negotiateWsDataPayloadBytes,
    wsDataFragmentCount,
    type WsDataLimits,
} from "./ws-data-codec";

const check = (condition: unknown, message: string) => {
    if (!condition) throw new Error(message);
};

let assertions = 0;
const ok = (condition: unknown, message: string) => {
    assertions++;
    check(condition, message);
};

const protocolRejected = (operation: () => unknown, message: string) => {
    assertions++;
    try {
        operation();
    } catch (error) {
        check(error instanceof WsDataProtocolError, `${message}: non-protocol error`);
        return;
    }
    throw new Error(`${message}: input was accepted`);
};

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

function deterministicRequestId(word: () => number, iteration: number): number {
    if (iteration % 17 === 0) return 0;
    if (iteration % 19 === 0) return Number.MAX_SAFE_INTEGER;
    return (word() & 0x1f_ffff) * 0x1_0000_0000 + word();
}

const requested: WsDataLimits = {
    maxPayloadBytes: 4 * 1024 * 1024,
    maxInflightRequests: 4,
    maxInflightBytes: 32 * 1024 * 1024,
};

{
    const payload = new Uint8Array([1, 0, 2, 255]);
    const encoded = encodeWsDataFrame(WsDataFrameType.PutPack, 42, payload, 1024);
    const decoded = decodeWsDataFrame(encoded, 1024);
    ok(decoded.type === WsDataFrameType.PutPack, "frame type changed");
    ok(decoded.requestId === 42, "request id changed");
    ok(decoded.payload.join(",") === "1,0,2,255", "binary payload changed");
    ok(encoded.join(",") === "79,66,87,49,5,0,42,0,0,0,0,0,0,0,4,0,0,0,1,0,2,255",
        "OBW1 bytes changed while adding OBW2");

    const malformed = [
        encoded.subarray(0, 17),
        (() => { const value = encoded.slice(); value[0] = 0; return value; })(),
        (() => { const value = encoded.slice(); value[4] = 255; return value; })(),
        (() => { const value = encoded.slice(); value[5] = 1; return value; })(),
        (() => {
            const value = encoded.slice();
            new DataView(value.buffer).setUint32(14, 100, true);
            return value;
        })(),
    ];
    for (const value of malformed) {
        let rejected = false;
        try { decodeWsDataFrame(value, 1024); } catch { rejected = true; }
        ok(rejected, "malformed frame was accepted");
    }
    let rejected = false;
    try { decodeWsDataFrame(encoded, payload.byteLength - 1); } catch { rejected = true; }
    ok(rejected, "payload cap was ignored");
}

{
    const word = deterministicWords(0x4f425731);
    const edgeLengths = [0, 1, 17, 255, 1023, 2048, 4096];
    for (let iteration = 0; iteration < 128; iteration++) {
        const length = iteration < edgeLengths.length
            ? edgeLengths[iteration]
            : word() % 4097;
        const payload = deterministicBytes(length, word);
        const type = 1 + word() % WsDataFrameType.Error as WsDataFrameType;
        const requestId = deterministicRequestId(word, iteration);
        const encoded = encodeWsDataFrame(type, requestId, payload, length);
        const padded = new Uint8Array(encoded.byteLength + 7);
        padded.set(encoded, 3);
        const input = iteration % 2 === 0 ? encoded : padded.subarray(3, 3 + encoded.byteLength);
        const decoded = decodeWsDataFrame(input, length);
        ok(decoded.type === type, `seeded OBW1 type changed at ${iteration}`);
        ok(decoded.requestId === requestId, `seeded OBW1 request id changed at ${iteration}`);
        ok(sameBytes(decoded.payload, payload), `seeded OBW1 payload changed at ${iteration}`);
        ok(encoded.byteLength === WS_DATA_FRAME_HEADER_BYTES + length,
            `seeded OBW1 encoded length changed at ${iteration}`);
    }
}

{
    const word = deterministicWords(0x31444142);
    for (let iteration = 0; iteration < 96; iteration++) {
        const payload = deterministicBytes(1 + word() % 67, word);
        const encoded = encodeWsDataFrame(
            1 + word() % WsDataFrameType.Error as WsDataFrameType,
            deterministicRequestId(word, iteration),
            payload,
            128,
        );
        let malformed: Uint8Array;
        let decodeLimit = 128;
        switch (iteration % 8) {
            case 0:
                malformed = encoded.slice(); malformed[0] ^= 0xff; break;
            case 1:
                malformed = encoded.slice(); malformed[4] = 0; break;
            case 2:
                malformed = encoded.slice(); malformed[5] = 0x80; break;
            case 3:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setBigUint64(6, BigInt(Number.MAX_SAFE_INTEGER) + 1n, true);
                break;
            case 4:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setUint32(14, payload.byteLength + 1, true);
                break;
            case 5:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setUint32(14, 0xffff_ffff, true);
                decodeLimit = 0xffff_ffff;
                break;
            case 6:
                malformed = encoded.subarray(0, encoded.byteLength - 1); break;
            default:
                malformed = new Uint8Array(encoded.byteLength + 1); malformed.set(encoded); break;
        }
        protocolRejected(
            () => decodeWsDataFrame(malformed, decodeLimit),
            `seeded malformed OBW1 frame ${iteration}`,
        );
    }
}

{
    const limits = initialWsDataV2Limits(requested);
    const payload = new Uint8Array(WS_DATA_V2_FRAGMENT_BYTES + 3);
    payload[0] = 1;
    payload[WS_DATA_V2_FRAGMENT_BYTES] = 2;
    payload[payload.length - 1] = 3;
    ok(wsDataFragmentCount(payload.byteLength) === 2, "OBW2 fragment count changed");
    ok(wsDataFragmentCount(WS_DATA_V2_LOGICAL_BYTES) === 64,
        "OBW2 logical ceiling is not exactly 64 fragments");
    const first = encodeWsDataFragment(WsDataFrameType.PutPack, 77, payload, 0, limits);
    const last = encodeWsDataFragment(WsDataFrameType.PutPack, 77, payload, 1, limits);
    const decodedFirst = decodeWsDataFragment(first, limits);
    const decodedLast = decodeWsDataFragment(last, limits);
    ok(decodedFirst.requestId === 77 && decodedFirst.fragmentIndex === 0 &&
        decodedFirst.fragmentCount === 2 && decodedFirst.offset === 0,
    "OBW2 first fragment metadata changed");
    ok(decodedFirst.payload.byteLength === WS_DATA_V2_FRAGMENT_BYTES &&
        decodedFirst.payload[0] === 1, "OBW2 full fragment payload changed");
    ok(decodedLast.fragmentIndex === 1 && decodedLast.offset === WS_DATA_V2_FRAGMENT_BYTES &&
        decodedLast.payload.join(",") === "2,0,3", "OBW2 last fragment changed");

    const empty = decodeWsDataFragment(encodeWsDataFragment(
        WsDataFrameType.Cancel,
        77,
        new Uint8Array(),
        0,
        limits,
    ), limits);
    ok(empty.fragmentCount === 1 && empty.payload.byteLength === 0,
        "zero-length OBW2 control was not canonical");

    const malformed = [
        (() => { const value = first.slice(); value[5] = 0; return value; })(),
        (() => {
            const value = first.slice();
            new DataView(value.buffer).setUint32(22, 1, true);
            return value;
        })(),
        (() => {
            const value = first.slice();
            new DataView(value.buffer).setUint16(28, 3, true);
            return value;
        })(),
        (() => {
            const value = first.slice();
            new DataView(value.buffer).setUint32(14, WS_DATA_V2_FRAGMENT_BYTES - 1, true);
            return value;
        })(),
        first.subarray(0, first.byteLength - 1),
    ];
    for (const value of malformed) {
        let rejected = false;
        try { decodeWsDataFragment(value, limits); } catch { rejected = true; }
        ok(rejected, "non-canonical OBW2 fragment was accepted");
    }
    let oversizedRejected = false;
    try {
        encodeWsDataFragment(
            WsDataFrameType.PutPack,
            77,
            new Uint8Array(WS_DATA_V2_LOGICAL_BYTES + 1),
            0,
            limits,
        );
    } catch { oversizedRejected = true; }
    ok(oversizedRejected, "OBW2 accepted a 65th logical fragment");
}

{
    const word = deterministicWords(0x4f425732);
    const fragmentCaps = [1, 3, 17, 64, 257, 1024];
    for (let iteration = 0; iteration < 96; iteration++) {
        const maxFragmentPayloadBytes = fragmentCaps[iteration % fragmentCaps.length];
        const maxFragments = 1 + word() % 64;
        const maxPayloadBytes = maxFragmentPayloadBytes * maxFragments;
        const edges = [
            0,
            1,
            Math.max(0, maxFragmentPayloadBytes - 1),
            maxFragmentPayloadBytes,
            Math.min(maxPayloadBytes, maxFragmentPayloadBytes + 1),
            maxPayloadBytes,
        ];
        const logicalLength = iteration < edges.length
            ? edges[iteration]
            : word() % (maxPayloadBytes + 1);
        const limits = { maxPayloadBytes, maxFragmentPayloadBytes, maxFragments };
        const payload = deterministicBytes(logicalLength, word);
        const requestId = deterministicRequestId(word, iteration);
        const type = 1 + word() % WsDataFrameType.Error as WsDataFrameType;
        const count = wsDataFragmentCount(logicalLength, maxFragmentPayloadBytes);
        const reassembled = new Uint8Array(logicalLength);
        for (let fragmentIndex = 0; fragmentIndex < count; fragmentIndex++) {
            const encoded = encodeWsDataFragment(type, requestId, payload, fragmentIndex, limits);
            const padded = new Uint8Array(encoded.byteLength + 5);
            padded.set(encoded, 2);
            const input = fragmentIndex % 2 === 0
                ? encoded
                : padded.subarray(2, 2 + encoded.byteLength);
            const decoded = decodeWsDataFragment(input, limits);
            const expectedOffset = fragmentIndex * maxFragmentPayloadBytes;
            const expected = payload.subarray(
                expectedOffset,
                Math.min(logicalLength, expectedOffset + maxFragmentPayloadBytes),
            );
            ok(decoded.type === type && decoded.requestId === requestId,
                `seeded OBW2 identity changed at ${iteration}/${fragmentIndex}`);
            ok(decoded.logicalLength === logicalLength && decoded.fragmentCount === count &&
                decoded.fragmentIndex === fragmentIndex && decoded.offset === expectedOffset,
            `seeded OBW2 metadata changed at ${iteration}/${fragmentIndex}`);
            ok(sameBytes(decoded.payload, expected),
                `seeded OBW2 payload changed at ${iteration}/${fragmentIndex}`);
            ok(encoded.byteLength === WS_DATA_FRAGMENT_HEADER_BYTES + expected.byteLength,
                `seeded OBW2 encoded length changed at ${iteration}/${fragmentIndex}`);
            reassembled.set(decoded.payload, decoded.offset);
        }
        ok(sameBytes(reassembled, payload), `seeded OBW2 reassembly changed at ${iteration}`);
    }
}

{
    const word = deterministicWords(0x32444142);
    const limits = { maxPayloadBytes: 136, maxFragmentPayloadBytes: 17, maxFragments: 8 };
    for (let iteration = 0; iteration < 140; iteration++) {
        const payload = deterministicBytes(1 + word() % 100, word);
        const count = wsDataFragmentCount(payload.byteLength, limits.maxFragmentPayloadBytes);
        const fragmentIndex = word() % count;
        const encoded = encodeWsDataFragment(
            1 + word() % WsDataFrameType.Error as WsDataFrameType,
            deterministicRequestId(word, iteration),
            payload,
            fragmentIndex,
            limits,
        );
        let malformed: Uint8Array;
        switch (iteration % 14) {
            case 0:
                malformed = encoded.slice(); malformed[0] ^= 0xff; break;
            case 1:
                malformed = encoded.slice(); malformed[4] = 0; break;
            case 2:
                malformed = encoded.slice(); malformed[5] |= 0x80; break;
            case 3:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setBigUint64(6, BigInt(Number.MAX_SAFE_INTEGER) + 1n, true);
                break;
            case 4:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setUint32(14,
                    new DataView(malformed.buffer).getUint32(14, true) + 1, true);
                break;
            case 5:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setUint32(14, 0xffff_ffff, true);
                break;
            case 6:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setUint32(18, limits.maxPayloadBytes + 1, true);
                break;
            case 7:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setUint32(22,
                    new DataView(malformed.buffer).getUint32(22, true) + 1, true);
                break;
            case 8:
                malformed = encoded.slice();
                new DataView(malformed.buffer).setUint16(26, count, true);
                break;
            case 9:
                malformed = encoded.slice(); new DataView(malformed.buffer).setUint16(28, 0, true); break;
            case 10:
                malformed = encoded.slice(); new DataView(malformed.buffer).setUint16(28, count + 1, true); break;
            case 11:
                malformed = encoded.slice(); malformed[5] ^= 0x01; break;
            case 12:
                malformed = encoded.subarray(0, encoded.byteLength - 1); break;
            default:
                malformed = new Uint8Array(encoded.byteLength + 1); malformed.set(encoded); break;
        }
        protocolRejected(
            () => decodeWsDataFragment(malformed, limits),
            `seeded malformed OBW2 fragment ${iteration}`,
        );
    }
}

{
    const declaredHugeFrame = encodeWsDataFrame(
        WsDataFrameType.GetResult,
        1,
        new Uint8Array(),
        0,
    );
    new DataView(declaredHugeFrame.buffer).setUint32(14, 0xffff_ffff, true);
    protocolRejected(
        () => decodeWsDataFrame(declaredHugeFrame, 0xffff_ffff),
        "header-only OBW1 frame with a 4-GiB declaration",
    );

    const tinyLimits = { maxPayloadBytes: 64, maxFragmentPayloadBytes: 1, maxFragments: 64 };
    const declaredHugeFragment = encodeWsDataFragment(
        WsDataFrameType.GetResult,
        1,
        new Uint8Array(),
        0,
        tinyLimits,
    );
    new DataView(declaredHugeFragment.buffer).setUint32(18, 0xffff_ffff, true);
    protocolRejected(
        () => decodeWsDataFragment(declaredHugeFragment, tinyLimits),
        "header-only OBW2 fragment with a 4-GiB logical declaration",
    );

    const declaredHugeError = new Uint8Array(14);
    declaredHugeError.set(new TextEncoder().encode("OWE1"));
    const errorView = new DataView(declaredHugeError.buffer);
    errorView.setUint16(4, WsDataErrorCode.InvalidRequest, true);
    errorView.setUint16(12, 0xffff, true);
    protocolRejected(
        () => decodeWsDataError(declaredHugeError),
        "header-only error payload with an oversized message declaration",
    );
}

{
    ok(negotiateWsDataPayloadBytes({ capabilities: [] }) === null,
        "old server unexpectedly enabled WS data");
    ok(negotiateWsDataPayloadBytes({
        capabilities: ["ws-data-v1"],
        limits: { ws_frame_bytes: 100 },
    }) === null, "malformed WS capability was accepted");
    ok(negotiateWsDataPayloadBytes({
        capabilities: ["ws-data-v1"],
        limits: { ws_frame_bytes: 16 * 1024 * 1024 },
    }) === 4 * 1024 * 1024, "advertised WS cap escaped the compiled ceiling");
    ok(negotiateWsDataCapability({
        capabilities: ["ws-data-v1", "ws-data-v2"],
        limits: { ws_frame_bytes: 4 * 1024 * 1024 },
    })?.preferredWireVersion === 2, "authenticated OBW2 capability was ignored");
}

{
    const requestedV2 = initialWsDataV2Limits(requested);
    const hello = encodeWsDataHelloV2(requestedV2);
    hello.set(new TextEncoder().encode("OWA2"), 0);
    new DataView(hello.buffer).setUint16(6, 2, true);
    new DataView(hello.buffer).setUint32(8, 2 * 1024 * 1024, true);
    new DataView(hello.buffer).setBigUint64(16, BigInt(8 * 1024 * 1024), true);
    const accepted = decodeWsDataHelloAckV2(hello, requestedV2);
    ok(accepted.maxInflightRequests === 2 && accepted.maxPayloadBytes === 2 * 1024 * 1024,
        "OBW2 HELLO credits changed");
    ok(accepted.maxFragmentPayloadBytes === 64 * 1024 && accepted.maxFragments === 64,
        "OBW2 HELLO fragmentation limits changed");
    const raised = hello.slice();
    new DataView(raised.buffer).setUint32(24, 128 * 1024, true);
    let rejected = false;
    try { decodeWsDataHelloAckV2(raised, requestedV2); } catch { rejected = true; }
    ok(rejected, "server raised the OBW2 fragment cap");
}

{
    const hello = encodeWsDataHello(requested);
    hello.set(new TextEncoder().encode("OWA1"), 0);
    new DataView(hello.buffer).setUint16(6, 2, true);
    new DataView(hello.buffer).setUint32(8, 2 * 1024 * 1024, true);
    new DataView(hello.buffer).setBigUint64(16, BigInt(8 * 1024 * 1024), true);
    const accepted = decodeWsDataHelloAck(hello, requested);
    ok(accepted.maxInflightRequests === 2, "HELLO request credits changed");
    ok(accepted.maxPayloadBytes === 2 * 1024 * 1024, "HELLO frame cap changed");
    ok(accepted.maxInflightBytes === 8 * 1024 * 1024, "HELLO byte credits changed");

    const raised = hello.slice();
    new DataView(raised.buffer).setUint16(6, 5, true);
    let rejected = false;
    try { decodeWsDataHelloAck(raised, requested); } catch { rejected = true; }
    ok(rejected, "server was allowed to raise client credits");
}

{
    const desktop = initialWsDataLimits("desktop", 16 * 1024 * 1024)!;
    const mobile = initialWsDataLimits("mobile", 16 * 1024 * 1024)!;
    ok(desktop.maxPayloadBytes === 4 * 1024 * 1024, "desktop frame cap changed");
    ok(desktop.maxInflightRequests === 4, "desktop request cap changed");
    ok(mobile.maxPayloadBytes === 2 * 1024 * 1024, "mobile frame cap changed");
    ok(mobile.maxInflightRequests === 2, "mobile request cap changed");
    ok(initialWsDataLimits("mobile", 100) === null, "tiny advertised cap was accepted");
}

{
    const message = new TextEncoder().encode("storage busy");
    const payload = new Uint8Array(14 + message.byteLength);
    payload.set(new TextEncoder().encode("OWE1"));
    const view = new DataView(payload.buffer);
    view.setUint16(4, WsDataErrorCode.Busy, true);
    view.setUint32(8, 25, true);
    view.setUint16(12, message.byteLength, true);
    payload.set(message, 14);
    const decoded = decodeWsDataError(payload);
    ok(decoded.code === WsDataErrorCode.Busy, "error code changed");
    ok(decoded.retryAfterMs === 25, "retry hint changed");
    ok(decoded.message === "storage busy", "error message changed");

    const trailing = new Uint8Array(payload.byteLength + 1);
    trailing.set(payload);
    let rejected = false;
    try { decodeWsDataError(trailing); } catch { rejected = true; }
    ok(rejected, "trailing error bytes were accepted");
}

console.log(`ws-data-codec.test: ${assertions} assertions passed`);
