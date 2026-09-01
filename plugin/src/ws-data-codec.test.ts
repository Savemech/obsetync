import {
    WsDataErrorCode,
    WsDataFrameType,
    decodeWsDataError,
    decodeWsDataFrame,
    decodeWsDataHelloAck,
    encodeWsDataFrame,
    encodeWsDataHello,
    initialWsDataLimits,
    negotiateWsDataPayloadBytes,
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
