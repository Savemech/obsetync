/** Strict inner framing for the sealed WebSocket data lane. */

export const WS_DATA_FRAME_MAGIC = "OBW1";
export const WS_DATA_FRAME_HEADER_BYTES = 18;
const HELLO_MAGIC = "OWH1";
const HELLO_ACK_MAGIC = "OWA1";
const HELLO_BYTES = 24;
const ERROR_MAGIC = "OWE1";
const ERROR_HEADER_BYTES = 14;
const MAX_ERROR_MESSAGE_BYTES = 256;
const MIN_PAYLOAD_BYTES = 1024;
const DESKTOP_PAYLOAD_BYTES = 4 * 1024 * 1024;
const MOBILE_PAYLOAD_BYTES = 2 * 1024 * 1024;

export enum WsDataFrameType {
    Hello = 1,
    HelloAck = 2,
    CheckObjects = 3,
    CheckResult = 4,
    PutPack = 5,
    PutAck = 6,
    GetPack = 7,
    GetResult = 8,
    Cancel = 9,
    Error = 10,
}

export enum WsDataErrorCode {
    InvalidRequest = 1,
    Busy = 2,
    Internal = 3,
    Cancelled = 4,
}

export interface WsDataLimits {
    maxPayloadBytes: number;
    maxInflightRequests: number;
    maxInflightBytes: number;
}

export interface WsDataFrame {
    type: WsDataFrameType;
    requestId: number;
    payload: Uint8Array;
}

export interface WsDataRemoteError {
    code: WsDataErrorCode;
    retryAfterMs: number;
    message: string;
}

export class WsDataProtocolError extends Error {}

const encoder = new TextEncoder();

function magic(bytes: Uint8Array, expected: string): boolean {
    if (bytes.byteLength < 4) return false;
    const expectedBytes = encoder.encode(expected);
    return bytes[0] === expectedBytes[0] && bytes[1] === expectedBytes[1] &&
        bytes[2] === expectedBytes[2] && bytes[3] === expectedBytes[3];
}

function knownType(value: number): value is WsDataFrameType {
    return Number.isInteger(value) &&
        value >= WsDataFrameType.Hello && value <= WsDataFrameType.Error;
}

function boundedInteger(value: number, label: string): number {
    if (!Number.isSafeInteger(value) || value < 0) {
        throw new WsDataProtocolError(`${label} is outside the safe integer range`);
    }
    return value;
}

export function encodeWsDataFrame(
    type: WsDataFrameType,
    requestId: number,
    payload: Uint8Array,
    maxPayloadBytes: number,
): Uint8Array {
    if (!knownType(type)) throw new WsDataProtocolError("unknown data frame type");
    boundedInteger(requestId, "request id");
    boundedInteger(maxPayloadBytes, "payload limit");
    if (payload.byteLength > maxPayloadBytes || payload.byteLength > 0xffff_ffff) {
        throw new WsDataProtocolError("data frame payload exceeds limit");
    }
    const output = new Uint8Array(WS_DATA_FRAME_HEADER_BYTES + payload.byteLength);
    output.set(encoder.encode(WS_DATA_FRAME_MAGIC), 0);
    output[4] = type;
    output[5] = 0;
    const view = new DataView(output.buffer);
    view.setBigUint64(6, BigInt(requestId), true);
    view.setUint32(14, payload.byteLength, true);
    output.set(payload, WS_DATA_FRAME_HEADER_BYTES);
    return output;
}

export function decodeWsDataFrame(
    input: Uint8Array,
    maxPayloadBytes: number,
): WsDataFrame {
    boundedInteger(maxPayloadBytes, "payload limit");
    if (input.byteLength < WS_DATA_FRAME_HEADER_BYTES) {
        throw new WsDataProtocolError("truncated data frame header");
    }
    if (!magic(input, WS_DATA_FRAME_MAGIC)) {
        throw new WsDataProtocolError("invalid data frame magic");
    }
    if (!knownType(input[4])) throw new WsDataProtocolError("unknown data frame type");
    if (input[5] !== 0) throw new WsDataProtocolError("unsupported data frame flags");
    const view = new DataView(input.buffer, input.byteOffset, input.byteLength);
    const requestIdBig = view.getBigUint64(6, true);
    if (requestIdBig > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new WsDataProtocolError("request id exceeds the safe integer range");
    }
    const payloadLength = view.getUint32(14, true);
    if (payloadLength > maxPayloadBytes) {
        throw new WsDataProtocolError("data frame payload exceeds limit");
    }
    const expected = WS_DATA_FRAME_HEADER_BYTES + payloadLength;
    if (input.byteLength !== expected) {
        throw new WsDataProtocolError("data frame length mismatch");
    }
    return {
        type: input[4],
        requestId: Number(requestIdBig),
        payload: input.subarray(WS_DATA_FRAME_HEADER_BYTES),
    };
}

export function initialWsDataLimits(
    runtime: "desktop" | "mobile",
    advertisedPayloadBytes: unknown,
): WsDataLimits | null {
    if (!Number.isSafeInteger(advertisedPayloadBytes) ||
        (advertisedPayloadBytes as number) < MIN_PAYLOAD_BYTES) {
        return null;
    }
    const runtimePayload = runtime === "mobile" ? MOBILE_PAYLOAD_BYTES : DESKTOP_PAYLOAD_BYTES;
    return {
        maxPayloadBytes: Math.min(advertisedPayloadBytes as number, runtimePayload),
        maxInflightRequests: runtime === "mobile" ? 2 : 4,
        maxInflightBytes: runtime === "mobile" ? 8 * 1024 * 1024 : 32 * 1024 * 1024,
    };
}

/** Accept the data lane only from an authenticated capability bundle. Older
 * servers, partial rolling upgrades, and malformed limits all select the
 * bulk-HTTP path without attempting a socket. The returned value is capped
 * again by `initialWsDataLimits` during HELLO negotiation. */
export function negotiateWsDataPayloadBytes(value: unknown): number | null {
    if (!value || typeof value !== "object") return null;
    const bundle = value as {
        capabilities?: unknown;
        limits?: { ws_frame_bytes?: unknown };
    };
    if (!Array.isArray(bundle.capabilities) ||
        !bundle.capabilities.includes("ws-data-v1")) {
        return null;
    }
    const advertised = bundle.limits?.ws_frame_bytes;
    if (!Number.isSafeInteger(advertised) || (advertised as number) < MIN_PAYLOAD_BYTES) {
        return null;
    }
    return Math.min(advertised as number, DESKTOP_PAYLOAD_BYTES);
}

function encodeHelloPayload(magicValue: string, limits: WsDataLimits): Uint8Array {
    validateLimits(limits);
    if (limits.maxInflightRequests > 0xffff || limits.maxPayloadBytes > 0xffff_ffff) {
        throw new WsDataProtocolError("data-lane hello limit overflows its field");
    }
    const output = new Uint8Array(HELLO_BYTES);
    output.set(encoder.encode(magicValue), 0);
    const view = new DataView(output.buffer);
    view.setUint16(4, 1, true);
    view.setUint16(6, limits.maxInflightRequests, true);
    view.setUint32(8, limits.maxPayloadBytes, true);
    view.setUint32(12, 0, true);
    view.setBigUint64(16, BigInt(limits.maxInflightBytes), true);
    return output;
}

export function encodeWsDataHello(limits: WsDataLimits): Uint8Array {
    return encodeHelloPayload(HELLO_MAGIC, limits);
}

export function decodeWsDataHelloAck(
    payload: Uint8Array,
    requested: WsDataLimits,
): WsDataLimits {
    if (payload.byteLength !== HELLO_BYTES || !magic(payload, HELLO_ACK_MAGIC)) {
        throw new WsDataProtocolError("invalid data-lane hello ACK");
    }
    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    if (view.getUint16(4, true) !== 1 || view.getUint32(12, true) !== 0) {
        throw new WsDataProtocolError("unsupported data-lane hello ACK");
    }
    const byteBudgetBig = view.getBigUint64(16, true);
    if (byteBudgetBig > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new WsDataProtocolError("data-lane byte budget exceeds safe range");
    }
    const limits: WsDataLimits = {
        maxInflightRequests: view.getUint16(6, true),
        maxPayloadBytes: view.getUint32(8, true),
        maxInflightBytes: Number(byteBudgetBig),
    };
    validateLimits(limits);
    if (limits.maxPayloadBytes > requested.maxPayloadBytes ||
        limits.maxInflightRequests > requested.maxInflightRequests ||
        limits.maxInflightBytes > requested.maxInflightBytes) {
        throw new WsDataProtocolError("server raised a client data-lane limit");
    }
    return limits;
}

function validateLimits(limits: WsDataLimits): void {
    boundedInteger(limits.maxPayloadBytes, "payload limit");
    boundedInteger(limits.maxInflightRequests, "in-flight request limit");
    boundedInteger(limits.maxInflightBytes, "in-flight byte limit");
    if (limits.maxPayloadBytes < MIN_PAYLOAD_BYTES || limits.maxInflightRequests < 1 ||
        limits.maxInflightBytes < limits.maxPayloadBytes) {
        throw new WsDataProtocolError("invalid data-lane limits");
    }
}

export function decodeWsDataError(payload: Uint8Array): WsDataRemoteError {
    if (payload.byteLength < ERROR_HEADER_BYTES || !magic(payload, ERROR_MAGIC)) {
        throw new WsDataProtocolError("invalid data-lane error payload");
    }
    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    const code = view.getUint16(4, true);
    if (code < WsDataErrorCode.InvalidRequest || code > WsDataErrorCode.Cancelled ||
        view.getUint16(6, true) !== 0) {
        throw new WsDataProtocolError("unknown data-lane error code");
    }
    const messageLength = view.getUint16(12, true);
    if (messageLength > MAX_ERROR_MESSAGE_BYTES ||
        payload.byteLength !== ERROR_HEADER_BYTES + messageLength) {
        throw new WsDataProtocolError("data-lane error length mismatch");
    }
    let message: string;
    try {
        message = new TextDecoder("utf-8", { fatal: true }).decode(
            payload.subarray(ERROR_HEADER_BYTES),
        );
    } catch {
        throw new WsDataProtocolError("data-lane error message is not UTF-8");
    }
    return {
        code,
        retryAfterMs: view.getUint32(8, true),
        message,
    };
}
