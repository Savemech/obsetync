/** Strict inner framing for the sealed WebSocket data lane. */

export const WS_DATA_FRAME_MAGIC = "OBW1";
export const WS_DATA_FRAME_HEADER_BYTES = 18;
export const WS_DATA_FRAGMENT_MAGIC = "OBW2";
export const WS_DATA_FRAGMENT_HEADER_BYTES = 30;
export const WS_DATA_V2_FRAGMENT_BYTES = 64 * 1024;
export const WS_DATA_V2_MAX_FRAGMENTS = 64;
export const WS_DATA_V2_LOGICAL_BYTES =
    WS_DATA_V2_FRAGMENT_BYTES * WS_DATA_V2_MAX_FRAGMENTS;
const HELLO_MAGIC = "OWH1";
const HELLO_ACK_MAGIC = "OWA1";
const HELLO_BYTES = 24;
const HELLO_V2_MAGIC = "OWH2";
const HELLO_ACK_V2_MAGIC = "OWA2";
const HELLO_V2_BYTES = 32;
const FRAGMENT_FIRST = 0x01;
const FRAGMENT_LAST = 0x02;
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

export type WsDataWireVersion = 1 | 2;

export interface WsDataV2Limits extends WsDataLimits {
    maxFragmentPayloadBytes: number;
    maxFragments: number;
}

export interface WsDataFrame {
    type: WsDataFrameType;
    requestId: number;
    payload: Uint8Array;
}

export interface WsDataFragment extends WsDataFrame {
    logicalLength: number;
    offset: number;
    fragmentIndex: number;
    fragmentCount: number;
}

export interface WsDataCapability {
    maxPayloadBytes: number;
    preferredWireVersion: WsDataWireVersion;
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

export function wsDataFragmentCount(
    logicalLength: number,
    maxFragmentPayloadBytes = WS_DATA_V2_FRAGMENT_BYTES,
): number {
    boundedInteger(logicalLength, "logical payload length");
    if (!Number.isSafeInteger(maxFragmentPayloadBytes) || maxFragmentPayloadBytes < 1) {
        throw new WsDataProtocolError("invalid OBW2 fragment cap");
    }
    return logicalLength === 0 ? 1 : Math.ceil(logicalLength / maxFragmentPayloadBytes);
}

export function encodeWsDataFragment(
    type: WsDataFrameType,
    requestId: number,
    logicalPayload: Uint8Array,
    fragmentIndex: number,
    limits: Pick<WsDataV2Limits, "maxPayloadBytes" | "maxFragmentPayloadBytes" | "maxFragments">,
): Uint8Array {
    if (!knownType(type)) throw new WsDataProtocolError("unknown data frame type");
    boundedInteger(requestId, "request id");
    boundedInteger(fragmentIndex, "fragment index");
    validateV2FragmentLimits(limits);
    if (logicalPayload.byteLength > limits.maxPayloadBytes ||
        logicalPayload.byteLength > 0xffff_ffff) {
        throw new WsDataProtocolError("OBW2 logical payload exceeds limit");
    }
    const fragmentCount = wsDataFragmentCount(
        logicalPayload.byteLength,
        limits.maxFragmentPayloadBytes,
    );
    if (fragmentCount > limits.maxFragments || fragmentIndex >= fragmentCount) {
        throw new WsDataProtocolError("OBW2 fragment index/count exceeds limit");
    }
    const offset = fragmentIndex * limits.maxFragmentPayloadBytes;
    const end = Math.min(logicalPayload.byteLength, offset + limits.maxFragmentPayloadBytes);
    const payload = logicalPayload.subarray(offset, end);
    const output = new Uint8Array(WS_DATA_FRAGMENT_HEADER_BYTES + payload.byteLength);
    output.set(encoder.encode(WS_DATA_FRAGMENT_MAGIC), 0);
    output[4] = type;
    output[5] = (fragmentIndex === 0 ? FRAGMENT_FIRST : 0) |
        (fragmentIndex + 1 === fragmentCount ? FRAGMENT_LAST : 0);
    const view = new DataView(output.buffer);
    view.setBigUint64(6, BigInt(requestId), true);
    view.setUint32(14, payload.byteLength, true);
    view.setUint32(18, logicalPayload.byteLength, true);
    view.setUint32(22, offset, true);
    view.setUint16(26, fragmentIndex, true);
    view.setUint16(28, fragmentCount, true);
    output.set(payload, WS_DATA_FRAGMENT_HEADER_BYTES);
    return output;
}

export function decodeWsDataFragment(
    input: Uint8Array,
    limits: Pick<WsDataV2Limits, "maxPayloadBytes" | "maxFragmentPayloadBytes" | "maxFragments">,
): WsDataFragment {
    validateV2FragmentLimits(limits);
    if (input.byteLength < WS_DATA_FRAGMENT_HEADER_BYTES) {
        throw new WsDataProtocolError("truncated OBW2 fragment header");
    }
    if (!magic(input, WS_DATA_FRAGMENT_MAGIC)) {
        throw new WsDataProtocolError("invalid OBW2 fragment magic");
    }
    if (!knownType(input[4])) throw new WsDataProtocolError("unknown data frame type");
    const flags = input[5];
    if ((flags & ~(FRAGMENT_FIRST | FRAGMENT_LAST)) !== 0) {
        throw new WsDataProtocolError("unsupported OBW2 fragment flags");
    }
    const view = new DataView(input.buffer, input.byteOffset, input.byteLength);
    const requestIdBig = view.getBigUint64(6, true);
    if (requestIdBig > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new WsDataProtocolError("request id exceeds the safe integer range");
    }
    const fragmentLength = view.getUint32(14, true);
    const logicalLength = view.getUint32(18, true);
    const offset = view.getUint32(22, true);
    const fragmentIndex = view.getUint16(26, true);
    const fragmentCount = view.getUint16(28, true);
    if (logicalLength > limits.maxPayloadBytes ||
        fragmentLength > limits.maxFragmentPayloadBytes || fragmentCount < 1 ||
        fragmentCount > limits.maxFragments) {
        throw new WsDataProtocolError("OBW2 fragment exceeds negotiated limits");
    }
    const canonicalCount = wsDataFragmentCount(logicalLength, limits.maxFragmentPayloadBytes);
    const canonicalOffset = fragmentIndex * limits.maxFragmentPayloadBytes;
    if (!Number.isSafeInteger(canonicalOffset) || canonicalOffset > logicalLength) {
        throw new WsDataProtocolError("OBW2 fragment starts past logical payload");
    }
    const canonicalLength = Math.min(
        logicalLength - canonicalOffset,
        limits.maxFragmentPayloadBytes,
    );
    const canonicalFlags = (fragmentIndex === 0 ? FRAGMENT_FIRST : 0) |
        (fragmentIndex + 1 === fragmentCount ? FRAGMENT_LAST : 0);
    if (fragmentCount !== canonicalCount || fragmentIndex >= fragmentCount ||
        offset !== canonicalOffset || fragmentLength !== canonicalLength ||
        flags !== canonicalFlags) {
        throw new WsDataProtocolError("non-canonical OBW2 fragment metadata");
    }
    if (input.byteLength !== WS_DATA_FRAGMENT_HEADER_BYTES + fragmentLength) {
        throw new WsDataProtocolError("OBW2 fragment length mismatch");
    }
    return {
        type: input[4],
        requestId: Number(requestIdBig),
        logicalLength,
        offset,
        fragmentIndex,
        fragmentCount,
        payload: input.subarray(WS_DATA_FRAGMENT_HEADER_BYTES),
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
    return negotiateWsDataCapability(value)?.maxPayloadBytes ?? null;
}

export function negotiateWsDataCapability(value: unknown): WsDataCapability | null {
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
    return {
        maxPayloadBytes: Math.min(advertised as number, DESKTOP_PAYLOAD_BYTES),
        preferredWireVersion: bundle.capabilities.includes("ws-data-v2") ? 2 : 1,
    };
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

export function initialWsDataV2Limits(limits: WsDataLimits): WsDataV2Limits {
    validateLimits(limits);
    return {
        ...limits,
        maxFragmentPayloadBytes: WS_DATA_V2_FRAGMENT_BYTES,
        maxFragments: WS_DATA_V2_MAX_FRAGMENTS,
    };
}

function encodeHelloV2Payload(magicValue: string, limits: WsDataV2Limits): Uint8Array {
    validateLimits(limits);
    validateV2FragmentLimits(limits);
    if (limits.maxInflightRequests > 0xffff || limits.maxPayloadBytes > 0xffff_ffff ||
        limits.maxFragmentPayloadBytes > 0xffff_ffff || limits.maxFragments > 0xffff) {
        throw new WsDataProtocolError("OBW2 data-lane hello limit overflows its field");
    }
    const output = new Uint8Array(HELLO_V2_BYTES);
    output.set(encoder.encode(magicValue), 0);
    const view = new DataView(output.buffer);
    view.setUint16(4, 2, true);
    view.setUint16(6, limits.maxInflightRequests, true);
    view.setUint32(8, limits.maxPayloadBytes, true);
    view.setUint32(12, 0, true);
    view.setBigUint64(16, BigInt(limits.maxInflightBytes), true);
    view.setUint32(24, limits.maxFragmentPayloadBytes, true);
    view.setUint16(28, limits.maxFragments, true);
    view.setUint16(30, 0, true);
    return output;
}

export function encodeWsDataHelloV2(limits: WsDataV2Limits): Uint8Array {
    return encodeHelloV2Payload(HELLO_V2_MAGIC, limits);
}

export function decodeWsDataHelloAckV2(
    payload: Uint8Array,
    requested: WsDataV2Limits,
): WsDataV2Limits {
    if (payload.byteLength !== HELLO_V2_BYTES || !magic(payload, HELLO_ACK_V2_MAGIC)) {
        throw new WsDataProtocolError("invalid OBW2 data-lane hello ACK");
    }
    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    if (view.getUint16(4, true) !== 2 || view.getUint32(12, true) !== 0 ||
        view.getUint16(30, true) !== 0) {
        throw new WsDataProtocolError("unsupported OBW2 data-lane hello ACK");
    }
    const byteBudgetBig = view.getBigUint64(16, true);
    if (byteBudgetBig > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new WsDataProtocolError("data-lane byte budget exceeds safe range");
    }
    const limits: WsDataV2Limits = {
        maxInflightRequests: view.getUint16(6, true),
        maxPayloadBytes: view.getUint32(8, true),
        maxInflightBytes: Number(byteBudgetBig),
        maxFragmentPayloadBytes: view.getUint32(24, true),
        maxFragments: view.getUint16(28, true),
    };
    validateLimits(limits);
    validateV2FragmentLimits(limits);
    if (limits.maxPayloadBytes > requested.maxPayloadBytes ||
        limits.maxInflightRequests > requested.maxInflightRequests ||
        limits.maxInflightBytes > requested.maxInflightBytes ||
        limits.maxFragmentPayloadBytes > requested.maxFragmentPayloadBytes ||
        limits.maxFragments > requested.maxFragments ||
        limits.maxPayloadBytes > limits.maxFragmentPayloadBytes * limits.maxFragments) {
        throw new WsDataProtocolError("server raised an OBW2 client limit");
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

function validateV2FragmentLimits(
    limits: Pick<WsDataV2Limits, "maxPayloadBytes" | "maxFragmentPayloadBytes" | "maxFragments">,
): void {
    boundedInteger(limits.maxPayloadBytes, "logical payload limit");
    boundedInteger(limits.maxFragmentPayloadBytes, "fragment payload limit");
    boundedInteger(limits.maxFragments, "fragment count limit");
    if (limits.maxPayloadBytes > WS_DATA_V2_LOGICAL_BYTES ||
        limits.maxFragmentPayloadBytes < 1 ||
        limits.maxFragmentPayloadBytes > WS_DATA_V2_FRAGMENT_BYTES ||
        limits.maxFragments < 1 || limits.maxFragments > WS_DATA_V2_MAX_FRAGMENTS ||
        limits.maxPayloadBytes > limits.maxFragmentPayloadBytes * limits.maxFragments) {
        throw new WsDataProtocolError("invalid OBW2 fragment limits");
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
