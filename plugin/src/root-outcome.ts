import { isSafeVaultPath } from "./delta-validation";

export const ROOT_MAX_BYTES = 512 * 1024;
export const ROOT_COMMIT_MAX_BYTES = 704 * 1024;
export const ROOT_QUERY_MAX_BYTES = 4 * 1024;
export const ROOT_RECEIPT_MAX_BYTES = 64 * 1024;
export const ROOT_RESPONSE_MAX_BYTES = ROOT_RECEIPT_MAX_BYTES + 512;
export const ROOT_CAPABILITY_MAX_BYTES = 64 * 1024;
export const ROOT_MAX_CONFLICTS = 256;

export interface RootOutcomeIdentity { sequence: number; mutation_id: string; request_hash: string }
export interface RootCommitRequest {
    protocol_version: 1; server_incarnation: string; sequence: number;
    mutation_id: string; parent_root: string; root: string;
}
/** request_hash is local durable identity, not an extra field on the wire. */
export interface RootCommitIntent extends RootCommitRequest { request_hash: string }
export interface RootCancelRequest extends RootOutcomeIdentity { protocol_version: 1; server_incarnation: string }
export interface RootConflict { path: string; base_hash: string; side_a_hash: string; side_b_hash: string }
export type RootAcceptedResult = { accepted: true; root_hash: string } | {
    merged: true; root_hash: string; conflicts: RootConflict[]; auto_resolved: number; text_merged: number;
};
interface OutcomeEnvelope { protocol_version: 1; server_incarnation: string }
export type RootTerminalOutcome = OutcomeEnvelope & RootOutcomeIdentity & (
    { status: "accepted"; result: RootAcceptedResult } |
    { status: "cancelled"; result: { cancelled: true } }
);
export type RootStreamOutcome = OutcomeEnvelope & {
    status: "stream" | "unknown" | "expired"; last_sequence: number; current_root_hash: string | null;
};
export type RootOutcome = RootTerminalOutcome | RootStreamOutcome;
export interface RootOutcomeCapabilities {
    serverIncarnation: string; maxSequence: number; commitBytes: number; rootBytes: number;
    queryBytes: number; cancelBytes: number; receiptBytes: number; streamsPerVault: number;
}
export type RootHashBytes = (bytes: Uint8Array) => string | Promise<string>;

export class RootOutcomeProtocolError extends Error {
    readonly code = "ROOT_OUTCOME_PROTOCOL";
    constructor(message = "Invalid root-outcome-v1 data") { super(message); this.name = "RootOutcomeProtocolError"; }
}
function fail(message?: string): never { throw new RootOutcomeProtocolError(message); }
const utf8 = new TextEncoder();
function object(value: unknown): Record<string, unknown> {
    if (value === null || typeof value !== "object" || Array.isArray(value)) fail();
    return value as Record<string, unknown>;
}
function shape(value: unknown, fields: readonly string[]): Record<string, unknown> {
    const row = object(value), keys = Object.keys(row);
    if (keys.length !== fields.length || fields.some(field => !Object.prototype.hasOwnProperty.call(row, field))) fail();
    return row;
}
function hex(value: unknown, length = 64): string {
    if (typeof value !== "string" || value.length !== length || !/^[0-9a-f]+$/.test(value)) fail();
    return value;
}
function counter(value: unknown, minimum = 0): number {
    if (typeof value !== "number" || !Number.isSafeInteger(value) || Object.is(value, -0) || value < minimum) fail();
    return value;
}
function version(value: unknown): 1 { if (value !== 1) fail(); return 1; }
function stringBytes(value: unknown, maximum: number): Uint8Array {
    if (typeof value !== "string" || value.length > maximum) fail();
    // TextEncoder silently replaces lone surrogates; they must not alias a
    // different persisted scope/path in a canonical application digest.
    for (let index = 0; index < value.length; index++) {
        const code = value.charCodeAt(index);
        if (code >= 0xd800 && code <= 0xdbff) {
            const next = value.charCodeAt(++index);
            if (!(next >= 0xdc00 && next <= 0xdfff)) fail();
        } else if (code >= 0xdc00 && code <= 0xdfff) fail();
    }
    const bytes = utf8.encode(value);
    if (bytes.length > maximum) fail();
    return bytes;
}
export function validateRootScopeId(value: string): string {
    if (!stringBytes(value, 128).length || /[\x00-\x1f\x7f/\\]/.test(value) || value === "." || value === "..") fail();
    return value;
}
function base64Digit(code: number): number {
    if (code >= 65 && code <= 90) return code - 65;
    if (code >= 97 && code <= 122) return code - 71;
    if (code >= 48 && code <= 57) return code + 4;
    return code === 43 ? 62 : code === 47 ? 63 : -1;
}
/** Validate canonical standard base64 and decoded length without atob or a
 * giant regular-expression repetition/backtracking stack. */
function base64Root(value: unknown): string {
    if (typeof value !== "string" || !value.length || value.length % 4 ||
        value.length > Math.ceil(ROOT_MAX_BYTES / 3) * 4) fail();
    const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
    for (let i = 0; i < value.length - padding; i++) if (base64Digit(value.charCodeAt(i)) < 0) fail();
    const bytes = value.length / 4 * 3 - padding;
    if (bytes < 1 || bytes > ROOT_MAX_BYTES) fail();
    const last = base64Digit(value.charCodeAt(value.length - padding - 1));
    if ((padding === 2 && (last & 15)) || (padding === 1 && (last & 3))) fail();
    return value;
}
function rootBytes(encoded: string): Uint8Array {
    const binary = atob(base64Root(encoded));
    // TypedArray.from(iterable) first materializes an unbounded element list.
    // Allocate the exact bounded target instead so byte-workset accounting can
    // conservatively cover this decoder without per-character JS objects.
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
    return bytes;
}
export function rootCommitRootByteLength(value: RootCommitRequest): number {
    const encoded = detachRootCommitRequest(value).root;
    const padding = encoded.endsWith("==") ? 2 : encoded.endsWith("=") ? 1 : 0;
    return encoded.length / 4 * 3 - padding;
}
function boundedJSON(value: unknown, maximum: number): Uint8Array {
    const text = JSON.stringify(value);
    if (text === undefined || text.length > maximum) fail("Root JSON exceeds byte limit");
    const bytes = utf8.encode(text);
    if (bytes.length > maximum) fail("Root JSON exceeds byte limit");
    return bytes;
}
export function detachRootOutcomeIdentity(value: unknown): RootOutcomeIdentity {
    const row = shape(value, ["sequence", "mutation_id", "request_hash"]);
    return { sequence: counter(row.sequence, 1), mutation_id: hex(row.mutation_id, 32), request_hash: hex(row.request_hash) };
}
export function detachRootCommitRequest(value: unknown): RootCommitRequest {
    const row = shape(value, ["protocol_version", "server_incarnation", "sequence", "mutation_id", "parent_root", "root"]);
    return { protocol_version: version(row.protocol_version), server_incarnation: hex(row.server_incarnation),
        sequence: counter(row.sequence, 1), mutation_id: hex(row.mutation_id, 32),
        parent_root: row.parent_root === "" ? "" : hex(row.parent_root), root: base64Root(row.root) };
}
export function detachRootCommitIntent(value: unknown): RootCommitIntent {
    const row = shape(value, ["protocol_version", "server_incarnation", "sequence", "mutation_id", "parent_root", "root", "request_hash"]);
    const { request_hash, ...request } = row;
    return { ...detachRootCommitRequest(request), request_hash: hex(request_hash) };
}
/** Bounded canonical preimage, portable to the existing wasm_hash callback.
 * The callback must be BLAKE3 and settle after its actual work. This helper
 * does not claim transient allocator/RSS coverage for caller-owned metadata. */
export async function computeRootRequestHash(
    vaultId: string, deviceId: string, request: RootCommitRequest, hashBytes: RootHashBytes,
): Promise<string> {
    const captured = detachRootCommitRequest(request);
    const fields = [utf8.encode(validateRootScopeId(vaultId)), utf8.encode(validateRootScopeId(deviceId)),
        utf8.encode(captured.server_incarnation), utf8.encode(captured.mutation_id),
        utf8.encode(captured.parent_root), rootBytes(captured.root)];
    const domain = utf8.encode("obsetync.root-commit.v1\0");
    const bytes = new Uint8Array(domain.length + 8 + fields.reduce((sum, field) => sum + 8 + field.length, 0));
    bytes.set(domain);
    const view = new DataView(bytes.buffer);
    let position = domain.length;
    const u64 = (value: number) => {
        view.setUint32(position, value % 0x100000000, true);
        view.setUint32(position + 4, Math.floor(value / 0x100000000), true);
        position += 8;
    };
    u64(captured.sequence);
    for (const field of fields) { u64(field.length); bytes.set(field, position); position += field.length; }
    return hex(await hashBytes(bytes));
}
export async function createRootCommitIntent(
    vaultId: string, deviceId: string, request: RootCommitRequest, hashBytes: RootHashBytes,
): Promise<RootCommitIntent> {
    const captured = detachRootCommitRequest(request);
    return { ...captured, request_hash: await computeRootRequestHash(vaultId, deviceId, captured, hashBytes) };
}
export function encodeRootCommitIntent(value: RootCommitIntent): Uint8Array {
    const { request_hash: _, ...request } = detachRootCommitIntent(value);
    return boundedJSON(request, ROOT_COMMIT_MAX_BYTES);
}
/** Exact canonical wire length without materializing the potentially 704 KiB
 * JSON string/UTF-8 body. base64Root() guarantees that root is unescaped ASCII,
 * so replacing the empty root adds exactly root.length encoded bytes. */
export function rootCommitIntentEncodedLength(value: RootCommitIntent): number {
    const { request_hash: _, root, ...request } = detachRootCommitIntent(value);
    const bytes = boundedJSON({ ...request, root: "" }, ROOT_COMMIT_MAX_BYTES).byteLength + root.length;
    if (bytes > ROOT_COMMIT_MAX_BYTES) fail("Root JSON exceeds byte limit");
    return bytes;
}
export function encodeRootOutcomeQuery(identity?: RootOutcomeIdentity): Uint8Array {
    return boundedJSON(identity === undefined ? { protocol_version: 1 } :
        { protocol_version: 1, ...detachRootOutcomeIdentity(identity) }, ROOT_QUERY_MAX_BYTES);
}
export function detachRootCancelRequest(value: unknown): RootCancelRequest {
    const row = shape(value, ["protocol_version", "server_incarnation", "sequence", "mutation_id", "request_hash"]);
    return { protocol_version: version(row.protocol_version), server_incarnation: hex(row.server_incarnation),
        ...detachRootOutcomeIdentity({ sequence: row.sequence, mutation_id: row.mutation_id, request_hash: row.request_hash }) };
}
export function encodeRootCancel(value: RootCancelRequest): Uint8Array {
    return boundedJSON(detachRootCancelRequest(value), ROOT_QUERY_MAX_BYTES);
}

function acceptedResult(value: unknown): RootAcceptedResult {
    const row = object(value);
    if (row.accepted === true) {
        shape(row, ["accepted", "root_hash"]);
        return { accepted: true, root_hash: hex(row.root_hash) };
    }
    shape(row, ["merged", "root_hash", "conflicts", "auto_resolved", "text_merged"]);
    if (row.merged !== true || !Array.isArray(row.conflicts) || row.conflicts.length > ROOT_MAX_CONFLICTS) fail();
    const paths = new Set<string>();
    const conflicts = row.conflicts.map(value => {
        const conflict = shape(value, ["path", "base_hash", "side_a_hash", "side_b_hash"]);
        if (!isSafeVaultPath(conflict.path)) fail();
        stringBytes(conflict.path, 4096);
        if (paths.has(conflict.path)) fail();
        paths.add(conflict.path);
        return { path: conflict.path, base_hash: hex(conflict.base_hash),
            side_a_hash: hex(conflict.side_a_hash), side_b_hash: hex(conflict.side_b_hash) };
    });
    return { merged: true, root_hash: hex(row.root_hash), conflicts,
        auto_resolved: counter(row.auto_resolved), text_merged: counter(row.text_merged) };
}
export function decodeRootOutcome(value: unknown, expected?: RootOutcomeIdentity): RootOutcome {
    const row = object(value);
    const envelope = { protocol_version: version(row.protocol_version), server_incarnation: hex(row.server_incarnation) };
    const identity = expected === undefined ? undefined : detachRootOutcomeIdentity(expected);
    if (row.status === "accepted" || row.status === "cancelled") {
        if (!identity) fail("Unexpected terminal root outcome without requested identity");
        shape(row, ["protocol_version", "server_incarnation", "status", "sequence", "mutation_id", "request_hash", "result"]);
        const echoed = detachRootOutcomeIdentity({ sequence: row.sequence, mutation_id: row.mutation_id, request_hash: row.request_hash });
        if (echoed.sequence !== identity.sequence || echoed.mutation_id !== identity.mutation_id || echoed.request_hash !== identity.request_hash) {
            fail("Root outcome identity mismatch");
        }
        let terminal: RootTerminalOutcome;
        if (row.status === "cancelled") {
            if (shape(row.result, ["cancelled"]).cancelled !== true) fail();
            terminal = { ...envelope, ...echoed, status: "cancelled", result: { cancelled: true } };
        } else terminal = { ...envelope, ...echoed, status: "accepted", result: acceptedResult(row.result) };
        boundedJSON({ ...echoed, result: terminal.result }, ROOT_RECEIPT_MAX_BYTES);
        return terminal;
    }
    shape(row, ["protocol_version", "server_incarnation", "status", "last_sequence", "current_root_hash"]);
    const last_sequence = counter(row.last_sequence);
    const current_root_hash = row.current_root_hash === null ? null : hex(row.current_root_hash);
    if (row.status === "stream") {
        if (identity) fail("Stream reply cannot resolve a requested root identity");
    } else if (row.status === "unknown") {
        if (!identity || identity.sequence <= last_sequence) fail();
    } else if (row.status === "expired") {
        if (!identity || identity.sequence >= last_sequence) fail();
    } else fail();
    return { ...envelope, status: row.status, last_sequence, current_root_hash };
}
export function validateRootTerminalOutcome(value: unknown, expected: RootOutcomeIdentity): RootTerminalOutcome {
    const decoded = decodeRootOutcome(value, expected);
    if (decoded.status !== "accepted" && decoded.status !== "cancelled") fail("Expected terminal root outcome");
    return decoded;
}

/** Syntax walk rejects duplicate (including escaped-equivalent) object keys
 * before whole-document JSON.parse. Bound input, depth and node count first;
 * JSON.parse still performs the final standard JSON syntax validation. */
export function parseRootProtocolJSON(bytes: Uint8Array, maximum = ROOT_RESPONSE_MAX_BYTES): unknown {
    if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > ROOT_RESPONSE_MAX_BYTES) fail("Invalid root response byte limit");
    if (bytes.byteLength > maximum) fail("Root response exceeds byte limit");
    let text: string;
    try { text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes); } catch { fail("Invalid root response UTF-8"); }
    let position = 0, nodes = 0;
    const space = () => { while (position < text.length && /[\t\n\r ]/.test(text[position])) position++; };
    const string = (): string => {
        const start = position++;
        while (position < text.length) {
            const character = text[position++];
            if (character === '"') {
                try { return JSON.parse(text.slice(start, position)); } catch { fail(); }
            }
            if (character === "\\") position++;
        }
        return fail();
    };
    const walk = (depth: number): void => {
        if (++nodes > 16384 || depth > 16) fail("Root JSON structure exceeds limit");
        space();
        const kind = text[position];
        if (kind === '"') { string(); return; }
        if (kind === "{" || kind === "[") {
            position++; space();
            const closing = kind === "{" ? "}" : "]", keys = new Set<string>();
            if (text[position] === closing) { position++; return; }
            for (;;) {
                space();
                if (kind === "{") {
                    if (text[position] !== '"') fail();
                    const key = string(); if (keys.has(key)) fail("Duplicate root JSON key"); keys.add(key);
                    space(); if (text[position++] !== ":") fail();
                }
                walk(depth + 1); space();
                if (text[position] === closing) { position++; return; }
                if (text[position++] !== ",") fail();
            }
        }
        const start = position;
        while (position < text.length && !/[\t\n\r ,\]}]/.test(text[position])) position++;
        if (start === position) fail();
    };
    walk(0); space(); if (position !== text.length) fail();
    try { return JSON.parse(text); } catch { return fail(); }
}
export function decodeRootOutcomeBytes(bytes: Uint8Array, expected?: RootOutcomeIdentity): RootOutcome {
    return decodeRootOutcome(parseRootProtocolJSON(bytes), expected);
}
/** Absence of either capability is explicitly unsupported. Malformed offers
 * claiming both fail closed. Observation freshness is not permission to retry
 * an absent intent after a changed server incarnation. */
export function decodeRootOutcomeCapabilities(value: unknown): RootOutcomeCapabilities | null {
    const bundle = object(value), capabilities = bundle.capabilities;
    if (!Array.isArray(capabilities) || !capabilities.includes("root-outcome-v1") || !capabilities.includes("root-cancel-v1")) return null;
    if (capabilities.length > 256 || capabilities.some(value => typeof value !== "string" || value.length > 128) ||
        new Set(capabilities).size !== capabilities.length) fail();
    const protocol = shape(bundle.root_outcome, ["protocol_version", "retained_per_device", "max_sequence"]);
    version(protocol.protocol_version);
    if (protocol.retained_per_device !== 1 || protocol.max_sequence !== Number.MAX_SAFE_INTEGER) fail();
    const limits = object(bundle.limits);
    const bound = (key: string, ceiling: number) => Math.min(counter(limits[key], 1), ceiling);
    return { serverIncarnation: hex(bundle.server_incarnation), maxSequence: Number.MAX_SAFE_INTEGER,
        commitBytes: bound("root_commit_request_bytes", ROOT_COMMIT_MAX_BYTES), rootBytes: bound("root_commit_root_bytes", ROOT_MAX_BYTES),
        queryBytes: bound("root_outcome_query_bytes", ROOT_QUERY_MAX_BYTES), cancelBytes: bound("root_cancel_request_bytes", ROOT_QUERY_MAX_BYTES),
        receiptBytes: bound("root_outcome_receipt_bytes", ROOT_RECEIPT_MAX_BYTES), streamsPerVault: bound("root_outcome_streams_per_vault", 128) };
}
