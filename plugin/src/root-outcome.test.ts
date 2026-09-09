import { strict as assert } from "node:assert";
// Existing @noble/curves transitive dependency, test oracle only. Production
// injects its existing WASM BLAKE3 implementation and gains no dependency.
import { blake3 } from "@noble/hashes/blake3";
import {
    ROOT_MAX_BYTES, ROOT_RESPONSE_MAX_BYTES, ROOT_MAX_CONFLICTS,
    computeRootRequestHash, createRootCommitIntent, decodeRootOutcome,
    decodeRootOutcomeBytes, decodeRootOutcomeCapabilities, detachRootCommitIntent,
    detachRootCancelRequest, detachRootCommitRequest, encodeRootCancel, encodeRootCommitIntent,
    encodeRootOutcomeQuery, parseRootProtocolJSON, validateRootTerminalOutcome,
    rootCommitIntentEncodedLength,
    type RootCommitRequest, type RootOutcomeIdentity,
} from "./root-outcome";

const hash = (bytes: Uint8Array) => Buffer.from(blake3(bytes)).toString("hex");
const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
const request = (): RootCommitRequest => ({ protocol_version: 1, server_incarnation: "a".repeat(64),
    sequence: 1, mutation_id: "b".repeat(32), parent_root: "", root: Buffer.from("exact-root-bytes").toString("base64") });
const identity: RootOutcomeIdentity = { sequence: 1, mutation_id: "b".repeat(32),
    request_hash: "e39b5ae8022adf0611a7136d172bfa1d088f2c873e47ac815adae4769b039c31" };
const terminal = () => ({ protocol_version: 1, server_incarnation: "c".repeat(64),
    status: "accepted", ...identity, result: { accepted: true, root_hash: "f".repeat(64) } });
const stream = (status = "stream", last_sequence = 0) => ({ protocol_version: 1,
    server_incarnation: "c".repeat(64), status, last_sequence, current_root_hash: null });
const offer = () => ({ capabilities: ["tree-v2", "root-outcome-v1", "root-cancel-v1"],
    server_incarnation: "a".repeat(64), root_outcome: { protocol_version: 1, retained_per_device: 1, max_sequence: Number.MAX_SAFE_INTEGER },
    limits: { root_commit_request_bytes: 704 * 1024, root_commit_root_bytes: ROOT_MAX_BYTES,
        root_outcome_query_bytes: 4096, root_cancel_request_bytes: 4096,
        root_outcome_receipt_bytes: 65536, root_outcome_streams_per_vault: 128 } });

async function run(): Promise<void> {
    // Same fixtures are hardcoded in Rust root_outcome.rs::tests::
    // request_digest_cross_language_golden_vectors, computed by actual decode_commit.
    assert.equal(await computeRootRequestHash("vault", "device", request(), hash), identity.request_hash);
    assert.equal(await computeRootRequestHash("vault", "device", { ...request(), sequence: Number.MAX_SAFE_INTEGER }, hash),
        "bf7c1de11c00a9f3a9319d5f2605ca41af857ce7c6f49722dbc453b49707553e");
    for (const patch of [{ sequence: 2 }, { mutation_id: "d".repeat(32) }, { parent_root: "e".repeat(64) },
        { server_incarnation: "d".repeat(64) }, { root: Buffer.from("changed exact bytes").toString("base64") }]) {
        assert.notEqual(await computeRootRequestHash("vault", "device", { ...request(), ...patch }, hash), identity.request_hash);
    }
    assert.notEqual(await computeRootRequestHash("ab", "c", request(), hash), await computeRootRequestHash("a", "bc", request(), hash));
    assert.notEqual(await computeRootRequestHash("vault", "different-device", request(), hash), identity.request_hash);
    const unicode = await computeRootRequestHash("våult", "device", request(), hash);
    assert.notEqual(unicode, identity.request_hash);
    await assert.rejects(computeRootRequestHash("v\ud800", "device", request(), hash));
    await assert.rejects(computeRootRequestHash("é".repeat(65), "device", request(), hash));
    await assert.rejects(computeRootRequestHash("vault", "device", request(), () => "X".repeat(64)));

    let release!: (value: string) => void;
    const pendingHash = new Promise<string>(resolve => { release = resolve; });
    const mutable = request();
    const creating = createRootCommitIntent("vault", "device", mutable, input => {
        assert.equal(hash(input), identity.request_hash); return pendingHash;
    });
    mutable.sequence = 2; mutable.root = "YQ==";
    release(identity.request_hash);
    const captured = await creating;
    assert.deepEqual(captured, { ...request(), request_hash: identity.request_hash });
    assert.deepEqual(JSON.parse(new TextDecoder().decode(encodeRootCommitIntent(captured))), request());
    assert.equal(rootCommitIntentEncodedLength(captured), encodeRootCommitIntent(captured).byteLength);
    assert.deepEqual(detachRootCommitIntent(captured), captured);
    assert.notEqual(detachRootCommitIntent(captured), captured);
    assert.throws(() => detachRootCommitRequest(captured), /Invalid/);
    for (const patch of [{ sequence: 0 }, { sequence: 1.5 }, { sequence: Number.MAX_SAFE_INTEGER + 1 },
        { mutation_id: "B".repeat(32) }, { parent_root: " " }, { protocol_version: 2 },
        { server_incarnation: "A".repeat(64) }, { extra: 1 }]) {
        assert.throws(() => detachRootCommitRequest({ ...request(), ...patch }));
    }
    for (const root of ["", "AA", "AB==", "AA==\n", "-_==", "A===", "===="])
        assert.throws(() => detachRootCommitRequest({ ...request(), root }));
    const atLimit = Buffer.alloc(ROOT_MAX_BYTES).toString("base64");
    const tooLarge = Buffer.alloc(ROOT_MAX_BYTES + 1).toString("base64");
    assert.equal(atLimit.length, tooLarge.length);
    assert.equal(detachRootCommitRequest({ ...request(), root: atLimit }).root, atLimit);
    const maximumIntent = { ...captured, sequence: Number.MAX_SAFE_INTEGER,
        parent_root: "f".repeat(64), root: atLimit };
    assert.equal(rootCommitIntentEncodedLength(maximumIntent), encodeRootCommitIntent(maximumIntent).byteLength,
        "exact length diverged at maximum root/sequence");
    assert.throws(() => detachRootCommitRequest({ ...request(), root: tooLarge }));

    assert.deepEqual(decodeRootOutcomeBytes(bytes(terminal()), identity), terminal());
    assert.deepEqual(validateRootTerminalOutcome(terminal(), identity), terminal());
    assert.throws(() => decodeRootOutcome(terminal()), /without requested identity/);
    for (const field of ["sequence", "mutation_id", "request_hash"] as const) {
        const wrong = { ...identity, [field]: field === "sequence" ? 2 : "d".repeat(field === "mutation_id" ? 32 : 64) };
        assert.throws(() => decodeRootOutcome(terminal(), wrong), /identity mismatch/);
    }
    const cancelled = { ...terminal(), status: "cancelled", result: { cancelled: true } };
    assert.deepEqual(decodeRootOutcome(cancelled, identity), cancelled);
    assert.throws(() => decodeRootOutcome({ ...cancelled, result: terminal().result }, identity));
    assert.throws(() => decodeRootOutcome({ ...terminal(), result: cancelled.result }, identity));
    assert.throws(() => decodeRootOutcome({ ...terminal(), result: { ...terminal().result, merged: true } }, identity));
    assert.throws(() => decodeRootOutcome({ ...terminal(), unknown: true }, identity));
    assert.deepEqual(decodeRootOutcome(stream()), stream());
    assert.deepEqual(decodeRootOutcome(stream("unknown"), identity), stream("unknown"));
    assert.deepEqual(decodeRootOutcome(stream("expired", 2), identity), stream("expired", 2));
    assert.deepEqual(decodeRootOutcome(stream("stream", 1)), stream("stream", 1), "Cancelled-first stream may have null current root");
    for (const bad of [stream("stream"), stream("unknown", 1), stream("expired", 1), stream("other"), stream("unknown", -1)]) {
        assert.throws(() => decodeRootOutcome(bad, identity));
    }
    assert.throws(() => validateRootTerminalOutcome(stream("unknown"), identity));
    assert.throws(() => decodeRootOutcome({ ...stream(), last_sequence: 1.5 }));
    assert.throws(() => decodeRootOutcome({ ...stream(), last_sequence: -0 }));

    const conflict = { path: "notes/test.md", base_hash: "1".repeat(64), side_a_hash: "2".repeat(64), side_b_hash: "3".repeat(64) };
    const merged = { ...terminal(), result: { merged: true, root_hash: "f".repeat(64), conflicts: [conflict], auto_resolved: 0, text_merged: 2 } };
    const detached = decodeRootOutcome(merged, identity);
    assert.deepEqual(detached, merged);
    conflict.path = "mutated.md";
    assert.equal((detached as any).result.conflicts[0].path, "notes/test.md");
    for (const path of ["../unsafe", "/absolute", "a\\b", "a\u0000b", "é".repeat(2049), "x\ud800"])
        assert.throws(() => decodeRootOutcome({ ...merged, result: { ...merged.result, conflicts: [{ ...conflict, path }] } }, identity));
    for (const extra of [{ auto_resolved: -1 }, { text_merged: Number.MAX_SAFE_INTEGER + 1 },
        { conflicts: [conflict, conflict] }, { conflicts: Array(ROOT_MAX_CONFLICTS + 1).fill(conflict) }]) {
        assert.throws(() => decodeRootOutcome({ ...merged, result: { ...merged.result, ...extra } }, identity));
    }
    const many = Array.from({ length: 20 }, (_, i) => ({ ...conflict, path: `${i}/` + "x".repeat(4000) }));
    assert.throws(() => decodeRootOutcome({ ...merged, result: { ...merged.result, conflicts: many } }, identity), /byte limit/);

    const encoded = JSON.stringify(terminal());
    for (const bad of [encoded.replace('{', '{"status":"cancelled",'),
        encoded.replace('{', '{"sequen\\u0063e":1,'),
        encoded.replace('"accepted":true', '"accepted":true,"accepted":true')]) {
        assert.throws(() => decodeRootOutcomeBytes(new TextEncoder().encode(bad), identity), /Duplicate/);
    }
    assert.throws(() => parseRootProtocolJSON(new Uint8Array([0xff])), /UTF-8/);
    assert.throws(() => parseRootProtocolJSON(new Uint8Array(ROOT_RESPONSE_MAX_BYTES + 1)), /byte limit/);
    assert.throws(() => parseRootProtocolJSON(bytes({}), Infinity), /byte limit/);
    assert.throws(() => parseRootProtocolJSON(new Uint8Array([0xef, 0xbb, 0xbf, ...bytes({})])));
    assert.throws(() => parseRootProtocolJSON(new TextEncoder().encode("[".repeat(18) + "0" + "]".repeat(18))), /structure/);
    assert.throws(() => parseRootProtocolJSON(new TextEncoder().encode(encoded + "null")));
    assert.throws(() => parseRootProtocolJSON(new TextEncoder().encode('{"x":"unterminated}')));

    assert.deepEqual(JSON.parse(new TextDecoder().decode(encodeRootOutcomeQuery())), { protocol_version: 1 });
    assert.deepEqual(JSON.parse(new TextDecoder().decode(encodeRootOutcomeQuery(identity))), { protocol_version: 1, ...identity });
    const cancellation = { protocol_version: 1 as const, server_incarnation: "c".repeat(64), ...identity };
    assert.deepEqual(JSON.parse(new TextDecoder().decode(encodeRootCancel(cancellation))), cancellation);
    assert.deepEqual(detachRootCancelRequest(cancellation), cancellation);
    assert.notEqual(detachRootCancelRequest(cancellation), cancellation);
    assert.throws(() => encodeRootOutcomeQuery({ ...identity, extra: true } as any));
    assert.throws(() => encodeRootOutcomeQuery(null as any));
    assert.throws(() => encodeRootCancel({ ...cancellation, root: "AA==" } as any));

    const negotiated = decodeRootOutcomeCapabilities(offer())!;
    assert.equal(negotiated.serverIncarnation, "a".repeat(64));
    assert.equal(negotiated.rootBytes, ROOT_MAX_BYTES);
    for (const capability of ["root-outcome-v1", "root-cancel-v1"]) {
        assert.equal(decodeRootOutcomeCapabilities({ ...offer(), capabilities: [capability] }), null);
    }
    assert.equal(decodeRootOutcomeCapabilities({}), null);
    assert.throws(() => decodeRootOutcomeCapabilities({ ...offer(), server_incarnation: null }));
    assert.throws(() => decodeRootOutcomeCapabilities({ ...offer(), root_outcome: { ...offer().root_outcome, retained_per_device: 2 } }));
    assert.throws(() => decodeRootOutcomeCapabilities({ ...offer(), limits: { ...offer().limits, root_cancel_request_bytes: 0 } }));
    assert.equal(decodeRootOutcomeCapabilities({ ...offer(), limits: { ...offer().limits, root_commit_root_bytes: 9999999 } })!.rootBytes, ROOT_MAX_BYTES);
    console.log("root-outcome.test: canonical Rust/client digest, strict codecs, response identity and capability gates passed");
}
void run().catch(error => { setTimeout(() => { throw error; }, 0); });
