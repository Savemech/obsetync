import { strict as assert } from "node:assert";
import { x25519 } from "@noble/curves/ed25519";
import { ObsetyncApi } from "./api";
import { ObsetyncSessionStaleError } from "./secure";
import { ROOT_CAPABILITY_MAX_BYTES, ROOT_RESPONSE_MAX_BYTES, rootCommitIntentEncodedLength,
    type RootCommitIntent } from "./root-outcome";
import { ResourceBudget } from "./resource-budget";
import { estimateRootOutcomeWorkset } from "./transient-memory";
// Run alongside the pre-existing global fake: per-origin routing must remain
// isolated even when both suites are imported concurrently by the aggregator.
import "./api-memory.test";

const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
const identity = { sequence: 1, mutation_id: "b".repeat(32),
    request_hash: "e39b5ae8022adf0611a7136d172bfa1d088f2c873e47ac815adae4769b039c31" };
const intent = (): RootCommitIntent => ({ ...identity, protocol_version: 1,
    server_incarnation: "a".repeat(64), parent_root: "", root: Buffer.from("exact-root-bytes").toString("base64") });
const terminal = (status = "accepted") => ({ protocol_version: 1, server_incarnation: "c".repeat(64), status, ...identity,
    result: status === "accepted" ? { accepted: true, root_hash: "f".repeat(64) } : { cancelled: true } });
const offer = (incarnation = "a".repeat(64)) => ({ capabilities: ["root-outcome-v1", "root-cancel-v1"],
    server_incarnation: incarnation, root_outcome: { protocol_version: 1, retained_per_device: 1, max_sequence: Number.MAX_SAFE_INTEGER },
    limits: { root_commit_request_bytes: 720896, root_commit_root_bytes: 524288, root_outcome_query_bytes: 4096,
        root_cancel_request_bytes: 4096, root_outcome_receipt_bytes: 65536, root_outcome_streams_per_vault: 128 } });
function gate<T>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>(yes => { resolve = yes; });
    return { promise, resolve };
}
const hostTurn = (): Promise<void> => new Promise(resolve => setImmediate(resolve));
interface Call { path: string; method: string; bytes: Uint8Array; body: any; transportSequence: number }
interface Reply { body?: Uint8Array; status?: number; wireStatus?: number; stale?: boolean }
let fixtureSequence = 0;
function fixture(memoryBudget = new ResourceBudget({ capacityBytes: 8 * 1024 * 1024 })) {
    const origin = `http://root-outcome-fixture-${++fixtureSequence}.invalid`;
    const api = new ObsetyncApi(origin, "", "", undefined, "desktop", memoryBudget), internal = api as any;
    const calls: Call[] = [], encryptions = new Map<number, Call>();
    const counts = { encrypt: 0, decrypt: 0, refresh: 0, recover: 0 };
    let respond: (call: Call) => Reply | Promise<Reply> = call => ({ body: bytes(call.path.endsWith("capabilities") ? offer() : terminal()) });
    let encryption = 0, sequence = 0;
    const channel = {
        async encryptRequest(method: string, path: string, body: Uint8Array, transportSequence: number) {
            counts.encrypt++;
            const raw = body.slice();
            encryptions.set(++encryption, { path, method, bytes: raw, body: raw.length ? JSON.parse(new TextDecoder().decode(raw)) : null, transportSequence });
            const wire = new Uint8Array(53);
            new DataView(wire.buffer).setUint32(13, encryption, true);
            return wire;
        },
        async decryptResponse(_method: string, _path: string, _nonce: Uint8Array, wire: Uint8Array) {
            counts.decrypt++;
            if (wire[0] === 0xdd) throw new ObsetyncSessionStaleError();
            return { status: new DataView(wire.buffer, wire.byteOffset).getUint16(29, true), body: wire.subarray(31) };
        },
    };
    internal.channelGeneration = 1;
    internal.getChannel = async () => { internal.channel = channel; return channel; };
    internal.nextSequence = async () => ++sequence;
    internal.recoverSequence = async () => { counts.recover++; sequence += 100; };
    internal.refreshServerEphemeral = async () => { counts.refresh++; internal.channelGeneration++; };
    const routes: Map<string, unknown> = (globalThis as any).__obsetyncTestRequestUrlRoutes ??= new Map();
    routes.set(origin, async (params: any) => {
        assert.equal(params.method, "POST");
        const id = new DataView(params.body).getUint32(13, true), call = encryptions.get(id)!;
        assert(call); calls.push(call);
        assert.equal(params.headers["X-Obsetync-Method"], call.method);
        assert.equal(new URL(params.url).pathname, call.path);
        const result = await respond(call), plain = result.body ?? new Uint8Array();
        const wire = new Uint8Array(plain.length + 31);
        wire[0] = result.stale ? 0xdd : 0x02;
        new DataView(wire.buffer).setUint16(29, result.status ?? 200, true);
        wire.set(plain, 31);
        return { status: result.wireStatus ?? 200, arrayBuffer: wire.buffer, json: null };
    });
    // Any accidental WS/legacy fallback is a hard fixture failure, not a
    // successful substitute. Actual sealed/sendEncrypted/negotiation stay real.
    internal.tryDataRpc = () => { throw new Error("Unexpected root WebSocket fallback"); };
    return { api, internal, calls, counts, memoryBudget,
        reply: (next: typeof respond) => { respond = next; },
        close: () => { routes.delete(origin); } };
}

async function completeAdmissionPrecedesCommitTransportAndLivesThroughNativeCompletion(): Promise<void> {
    const capacityBytes = 8 * 1024 * 1024;
    const budget = new ResourceBudget({ capacityBytes });
    const blocker = await budget.reserve(capacityBytes);
    const f = fixture(budget), response = gate<Reply>();
    try {
        f.reply(call => call.path.endsWith("capabilities") ? { body: bytes(offer()) } : response.promise);
        const mutable = intent(), pending = f.api.commitRootOutcome("vault", mutable);
        mutable.root = "YQ=="; mutable.sequence = 2; mutable.server_incarnation = "d".repeat(64);
        await hostTurn();
        assert.equal(f.counts.encrypt, 0, "commit reached encryption before complete memory admission");
        assert.equal(f.calls.length, 0, "commit reached native HTTP before complete memory admission");
        assert.equal(budget.snapshot().queuedRequests, 1);
        blocker.release();
        await hostTurn();
        assert.equal(f.calls.filter(call => call.path.includes("root-commit")).length, 1);
        const { request_hash: _, ...original } = intent();
        assert.deepEqual(f.calls.find(call => call.path.includes("root-commit"))!.body, original,
            "queued admission rebound the captured root request");
        assert.ok(budget.snapshot().usedBytes > 0, "root response wait released its complete admission");
        assert.equal(budget.snapshot().queuedRequests, 0, "root transport nested a second global admission");
        response.resolve({ body: bytes(terminal()) });
        assert.deepEqual(await pending, terminal());
        assert.equal(budget.snapshot().usedBytes, 0, "settled root request leaked its complete admission");

        const plan = estimateRootOutcomeWorkset(
            rootCommitIntentEncodedLength(intent()),
            ROOT_RESPONSE_MAX_BYTES,
        );
        const tooSmall = fixture(new ResourceBudget({ capacityBytes: plan.totalBytes - 1 }));
        try {
            await assert.rejects(tooSmall.api.commitRootOutcome("vault", intent()), /exceeds capacity/);
            assert.equal(tooSmall.counts.encrypt, 0, "oversized workset dispatched encryption");
            assert.equal(tooSmall.calls.length, 0, "oversized workset dispatched native HTTP");
        } finally { tooSmall.close(); }
    } finally {
        blocker.release();
        f.close();
    }
}

async function queryAndCancelCaptureBeforeAdmissionAndRetainTheirLease(): Promise<void> {
    const capacityBytes = 2 * 1024 * 1024;
    const budget = new ResourceBudget({ capacityBytes });
    const f = fixture(budget);
    try {
        const queryBlocker = await budget.reserve(capacityBytes), queryReply = gate<Reply>();
        f.reply(() => queryReply.promise);
        const mutableIdentity = { ...identity };
        const query = f.api.queryRootOutcome("vault", mutableIdentity);
        mutableIdentity.sequence = 2; mutableIdentity.request_hash = "0".repeat(64);
        await hostTurn();
        assert.equal(f.counts.encrypt, 0, "query reached encryption before admission");
        queryBlocker.release();
        await hostTurn();
        assert.deepEqual(f.calls.at(-1)!.body, { protocol_version: 1, ...identity },
            "queued query rebound caller-owned identity");
        assert.ok(budget.snapshot().usedBytes > 0, "query released admission before native completion");
        queryReply.resolve({ body: bytes(terminal()) });
        assert.deepEqual(await query, terminal());
        assert.equal(budget.snapshot().usedBytes, 0, "query leaked admission");

        const cancelBlocker = await budget.reserve(capacityBytes), cancelReply = gate<Reply>();
        const beforeCancelEncrypt = f.counts.encrypt;
        f.reply(() => cancelReply.promise);
        const originalCancel = { protocol_version: 1 as const,
            server_incarnation: "d".repeat(64), ...identity };
        const mutableCancel = { ...originalCancel };
        const cancel = f.api.cancelRootOutcome("vault", mutableCancel);
        mutableCancel.sequence = 2; mutableCancel.server_incarnation = "e".repeat(64);
        await hostTurn();
        assert.equal(f.counts.encrypt, beforeCancelEncrypt,
            "cancel reached encryption before admission");
        cancelBlocker.release();
        await hostTurn();
        assert.deepEqual(f.calls.at(-1)!.body, originalCancel,
            "queued cancellation rebound caller-owned request");
        assert.ok(budget.snapshot().usedBytes > 0, "cancel released admission before native completion");
        cancelReply.resolve({ body: bytes(terminal("cancelled")) });
        assert.deepEqual(await cancel, terminal("cancelled"));
        assert.equal(budget.snapshot().usedBytes, 0, "cancel leaked admission");
    } finally { f.close(); }
}

async function bootstrapResponseBoundPrecedesRootDispatch(): Promise<void> {
    const origin = `http://root-outcome-bootstrap-${++fixtureSequence}.invalid`;
    const privateKey = new Uint8Array(32).fill(7);
    const serverBoxPub = Buffer.from(x25519.getPublicKey(privateKey)).toString("base64");
    privateKey.fill(0);
    const initial = { wireVersion: "0x02", esPub: "", esPubValidUntil: 0, lastOutgoingSeq: 0 };
    let state = { ...initial }, persistenceWrites = 0;
    const budget = new ResourceBudget({ capacityBytes: 8 * 1024 * 1024 });
    const api = new ObsetyncApi(origin, serverBoxPub, "a".repeat(64), {
        get: () => ({ ...state }),
        update: async (patch) => { persistenceWrites++; state = { ...state, ...patch }; },
    }, "desktop", budget);
    const paths: string[] = [];
    const routes: Map<string, unknown> = (globalThis as any).__obsetyncTestRequestUrlRoutes ??= new Map();
    routes.set(origin, async (params: any) => {
        paths.push(new URL(params.url).pathname);
        return { status: 200,
            arrayBuffer: new Uint8Array(ROOT_CAPABILITY_MAX_BYTES + 31 + 1).buffer,
            json: null };
    });
    try {
        await assert.rejects(api.commitRootOutcome("vault", intent()), /admitted byte bound/);
        assert.deepEqual(paths, ["/api/v1/server-eph"],
            "oversized bootstrap response reached capability/root dispatch");
        assert.equal(persistenceWrites, 0, "oversized bootstrap response changed durable transport state");
        assert.deepEqual(state, initial);
        assert.equal(budget.snapshot().usedBytes, 0, "bootstrap rejection leaked root admission");
    } finally { routes.delete(origin); }
}

async function negotiationAndFreshness(): Promise<void> {
    const f = fixture();
    try {
        const first = await f.api.negotiateRootOutcomes();
        assert.equal(first?.serverIncarnation, "a".repeat(64));
        first!.serverIncarnation = "caller-mutated";
        assert.equal((await f.api.negotiateRootOutcomes())?.serverIncarnation, "a".repeat(64));
        assert.equal(f.calls.length, 1);
        f.reply(() => ({ body: bytes(offer("c".repeat(64))) }));
        assert.equal((await f.api.negotiateRootOutcomes(true))?.serverIncarnation, "c".repeat(64));
        assert.equal(f.calls.length, 2);
        f.internal.channelGeneration++;
        await f.api.negotiateRootOutcomes();
        assert.equal(f.calls.length, 3, "Channel rotation invalidates cached capability observation");
        f.reply(() => ({ body: bytes({ ...offer(), capabilities: ["root-outcome-v1"] }) }));
        assert.equal(await f.api.negotiateRootOutcomes(true), null, "Outcome-only support cannot enable durable cancellation coordinator");
        f.reply(() => ({ status: 404 }));
        assert.equal(await f.api.negotiateRootOutcomes(true), null);
        f.reply(() => ({ status: 500 }));
        await assert.rejects(f.api.negotiateRootOutcomes(true), /500/);
        f.reply(() => ({ body: bytes({ ...offer(), server_incarnation: "invalid" }) }));
        await assert.rejects(f.api.negotiateRootOutcomes(true), /Invalid/);
    } finally { f.close(); }
}

async function exactRequestsSurviveRetriesAndCallerMutation(): Promise<void> {
    const f = fixture(), negotiation = gate<Reply>();
    try {
        let attempts = 0;
        f.reply(call => {
            if (call.path.endsWith("capabilities")) return negotiation.promise;
            if (++attempts === 1) return { status: 401, body: bytes({ error: "replay", last_seen_seq: 4096 }) };
            return { body: bytes(terminal()) };
        });
        const mutable = intent(), pending = f.api.commitRootOutcome("vault", mutable);
        mutable.root = "YQ=="; mutable.server_incarnation = "d".repeat(64); mutable.sequence = 2;
        await hostTurn();
        assert.ok(f.memoryBudget.snapshot().usedBytes > 0,
            "tree negotiation wait released root admission");
        assert.equal(f.calls.filter(call => call.path.includes("root-commit")).length, 0,
            "root mutation dispatched before negotiation completed");
        negotiation.resolve({ body: bytes(offer()) });
        const result = await pending;
        assert.deepEqual(result, terminal());
        assert.equal(f.counts.recover, 1);
        assert.equal(f.calls[0].path, "/api/v1/capabilities", "Tree session proof precedes new root commit");
        assert.deepEqual(f.calls[0].body.capabilities, ["tree-v2"]);
        const commits = f.calls.filter(call => call.path.includes("root-commit"));
        assert.equal(commits.length, 2);
        assert.deepEqual(commits[0].bytes, commits[1].bytes, "Transport retry never rebinds application identity");
        const { request_hash: _, ...original } = intent();
        assert.deepEqual(commits[0].body, original);
        assert.notEqual(commits[0].transportSequence, commits[1].transportSequence);
        assert.equal(result.server_incarnation, "c".repeat(64), "Outer current incarnation may differ from original intent");
        assert.equal(f.memoryBudget.snapshot().usedBytes, 0, "replay retry leaked root admission");
    } finally { f.close(); }
}

async function staleSessionStillUsesOriginalIncarnationAndCancellationIdentity(): Promise<void> {
    const f = fixture();
    try {
        let attempts = 0;
        f.reply(call => call.path.endsWith("capabilities") ? { body: bytes(offer()) } :
            ++attempts === 1 ? { stale: true } : { body: bytes(terminal()) });
        await f.api.commitRootOutcome("vault", intent());
        assert.equal(f.counts.refresh, 1);
        const commits = f.calls.filter(call => call.path.includes("root-commit"));
        assert.deepEqual(commits[0].bytes, commits[1].bytes);
        assert.equal(commits[1].body.server_incarnation, "a".repeat(64));
        f.reply(() => ({ body: bytes(terminal("cancelled")) }));
        const cancellation = { protocol_version: 1 as const, server_incarnation: "d".repeat(64), ...identity };
        const cancelled = await f.api.cancelRootOutcome("vault", cancellation);
        assert.deepEqual(cancelled, terminal("cancelled"));
        assert.deepEqual(f.calls.at(-1)!.body, cancellation);
        assert.equal(f.calls.at(-1)!.path, "/api/v1/root-cancel/vault");
        assert.equal(f.calls.at(-1)!.body.request_hash, intent().request_hash);
        f.reply(() => ({ body: bytes(terminal()) }));
        assert.deepEqual(await f.api.cancelRootOutcome("vault", cancellation), terminal(), "Cancel after accepted is acceptance, not rollback");
    } finally { f.close(); }
}

async function queryAndFailuresNeverBecomeLegacyAcceptance(): Promise<void> {
    const f = fixture();
    try {
        f.reply(() => ({ body: bytes({ protocol_version: 1, server_incarnation: "c".repeat(64),
            status: "stream", last_sequence: 1, current_root_hash: null }) }));
        assert.equal((await f.api.queryRootOutcome("vault")).status, "stream");
        assert.deepEqual(f.calls[0].body, { protocol_version: 1 });
        f.reply(() => ({ body: bytes(terminal()) }));
        assert.deepEqual(await f.api.queryRootOutcome("vault", identity), terminal());
        assert.deepEqual(f.calls.at(-1)!.body, { protocol_version: 1, ...identity });
        f.reply(() => ({ body: bytes({ ...terminal(), request_hash: "0".repeat(64) }) }));
        await assert.rejects(f.api.queryRootOutcome("vault", identity), /identity mismatch/);
        for (const status of [404, 405, 409, 413, 500, 503, 426]) {
            f.reply(call => call.path.endsWith("capabilities") ? { body: bytes(offer()) } :
                { status, body: bytes({ error: "synthetic rejection" }) });
            const before = f.calls.filter(call => call.path.includes("root-commit")).length;
            await assert.rejects(f.api.commitRootOutcome("vault", intent()));
            assert.equal(f.calls.filter(call => call.path.includes("root-commit")).length, before + 1);
        }
        assert(!f.calls.some(call => /\/root\//.test(call.path)), "No failure falls back to legacy root mutation");
        const before = f.calls.length;
        await assert.rejects(f.api.commitRootOutcome("../unsafe", intent()));
        await assert.rejects(f.api.commitRootOutcome("vault", { ...intent(), sequence: 0 }));
        await assert.rejects(f.api.queryRootOutcome("vault", { ...identity, request_hash: "bad" }));
        assert.equal(f.calls.length, before, "Malformed local identity rejected before any HTTP request");
    } finally { f.close(); }
}

async function actualWireAndJSONBoundsPrecedeDecryptAndAcceptance(): Promise<void> {
    const f = fixture();
    try {
        f.reply(() => ({ body: new Uint8Array(ROOT_RESPONSE_MAX_BYTES + 1) }));
        await assert.rejects(f.api.queryRootOutcome("vault", identity), /admitted byte bound/);
        assert.equal(f.counts.decrypt, 0, "Oversized native wire result rejected before decrypt/copies");
        const duplicated = JSON.stringify(terminal()).replace('{', '{"sequence":1,');
        f.reply(() => ({ body: new TextEncoder().encode(duplicated) }));
        await assert.rejects(f.api.queryRootOutcome("vault", identity), /Duplicate/);
        f.reply(() => ({ body: new Uint8Array([0xff]) }));
        await assert.rejects(f.api.queryRootOutcome("vault", identity), /UTF-8/);
        f.reply(() => ({ body: bytes({ ...terminal(), status: "unknown" }) }));
        await assert.rejects(f.api.queryRootOutcome("vault", identity));
        const beforeCapabilityDecrypt = f.counts.decrypt;
        f.reply(call => call.path.endsWith("capabilities")
            ? { body: new Uint8Array(ROOT_CAPABILITY_MAX_BYTES + 1) }
            : { body: bytes(terminal()) });
        await assert.rejects(f.api.commitRootOutcome("vault", intent()), /admitted byte bound/);
        assert.equal(f.counts.decrypt, beforeCapabilityDecrypt,
            "oversized tree capability result reached decrypt/copies");
        assert(!f.calls.some(call => call.path.includes("root-commit")),
            "oversized tree capability result reached root mutation");
        assert.equal(f.memoryBudget.snapshot().usedBytes, 0,
            "root decode/capability failures leaked admission");
    } finally { f.close(); }
}

void (async () => {
    await completeAdmissionPrecedesCommitTransportAndLivesThroughNativeCompletion();
    await queryAndCancelCaptureBeforeAdmissionAndRetainTheirLease();
    await bootstrapResponseBoundPrecedesRootDispatch();
    await negotiationAndFreshness();
    await exactRequestsSurviveRetriesAndCallerMutation();
    await staleSessionStillUsesOriginalIncarnationAndCancellationIdentity();
    await queryAndFailuresNeverBecomeLegacyAcceptance();
    await actualWireAndJSONBoundsPrecedeDecryptAndAcceptance();
    console.log("api-root-outcome.test: 8 admitted sealed HTTP/capability/retry regression scenarios passed");
})().catch(error => { setTimeout(() => { throw error; }, 0); });
