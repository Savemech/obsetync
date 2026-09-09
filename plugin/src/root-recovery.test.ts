import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { ObsetyncJournal, JOURNAL_STORE_PATH } from "./journal";
import { ObsetyncSyncBase, SYNC_BASE_STORE_PATH } from "./sync-base";
import { RootIntentStore, ROOT_INTENT_PATH, type StoredRootIntent } from "./root-intent";
import { RootSettlementCoordinator } from "./root-settlement";
import { RootRecoveryCoordinator, type RootRecoveryApi, type RootRecoveryOptions } from "./root-recovery";
import { createRootCommitIntent, decodeRootOutcomeCapabilities, type RootOutcomeCapabilities,
    type RootOutcome, type RootOutcomeIdentity, type RootTerminalOutcome, type RootCancelRequest,
    type RootCommitIntent } from "./root-outcome";
import { MemorySegmentedIO, storeTestIOError } from "./segmented-store-test-io";

let assertions = 0;
const check = (value: unknown, message: string) => { assertions++; assert.ok(value, message); };
const same = (actual: unknown, expected: unknown, message: string) => { assertions++; assert.deepEqual(actual, expected, message); };
async function rejects(promise: Promise<unknown>, expression: RegExp, message: string) {
    assertions++; await assert.rejects(promise, expression, message);
}
const h = (value: number) => value.toString(16).padStart(64, "0");
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value));
// Persistence/control-flow integration only. The wire codec's separate tests
// exercise BLAKE3; this deterministic injection is not a protocol digest claim.
const digest = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const expectedScope = { scopeHash: h(1), vaultId: "vault", deviceId: "device" };
const capabilities: RootOutcomeCapabilities = { serverIncarnation: h(2), maxSequence: Number.MAX_SAFE_INTEGER,
    commitBytes: 704 * 1024, rootBytes: 512 * 1024, queryBytes: 4096, cancelBytes: 4096,
    receiptBytes: 64 * 1024, streamsPerVault: 128 };
function wire(intent: StoredRootIntent): RootOutcomeIdentity {
    return { sequence: intent.request.sequence, mutation_id: intent.request.mutation_id,
        request_hash: intent.request.request_hash };
}
function receipt(intent: StoredRootIntent, status: "accepted" | "cancelled" = "accepted", incarnation = h(2)): RootTerminalOutcome {
    const envelope = { protocol_version: 1 as const, server_incarnation: incarnation, ...wire(intent) };
    return status === "cancelled" ? { ...envelope, status, result: { cancelled: true } } : {
        ...envelope, status, result: { merged: true, root_hash: h(5), conflicts: [], auto_resolved: 0, text_merged: 0 } };
}
function gate() {
    let open!: () => void, enter!: () => void;
    const held = new Promise<void>(resolve => { open = resolve; });
    const entered = new Promise<void>(resolve => { enter = resolve; });
    return { held, entered, open, enter };
}
async function turns() { for (let i = 0; i < 12; i++) await Promise.resolve(); }

/** Typed synthetic network port; all client persistence/ACKs below use the real
 * stores. These tests do not emulate the sealed transport or native server. */
class Network implements RootRecoveryApi {
    readonly calls: Array<{ method: string; vault?: string; value?: unknown }> = [];
    caps: RootOutcomeCapabilities | null = { ...capabilities };
    intent!: StoredRootIntent;
    currentIncarnation = h(2);
    currentRoot = h(90);
    terminal: RootTerminalOutcome | null = null;
    onNegotiate?: () => Promise<RootOutcomeCapabilities | null>;
    onQuery?: (identity: RootOutcomeIdentity) => Promise<RootOutcome>;
    onCommit?: (request: RootCommitIntent) => Promise<RootTerminalOutcome>;
    onCancel?: (request: RootCancelRequest) => Promise<RootTerminalOutcome>;
    async negotiateRootOutcomes(force?: boolean): Promise<RootOutcomeCapabilities | null> {
        this.calls.push({ method: "negotiate", value: force });
        return this.onNegotiate ? this.onNegotiate() : this.caps && { ...this.caps };
    }
    async queryRootOutcome(vault: string, identity?: RootOutcomeIdentity): Promise<RootOutcome> {
        this.calls.push({ method: "query", vault, value: clone(identity) });
        assert.ok(identity, "recovery must never perform a stream-only query");
        if (this.onQuery) return this.onQuery(identity);
        return this.terminal ? { ...clone(this.terminal), server_incarnation: this.currentIncarnation } : {
            protocol_version: 1, server_incarnation: this.currentIncarnation,
            status: "unknown", last_sequence: this.intent.request.sequence - 1, current_root_hash: this.currentRoot };
    }
    async commitRootOutcome(vault: string, request: RootCommitIntent): Promise<RootTerminalOutcome> {
        this.calls.push({ method: "commit", vault, value: clone(request) });
        if (this.onCommit) return this.onCommit(request);
        this.terminal ??= receipt(this.intent, "accepted", this.currentIncarnation);
        return clone(this.terminal);
    }
    async cancelRootOutcome(vault: string, request: RootCancelRequest): Promise<RootTerminalOutcome> {
        this.calls.push({ method: "cancel", vault, value: clone(request) });
        if (this.onCancel) return this.onCancel(request);
        this.terminal ??= receipt(this.intent, "cancelled", this.currentIncarnation);
        return clone(this.terminal);
    }
    count(method: string): number { return this.calls.filter(call => call.method === method).length; }
}
async function opened(io: MemorySegmentedIO, network: Network, options: Partial<RootRecoveryOptions> = {}) {
    const app = { vault: { adapter: io } } as any;
    const base = new ObsetyncSyncBase(app), journal = new ObsetyncJournal(app);
    const intents = new RootIntentStore(io, h(1), digest);
    await base.load(); await journal.load(); await intents.load();
    const settlement = new RootSettlementCoordinator(intents, base, journal);
    let scopeChecks = 0;
    const coordinator = new RootRecoveryCoordinator(intents, settlement, network, { ...expectedScope,
        assertApiScope(scope) { scopeChecks++; same(scope, expectedScope, "API owner was bound to a different scope"); }, ...options });
    return { io, network, base, journal, intents, settlement, coordinator, scopeChecks: () => scopeChecks };
}
async function fixture(prepared = true) {
    const network = new Network(), value = await opened(new MemorySegmentedIO(), network);
    const throughId = await value.journal.append({ action: "modified", path: "note.md", ts: 1, synced: false });
    const request = await createRootCommitIntent("vault", "device", { protocol_version: 1, server_incarnation: h(2),
        sequence: 1, mutation_id: "1".repeat(32), parent_root: "", root: "AAAA" }, digest);
    const intent: StoredRootIntent = { vaultId: "vault", deviceId: "device", request,
        journalEpoch: value.journal.validatedEpoch!, journalCuts: [{ path: "note.md", throughId }],
        publication: { identity: { scopeHash: h(1), sequence: 1, mutationId: request.mutation_id, requestHash: request.request_hash },
            candidateRoot: h(3), committedAt: 10,
            entries: [{ action: "upsert", path: "note.md", hash: h(4), mtime: 1, size: 3 }] } };
    network.intent = clone(intent);
    if (prepared) await value.intents.prepare(intent);
    return { ...value, intent };
}
function untouched(f: Awaited<ReturnType<typeof fixture>>, message: string) {
    check(f.intents.pending() !== null && f.intents.pending()?.terminal === null, `${message}: lost unresolved intent`);
    check(f.base.lastAppliedPublication === null && f.journal.unsyncedCount() === 1, `${message}: mutated base or ACKed`);
}

async function freshPublicationAndActualDrain() {
    const f = await fixture(false), prepareGate = gate(), terminalGate = gate();
    let writes = 0;
    f.io.onBoundary = async event => {
        if (event.method === "write" && event.phase === "after" && event.path.startsWith(`${ROOT_INTENT_PATH}/wal-`)) {
            writes++;
            if (writes === 1) { prepareGate.enter(); await prepareGate.held; }
            if (writes === 2) { terminalGate.enter(); await terminalGate.held; }
        }
    };
    const raw = clone(f.intent), operation = f.coordinator.prepareAndSend(raw);
    await prepareGate.entered;
    same(f.network.calls, [], "network began before actual intent prepare completed");
    same(f.coordinator.snapshot(), { closed: false, active: 1, drained: false }, "prepare was not a synchronously admitted owner");
    raw.request.root = "BBBB";
    (raw.publication.entries[0] as any).hash = h(77);
    await rejects(f.coordinator.resolvePending(), /busy/, "second owner queued during native prepare");
    prepareGate.open(); await terminalGate.entered;
    check(f.network.count("commit") === 1 && f.network.count("query") === 0, "fresh publication did not send exactly once");
    same(f.network.calls.find(call => call.method === "commit")?.value, f.intent.request, "caller mutation changed durable request bytes");
    check(f.base.lastAppliedPublication === null && f.journal.unsyncedCount() === 1,
        "base/ACK escaped before terminal record native completion");
    const later = await f.journal.append({ action: "modified", path: "note.md", ts: 2, synced: false });
    const drain = f.coordinator.closeAndDrain(); let drained = false; void drain.then(() => { drained = true; });
    check(drain === f.coordinator.closeAndDrain(), "close did not share one stable actual drain");
    await turns(); check(!drained, "close virtually completed a held terminal write");
    await rejects(f.coordinator.resolvePending(), /closed/, "close admitted another operation");
    terminalGate.open();
    same(await operation, { status: "accepted", candidateRoot: h(3), observedRoot: h(5) }, "candidate root was replaced by merged receipt root");
    await drain;
    check(f.base.treeBaseRoot === h(3) && f.base.getHash("note.md") === h(4), "detached candidate publication changed");
    same(f.journal.unsynced().map(row => row.id), [later], "accepted captured cut acknowledged later local edit");
    check(f.intents.pending() === null && f.intents.lastSequence === 1, "accepted tail was abandoned on close");
    same(f.coordinator.snapshot(), { closed: true, active: 0, drained: true }, "drained owner remained active");
}

async function lostResponseAndLaterRemoteRoot() {
    const f = await fixture(false);
    f.network.onCommit = async () => { f.network.terminal = receipt(f.intent); throw new Error("lost response 503"); };
    await rejects(f.coordinator.prepareAndSend(f.intent), /lost response/, "lost commit response was hidden");
    untouched(f, "lost accepted response");
    const later = await f.journal.append({ action: "modified", path: "note.md", ts: 2, synced: false });
    f.network.currentRoot = h(99); f.network.currentIncarnation = h(10);
    const restarted = await opened(f.io.clone(), f.network);
    same(await restarted.coordinator.resolvePending(), { status: "accepted", candidateRoot: h(3), observedRoot: h(5) },
        "historical acceptance was confused with newer remote current root");
    check(f.network.count("commit") === 1 && f.network.count("query") === 1 && f.network.count("cancel") === 0,
        "recovery blindly resent after lost response or cancelled known acceptance");
    same(f.network.calls.find(call => call.method === "query")?.value, wire(f.intent), "restart queried rewritten identity");
    check(restarted.base.treeBaseRoot === h(3), "merged/current root became local candidate base");
    same(restarted.journal.unsynced().map(row => row.id), [later], "restart ACK crossed queued generation");
    check(restarted.intents.pending() === null && restarted.intents.lastSequence === 1, "restart lost stream watermark");
}

async function terminalRaceAndIncarnationChanges() {
    for (const winner of ["cancelled", "accepted"] as const) {
        const f = await fixture(false);
        f.network.onCommit = async () => { throw new Error("lost initial response"); };
        await rejects(f.coordinator.prepareAndSend(f.intent), /lost initial/, "failed first dispatch was hidden");
        f.network.caps = { ...capabilities, serverIncarnation: h(10) };
        f.network.currentIncarnation = h(11); // query observes a newer process than negotiation
        f.network.onCancel = async request => {
            same(request, { protocol_version: 1, server_incarnation: h(11), ...wire(f.intent) },
                "cancellation changed the original digest or ignored fresh query incarnation");
            f.network.terminal = receipt(f.intent, winner, h(12)); // actual accepted/cancelled winner
            return clone(f.network.terminal);
        };
        const result = await f.coordinator.prepareAndSend(clone(f.intent));
        same(result, winner === "accepted" ? { status: "accepted", candidateRoot: h(3), observedRoot: h(5) } : { status: "cancelled" },
            "unknown cancellation invented a terminal winner");
        check(f.network.count("commit") === 1 && f.network.count("query") === 1 && f.network.count("cancel") === 1,
            "already-prepared reused initial-send permission");
        check(f.journal.unsyncedCount() === (winner === "accepted" ? 0 : 1), "cancelled outcome ACKed or accepted outcome failed its exact ACK");
        const restored = await opened(f.io.clone(), f.network);
        check(restored.intents.pending() === null && restored.intents.lastSequence === 1, "terminal retirement lost sequence on restart");
        check(restored.journal.unsyncedCount() === (winner === "accepted" ? 0 : 1), "restart invented cancellation ACK");
    }
    // Even a newly prepared intent cannot be rebound after server restart.
    const fresh = await fixture(false);
    fresh.network.caps = { ...capabilities, serverIncarnation: h(10) };
    fresh.network.currentIncarnation = h(10);
    same(await fresh.coordinator.prepareAndSend(fresh.intent), { status: "cancelled" }, "fresh stale incarnation was blindly submitted");
    check(fresh.network.count("commit") === 0 && fresh.network.count("query") === 1, "stale candidate bypassed resolution");
}

async function retainedFailureOutcomes() {
    for (const failure of ["query-503", "expired", "mismatch", "stream", "gap", "cancel-409", "cancel-mismatch"] as const) {
        const f = await fixture();
        if (failure === "query-503") f.network.onQuery = async () => { throw new Error("query 503"); };
        if (failure === "expired") f.network.onQuery = async () => ({ protocol_version: 1, server_incarnation: h(9),
            status: "expired", last_sequence: 2, current_root_hash: h(70) });
        if (failure === "mismatch") f.network.onQuery = async () => ({ ...receipt(f.intent), request_hash: h(71) });
        if (failure === "stream") f.network.onQuery = async () => ({ protocol_version: 1, server_incarnation: h(9),
            status: "stream", last_sequence: 0, current_root_hash: null });
        if (failure === "gap") {
            // A higher local sequence after a restored server must not adopt a
            // remote floor or try to fill missing operations with this candidate.
            await f.intents.recordTerminal(receipt(f.intent, "cancelled"));
            await f.intents.retire(f.intent.publication.identity);
            const { request_hash: _, ...old } = f.intent.request;
            const request = await createRootCommitIntent("vault", "device", {
                ...old, sequence: 2, mutation_id: "2".repeat(32) }, digest);
            f.intent = { ...f.intent, request, publication: { ...f.intent.publication, identity: {
                ...f.intent.publication.identity, sequence: 2, mutationId: request.mutation_id, requestHash: request.request_hash } } };
            f.network.intent = clone(f.intent); await f.intents.prepare(f.intent);
            f.network.onQuery = async () => ({ protocol_version: 1, server_incarnation: h(9),
                status: "unknown", last_sequence: 0, current_root_hash: null });
        }
        if (failure === "cancel-409") f.network.onCancel = async () => { throw new Error("stale incarnation 409"); };
        if (failure === "cancel-mismatch") f.network.onCancel = async () => ({ ...receipt(f.intent), mutation_id: "9".repeat(32) });
        const disk = f.io.snapshot();
        if (failure === "expired" || failure === "gap") {
            same(await f.coordinator.resolvePending(), { status: "deferred", reason: failure === "expired" ? "expired" : "stream-diverged" },
                "unresolvable stream acquired acceptance authority");
        } else await rejects(f.coordinator.resolvePending(), /503|409|identity mismatch|Stream reply/, `${failure} was hidden`);
        untouched(f, failure); same(f.io.snapshot(), disk, `${failure} mutated durable pending state`);
        check(f.network.count("commit") === 0, `${failure} resubmitted candidate`);
        check(f.intents.lastSequence === (failure === "gap" ? 2 : 1), "remote stream watermark replaced local authority");
    }
}

async function capabilitiesAndScope() {
    for (const offered of [[], ["root-outcome-v1"], ["root-cancel-v1"]]) {
        const f = await fixture(); f.network.caps = decodeRootOutcomeCapabilities({ capabilities: offered });
        same(await f.coordinator.resolvePending(), { status: "deferred", reason: "unsupported" }, "missing either capability enabled recovery mutation");
        same(f.network.calls, [{ method: "negotiate", value: true }], "unsupported server received outcome work");
        untouched(f, "unsupported server");
    }
    for (const field of ["commitBytes", "rootBytes", "queryBytes", "cancelBytes"] as const) {
        const f = await fixture(field !== "commitBytes" && field !== "rootBytes");
        f.network.caps = { ...capabilities, [field]: 1 };
        const result = field === "commitBytes" || field === "rootBytes" ?
            await f.coordinator.prepareAndSend(f.intent) : await f.coordinator.resolvePending();
        same(result, { status: "deferred", reason: "limits" }, `${field} offer was ignored`);
        untouched(f, field); check(f.network.count("commit") === 0 && f.network.count("cancel") === 0, "limit refusal initiated a mutation");
    }
    for (const field of ["vaultId", "deviceId", "scopeHash"] as const) {
        const f = await fixture(false), raw = clone(f.intent), disk = f.io.snapshot();
        if (field === "scopeHash") raw.publication.identity.scopeHash = h(9); else raw[field] = "foreign";
        await rejects(f.coordinator.prepareAndSend(raw), /scope/, `${field} mismatch reached store prepare`);
        same(f.io.snapshot(), disk, "scope mismatch wrote durable data"); same(f.network.calls, [], "scope mismatch sent network request");
    }
    const f = await fixture(), options: RootRecoveryOptions = { ...expectedScope, assertApiScope() {} };
    const coordinator = new RootRecoveryCoordinator(f.intents, f.settlement, f.network, options);
    (options as any).deviceId = "foreign";
    same(await coordinator.resolvePending(), { status: "cancelled" }, "caller mutation changed captured owner scope");
    const foreign = await fixture();
    const mismatched = new RootRecoveryCoordinator(foreign.intents, foreign.settlement, foreign.network,
        { ...expectedScope, deviceId: "foreign", assertApiScope() {} });
    await rejects(mismatched.resolvePending(), /scope/, "loaded foreign pending intent was settled");
    same(foreign.network.calls, [], "foreign pending was queried"); untouched(foreign, "foreign pending");
    const changed = await fixture();
    let current = true;
    const guarded = new RootRecoveryCoordinator(changed.intents, changed.settlement, changed.network,
        { ...expectedScope, assertApiScope() { if (!current) throw new Error("API owner changed"); } });
    changed.network.onNegotiate = async () => { current = false; return { ...capabilities }; };
    await rejects(guarded.resolvePending(), /API owner changed/, "scope changed during negotiate but query still dispatched");
    check(changed.network.count("query") === 0, "new request used a replaced API owner"); untouched(changed, "replaced API");
}

async function durableTerminalBeforeSettlementAndRestart() {
    for (const status of ["accepted", "cancelled"] as const) {
        const f = await fixture(); f.network.terminal = receipt(f.intent, status);
        // Reject after the full new head exists. Restart may recover terminal,
        // but this failed owner must neither infer ACK nor continue settlement.
        let failed = false;
        f.io.onBoundary = event => {
            if (!failed && event.method === "rename" && event.phase === "after" &&
                event.to === `${ROOT_INTENT_PATH}/head.json`) { failed = true; throw storeTestIOError("EIO"); }
        };
        await rejects(f.coordinator.resolvePending(), /EIO/, "ambiguous terminal write was hidden");
        check(failed && f.base.lastAppliedPublication === null && f.journal.unsyncedCount() === 1,
            "failed terminal persistence permitted base/ACK");
        const restored = await opened(f.io.clone(), f.network);
        check(restored.intents.pending()?.terminal?.status === status, "complete terminal head did not survive reload");
        const calls = f.network.calls.length;
        const result = await restored.coordinator.resolvePending();
        same(result, status === "accepted" ? { status: "accepted", candidateRoot: h(3), observedRoot: h(5) } : { status: "cancelled" },
            "persisted terminal was not settled after restart");
        check(f.network.calls.length === calls, "already-terminal recovery required network access");
        check(restored.journal.unsyncedCount() === (status === "accepted" ? 0 : 1), "terminal restart used wrong ACK authority");
    }
    const f = await fixture();
    await f.intents.recordTerminal(receipt(f.intent));
    const foreign = await opened(new MemorySegmentedIO(), new Network());
    const wrongEpoch = new RootRecoveryCoordinator(f.intents,
        new RootSettlementCoordinator(f.intents, f.base, foreign.journal), f.network,
        { ...expectedScope, assertApiScope() { throw new Error("offline API must not be checked"); } });
    await rejects(wrongEpoch.resolvePending(), /original validated journal epoch/, "accepted receipt ACKed an unrelated restored journal epoch");
    check(f.intents.pending()?.terminal?.status === "accepted" && f.base.lastAppliedPublication === null,
        "wrong epoch retired terminal or modified local base");
    same(f.network.calls, [], "terminal old-epoch resolution attempted network");
}

async function restartAfterRejectedBookkeeping() {
    for (const cut of ["terminal", "ack", "retire"] as const) {
        const f = await fixture(); f.network.terminal = receipt(f.intent);
        let intentWrites = 0, failed = false;
        f.io.onBoundary = event => {
            if (event.method !== "write" || event.phase !== "before") return;
            const intentWrite = event.path.startsWith(`${ROOT_INTENT_PATH}/wal-`);
            if (intentWrite) intentWrites++;
            if (!failed && ((cut === "terminal" && intentWrite && intentWrites === 1) ||
                (cut === "retire" && intentWrite && intentWrites === 2) ||
                (cut === "ack" && event.path.startsWith(`${JOURNAL_STORE_PATH}/wal-`)))) {
                failed = true; throw storeTestIOError("ENOSPC");
            }
        };
        await rejects(f.coordinator.resolvePending(), /ENOSPC/, `${cut} write failure was hidden`);
        check(failed, `${cut} native failure boundary was not exercised`);
        const restored = await opened(f.io.clone(), f.network);
        check(restored.intents.pending() !== null, `${cut} failed tail lost its pending owner`);
        check(restored.intents.pending()?.terminal?.status === (cut === "terminal" ? undefined : "accepted"),
            `${cut} terminal persistence ordering changed`);
        const newer = await restored.journal.append({ action: "modified", path: "note.md", ts: 3, synced: false });
        if (cut !== "terminal") { restored.base.setEntry("note.md", h(88), 20, 30); await restored.base.save(); }
        const calls = f.network.calls.length;
        same(await restored.coordinator.resolvePending(), { status: "accepted", candidateRoot: h(3), observedRoot: h(5) },
            `${cut} restart failed accepted settlement`);
        check(f.network.calls.length === calls + (cut === "terminal" ? 2 : 0),
            `${cut} recovery retried request or required network for saved outcome`);
        same(restored.journal.unsynced().map(row => row.id), [newer], `${cut} retry consumed a newer queued generation`);
        check(restored.base.getHash("note.md") === (cut === "terminal" ? h(4) : h(88)),
            `${cut} retry overwrote a later base mutation`);
        const again = await opened(restored.io.clone(), f.network);
        check(again.intents.pending() === null && again.intents.lastSequence === 1,
            `${cut} completed restart lost retained sequence`);
        same(again.journal.unsynced().map(row => row.id), [newer], `${cut} exact ACK did not survive second restart`);
    }
}

async function nativeFailureAndSettlementDrains() {
    for (const phase of ["query", "cancel", "base"] as const) {
        const f = await fixture(), native = gate();
        if (phase === "query") f.network.onQuery = async () => { native.enter(); await native.held; throw new Error("native query failed"); };
        if (phase === "cancel") f.network.onCancel = async () => { native.enter(); await native.held; throw new Error("native cancel failed"); };
        if (phase === "base") {
            f.network.terminal = receipt(f.intent);
            let held = false;
            f.io.onBoundary = async event => {
                if (!held && event.method === "write" && event.phase === "after" && event.path.startsWith(`${SYNC_BASE_STORE_PATH}/wal-`)) {
                    held = true; native.enter(); await native.held;
                }
            };
        }
        const operation = f.coordinator.resolvePending();
        // Observe rejection immediately so assertions around native ownership do
        // not create a test-only unhandled rejection.
        const result = operation.then(value => ({ value }), error => ({ error }));
        await native.entered;
        const drain = f.coordinator.closeAndDrain(); let done = false; void drain.then(() => { done = true; });
        await turns(); check(!done, `${phase} drain abandoned actual native completion`);
        if (phase === "base") check(f.intents.pending()?.terminal?.status === "accepted", "base began before actual saved receipt");
        native.open(); const settled = await result; await drain;
        check(phase === "base" ? "value" in settled : "error" in settled, `${phase} actual result was changed by close`);
        if (phase === "base") check(f.intents.pending() === null && f.journal.unsyncedCount() === 0, "close interrupted admitted accepted ACK tail");
        else untouched(f, `${phase} rejection`);
        same(f.coordinator.snapshot(), { closed: true, active: 0, drained: true }, `${phase} owner leaked after actual failure`);
    }
    const empty = await fixture(false);
    same(await empty.coordinator.resolvePending(), { status: "idle" }, "empty stream was not idle");
    same(empty.network.calls, [], "idle stream performed network work");
    const drain = empty.coordinator.closeAndDrain(); await drain;
    check(drain === empty.coordinator.closeAndDrain(), "empty drain promise was unstable");
}

async function closeStopsOnlyNewNetworkAdmission() {
    const phases = ["prepare", "negotiate-fresh", "negotiate-recovery", "query-unknown", "query-accepted",
        "query-cancelled", "commit-lost", "commit-accepted", "cancel-cancelled", "cancel-accepted"] as const;
    for (const phase of phases) {
        const fresh = phase === "prepare" || phase === "negotiate-fresh" || phase.startsWith("commit-");
        const f = await fixture(!fresh), native = gate();
        const wait = async () => { native.enter(); await native.held; };
        if (phase === "prepare") {
            let held = false;
            f.io.onBoundary = async event => {
                if (!held && event.method === "write" && event.phase === "after" && event.path.startsWith(`${ROOT_INTENT_PATH}/wal-`)) {
                    held = true; await wait();
                }
            };
        }
        if (phase.startsWith("negotiate-")) f.network.onNegotiate = async () => { await wait(); return { ...capabilities }; };
        if (phase.startsWith("query-")) f.network.onQuery = async () => {
            await wait();
            return phase === "query-unknown" ? { protocol_version: 1, server_incarnation: h(10),
                status: "unknown", last_sequence: 0, current_root_hash: h(99) } :
                receipt(f.intent, phase === "query-accepted" ? "accepted" : "cancelled");
        };
        if (phase.startsWith("commit-")) f.network.onCommit = async () => {
            await wait(); f.network.terminal = receipt(f.intent);
            if (phase === "commit-lost") throw new Error("lost native commit response");
            return clone(f.network.terminal);
        };
        if (phase.startsWith("cancel-")) f.network.onCancel = async () => {
            await wait(); return receipt(f.intent, phase === "cancel-accepted" ? "accepted" : "cancelled");
        };
        const raw = clone(f.intent);
        const operation = fresh ? f.coordinator.prepareAndSend(raw) : f.coordinator.resolvePending();
        const result = operation.then(value => ({ value }), error => ({ error }));
        await native.entered;
        const callsBeforeClose = clone(f.network.calls);
        const drain = f.coordinator.closeAndDrain(); let drained = false; void drain.then(() => { drained = true; });
        raw.request.root = "BBBB"; // No late reads from submitted caller memory.
        await turns(); check(!drained, `${phase} close detached an actual in-flight owner`);
        native.open();
        const completed = await result; await drain;
        same(f.network.calls, callsBeforeClose, `${phase} dispatched a NEW network operation after close`);
        if (phase === "prepare") same(f.network.calls, [], "closed preparation still negotiated capabilities");
        if (phase.startsWith("negotiate-")) same(f.network.calls, [{ method: "negotiate", value: true }],
            "closed negotiation still queried/submitted an intent");
        if (phase === "query-unknown") check(f.network.count("cancel") === 0, "closed unknown query still dispatched cancellation");
        if (phase.endsWith("accepted")) {
            same(completed, { value: { status: "accepted", candidateRoot: h(3), observedRoot: h(5) } },
                `${phase} received terminal was discarded on close`);
            check(f.intents.pending() === null && f.journal.unsyncedCount() === 0 && f.base.treeBaseRoot === h(3),
                `${phase} close interrupted receipt/base/ACK/retirement tail`);
        } else if (phase.endsWith("cancelled")) {
            same(completed, { value: { status: "cancelled" } }, `${phase} terminal cancellation was discarded on close`);
            check(f.intents.pending() === null && f.journal.unsyncedCount() === 1 && f.base.lastAppliedPublication === null,
                `${phase} cancellation ACKed local work or lost retirement`);
        } else {
            if (phase === "commit-lost") check("error" in completed && /lost native commit/.test(String(completed.error)),
                "close masked an actually failed/lost commit response");
            else same(completed, { value: { status: "deferred", reason: "closing" } }, `${phase} did not explicitly defer on close`);
            untouched(f, `${phase} close`);
            const restored = await opened(f.io.clone(), f.network);
            same(restored.intents.pending()?.intent, f.intent, `${phase} close/restart changed original durable request bytes`);
            if (phase === "commit-lost") {
                same(await restored.coordinator.resolvePending(), { status: "accepted", candidateRoot: h(3), observedRoot: h(5) },
                    "closed lost response could not resolve original identity after restart");
                check(f.network.count("commit") === 1, "closed lost response was blindly submitted again");
            }
        }
        same(f.coordinator.snapshot(), { closed: true, active: 0, drained: true }, `${phase} actual completed owner leaked`);
    }
    // Scope checking is a synchronous user hook: it can also initiate close.
    // Recheck before the native call, not only before invoking that hook.
    const f = await fixture();
    let coordinator!: RootRecoveryCoordinator, drain!: Promise<void>;
    coordinator = new RootRecoveryCoordinator(f.intents, f.settlement, f.network, {
        ...expectedScope, assertApiScope() { drain = coordinator.closeAndDrain(); } });
    same(await coordinator.resolvePending(), { status: "deferred", reason: "closing" }, "reentrant scope-hook close was ignored");
    await drain; same(f.network.calls, [], "scope hook admitted network after synchronously closing");
    untouched(f, "scope-hook close");
}

async function freshSendGuardRechecksAfterActualPrepareAndNegotiation() {
    for (const phase of ["prepare", "negotiate"] as const) {
        const f = await fixture(false), native = gate();
        let applicable = true, guardCalls = 0;
        const failure = new Error(`review changed during ${phase}`);
        if (phase === "prepare") {
            let held = false;
            f.io.onBoundary = async event => {
                if (!held && event.method === "write" && event.phase === "after" && event.path.startsWith(`${ROOT_INTENT_PATH}/wal-`)) {
                    held = true; native.enter(); await native.held;
                }
            };
        } else f.network.onNegotiate = async () => { native.enter(); await native.held; return { ...capabilities }; };
        const operation = f.coordinator.prepareAndSend(f.intent, () => {
            guardCalls++;
            if (!applicable) throw failure;
        });
        const result = operation.then(value => ({ value }), error => ({ error }));
        await native.entered;
        check(guardCalls === 0 && f.network.count("commit") === 0, `${phase}: guard ran before the actual final admission boundary`);
        applicable = false;
        native.open();
        same(await result, { error: failure }, `${phase}: stale review still acquired initial-send authority`);
        same(guardCalls, 1, `${phase}: initial-send guard was not invoked exactly once`);
        same(f.network.count("commit"), 0, `${phase}: rejected guard dispatched the candidate`);
        untouched(f, `${phase}: rejected fresh-send guard`);
        same(f.intents.pending()?.intent, f.intent, `${phase}: guard rejection replaced the persisted identity`);
        const restarted = await opened(f.io.clone(), f.network);
        same(restarted.intents.pending()?.intent, f.intent, `${phase}: restart lost the unsent durable intent`);
        same(await restarted.coordinator.resolvePending(), { status: "cancelled" }, `${phase}: rejected initial send could not resolve normally`);
        same(f.network.count("commit"), 0, `${phase}: recovery blindly resent a rejected candidate`);
        check(restarted.journal.unsyncedCount() === 1 && restarted.base.lastAppliedPublication === null,
            `${phase}: cancelled unsent intent ACKed local work`);
        same(guardCalls, 1, `${phase}: old invocation guard leaked into another recovery owner`);
    }

    // The final API scope callback is itself synchronous user code. A guard
    // before that callback would miss an invalidation caused by the callback.
    const f = await fixture(false), order: string[] = [];
    let applicable = true, checks = 0;
    const coordinator = new RootRecoveryCoordinator(f.intents, f.settlement, f.network, {
        ...expectedScope, assertApiScope() { order.push("scope"); if (++checks === 3) applicable = false; },
    });
    await rejects(coordinator.prepareAndSend(f.intent, () => {
        order.push("guard"); if (!applicable) throw new Error("final scope callback invalidated review");
    }), /invalidated review/, "guard ran before final synchronous API hook");
    same(order, ["scope", "scope", "scope", "guard"], "fresh-send admission hook ordering changed");
    same(f.network.count("commit"), 0, "final scope-hook invalidation still dispatched root");
    untouched(f, "final scope-hook invalidation");
}

async function freshSendGuardNeverOwnsHistoricalResolutionOrReceivedTail() {
    for (const mode of ["unknown", "accepted", "terminal", "old-incarnation"] as const) {
        const f = await fixture(mode !== "old-incarnation");
        if (mode === "accepted") f.network.terminal = receipt(f.intent);
        if (mode === "terminal") await f.intents.recordTerminal(receipt(f.intent));
        if (mode === "old-incarnation") {
            f.network.caps = { ...capabilities, serverIncarnation: h(10) };
            f.network.currentIncarnation = h(10);
        }
        let guards = 0;
        const outcome = await f.coordinator.prepareAndSend(f.intent, () => { guards++; throw new Error("stale historical review"); });
        same(outcome, mode === "accepted" || mode === "terminal"
            ? { status: "accepted", candidateRoot: h(3), observedRoot: h(5) } : { status: "cancelled" },
        `${mode}: initial-send guard blocked historical resolution`);
        same(guards, 0, `${mode}: historical resolution invoked a stale fresh-send guard`);
        same(f.network.count("commit"), 0, `${mode}: existing/stale-incarnation intent was freshly sent`);
        same(f.journal.unsyncedCount(), mode === "accepted" || mode === "terminal" ? 0 : 1,
            `${mode}: historical receipt acquired wrong ACK authority`);
    }

    const f = await fixture(false), native = gate();
    let current = true, guards = 0;
    const coordinator = new RootRecoveryCoordinator(f.intents, f.settlement, f.network, {
        ...expectedScope, assertApiScope() { if (!current) throw new Error("later API scope changed"); },
    });
    f.network.onCommit = async () => { native.enter(); await native.held; return receipt(f.intent); };
    const operation = coordinator.prepareAndSend(f.intent, () => {
        guards++; if (!current) throw new Error("later review changed");
    });
    await native.entered;
    same(guards, 1, "initial request lacked its final guard");
    current = false;
    const later = await f.journal.append({ action: "modified", path: "note.md", ts: 3, synced: false });
    native.open();
    same(await operation, { status: "accepted", candidateRoot: h(3), observedRoot: h(5) },
        "post-send source/scope change interrupted an actual accepted tail");
    same(guards, 1, "accepted tail re-ran a stale invocation guard");
    same(f.journal.unsynced().map(entry => entry.id), [later], "accepted old cut ACKed the later generation");
    check(f.intents.pending() === null && f.base.treeBaseRoot === h(3), "accepted tail failed durable completion after source change");
}

async function freshSendGuardCanSynchronouslyCloseItsOwner() {
    const f = await fixture(false);
    let drain: Promise<void> | undefined, drained = false;
    const operation = f.coordinator.prepareAndSend(f.intent, () => {
        drain = f.coordinator.closeAndDrain();
        void drain.then(() => { drained = true; });
        same(f.coordinator.snapshot(), { closed: true, active: 1, drained: false },
            "guard reentrant close virtually completed its admitted owner");
        check(!drained, "guard completed a drain synchronously");
    });
    same(await operation, { status: "deferred", reason: "closing" }, "guard close still admitted a fresh root");
    check(drain !== undefined && drain === f.coordinator.closeAndDrain(), "guard close lost stable drain ownership");
    await drain;
    same(f.network.count("commit"), 0, "synchronously closed guard dispatched native commit");
    untouched(f, "reentrant fresh-send guard close");
    same(f.coordinator.snapshot(), { closed: true, active: 0, drained: true }, "guard close leaked actual work");
}

async function run() {
    await freshPublicationAndActualDrain(); await lostResponseAndLaterRemoteRoot();
    await terminalRaceAndIncarnationChanges(); await retainedFailureOutcomes(); await capabilitiesAndScope();
    await durableTerminalBeforeSettlementAndRestart(); await restartAfterRejectedBookkeeping();
    await nativeFailureAndSettlementDrains(); await closeStopsOnlyNewNetworkAdmission();
    await freshSendGuardRechecksAfterActualPrepareAndNegotiation();
    await freshSendGuardNeverOwnsHistoricalResolutionOrReceivedTail();
    await freshSendGuardCanSynchronouslyCloseItsOwner();
    console.log(`root-recovery: ${assertions} assertions passed`);
}
let completed = false;
process.once("beforeExit", () => {
    if (!completed) { console.error("root-recovery suite did not finish"); process.exitCode = 1; }
});
void run().then(() => { completed = true; }).catch(error => { completed = true; console.error(error); process.exitCode = 1; });
