import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { ObsetyncJournal } from "./journal";
import { ObsetyncSyncBase, SYNC_BASE_STORE_PATH } from "./sync-base";
import { RootIntentStore, type StoredRootIntent } from "./root-intent";
import { createRootCommitIntent, type RootTerminalOutcome } from "./root-outcome";
import { RootSettlementCoordinator } from "./root-settlement";
import { MemorySegmentedIO } from "./segmented-store-test-io";

let assertions = 0;
const check = (v: unknown, message: string) => { assertions++; assert.ok(v, message); };
const same = (a: unknown, b: unknown, message: string) => { assertions++; assert.deepEqual(a, b, message); };
const h = (n: number) => n.toString(16).padStart(64, "0");
// Deterministic injection for persistence integration; codec tests supply BLAKE3.
const digest = (v: Uint8Array) => createHash("sha256").update(v).digest("hex");
async function opened(io: MemorySegmentedIO) {
    const app = { vault: { adapter: io } } as any;
    const base = new ObsetyncSyncBase(app), journal = new ObsetyncJournal(app);
    const intents = new RootIntentStore(io, h(1), digest);
    await base.load(); await journal.load(); await intents.load();
    return { io, base, journal, intents, coordinator: new RootSettlementCoordinator(intents, base, journal) };
}
async function fixture(outcome: "accepted" | "cancelled" | "conflict" | "unresolved" = "accepted") {
    const value = await opened(new MemorySegmentedIO());
    const throughId = await value.journal.append({ action: "modified", path: "note.md", ts: 1, synced: false });
    const request = await createRootCommitIntent("vault", "device", { protocol_version: 1, server_incarnation: h(2),
        sequence: 1, mutation_id: "1".repeat(32), parent_root: "", root: "AAAA" }, digest);
    const intent: StoredRootIntent = { vaultId: "vault", deviceId: "device", request,
        journalEpoch: value.journal.validatedEpoch!, journalCuts: [{ path: "note.md", throughId }],
        publication: { identity: { scopeHash: h(1), sequence: 1, mutationId: request.mutation_id, requestHash: request.request_hash },
            candidateRoot: h(3), committedAt: 10, entries: [{ action: "upsert", path: "note.md", hash: h(4), size: 3, mtime: 1 }] } };
    await value.intents.prepare(intent);
    const envelope = { protocol_version: 1 as const, server_incarnation: h(9), sequence: 1,
        mutation_id: request.mutation_id, request_hash: request.request_hash };
    const receipt: RootTerminalOutcome = outcome === "cancelled" ? { ...envelope, status: "cancelled", result: { cancelled: true } } :
        { ...envelope, status: "accepted", result: { merged: true, root_hash: h(5), auto_resolved: 0, text_merged: 0,
            conflicts: outcome === "conflict" ? [{ path: "note.md", base_hash: h(6), side_a_hash: h(7), side_b_hash: h(4) }] : [] } };
    if (outcome !== "unresolved") await value.intents.recordTerminal(receipt);
    return { ...value, intent };
}
async function restartCuts() {
    for (const cut of ["receipt", "base", "ack", "retired"] as const) {
        const f = await fixture();
        if (cut !== "receipt") await f.base.commitRootPublication(f.intent.publication);
        if (cut === "ack" || cut === "retired") await f.journal.acknowledgeOwned(f.intent.journalEpoch, f.intent.journalCuts);
        if (cut === "retired") await f.intents.retire(f.intent.publication.identity);
        // Work that belongs to a later generation must survive every replay.
        const newest = await f.journal.append({ action: "modified", path: "note.md", ts: 2, synced: false });
        if (cut !== "receipt") { f.base.setEntry("note.md", h(88), 22, 33); await f.base.save(); }
        const restored = await opened(f.io.clone());
        const result = await restored.coordinator.settle();
        same(result, cut === "retired" ? { status: "idle" } : { status: "accepted", candidateRoot: h(3), observedRoot: h(5) },
            `restart at ${cut} confused candidate/merged roots`);
        check(restored.intents.pending() === null && restored.intents.lastSequence === 1, "settlement left or reused intent authority");
        check(restored.journal.unsynced().length === 1 && restored.journal.unsynced()[0].id === newest,
            `restart at ${cut} acknowledged later journal work`);
        check(restored.base.getHash("note.md") === (cut === "receipt" ? h(4) : h(88)),
            `restart at ${cut} replayed accepted entries over a later local base`);
        same(await restored.coordinator.settle(), { status: "idle" }, "settled operation was run twice");
    }
}
async function terminalBoundaries() {
    for (const status of ["cancelled", "conflict", "unresolved"] as const) {
        const f = await fixture(status), before = f.io.snapshot();
        const result = await f.coordinator.settle();
        same(result, { status: status === "conflict" ? "conflicts-pending" : status }, "wrong terminal disposition");
        check(f.base.lastAppliedPublication === null && f.journal.unsyncedCount() === 1,
            "non-settleable outcome mutated base or acknowledged journal");
        if (status !== "cancelled") same(f.io.snapshot(), before, "unresolved/conflict state performed persistence writes");
        check((f.intents.pending() === null) === (status === "cancelled"), "unresolved or conflict intent was retired");
    }
    const f = await fixture();
    const foreign = await opened(new MemorySegmentedIO());
    let failed = false;
    try { await new RootSettlementCoordinator(f.intents, f.base, foreign.journal).settle(); } catch { failed = true; }
    check(failed && f.base.lastAppliedPublication === null && f.intents.pending() !== null,
        "foreign journal epoch was accepted before local base mutation");
    for (const loaded of [false, true]) {
        const cancelled = await fixture("cancelled");
        const foreignIO = new MemorySegmentedIO();
        const journal = new ObsetyncJournal({ vault: { adapter: foreignIO } } as any);
        if (loaded) {
            await journal.load();
            await journal.append({ action: "modified", path: "note.md", ts: 80, synced: false });
        }
        const foreignBefore = foreignIO.snapshot();
        const unrelated = (io: MemorySegmentedIO) => [...io.files].filter(([path]) => !path.includes("root-intent.store-v1"));
        const localBefore = unrelated(cancelled.io);
        same(await new RootSettlementCoordinator(cancelled.intents, cancelled.base, journal).settle(),
            { status: "cancelled" }, "cancellation waited for an unrelated or unloaded journal");
        same(foreignIO.snapshot(), foreignBefore, "cancellation mutated a foreign journal");
        same(unrelated(cancelled.io), localBefore, "cancellation mutated local base or journal");
        check(cancelled.intents.pending() === null && cancelled.intents.lastSequence === 1,
            "cancelled stream retirement lost its sequence");
    }
}
async function sharedOwnerAndLateAppend() {
    const f = await fixture();
    let release!: () => void, entered!: () => void;
    const held = new Promise<void>(done => { release = done; });
    const started = new Promise<void>(done => { entered = done; });
    let blocked = false;
    f.io.onBoundary = async event => {
        if (!blocked && event.method === "write" && event.phase === "after" &&
            event.path.startsWith(`${SYNC_BASE_STORE_PATH}/wal-`)) { blocked = true; entered(); await held; }
    };
    const one = f.coordinator.settle(); await started;
    const two = f.coordinator.settle(); check(one === two, "concurrent settlement queued another owner");
    const late = await f.journal.append({ action: "modified", path: "note.md", ts: 3, synced: false });
    release(); await one;
    check(f.journal.unsynced()[0]?.id === late, "accepted tail consumed mutation captured during native base write");
}
async function run() {
    await restartCuts(); await terminalBoundaries(); await sharedOwnerAndLateAppend();
    console.log(`root-settlement: ${assertions} assertions passed`);
}
void run().catch(error => { console.error(error); process.exitCode = 1; });
