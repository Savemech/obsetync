import { strict as assert } from "node:assert";
import { ObsetyncJournal, JOURNAL_STORE_PATH } from "./journal";
import { MemorySegmentedIO } from "./segmented-store-test-io";

let assertions = 0;
const check = (v: unknown, message: string) => { assertions++; assert.ok(v, message); };
async function rejects(p: Promise<unknown>, message: string) { let failed = false; try { await p; } catch { failed = true; } check(failed, message); }
async function fixture(io = new MemorySegmentedIO()) {
    const journal = new ObsetyncJournal({ vault: { adapter: io } } as any);
    check(journal.validatedEpoch === null, "unloaded journal exposed an epoch");
    await journal.load(); return { io, journal };
}
async function run() {
    const { io, journal } = await fixture(); const epoch = journal.validatedEpoch!;
    check(/^[a-f0-9]{32}$/.test(epoch), "loaded journal has no validated identity");
    const old = await journal.append({ action: "modified", path: "a.md", synced: false, ts: 1 });
    const newer = await journal.append({ action: "modified", path: "a.md", synced: false, ts: 2 });
    await journal.acknowledgeOwned(epoch, [{ path: "a.md", throughId: old }]);
    check(journal.unsynced()[0]?.id === newer, "old owned ACK consumed a later generation");
    await journal.compact(); const restored = (await fixture(io)).journal;
    check(restored.validatedEpoch === epoch, "compaction/reload changed journal identity");
    await restored.acknowledgeOwned(epoch, [{ path: "a.md", throughId: newer }]);
    await restored.acknowledgeOwned(epoch, [{ path: "a.md", throughId: newer }]);
    check(restored.unsyncedCount() === 0, "exact repeated owned ACK was not idempotent");
    const [rename] = await restored.appendGroup([
        { action: "deleted", path: "a.md", synced: false, ts: 3 },
        { action: "created", path: "b.md", synced: false, ts: 3 },
    ]);
    await restored.acknowledgeOwned(epoch, [{ path: "a.md", throughId: rename }]);
    check(restored.unsynced().length === 1 && restored.unsynced()[0].path === "b.md", "owned ACK consumed an uncommitted rename peer");
    const before = io.snapshot();
    await rejects(restored.acknowledgeOwned(epoch, [{ path: "b.md", throughId: rename + 1 }]), "unissued journal generation was acknowledged");
    assertions++; assert.deepEqual(io.snapshot(), before, "unissued owned ACK rewrote storage");
    check(restored.validatedEpoch === null, "failed writer kept exposing a validated epoch");
    await restored.load();
    await rejects(restored.acknowledgeOwned("f".repeat(32), []), "empty cut bypassed epoch validation");
    await restored.load();
    // Queue a reload against another valid journal, then submit the old epoch.
    // The submission-time view still refers to the original identity.
    const replacement = await fixture(); await replacement.journal.append({ action: "modified", path: "b.md", synced: false, ts: 99 });
    io.files.clear(); for (const [path, data] of replacement.io.files) io.files.set(path, data);
    io.directories.clear(); for (const path of replacement.io.directories) io.directories.add(path);
    const head = io.files.get(`${JOURNAL_STORE_PATH}/head.json`);
    const reload = restored.load();
    const stale = restored.acknowledgeOwned(epoch, [{ path: "b.md", throughId: 1 }]);
    await reload; await rejects(stale, "queued reload turned an old epoch into new-journal authority");
    check(io.files.get(`${JOURNAL_STORE_PATH}/head.json`) === head, "wrong-epoch ACK published a new head");
    await restored.load(); check(restored.unsyncedCount() === 1, "wrong-epoch ACK lost the new journal's work");
    console.log(`journal-owned: ${assertions} assertions passed`);
}
void run().catch(error => { console.error(error); process.exitCode = 1; });
