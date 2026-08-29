import { DirtyPathSet } from "./dirty-set";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const dirty = new DirtyPathSet();
dirty.add({ action: "modified", path: "a.md", data: new Uint8Array(1024) }, 1);
dirty.add({ action: "deleted", path: "a.md" }, 2);
check(dirty.size === 1, "same path was not coalesced");
let batch = dirty.take();
check(batch[0].action === "deleted", "modify then delete must finish deleted");
check(!("data" in batch[0]), "dirty set retained file bytes");
check(batch[0].journalId === 2, "latest journal watermark was lost");

dirty.add({ action: "deleted", path: "b.md" }, 3);
dirty.add({ action: "created", path: "b.md", hash: "new" }, 4);
batch = dirty.take();
check(batch.length === 1 && batch[0].action === "created", "delete then create must finish created");
check(batch[0].hash === "new", "latest upsert metadata was lost");

dirty.add({ action: "modified", path: "race.md", hash: "old" }, 5);
const inFlight = dirty.take();
dirty.add({ action: "modified", path: "race.md", hash: "new" }, 6);
dirty.restore(inFlight);
batch = dirty.take();
check(batch[0].hash === "new", "failed push overwrote a newer in-flight event");
check(batch[0].journalId === 6, "newer journal watermark was overwritten");

dirty.add({ action: "modified", path: "scan-race.md", hash: "journaled" }, 7);
const failedJournaled = dirty.take();
dirty.add({ action: "modified", path: "scan-race.md", hash: "scan" });
dirty.restore(failedJournaled);
batch = dirty.take();
check(batch[0].hash === "scan", "restore replaced a newer scan result");
check(batch[0].journalId === 7, "restore orphaned an older durable journal row");

console.log(`dirty-set.test: ${assertions} assertions passed`);
