import { ObsetyncJournal } from "./journal";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

class MemoryAdapter {
    value: string;
    appendCalls = 0;
    writeCalls = 0;

    constructor(value = [
        JSON.stringify({ action: "modified", path: "same.md", ts: 1, synced: false }),
        JSON.stringify({ action: "modified", path: "same.md", ts: 2, synced: false }),
        '{"action":"modified"',
    ].join("\n")) {
        this.value = value;
    }

    async read(): Promise<string> { return this.value; }
    async exists(): Promise<boolean> { return true; }
    async mkdir(): Promise<void> {}
    async append(_path: string, data: string): Promise<void> {
        this.appendCalls++;
        this.value += data;
    }
    async write(_path: string, data: string): Promise<void> {
        this.writeCalls++;
        this.value = data;
    }
}

async function run(): Promise<void> {
    const adapter = new MemoryAdapter();
    const journal = new ObsetyncJournal({ vault: { adapter } } as any);
    await journal.load();

    check(journal.unsynced().length === 2, "torn tail discarded valid WAL rows");
    check(journal.unsynced()[0].id === 1, "legacy row did not receive a stable id");

    const third = await journal.append({
        action: "modified",
        path: "other.md",
        ts: 3,
        synced: false,
    });
    check(third === 3, "append id did not continue after legacy rows");
    check(adapter.appendCalls === 1, "append rewrote instead of extending the WAL");
    check(adapter.writeCalls === 0, "ordinary append caused a full journal rewrite");

    await journal.acknowledge([{ path: "same.md", throughId: 1 }]);
    check(
        journal.unsynced().some((entry) => entry.path === "same.md" && entry.id === 2),
        "acknowledgement erased a newer edit on the same path",
    );
    await journal.acknowledge([{ path: "same.md", throughId: 2 }]);
    check(
        journal.unsynced().length === 1 && journal.unsynced()[0].path === "other.md",
        "exact journal watermark was not acknowledged",
    );
    check(adapter.writeCalls === 0, "acknowledgement rewrote the whole journal");

    const restarted = new ObsetyncJournal({ vault: { adapter } } as any);
    await restarted.load();
    check(
        restarted.unsynced().length === 1 && restarted.unsynced()[0].path === "other.md",
        "durable acknowledgement was not replayed after restart",
    );

    const ids = await Promise.all([
        journal.append({ action: "created", path: "c.md", ts: 4, synced: false }),
        journal.append({ action: "created", path: "d.md", ts: 5, synced: false }),
    ]);
    check(ids[1] === ids[0] + 1, "concurrent appends were not serialized");
    check(journal.unsynced().length === 3, "serialized appends lost an entry");

    const legacyRenameRows = [
        JSON.stringify({ id: 1, action: "renamed", path: "new.md", oldPath: "old.md", ts: 1 }),
        ...Array.from({ length: 9_999 }, (_, index) => JSON.stringify({
            id: index + 2,
            action: "modified",
            path: "new.md",
            ts: index + 2,
        })),
    ].join("\n") + "\n";
    const compactingAdapter = new MemoryAdapter(legacyRenameRows);
    const compacted = new ObsetyncJournal({ vault: { adapter: compactingAdapter } } as any);
    await compacted.load();
    check(
        compacted.unsynced().some((entry) => entry.path === "old.md" && entry.action === "deleted"),
        "legacy rename compaction lost the old-path deletion",
    );
}

void run()
    .then(() => console.log(`journal.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
