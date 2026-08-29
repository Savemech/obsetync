import { ObsetyncSyncBase } from "./sync-base";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

class MemoryAdapter {
    files = new Map<string, string>();
    appendCalls = 0;
    duringNextAppend: (() => void) | null = null;

    async read(path: string): Promise<string> {
        const value = this.files.get(path);
        if (value === undefined) throw new Error("missing");
        return value;
    }
    async write(path: string, value: string): Promise<void> { this.files.set(path, value); }
    async append(path: string, value: string): Promise<void> {
        this.appendCalls++;
        const hook = this.duringNextAppend;
        this.duringNextAppend = null;
        hook?.();
        await Promise.resolve();
        this.files.set(path, (this.files.get(path) ?? "") + value);
    }
    async exists(path: string): Promise<boolean> { return this.files.has(path); }
    async mkdir(): Promise<void> {}
    async remove(path: string): Promise<void> { this.files.delete(path); }
    async rename(from: string, to: string): Promise<void> {
        const value = this.files.get(from);
        if (value === undefined) throw new Error("missing");
        this.files.set(to, value);
        this.files.delete(from);
    }
}

async function run(): Promise<void> {
    const adapter = new MemoryAdapter();
    const app = { vault: { adapter } } as any;
    const first = new ObsetyncSyncBase(app);
    await first.load();
    first.setEntry("done.md", "a".repeat(64), 10, 4, 9);
    await first.checkpoint();
    check(adapter.appendCalls === 1, "batch checkpoint rewrote the full snapshot");

    // Simulate a hard process restart before save() compacts the snapshot.
    adapter.files.set(
        ".obsidian/plugins/obsetync/sync-base.wal.ndjson",
        adapter.files.get(".obsidian/plugins/obsetync/sync-base.wal.ndjson") + '{"op":"set"',
    );
    const recovered = new ObsetyncSyncBase(app);
    await recovered.load();
    check(recovered.getHash("done.md") === "a".repeat(64), "WAL checkpoint was not recovered");
    check(recovered.getTreeMtime("done.md") === 9, "server tree mtime was lost in WAL");

    recovered.setEntry("later.md", "b".repeat(64), 20, 5);
    await recovered.save();
    check(recovered.entryCount() === 2, "snapshot compaction lost an entry");
    check(
        adapter.files.get(".obsidian/plugins/obsetync/sync-base.wal.ndjson") === "",
        "snapshot compaction did not clear the WAL",
    );

    const compacted = new ObsetyncSyncBase(app);
    await compacted.load();
    check(compacted.getHash("done.md") === "a".repeat(64), "compacted snapshot lost old state");
    check(compacted.getHash("later.md") === "b".repeat(64), "compacted snapshot lost new state");

    const raceAdapter = new MemoryAdapter();
    const racing = new ObsetyncSyncBase({ vault: { adapter: raceAdapter } } as any);
    await racing.load();
    racing.setEntry("first.md", "c".repeat(64), 30, 6);
    raceAdapter.duringNextAppend = () => {
        racing.setEntry("during-append.md", "d".repeat(64), 31, 7);
    };
    await racing.checkpoint();
    const raceRecovered = new ObsetyncSyncBase({ vault: { adapter: raceAdapter } } as any);
    await raceRecovered.load();
    check(
        raceRecovered.getHash("during-append.md") === "d".repeat(64),
        "mutation arriving during final WAL append remained memory-only",
    );
}

void run()
    .then(() => console.log(`sync-base.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
