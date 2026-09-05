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
    check(
        recovered.refreshLocalMetadata("done.md", 50, 4),
        "verified local metadata refresh was ignored",
    );
    check(recovered.getEntry("done.md")?.mtime === 50, "local mtime was not refreshed");
    check(
        recovered.getTreeMtime("done.md") === 9,
        "local metadata refresh changed the committed tree mtime",
    );
    check(
        !recovered.refreshLocalMetadata("done.md", 50, 4),
        "identical local metadata created a redundant mutation",
    );
    const approval = recovered.approveBulkChange("vault", 10_000, 58, 1234);
    check(approval.changeLimit === 10_100, "bulk approval growth allowance is not bounded");
    check(
        recovered.bulkChangeApprovalCovers("vault", 10_100, 58),
        "persisted approval did not cover its bounded restart workload",
    );
    check(
        !recovered.bulkChangeApprovalCovers("vault", 10_101, 58),
        "persisted approval covered an oversized workload",
    );
    check(
        !recovered.bulkChangeApprovalCovers("vault", 10_000, 59),
        "persisted approval covered additional tracked deletions",
    );
    check(
        !recovered.bulkChangeApprovalCovers("another-vault", 10_000, 58),
        "persisted approval escaped its vault",
    );
    recovered.setDiffPageCheckpoint({
        version: 1,
        vaultId: "vault",
        fromRoot: "1".repeat(64),
        toRoot: "2".repeat(64),
        nextCursorHex: "abcd",
        complete: false,
        recordsSeen: 10,
        filesApplied: 9,
        bytesTotal: 123,
        downloaded: 4,
        bytesDownloaded: 88,
        deltasHadMtime: true,
    });
    await recovered.checkpoint();

    const cursorRecovered = new ObsetyncSyncBase(app);
    await cursorRecovered.load();
    check(cursorRecovered.diffPageCheckpoint?.nextCursorHex === "abcd",
        "diff page cursor was not recovered from WAL");
    check(cursorRecovered.diffPageCheckpoint?.filesApplied === 9,
        "diff page aggregate progress was lost");
    check(cursorRecovered.getEntry("done.md")?.mtime === 50,
        "refreshed local metadata was not recovered from WAL");
    check(cursorRecovered.getTreeMtime("done.md") === 9,
        "recovered metadata refresh lost the committed tree mtime");
    check(cursorRecovered.bulkChangeApproval?.approvedAt === 1234,
        "bulk approval was not recovered from WAL");
    check(cursorRecovered.clearDiffPageCheckpoint(), "diff page cursor did not clear");
    check(cursorRecovered.clearBulkChangeApproval(), "bulk approval did not clear");
    await cursorRecovered.checkpoint();
    const cursorCleared = new ObsetyncSyncBase(app);
    await cursorCleared.load();
    check(cursorCleared.diffPageCheckpoint === null, "cleared diff cursor resurrected after restart");
    check(cursorCleared.bulkChangeApproval === null, "cleared bulk approval resurrected after restart");

    cursorCleared.setEntry("later.md", "b".repeat(64), 20, 5);
    await cursorCleared.save();
    check(cursorCleared.entryCount() === 2, "snapshot compaction lost an entry");
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
