/** One disk mutation the pull pipeline is expected to cause. */
export interface PullWriteExpectation {
    path: string;
    action: "upsert" | "delete";
    /** Required for upserts. The vault event is suppressed only when the
     *  bytes currently on disk hash to this exact value. */
    hash?: string;
}

interface TrackedExpectation extends PullWriteExpectation {
    expiresAt: number;
}

/**
 * Matches Obsidian vault events against writes made by the pull pipeline.
 *
 * A path-only set is unsafe: a real user edit can land after a path was
 * registered but before pull writes it, and would then be discarded as an
 * "echo". Upserts therefore require the on-disk content hash to match the
 * server hash. Expectations survive the end of the pull briefly because
 * Obsidian may deliver adapter events asynchronously after writeBinary()
 * resolves.
 */
export class PullEchoTracker {
    private expected = new Map<string, TrackedExpectation>();

    constructor(
        private readonly ttlMs = 60_000,
        private readonly now: () => number = () => Date.now(),
    ) {}

    register(writes: PullWriteExpectation[]): void {
        this.sweep();
        const expiresAt = this.now() + this.ttlMs;
        for (const write of writes) {
            this.expected.set(write.path, { ...write, expiresAt });
        }
    }

    /** Lets event handlers avoid reading and hashing ordinary user edits.
     *  Hashing is needed only when there is a live pull write to authenticate. */
    expectsUpsert(path: string): boolean {
        return this.live(path)?.action === "upsert";
    }

    expectsRename(oldPath: string, newPath: string): boolean {
        return this.live(oldPath)?.action === "delete" && this.live(newPath)?.action === "upsert";
    }

    /** Consume a create/modify echo only when its bytes are exactly the
     *  bytes pull intended to write. A mismatch is a genuine local edit. */
    consumeUpsert(path: string, actualHash: string): boolean {
        const entry = this.live(path);
        if (!entry) return false;
        this.expected.delete(path);
        return entry.action === "upsert" && entry.hash === actualHash;
    }

    /** A delete has no bytes to authenticate. If pull intended to delete the
     *  same path, suppressing an indistinguishable concurrent user delete is
     *  safe because both operations have the same final state. */
    consumeDelete(path: string): boolean {
        const entry = this.live(path);
        if (!entry) return false;
        this.expected.delete(path);
        return entry.action === "delete";
    }

    /** Obsidian normally reports adapter.rename() as one event. Both halves
     *  must match the intended server rename before the event is suppressed. */
    consumeRename(oldPath: string, newPath: string, actualHash: string): boolean {
        const oldEntry = this.live(oldPath);
        const newEntry = this.live(newPath);
        this.expected.delete(oldPath);
        this.expected.delete(newPath);
        return (
            oldEntry?.action === "delete" &&
            newEntry?.action === "upsert" &&
            newEntry.hash === actualHash
        );
    }

    clear(): void {
        this.expected.clear();
    }

    get size(): number {
        this.sweep();
        return this.expected.size;
    }

    private live(path: string): TrackedExpectation | null {
        const entry = this.expected.get(path);
        if (!entry) return null;
        if (entry.expiresAt <= this.now()) {
            this.expected.delete(path);
            return null;
        }
        return entry;
    }

    private sweep(): void {
        const now = this.now();
        for (const [path, entry] of this.expected) {
            if (entry.expiresAt <= now) this.expected.delete(path);
        }
    }
}
