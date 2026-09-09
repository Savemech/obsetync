const ACTIVE_PATH = ".obsidian/plugins/obsetync/operation.active.json";
const LAST_INTERRUPTION_PATH = ".obsidian/plugins/obsetync/last-interruption.json";
const MAX_DETAIL_CHARS = 500;
const PROGRESS_WRITE_INTERVAL_MS = 3000;

export interface OperationCheckpointIO {
    readFile(path: string): Promise<Uint8Array>;
    writeFile(path: string, data: Uint8Array): Promise<void>;
    deleteFile(path: string): Promise<void>;
}

export interface OperationRecord {
    schema: 1;
    operationId: string;
    phase: string;
    pluginVersion: string;
    startedAt: number;
    updatedAt: number;
    detail: string;
    failed?: boolean;
}

interface CheckpointBarrier {
    work: () => Promise<void>;
    resolve: () => void;
    next: CheckpointBarrier | null;
}

/**
 * Tiny durable breadcrumb for renderer-level terminations (notably iOS
 * Jetsam). JavaScript gets no exception when the OS kills the process, so an
 * active marker is the only reliable way to learn which phase was running.
 */
export class OperationCheckpoint {
    private current: OperationRecord | null = null;
    private lastInterruption: OperationRecord | null = null;
    private draining = false;
    private barrierHead: CheckpointBarrier | null = null;
    private barrierTail: CheckpointBarrier | null = null;
    private pendingProgress: OperationRecord | null = null;
    private lastProgressWrite = 0;
    private serial = 0;

    constructor(
        private readonly io: OperationCheckpointIO,
        private readonly pluginVersion: string,
        private readonly now: () => number = () => Date.now(),
    ) {}

    /** Promote an orphaned active marker to durable postmortem evidence. */
    async initialize(): Promise<OperationRecord | null> {
        let orphan: OperationRecord | null = null;
        await this.enqueue(async () => {
            const active = await this.readRecord(ACTIVE_PATH);
            if (active) {
                this.lastInterruption = active;
                await this.writeRecord(LAST_INTERRUPTION_PATH, active);
                await this.deleteRecord(ACTIVE_PATH);
                console.warn(
                    `[obsetync] previous renderer stopped during ${active.phase}: ${active.detail}`,
                );
                orphan = { ...active };
            } else {
                this.lastInterruption = await this.readRecord(LAST_INTERRUPTION_PATH);
            }
        });
        return orphan;
    }

    async begin(phase: string, detail = "started"): Promise<string> {
        const timestamp = this.now();
        const operationId = `${timestamp.toString(36)}-${++this.serial}`;
        this.current = {
            schema: 1,
            operationId,
            phase: clean(phase),
            pluginVersion: this.pluginVersion,
            startedAt: timestamp,
            updatedAt: timestamp,
            detail: clean(detail),
        };
        this.lastProgressWrite = timestamp;
        await this.safeWrite(ACTIVE_PATH, this.current);
        return operationId;
    }

    /** Rate-limited progress update; intentionally fire-and-forget safe.
     * A stalled adapter retains only the latest pending progress, not a promise
     * and encoded record for every interval. Lifecycle barriers supersede it. */
    progress(operationId: string, detail: string): void {
        if (this.current?.operationId !== operationId || this.current.failed) return;
        const timestamp = this.now();
        this.current.detail = clean(detail);
        this.current.updatedAt = timestamp;
        if (this.pendingProgress) {
            this.pendingProgress = { ...this.current };
            this.lastProgressWrite = timestamp;
            return;
        }
        if (timestamp - this.lastProgressWrite < PROGRESS_WRITE_INTERVAL_MS) return;
        this.lastProgressWrite = timestamp;
        this.pendingProgress = { ...this.current };
        this.startDrain();
    }

    /** Leave the marker behind as useful evidence for a caught failure. */
    async fail(operationId: string, error: unknown): Promise<void> {
        if (this.current?.operationId !== operationId) return;
        this.current.failed = true;
        this.current.updatedAt = this.now();
        this.current.detail = clean(
            `${this.current.detail}; error=${String((error as any)?.message ?? error)}`,
        );
        await this.safeWrite(ACTIVE_PATH, { ...this.current });
    }

    async complete(operationId: string): Promise<void> {
        if (this.current?.operationId !== operationId) return;
        this.current = null;
        await this.safeDelete(ACTIVE_PATH);
    }

    getLastInterruption(): OperationRecord | null {
        return this.lastInterruption ? { ...this.lastInterruption } : null;
    }

    private async readRecord(path: string): Promise<OperationRecord | null> {
        try {
            const bytes = await this.io.readFile(path);
            const value = JSON.parse(new TextDecoder().decode(bytes));
            if (
                value?.schema !== 1 ||
                typeof value.operationId !== "string" ||
                typeof value.phase !== "string" ||
                typeof value.startedAt !== "number" ||
                typeof value.updatedAt !== "number" ||
                typeof value.detail !== "string"
            ) {
                return null;
            }
            return value as OperationRecord;
        } catch {
            return null;
        }
    }

    private safeWrite(path: string, record: OperationRecord): Promise<void> {
        // current is mutable, whereas a queued begin/fail must preserve the
        // exact barrier snapshot even if another operation starts meanwhile.
        const snapshot = { ...record };
        return this.enqueue(() => this.writeRecord(path, snapshot));
    }

    private safeDelete(path: string): Promise<void> {
        return this.enqueue(() => this.deleteRecord(path));
    }

    private enqueue(work: () => Promise<void>): Promise<void> {
        // Progress is best-effort; a begin/fail carries its newer snapshot and
        // a complete deletes it. Never let an obsolete pending write run after
        // one of these barriers and resurrect or overwrite an operation.
        this.pendingProgress = null;
        const result = new Promise<void>((resolve) => {
            const barrier: CheckpointBarrier = { work, resolve, next: null };
            if (this.barrierTail) this.barrierTail.next = barrier;
            else this.barrierHead = barrier;
            this.barrierTail = barrier;
        });
        this.startDrain();
        return result;
    }

    private async writeRecord(path: string, record: OperationRecord): Promise<void> {
        try {
            const bytes = new TextEncoder().encode(JSON.stringify(record));
            await this.io.writeFile(path, bytes);
        } catch {
            // Diagnostics must never make sync fail.
        }
    }

    private async deleteRecord(path: string): Promise<void> {
        try {
            await this.io.deleteFile(path);
        } catch {
            // Missing/unavailable diagnostics are non-fatal.
        }
    }

    private startDrain(): void {
        if (this.draining) return;
        this.draining = true;
        void this.drain();
    }

    /** Only explicit awaited lifecycle calls occupy FIFO barriers. Progress
     * has one replaceable slot across the entire queue, and at most one native
     * mutation runs at a time. No history of superseded updates is retained. */
    private async drain(): Promise<void> {
        try {
            while (this.barrierHead || this.pendingProgress) {
                const barrier = this.barrierHead;
                if (barrier) {
                    this.barrierHead = barrier.next;
                    if (!this.barrierHead) this.barrierTail = null;
                    try {
                        await barrier.work();
                    } catch {
                        // Also cover synchronous adapter errors: every waiting
                        // lifecycle caller must settle and later work must run.
                    } finally {
                        barrier.resolve();
                    }
                    continue;
                }
                const progress = this.pendingProgress!;
                this.pendingProgress = null;
                await this.writeRecord(ACTIVE_PATH, progress);
            }
        } finally {
            this.draining = false;
        }
    }
}

function clean(value: string): string {
    const oneLine = String(value).replace(/[\r\n]+/g, " ").trim();
    return oneLine.slice(0, MAX_DETAIL_CHARS);
}

export const OPERATION_CHECKPOINT_PATHS = {
    active: ACTIVE_PATH,
    lastInterruption: LAST_INTERRUPTION_PATH,
} as const;
