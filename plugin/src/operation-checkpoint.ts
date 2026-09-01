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

/**
 * Tiny durable breadcrumb for renderer-level terminations (notably iOS
 * Jetsam). JavaScript gets no exception when the OS kills the process, so an
 * active marker is the only reliable way to learn which phase was running.
 */
export class OperationCheckpoint {
    private current: OperationRecord | null = null;
    private lastInterruption: OperationRecord | null = null;
    private writeChain: Promise<void> = Promise.resolve();
    private lastProgressWrite = 0;
    private serial = 0;

    constructor(
        private readonly io: OperationCheckpointIO,
        private readonly pluginVersion: string,
        private readonly now: () => number = () => Date.now(),
    ) {}

    /** Promote an orphaned active marker to durable postmortem evidence. */
    async initialize(): Promise<OperationRecord | null> {
        const active = await this.readRecord(ACTIVE_PATH);
        if (active) {
            this.lastInterruption = active;
            await this.safeWrite(LAST_INTERRUPTION_PATH, active);
            await this.safeDelete(ACTIVE_PATH);
            console.warn(
                `[obsetync] previous renderer stopped during ${active.phase}: ${active.detail}`,
            );
            return { ...active };
        }
        this.lastInterruption = await this.readRecord(LAST_INTERRUPTION_PATH);
        return null;
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

    /** Rate-limited progress update; intentionally fire-and-forget safe. */
    progress(operationId: string, detail: string): void {
        if (this.current?.operationId !== operationId) return;
        const timestamp = this.now();
        this.current.detail = clean(detail);
        this.current.updatedAt = timestamp;
        if (timestamp - this.lastProgressWrite < PROGRESS_WRITE_INTERVAL_MS) return;
        this.lastProgressWrite = timestamp;
        void this.safeWrite(ACTIVE_PATH, { ...this.current });
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
        const bytes = new TextEncoder().encode(JSON.stringify(record));
        return this.enqueue(async () => {
            try {
                await this.io.writeFile(path, bytes);
            } catch {
                // Diagnostics must never make sync fail.
            }
        });
    }

    private safeDelete(path: string): Promise<void> {
        return this.enqueue(async () => {
            try {
                await this.io.deleteFile(path);
            } catch {
                // Missing/unavailable diagnostics are non-fatal.
            }
        });
    }

    private enqueue(work: () => Promise<void>): Promise<void> {
        const result = this.writeChain.then(work, work);
        this.writeChain = result.catch(() => {});
        return result;
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
