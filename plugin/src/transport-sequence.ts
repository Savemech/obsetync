/** Persistence boundary for transport-v2 outgoing sequence reservations. */
export interface SequencePersistence {
    readReservedThrough(): number;
    persistReservedThrough(value: number): Promise<void>;
}

/**
 * Concurrent, crash-safe sequence allocator.
 *
 * A complete block is persisted before its first value can leave this class.
 * Crashes therefore waste numbers but never reuse them. Only allocation is
 * serialized; callers can still pipeline the network requests themselves.
 */
export class DurableSequenceAllocator {
    private nextValue = 1;
    private reservedThrough = 0;
    private lockTail: Promise<void> = Promise.resolve();

    constructor(
        private readonly persistence: SequencePersistence,
        private readonly blockSize = 4096,
    ) {
        if (!Number.isSafeInteger(blockSize) || blockSize <= 0) {
            throw new Error("transport-v2 sequence block size must be a positive safe integer");
        }
    }

    next(): Promise<number> {
        return this.exclusive(async () => {
            if (this.nextValue <= this.reservedThrough) return this.nextValue++;

            const durable = this.validHighWater(this.persistence.readReservedThrough());
            const previousEnd = Math.max(durable, this.reservedThrough);
            const newEnd = this.checkedAdd(previousEnd, this.blockSize, "space exhausted");

            // Persist BEFORE publishing any value from the block. A crash at
            // any later point leaves only a harmless gap.
            await this.persistence.persistReservedThrough(newEnd);
            this.nextValue = previousEnd + 1;
            this.reservedThrough = newEnd;
            return this.nextValue++;
        });
    }

    /** Jump past the server's durable ceiling after a replay response. */
    recover(serverReservedThrough: number): Promise<void> {
        return this.exclusive(async () => {
            const server = this.validHighWater(serverReservedThrough);
            const durable = this.validHighWater(this.persistence.readReservedThrough());
            const previousEnd = Math.max(server, durable, this.reservedThrough);
            const newEnd = this.checkedAdd(previousEnd, this.blockSize, "recovery overflow");
            await this.persistence.persistReservedThrough(newEnd);
            this.nextValue = previousEnd + 1;
            this.reservedThrough = newEnd;
        });
    }

    private validHighWater(value: number): number {
        if (!Number.isSafeInteger(value) || value < 0) {
            throw new Error("transport-v2 sequence state is invalid; re-enroll device");
        }
        return value;
    }

    private checkedAdd(value: number, increment: number, reason: string): number {
        const result = value + increment;
        if (!Number.isSafeInteger(result)) {
            throw new Error(`transport-v2 sequence ${reason}; re-enroll device`);
        }
        return result;
    }

    private async exclusive<T>(work: () => Promise<T>): Promise<T> {
        const predecessor = this.lockTail;
        let release!: () => void;
        this.lockTail = new Promise<void>((resolve) => {
            release = resolve;
        });
        await predecessor;
        try {
            return await work();
        } finally {
            release();
        }
    }
}
