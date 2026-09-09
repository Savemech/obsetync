export interface LocalEventGuard {
    /** False after any newer callback for the same path began. */
    isCurrent(): boolean;
    release(): void;
}

/** One counter/latest token per active path, not a retained list of events.
 * A burst beyond the path cap temporarily protects ALL paths from pull until
 * overflow callbacks settle; bounded bookkeeping must never silently discard
 * overwrite protection. Superseded callbacks also lose their hash/echo claim. */
export class LocalEventGuards {
    private readonly paths = new Map<string, { count: number; latest: symbol }>();
    private overflow = 0;

    constructor(private readonly maxPaths = 4096) {
        if (!Number.isSafeInteger(maxPaths) || maxPaths <= 0) {
            throw new RangeError("event guard path limit must be a positive safe integer");
        }
    }

    acquire(path: string): LocalEventGuard {
        const generation = Symbol();
        let record = this.paths.get(path);
        const overflow = !record && this.paths.size >= this.maxPaths;
        if (overflow) this.overflow++;
        else {
            if (!record) { record = { count: 0, latest: generation }; this.paths.set(path, record); }
            record.count++;
            record.latest = generation;
        }
        let released = false;
        return {
            isCurrent: () => !released && !overflow && this.overflow === 0 &&
                this.paths.get(path) === record && record?.latest === generation,
            release: () => {
                if (released) return;
                released = true;
                if (overflow) this.overflow--;
                else if (record && --record.count === 0) this.paths.delete(path);
            },
        };
    }

    has(path: string): boolean { return this.overflow > 0 || this.paths.has(path); }
    /** Nonzero also represents fail-closed global overflow protection. */
    get size(): number { return this.paths.size + (this.overflow > 0 ? 1 : 0); }
}
