export class EngineWorkClosedError extends Error {
    readonly name = "EngineWorkClosedError";
    readonly code = "ENGINE_WORK_CLOSED";
    constructor() { super("Engine work admission is closed"); }
}

export interface EngineWorkSnapshot {
    closed: boolean;
    active: number;
    drained: boolean;
}

/** One engine lifetime's work owners, not caller-facing promise completion.
 *
 * Register synchronously BEFORE starting work; release in its real finally,
 * after all admitted IO and cleanup have settled. In particular, releasing
 * when a timeout, abort notification, or external Promise.race settles can
 * falsely license a replacement engine while the old owner is still writing.
 *
 * This tracker never adopts a promise or invents completion. Cancellation is
 * separate: close forbids new owners but cannot stop or release existing ones.
 * Construct a new tracker for a new engine; a closed lifetime never reopens. */
export class EngineWorkTracker {
    private closed = false;
    private active = 0;
    private drainPromise: Promise<void> | null = null;
    private resolveDrain: (() => void) | null = null;

    enter(): () => void {
        if (this.closed) throw new EngineWorkClosedError();
        this.active++;
        let released = false;
        return () => {
            if (released) return;
            released = true;
            this.active--;
            this.completeIfDrained();
        };
    }

    /** Deliberately not async: repeated calls return the SAME promise, before
     * and after settlement. Existing owners alone determine its completion. */
    closeAndDrain(): Promise<void> {
        if (this.drainPromise) return this.drainPromise;
        this.closed = true;
        this.drainPromise = new Promise<void>(resolve => { this.resolveDrain = resolve; });
        this.completeIfDrained();
        return this.drainPromise;
    }

    snapshot(): EngineWorkSnapshot {
        return { closed: this.closed, active: this.active, drained: this.closed && this.active === 0 };
    }

    private completeIfDrained(): void {
        if (!this.closed || this.active !== 0 || !this.resolveDrain) return;
        const resolve = this.resolveDrain;
        this.resolveDrain = null;
        resolve();
    }
}
