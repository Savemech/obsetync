export class SettingsWriterClosedError extends Error {
    readonly name = "SettingsWriterClosedError";
    readonly code = "SETTINGS_WRITER_CLOSED";
    constructor() { super("Settings write admission is closed"); }
}

export interface SettingsPersistenceSnapshot {
    closed: boolean;
    active: boolean;
    pending: boolean;
    drained: boolean;
}

interface Publication<T> {
    value: T;
    promise: Promise<void>;
    resolve(): void;
    reject(error: unknown): void;
}

function copyJSON<T>(value: T): T {
    const encoded = JSON.stringify(value);
    if (encoded === undefined) throw new TypeError("Settings snapshot is not JSON serializable");
    return JSON.parse(encoded) as T;
}

/** Serial actual-native publication, retaining at most one active snapshot
 * and one replaceable latest pending snapshot. Pending callers share ONE
 * promise for that latest publication; there is no growing waiter list.
 *
 * The default snapshot has JSON persistence semantics (including omission of
 * undefined object fields). A custom synchronous copy must return a detached
 * value, with no retained caller-owned mutable objects. Neither path logs data.
 * The bound is retained snapshot COUNT, not bytes, JS heap or native RSS:
 * caller input, transient JSON encoding/copying and the adapter are separate.
 *
 * write() must settle only after its actual native operation. Returning an
 * abort/timeout race instead would surrender serialization while old IO is
 * still writing. Drain waits actual settlement, including failed writes;
 * each save promise separately communicates its publication success/failure. */
export class SerialSettingsWriter<T> {
    private closed = false;
    private active: Publication<T> | null = null;
    private pending: Publication<T> | null = null;
    private drainPromise: Promise<void> | null = null;
    private resolveDrain: (() => void) | null = null;

    constructor(
        private readonly write: (snapshot: T) => Promise<void>,
        private readonly copy: (value: T) => T = copyJSON,
    ) {}

    /** Capture before returning; later caller mutation cannot alter a queued
     * snapshot. Not async, to preserve coalesced callers' promise identity. */
    save(value: T): Promise<void> {
        if (this.closed) return Promise.reject(new SettingsWriterClosedError());
        let captured: T;
        try { captured = this.copy(value); }
        catch (error) { return Promise.reject(error); }
        // A host/custom snapshot hook may synchronously trigger unload.
        if (this.closed) return Promise.reject(new SettingsWriterClosedError());
        if (this.pending) {
            this.pending.value = captured;
            return this.pending.promise;
        }
        let resolve!: () => void;
        let reject!: (error: unknown) => void;
        const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
        const publication: Publication<T> = { value: captured, promise, resolve, reject };
        if (this.active) this.pending = publication;
        else this.start(publication);
        return promise;
    }

    /** Close admission synchronously; already-admitted pending state still
     * publishes after the active write. Repeated calls share a stable promise. */
    closeAndDrain(): Promise<void> {
        if (this.drainPromise) return this.drainPromise;
        this.closed = true;
        this.drainPromise = new Promise<void>(resolve => { this.resolveDrain = resolve; });
        this.completeDrainIfReady();
        return this.drainPromise;
    }

    snapshot(): SettingsPersistenceSnapshot {
        return { closed: this.closed, active: this.active !== null, pending: this.pending !== null,
            drained: this.closed && this.active === null && this.pending === null };
    }

    private start(publication: Publication<T>): void {
        // Publish ownership before invoking the adapter: synchronous callbacks
        // may enqueue another save or close admission reentrantly.
        this.active = publication;
        let native: Promise<void>;
        try { native = this.write(publication.value); }
        catch (error) { native = Promise.reject(error); }
        void Promise.resolve(native).then(
            () => { publication.resolve(); this.finished(); },
            error => { publication.reject(error); this.finished(); },
        );
    }

    private finished(): void {
        this.active = null;
        const next = this.pending;
        this.pending = null;
        if (next) this.start(next);
        else this.completeDrainIfReady();
    }

    private completeDrainIfReady(): void {
        if (!this.closed || this.active || this.pending || !this.resolveDrain) return;
        const resolve = this.resolveDrain;
        this.resolveDrain = null;
        resolve();
    }
}
