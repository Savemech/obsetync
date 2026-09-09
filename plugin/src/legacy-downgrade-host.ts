export type LegacyDowngradeHostState =
    | "none"
    | "interrupted"
    | "active-awaiting-import"
    | "quiescing"
    | "activating"
    | "legacy-handoff"
    | "import-authorized"
    | "current-active"
    | "retired";

export type LegacyDowngradeHostAction = "init" | "settings" | "sync";
export type LegacyDowngradeOperation = "prepare" | "resume" | "import";

export class LegacyDowngradeHostError extends Error {
    readonly name = "LegacyDowngradeHostError";
    constructor(readonly code: "BUSY" | "STATE" | "STALE" | "RETIRED", message: string) {
        super(message);
    }
}

export interface LegacyDowngradeGenerationToken {
    readonly serial: number;
    readonly action: LegacyDowngradeHostAction;
    assertCurrent(): void;
}

/** Represents a host whose sync/listener/settings admission is already
 * stopped and whose admitted async tails have drained. Retirement is cleanup,
 * never permission to restart the stopped bundle. */
export interface LegacyDowngradeStoppedHost {
    assertStopped(): void;
    retire(): Promise<void>;
}

export interface LegacyDowngradeOperationContext {
    readonly signal: AbortSignal;
    readonly generation: number;
    assertCurrent(): void;
}

export interface LegacyDowngradeActivationPorts {
    quiesce(context: LegacyDowngradeOperationContext): Promise<LegacyDowngradeStoppedHost>;
    activate(context: LegacyDowngradeOperationContext): Promise<void>;
    handoff(context: LegacyDowngradeOperationContext): Promise<void>;
}

export interface LegacyDowngradeImportPorts {
    quiesce(context: LegacyDowngradeOperationContext): Promise<LegacyDowngradeStoppedHost>;
    importLegacy(context: LegacyDowngradeOperationContext): Promise<void>;
}

export interface LegacyDowngradeHostSnapshot {
    state: LegacyDowngradeHostState;
    generation: number;
    operation: LegacyDowngradeOperation | null;
    stopped: boolean;
    unloaded: boolean;
}

type OperationOwner = { kind: LegacyDowngradeOperation; abort: AbortController; promise: Promise<void> };
type StableInitialState = "none" | "interrupted" | "active-awaiting-import" | "import-authorized" | "current-active";

function allowsCurrentHostWork(state: LegacyDowngradeHostState): boolean {
    return state === "none" || state === "current-active";
}

function fail(code: LegacyDowngradeHostError["code"], message: string): never {
    throw new LegacyDowngradeHostError(code, message);
}

/** Pure host coordinator. It owns only ordering/generation/lifetime; archive
 * projection and import remain in the durable downgrade module. There is no
 * reset/delete transition by design. */
export class LegacyDowngradeHostCoordinator {
    private state: LegacyDowngradeHostState;
    private generation = 1;
    private generationIdentity: object = Object.freeze({});
    private operation: OperationOwner | null = null;
    private stoppedHost: LegacyDowngradeStoppedHost | null = null;
    private unloaded = false;
    private retirement: Promise<void> | null = null;

    constructor(initial: StableInitialState = "none") { this.state = initial; }

    snapshot(): LegacyDowngradeHostSnapshot {
        return { state: this.state, generation: this.generation,
            operation: this.operation?.kind ?? null, stopped: this.stoppedHost !== null, unloaded: this.unloaded };
    }

    /** Synchronous pre-await fence for init/settings/sync entry points. */
    authorize(action: LegacyDowngradeHostAction): LegacyDowngradeGenerationToken {
        if (this.unloaded || this.state === "retired") fail("RETIRED", "legacy downgrade host is retired");
        if (!allowsCurrentHostWork(this.state)) {
            fail("STATE", `host ${action} is blocked by legacy downgrade state ${this.state}`);
        }
        const identity = this.generationIdentity, serial = this.generation;
        return { action, serial, assertCurrent: () => {
            if (this.unloaded || this.state === "retired") fail("RETIRED", "legacy downgrade host is retired");
            if (this.generationIdentity !== identity || this.generation !== serial || !allowsCurrentHostWork(this.state)) {
                fail("STALE", `stale host ${action} generation`);
            }
        } };
    }

    prepare(ports: LegacyDowngradeActivationPorts): Promise<void> {
        if (this.state !== "none") return this.rejectStart("prepare", "none");
        return this.startActivation("prepare", ports);
    }

    resume(ports: LegacyDowngradeActivationPorts): Promise<void> {
        if (this.state !== "interrupted") return this.rejectStart("resume", "interrupted");
        return this.startActivation("resume", ports);
    }

    importLegacy(ports: LegacyDowngradeImportPorts): Promise<void> {
        const recoveryState = this.state;
        if (recoveryState !== "active-awaiting-import" && recoveryState !== "import-authorized") {
            return this.rejectStart("import", "active-awaiting-import");
        }
        return this.start("import", async context => {
            context.assertCurrent();
            await this.ensureStopped(ports.quiesce, context);
            context.assertCurrent();
            this.state = "import-authorized";
            await ports.importLegacy(context);
            context.assertCurrent();
            const stopped = this.stoppedHost;
            await stopped?.retire();
            if (this.stoppedHost === stopped) this.stoppedHost = null;
            context.assertCurrent();
            this.state = "current-active";
            this.bumpGeneration();
        }, recoveryState);
    }

    /** Revoke synchronously, then join the exact in-flight operation and its
     * stopped-host retirement. Repeated unloads share one finite tail. */
    unload(): Promise<void> {
        if (this.retirement) return this.retirement;
        if (!this.unloaded) {
            this.unloaded = true;
            this.state = "retired";
            this.bumpGeneration();
        }
        const operation = this.operation;
        operation?.abort.abort(new LegacyDowngradeHostError("RETIRED", "legacy downgrade host unloaded"));
        const retirement = (async () => {
            await operation?.promise.catch(() => {});
            const stopped = this.stoppedHost;
            await stopped?.retire();
            if (this.stoppedHost === stopped) this.stoppedHost = null;
        })();
        this.retirement = retirement;
        void retirement.catch(() => {
            if (this.retirement === retirement) this.retirement = null;
        });
        return retirement;
    }

    private startActivation(kind: "prepare" | "resume", ports: LegacyDowngradeActivationPorts): Promise<void> {
        return this.start(kind, async context => {
            context.assertCurrent();
            await this.ensureStopped(ports.quiesce, context);
            context.assertCurrent();
            this.state = "activating";
            await ports.activate(context);
            context.assertCurrent();
            this.stoppedHost!.assertStopped();
            this.state = "legacy-handoff";
            await ports.handoff(context);
            context.assertCurrent();
            this.stoppedHost!.assertStopped();
            // Deliberately no host restart: the current bundle stays stopped
            // while the operator installs/runs 1.11.3.
            this.state = "active-awaiting-import";
        }, kind === "prepare" ? "none" : "interrupted");
    }

    private start(kind: LegacyDowngradeOperation,
        body: (context: LegacyDowngradeOperationContext) => Promise<void>, failureState: StableInitialState): Promise<void> {
        if (this.unloaded || this.state === "retired") fail("RETIRED", "legacy downgrade host is retired");
        if (this.operation) fail("BUSY", `legacy downgrade ${this.operation.kind} is already running`);
        this.bumpGeneration();
        this.state = "quiescing";
        const identity = this.generationIdentity, generation = this.generation, abort = new AbortController();
        let owner!: OperationOwner;
        const context: LegacyDowngradeOperationContext = { signal: abort.signal, generation,
            assertCurrent: () => {
                if (this.unloaded || this.state === "retired") fail("RETIRED", "legacy downgrade host is retired");
                if (this.generationIdentity !== identity || this.generation !== generation || this.operation !== owner) {
                    fail("STALE", "legacy downgrade operation generation changed");
                }
                if (abort.signal.aborted) throw abort.signal.reason;
            } };
        const promise = Promise.resolve().then(() => body(context)).catch(async error => {
            if (!this.unloaded && this.operation === owner) {
                this.state = failureState === "none" && this.stoppedHost !== null
                    ? "interrupted"
                    : failureState;
            }
            throw error;
        }).finally(() => { if (this.operation === owner) this.operation = null; });
        owner = { kind, abort, promise };
        this.operation = owner;
        return promise;
    }

    private async ensureStopped(quiesce: LegacyDowngradeActivationPorts["quiesce"],
        context: LegacyDowngradeOperationContext): Promise<void> {
        if (!this.stoppedHost) {
            const stopped = await quiesce(context);
            try {
                stopped.assertStopped();
                context.assertCurrent();
                this.stoppedHost = stopped;
            } catch (error) {
                try { await stopped.retire(); }
                catch (retirementError) {
                    // A failed retirement is still an owned stopped host. Keep
                    // it reachable so unload/recovery can retry its exact tail.
                    this.stoppedHost = stopped;
                    throw retirementError;
                }
                throw error;
            }
        } else {
            this.stoppedHost.assertStopped();
            context.assertCurrent();
        }
    }

    private rejectStart(kind: LegacyDowngradeOperation, expected: StableInitialState): Promise<void> {
        if (this.unloaded || this.state === "retired") fail("RETIRED", "legacy downgrade host is retired");
        if (this.operation) fail("BUSY", `legacy downgrade ${this.operation.kind} is already running`);
        fail("STATE", `${kind} requires ${expected}, found ${this.state}`);
    }

    private bumpGeneration(): void {
        if (this.generation >= Number.MAX_SAFE_INTEGER) fail("RETIRED", "legacy downgrade generation exhausted");
        this.generation++;
        this.generationIdentity = Object.freeze({});
    }
}
