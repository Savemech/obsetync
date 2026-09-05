import { strict as assert } from "node:assert";
import type { PerfOperation } from "./perf-trace";
import type { PlatformIO } from "./platform";
import { chunkFileStreaming, hashFileStreaming, streamingHash, type WasmModule } from "./push";
import { WorkScheduler, type WorkSchedulerRuntime } from "./work-scheduler";

class FakeHost implements WorkSchedulerRuntime {
    private nextId = 0;
    readonly tasks = new Map<number, () => void>();
    immediate?: WorkSchedulerRuntime["immediate"];
    messageChannels = 0;
    closedPorts = 0;
    referencedPorts = 0;
    unreferencedPorts = 0;
    timerSchedules = 0;
    failMessageSend = false;
    failTimer = false;

    constructor(immediate = false) {
        if (immediate) {
            this.immediate = {
                schedule: (callback) => this.enqueue(callback),
                cancel: (handle) => { this.tasks.delete(handle as number); },
            };
        }
    }

    private enqueue(callback: () => void): number {
        const id = ++this.nextId;
        this.tasks.set(id, callback);
        return id;
    }

    createMessageChannel: WorkSchedulerRuntime["createMessageChannel"] = () => {
        this.messageChannels++;
        let closed = false;
        const port1 = {
            onmessage: null as ((event: { data: unknown }) => void) | null,
            postMessage: () => {},
            close: () => { this.closedPorts++; closed = true; },
            ref: () => { this.referencedPorts++; },
            unref: () => { this.unreferencedPorts++; },
        };
        const port2 = {
            onmessage: null,
            postMessage: (message: unknown) => {
                if (this.failMessageSend) throw new Error("message send failed");
                this.enqueue(() => {
                    if (!closed) port1.onmessage?.({ data: message });
                });
            },
            close: () => { this.closedPorts++; closed = true; },
            unref: () => { this.unreferencedPorts++; },
        };
        return { port1, port2 };
    };

    scheduleTimer(callback: () => void): unknown {
        this.timerSchedules++;
        if (this.failTimer) throw new Error("timer failed");
        return this.enqueue(callback);
    }

    cancelTimer(handle: unknown): void {
        this.tasks.delete(handle as number);
    }

    tick(): void {
        const next = this.tasks.entries().next().value as [number, () => void] | undefined;
        assert.ok(next, "expected a pending host task");
        this.tasks.delete(next[0]);
        next[1]();
    }
}

function perfProbe(): { perf: PerfOperation; starts: string[]; ends: string[] } {
    const starts: string[] = [];
    const ends: string[] = [];
    return {
        starts,
        ends,
        perf: {
            phase(phase: string) {
                starts.push(phase);
                return () => ends.push(phase);
            },
        } as unknown as PerfOperation,
    };
}

async function boundedFifoNeedsRealTasks(): Promise<void> {
    const host = new FakeHost(true);
    const scheduler = new WorkScheduler({ runtime: host, maxPendingJobs: 2 });
    const probe = perfProbe();
    const order: number[] = [];
    const first = scheduler.yield({ perf: probe.perf }).then(() => order.push(1));
    const second = scheduler.yield({ perf: probe.perf }).then(() => order.push(2));
    await assert.rejects(scheduler.yield(), /queue is full/);
    await Promise.resolve();
    assert.deepEqual(order, [], "microtasks alone must not resume sync work");
    assert.equal(host.tasks.size, 1, "only one host callback may be pending");
    assert.equal(scheduler.snapshot().backend, "node-immediate");
    host.tick();
    await first;
    assert.deepEqual(order, [1], "one task must not flush the whole queue");
    assert.equal(host.tasks.size, 1);
    host.tick();
    await second;
    assert.deepEqual(order, [1, 2]);
    assert.deepEqual(probe.starts, ["scheduler_wait", "scheduler_wait"]);
    assert.deepEqual(probe.ends, probe.starts);
    assert.equal(host.messageChannels, 0);
    assert.equal(host.timerSchedules, 0);
    assert.equal(scheduler.snapshot().pendingJobs, 0);
    scheduler.dispose();
}

async function abortsAreBoundedAndDoNotBlockLaterJobs(): Promise<void> {
    const host = new FakeHost();
    const scheduler = new WorkScheduler({ runtime: host, maxPendingJobs: 2 });
    const probe = perfProbe();
    const reason = new Error("cancelled by caller");
    const alreadyAborted = new AbortController();
    alreadyAborted.abort(reason);
    await assert.rejects(scheduler.yield({ signal: alreadyAborted.signal }), (error) => error === reason);
    assert.equal(host.tasks.size, 0);
    assert.equal(host.messageChannels, 0);

    for (let index = 0; index < 500; index++) {
        const controller = new AbortController();
        const pending = scheduler.yield({ signal: controller.signal, perf: probe.perf });
        const rejected = assert.rejects(pending, (error) => error === reason);
        controller.abort(reason);
        await rejected;
        assert.equal(scheduler.snapshot().pendingJobs, 0);
        assert.equal(host.tasks.size, 1, "abort churn must not flood native messages");
    }
    const next = scheduler.yield();
    assert.equal(host.referencedPorts, 1, "outstanding work must keep the Node port live");
    host.tick();
    await next;
    assert.equal(host.messageChannels, 1, "reuse one channel across all yields");
    assert.equal(host.unreferencedPorts, 3, "the completed task must unref the receiving port");
    assert.equal(probe.starts.length, 500);
    assert.equal(probe.ends.length, 500);
    assert.equal(host.timerSchedules, 0);
    const again = scheduler.yield();
    host.tick();
    await again;
    assert.equal(host.messageChannels, 1);
    scheduler.dispose();
    assert.equal(host.closedPorts, 2);
}

async function disposingCancelsPendingAndReleasesResources(): Promise<void> {
    for (const immediate of [false, true]) {
        const host = new FakeHost(immediate);
        const scheduler = new WorkScheduler({ runtime: host });
        const controller = new AbortController();
        const probe = perfProbe();
        const pending = scheduler.yield({ signal: controller.signal, perf: probe.perf });
        const rejected = assert.rejects(pending, { name: "AbortError" });
        scheduler.dispose();
        scheduler.dispose();
        await rejected;
        controller.abort();
        assert.equal(probe.ends.length, 1, "cleanup must remove the abort listener");
        assert.equal(scheduler.snapshot().backend, "closed");
        assert.equal(scheduler.snapshot().pendingJobs, 0);
        await assert.rejects(scheduler.yield(), { name: "AbortError" });
        if (immediate) assert.equal(host.tasks.size, 0);
        else {
            assert.equal(host.closedPorts, 2);
            host.tick(); // Already queued browser messages cannot revive disposed work.
            assert.equal(host.tasks.size, 0);
        }
    }
}

async function brokenCapabilitiesFallBackWithoutLeaking(): Promise<void> {
    const host = new FakeHost(true);
    host.immediate!.schedule = () => { throw new Error("Node timer denied"); };
    const scheduler = new WorkScheduler({ runtime: host });
    const pending = scheduler.yield();
    assert.equal(scheduler.snapshot().backend, "message-channel");
    assert.equal(scheduler.snapshot().fallbackCount, 1);
    host.tick();
    await pending;
    scheduler.dispose();

    const brokenMessage = new FakeHost();
    brokenMessage.failMessageSend = true;
    const timerScheduler = new WorkScheduler({ runtime: brokenMessage });
    const timerYield = timerScheduler.yield();
    assert.equal(timerScheduler.snapshot().backend, "timer");
    assert.equal(brokenMessage.closedPorts, 2);
    brokenMessage.tick();
    await timerYield;
    timerScheduler.dispose();

    const brokenEverything = new FakeHost();
    brokenEverything.createMessageChannel = () => { throw new Error("constructor denied"); };
    brokenEverything.failTimer = true;
    const unavailable = new WorkScheduler({ runtime: brokenEverything });
    const probe = perfProbe();
    await assert.rejects(unavailable.yield({ perf: probe.perf }), /timer failed/);
    assert.equal(unavailable.snapshot().pendingJobs, 0);
    assert.equal(unavailable.snapshot().fallbackCount, 2);
    assert.equal(probe.ends.length, 1);
    unavailable.dispose();
}

async function actualNodeYieldIsNotAMicrotask(): Promise<void> {
    const scheduler = new WorkScheduler();
    let resumed = false;
    const pending = scheduler.yield().then(() => { resumed = true; });
    await Promise.resolve();
    assert.equal(resumed, false);
    await pending;
    assert.equal(resumed, true);
    assert.equal(scheduler.snapshot().backend, "node-immediate");
    scheduler.dispose();

    // A real Node MessageChannel must keep this await alive without a timer
    // watchdog, then release the event loop again after its work is done.
    const channelScheduler = new WorkScheduler({ runtime: {
        createMessageChannel: () => new MessageChannel(),
        scheduleTimer: () => { throw new Error("MessageChannel must not need timer fallback"); },
        cancelTimer: () => {},
    } });
    await channelScheduler.yield();
    assert.equal(channelScheduler.snapshot().backend, "message-channel");
    await channelScheduler.yield();
    channelScheduler.dispose();
}

async function cooperativeHashPreservesBytesAndFreesOnAbort(): Promise<void> {
    const bytes = new Uint8Array(512 * 1024 + 13).fill(7);
    let freed = 0;
    let abortOnFeed: AbortController | undefined;
    class Hasher {
        private sum = 0;
        update(data: Uint8Array): void {
            for (const byte of data) this.sum = (this.sum + byte) >>> 0;
            abortOnFeed?.abort();
        }
        finalize(): string { return String(this.sum); }
        free(): void { freed++; }
    }
    const wasm = { Hasher } as unknown as WasmModule;
    const io = {
        getAbsolutePath: () => null,
        readFile: async () => bytes,
    } as unknown as PlatformIO;
    const expected = streamingHash(wasm, bytes);
    assert.equal(await hashFileStreaming("note.md", io, wasm), expected);
    assert.equal(freed, 2);
    abortOnFeed = new AbortController();
    await assert.rejects(
        hashFileStreaming("note.md", io, wasm, undefined, 1, abortOnFeed.signal),
        { name: "AbortError" },
    );
    assert.equal(freed, 3);

    let chunkerFreed = 0;
    const abortChunk = new AbortController();
    class Chunker {
        update(): void { abortChunk.abort(); }
        finish(): never { throw new Error("cancelled chunker must not finish"); }
        free(): void { chunkerFreed++; }
    }
    await assert.rejects(chunkFileStreaming(
        { WasmChunker: Chunker } as unknown as WasmModule,
        bytes,
        undefined,
        abortChunk.signal,
    ), { name: "AbortError" });
    assert.equal(chunkerFreed, 1);
}

async function run(): Promise<void> {
    assert.throws(() => new WorkScheduler({ maxPendingJobs: 0 }), RangeError);
    assert.throws(() => new WorkScheduler({ maxPendingJobs: 1025 }), RangeError);
    await boundedFifoNeedsRealTasks();
    await abortsAreBoundedAndDoNotBlockLaterJobs();
    await disposingCancelsPendingAndReleasesResources();
    await brokenCapabilitiesFallBackWithoutLeaking();
    await actualNodeYieldIsNotAMicrotask();
    await cooperativeHashPreservesBytesAndFreesOnAbort();
    console.log("work-scheduler.test: bounded tasks, fallback, cancellation and hash cleanup passed");
}

void run().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});
