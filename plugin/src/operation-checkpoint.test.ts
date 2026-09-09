import { strict as assert } from "node:assert";
import {
    OperationCheckpoint,
    OPERATION_CHECKPOINT_PATHS,
    type OperationRecord,
} from "./operation-checkpoint";

class MemoryIO {
    files = new Map<string, Uint8Array>();

    async readFile(path: string): Promise<Uint8Array> {
        const value = this.files.get(path);
        if (!value) throw new Error("missing");
        return value.slice();
    }

    async writeFile(path: string, data: Uint8Array): Promise<void> {
        this.files.set(path, data.slice());
    }

    async deleteFile(path: string): Promise<void> {
        this.files.delete(path);
    }
}

const read = (io: MemoryIO, path: string = OPERATION_CHECKPOINT_PATHS.active): OperationRecord | undefined => {
    const bytes = io.files.get(path);
    return bytes ? JSON.parse(new TextDecoder().decode(bytes)) : undefined;
};

// Deterministic promise flushing only: no timers, native filesystem, or runtime
// suspension is emulated by the deferred-IO tests below.
async function flush(): Promise<void> {
    for (let step = 0; step < 20; step++) await Promise.resolve();
}

interface Mutation {
    kind: "write" | "delete";
    path: string;
    data?: Uint8Array;
    finish(error?: Error): void;
}

class DeferredIO extends MemoryIO {
    readonly calls: Array<Omit<Mutation, "finish">> = [];
    readonly pending: Mutation[] = [];
    active = 0;
    peakActive = 0;

    writeFile(path: string, data: Uint8Array): Promise<void> {
        return this.defer({ kind: "write", path, data: data.slice() });
    }

    deleteFile(path: string): Promise<void> {
        return this.defer({ kind: "delete", path });
    }

    private defer(call: Omit<Mutation, "finish">): Promise<void> {
        this.calls.push(call);
        this.active++;
        this.peakActive = Math.max(this.peakActive, this.active);
        return new Promise<void>((resolve, reject) => {
            this.pending.push({ ...call, finish: (error) => {
                this.active--;
                if (error) reject(error);
                else {
                    if (call.kind === "write") this.files.set(call.path, call.data!);
                    else this.files.delete(call.path);
                    resolve();
                }
            } });
        });
    }

    async next(): Promise<Mutation> {
        await flush();
        assert.equal(this.pending.length, 1, "expected exactly one outstanding native diagnostic mutation");
        return this.pending[0];
    }

    async finish(error?: Error): Promise<void> {
        const next = await this.next();
        this.pending.shift();
        next.finish(error);
        await flush();
    }
}

const recordOf = (mutation: Omit<Mutation, "finish">): OperationRecord => {
    assert.equal(mutation.kind, "write");
    return JSON.parse(new TextDecoder().decode(mutation.data));
};

async function basicRestartEvidence(): Promise<void> {
    let clock = 1000;
    const io = new MemoryIO();
    const first = new OperationCheckpoint(io, "1.10.1", () => clock);
    assert.equal(await first.initialize(), null);
    const id = await first.begin("pull", "0/20 files");
    assert.ok(io.files.has(OPERATION_CHECKPOINT_PATHS.active));

    clock += 4000;
    first.progress(id, "10/20 files\n12 MB");
    await flush();
    const active = JSON.parse(
        new TextDecoder().decode(io.files.get(OPERATION_CHECKPOINT_PATHS.active)),
    );
    assert.equal(active.detail, "10/20 files 12 MB");

    // Simulate a renderer kill: no complete() call, then a fresh instance.
    const restarted = new OperationCheckpoint(io, "1.10.1", () => 9000);
    const orphan = await restarted.initialize();
    assert.equal(orphan?.phase, "pull");
    assert.equal(restarted.getLastInterruption()?.phase, "pull");
    assert.ok(io.files.has(OPERATION_CHECKPOINT_PATHS.lastInterruption));
    assert.ok(!io.files.has(OPERATION_CHECKPOINT_PATHS.active));

    const nextId = await restarted.begin("push", "3 paths");
    await restarted.complete(nextId);
    assert.ok(!io.files.has(OPERATION_CHECKPOINT_PATHS.active));

    const cleanRestart = new OperationCheckpoint(io, "1.10.1", () => 10_000);
    assert.equal(await cleanRestart.initialize(), null);
    assert.equal(cleanRestart.getLastInterruption()?.phase, "pull");
}

async function thousandsOfProgressUpdatesHaveOnePendingSnapshot(): Promise<void> {
    let clock = 1000;
    const io = new DeferredIO();
    const checkpoint = new OperationCheckpoint(io, "test", () => clock);
    const started = checkpoint.begin("full-scan", "begin");
    await io.finish();
    const id = await started;

    clock += 3000;
    checkpoint.progress(id, "progress 1");
    await io.next();
    for (let progress = 2; progress <= 10_000; progress++) {
        clock += 3000;
        checkpoint.progress(id, `progress ${progress}`);
    }
    assert.equal(io.calls.length, 2, "progress started extra IO while the adapter was stalled");
    assert.equal(io.peakActive, 1);
    await io.finish();
    const latest = recordOf(await io.next());
    assert.equal(latest.detail, "progress 10000");
    assert.equal(latest.updatedAt, clock);
    await io.finish();
    assert.equal(io.calls.length, 3, "stalled progress accumulated a historical write chain");
    assert.equal(io.pending.length, 0);
    assert.equal(read(io)?.detail, "progress 10000");

    const completed = checkpoint.complete(id);
    await io.finish();
    await completed;
    assert.equal(read(io), undefined);
}

async function lifecycleBarriersSupersedeProgressAndSettleInOrder(): Promise<void> {
    let clock = 1000;
    const io = new DeferredIO();
    const checkpoint = new OperationCheckpoint(io, "test", () => clock);
    const started = checkpoint.begin("scan", "A begin");
    await io.finish();
    const firstId = await started;
    for (let progress = 1; progress <= 5000; progress++) {
        clock += 3000;
        checkpoint.progress(firstId, `A progress ${progress}`);
    }

    const settled: string[] = [];
    const failed = checkpoint.fail(firstId, new Error("failure evidence")).then(() => { settled.push("fail A"); });
    const completed = checkpoint.complete(firstId).then(() => { settled.push("complete A"); });
    const next = checkpoint.begin("push", "B begin").then((id) => { settled.push("begin B"); return id; });
    for (let progress = 0; progress < 5000; progress++) {
        clock += 3000;
        checkpoint.progress(firstId, "obsolete A progress");
    }
    await flush();
    assert.deepEqual(settled, [], "a lifecycle caller completed before its diagnostic IO");
    assert.equal(io.calls.length, 2, "barriers started concurrent IO");

    await io.finish(); // active A progress; pending A progress was superseded.
    const failure = recordOf(await io.next());
    assert.equal(failure.failed, true);
    assert.equal(failure.detail, "A progress 5000; error=failure evidence");
    assert.equal(failure.phase, "scan");
    await io.finish();
    await failed;
    assert.deepEqual(settled, ["fail A"]);
    assert.equal((await io.next()).kind, "delete");
    await io.finish();
    await completed;
    assert.deepEqual(settled, ["fail A", "complete A"]);
    const beginning = recordOf(await io.next());
    assert.equal(beginning.detail, "B begin");
    assert.equal(beginning.failed, undefined, "an older failure contaminated the next operation");
    await io.finish();
    const nextId = await next;
    assert.deepEqual(settled, ["fail A", "complete A", "begin B"]);
    assert.equal(read(io)?.operationId, nextId);
    assert.equal(io.calls.length, 5, "superseded progress ran between lifecycle barriers");

    // Old callers arriving after the new operation starts cannot delete it.
    await checkpoint.complete(firstId);
    await checkpoint.fail(firstId, "late failure");
    checkpoint.progress(firstId, "late progress");
    await flush();
    assert.equal(io.calls.length, 5);
    assert.equal(read(io)?.operationId, nextId);
    const end = checkpoint.complete(nextId);
    await io.finish();
    await end;
    assert.equal(read(io), undefined);
    assert.equal(io.pending.length, 0);
    assert.equal(io.peakActive, 1);
}

async function completeAndNewBeginDiscardTheirPredecessorsPendingProgress(): Promise<void> {
    let clock = 1000;
    const io = new DeferredIO();
    const checkpoint = new OperationCheckpoint(io, "test", () => clock);
    const beginning = checkpoint.begin("scan");
    await io.finish();
    const firstId = await beginning;
    clock += 3000;
    checkpoint.progress(firstId, "in flight");
    clock += 3000;
    checkpoint.progress(firstId, "must not run after completion");
    const completed = checkpoint.complete(firstId);
    await io.finish();
    assert.equal((await io.next()).kind, "delete");
    await io.finish();
    await completed;
    assert.equal(io.calls.length, 3);
    assert.equal(read(io), undefined);
    assert.equal(io.pending.length, 0, "old progress resurrected the completed marker");

    const second = checkpoint.begin("scan", "second begin");
    await io.finish();
    const secondId = await second;
    clock += 3000;
    checkpoint.progress(secondId, "second in flight");
    clock += 3000;
    checkpoint.progress(secondId, "must not overwrite replacement");
    const third = checkpoint.begin("push", "third begin");
    await io.finish();
    assert.equal(recordOf(await io.next()).detail, "third begin");
    await io.finish();
    const thirdId = await third;
    assert.equal(read(io)?.operationId, thirdId);
    assert.equal(io.calls.length, 6);
    assert.equal(io.pending.length, 0);
}

async function latestPendingProgressStillHonorsTheThrottle(): Promise<void> {
    let clock = 1000;
    const io = new DeferredIO();
    const checkpoint = new OperationCheckpoint(io, "test", () => clock);
    const beginning = checkpoint.begin("scan");
    await io.finish();
    const id = await beginning;
    clock = 1001;
    checkpoint.progress(id, "too soon");
    assert.equal(io.calls.length, 1);
    clock = 4000;
    checkpoint.progress(id, "first interval");
    clock = 7000;
    checkpoint.progress(id, "pending interval");
    clock = 7001;
    checkpoint.progress(id, "latest pending\n  detail");
    await io.finish();
    assert.equal(recordOf(await io.next()).detail, "latest pending   detail");
    await io.finish();
    clock = 7002;
    checkpoint.progress(id, "too soon after latest");
    assert.equal(io.calls.length, 3);
    clock = 10_001;
    checkpoint.progress(id, "next interval");
    await io.finish();
    assert.equal(read(io)?.detail, "next interval");
}

async function failedIoSettlesCallersAndDoesNotPoisonLaterBarriers(): Promise<void> {
    let clock = 1000;
    const io = new DeferredIO();
    const checkpoint = new OperationCheckpoint(io, "test", () => clock);
    const beginning = checkpoint.begin("scan");
    await io.finish(new Error("begin write rejected"));
    const id = await beginning;
    clock += 3000;
    checkpoint.progress(id, "in-flight failure");
    clock += 3000;
    checkpoint.progress(id, "latest survives IO error");
    await io.finish(new Error("progress write rejected"));
    assert.equal(recordOf(await io.next()).detail, "latest survives IO error");
    await io.finish();
    const failed = checkpoint.fail(id, new Error("caught failure"));
    clock += 3000;
    checkpoint.progress(id, "must not erase failure evidence");
    const completed = checkpoint.complete(id);
    const replacement = checkpoint.begin("push", "healthy replacement");
    await io.finish(new Error("failure marker write rejected"));
    await failed;
    assert.equal((await io.next()).kind, "delete");
    await io.finish(new Error("delete rejected"));
    await completed;
    assert.equal(recordOf(await io.next()).detail, "healthy replacement");
    await io.finish();
    assert.equal(read(io)?.operationId, await replacement);
    assert.equal(io.pending.length, 0);
    assert.equal(io.peakActive, 1);

    // An adapter is allowed to throw before returning its promised IO too.
    let unavailable = true;
    const synchronous = new MemoryIO();
    const write = synchronous.writeFile.bind(synchronous);
    const remove = synchronous.deleteFile.bind(synchronous);
    synchronous.writeFile = (path, data) => {
        if (unavailable) throw new Error("synchronous write failure");
        return write(path, data);
    };
    synchronous.deleteFile = (path) => {
        if (unavailable) throw new Error("synchronous delete failure");
        return remove(path);
    };
    const other = new OperationCheckpoint(synchronous, "test");
    const rejectedId = await other.begin("scan");
    await other.fail(rejectedId, "failed");
    await other.complete(rejectedId);
    unavailable = false;
    const healthyId = await other.begin("push");
    assert.equal(read(synchronous)?.operationId, healthyId);
}

async function failureFreezesProgressButAllowsCompletion(): Promise<void> {
    let clock = 1000;
    const io = new DeferredIO();
    const checkpoint = new OperationCheckpoint(io, "test", () => clock);
    const beginning = checkpoint.begin("scan", "latest detail");
    await io.finish();
    const id = await beginning;
    const failed = checkpoint.fail(id, new Error("preserve me"));
    for (let progress = 0; progress < 5000; progress++) {
        clock += 3000;
        checkpoint.progress(id, "late progress after failure");
    }
    await io.finish();
    await failed;
    assert.equal(read(io)?.detail, "latest detail; error=preserve me");
    assert.equal(read(io)?.failed, true);
    assert.equal(io.calls.length, 2);
    assert.equal(io.pending.length, 0);
    const completed = checkpoint.complete(id);
    await io.finish();
    await completed;
    assert.equal(read(io), undefined);
}

async function initializationCannotDeleteAConcurrentNewBegin(): Promise<void> {
    const io = new DeferredIO();
    const orphan: OperationRecord = {
        schema: 1, operationId: "old", phase: "scan", pluginVersion: "test",
        startedAt: 1, updatedAt: 2, detail: "orphaned",
    };
    io.files.set(OPERATION_CHECKPOINT_PATHS.active, new TextEncoder().encode(JSON.stringify(orphan)));
    const checkpoint = new OperationCheckpoint(io, "test", () => 10_000);
    const initializing = checkpoint.initialize();
    assert.equal((await io.next()).path, OPERATION_CHECKPOINT_PATHS.lastInterruption);
    const beginning = checkpoint.begin("push", "new marker");
    await io.finish();
    assert.equal((await io.next()).kind, "delete");
    await io.finish();
    assert.equal((await initializing)?.operationId, "old");
    assert.equal(recordOf(await io.next()).detail, "new marker");
    await io.finish();
    assert.equal(read(io)?.operationId, await beginning);
    assert.equal(read(io, OPERATION_CHECKPOINT_PATHS.lastInterruption)?.operationId, "old");
    assert.equal(io.peakActive, 1);
}

async function run(): Promise<void> {
    await basicRestartEvidence();
    await thousandsOfProgressUpdatesHaveOnePendingSnapshot();
    await lifecycleBarriersSupersedeProgressAndSettleInOrder();
    await completeAndNewBeginDiscardTheirPredecessorsPendingProgress();
    await latestPendingProgressStillHonorsTheThrottle();
    await failedIoSettlesCallersAndDoesNotPoisonLaterBarriers();
    await failureFreezesProgressButAllowsCompletion();
    await initializationCannotDeleteAConcurrentNewBegin();
    console.log("operation-checkpoint.test: bounded progress, ordered barriers, deferred IO and restart regressions passed");
}

void run().catch((error) => { console.error(error); process.exitCode = 1; });
