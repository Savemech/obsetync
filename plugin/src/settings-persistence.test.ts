import { strict as assert } from "node:assert";
import { SerialSettingsWriter, SettingsWriterClosedError } from "./settings-persistence";

function gate<T = void>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const tick = async () => { for (let step = 0; step < 8; step++) await Promise.resolve(); };
type Settings = { revision: number; nested: { names: string[] }; optional?: string };
const settings = (revision: number): Settings => ({ revision, nested: { names: [`revision-${revision}`] } });

async function thousandsOfSavesShareLatestPendingCut(): Promise<void> {
    const writes: Settings[] = [];
    const active = gate();
    const pending = gate();
    const writer = new SerialSettingsWriter<Settings>(value => {
        writes.push(value);
        return writes.length === 1 ? active.promise : pending.promise;
    });
    const first = writer.save(settings(1));
    assert.equal(writes.length, 1, "first native write was not synchronously admitted");
    const second = writer.save(settings(2));
    for (let revision = 3; revision <= 10_000; revision++) {
        assert.equal(writer.save(settings(revision)), second, "coalescing allocated a separate waiting promise");
    }
    assert.deepEqual(writer.snapshot(), { closed: false, active: true, pending: true, drained: false });
    assert.equal(writes.length, 1);
    let secondDone = false;
    void second.then(() => { secondDone = true; });
    const drain = writer.closeAndDrain();
    assert.equal(drain, writer.closeAndDrain());
    let drained = false;
    void drain.then(() => { drained = true; });
    await tick();
    assert.equal(secondDone, false);
    assert.equal(drained, false);
    active.resolve();
    await first;
    await tick();
    assert.equal(writes.length, 2);
    assert.equal(writes[1].revision, 10_000);
    assert.deepEqual(writer.snapshot(), { closed: true, active: true, pending: false, drained: false });
    assert.equal(secondDone, false, "pending caller resolved at the previous publication");
    pending.resolve();
    await second;
    await drain;
    assert.equal(drained, true);
    assert.equal(writer.closeAndDrain(), drain);
    assert.deepEqual(writer.snapshot(), { closed: true, active: false, pending: false, drained: true });
}

async function actualNativeCompletionPreservesPublicationOrder(): Promise<void> {
    const gates = [gate(), gate(), gate()];
    const started: number[] = [];
    const completed: number[] = [];
    let activeCount = 0;
    const writer = new SerialSettingsWriter<Settings>(async value => {
        const index = started.length;
        started.push(value.revision);
        activeCount++;
        assert.equal(activeCount, 1, "new settings write overlapped an old native write");
        try { await gates[index].promise; completed.push(value.revision); }
        finally { activeCount--; }
    });
    const old = writer.save(settings(1));
    const newer = writer.save(settings(2));
    gates[1].resolve(); // Even a pre-resolved later IO gate cannot start it early.
    await tick();
    assert.deepEqual(started, [1]);
    assert.deepEqual(completed, []);
    gates[0].resolve();
    await old;
    await newer;
    assert.deepEqual(completed, [1, 2]);
    const latest = writer.save(settings(3));
    assert.deepEqual(started, [1, 2, 3]);
    const drain = writer.closeAndDrain();
    gates[2].resolve();
    await latest;
    await drain;
    assert.deepEqual(completed, [1, 2, 3]);
}

async function failuresDoNotForgetPendingOrMisreportDrain(): Promise<void> {
    for (const activeFails of [false, true]) {
        for (const pendingFails of [false, true]) {
            const firstGate = gate();
            const nextGate = gate();
            const firstError = new Error("first native write failed");
            const nextError = new Error("latest native write failed");
            const writes: number[] = [];
            const writer = new SerialSettingsWriter<Settings>(value => {
                writes.push(value.revision);
                return writes.length === 1 ? firstGate.promise : nextGate.promise;
            });
            const first = writer.save(settings(1));
            const firstObserved = activeFails ? assert.rejects(first, error => error === firstError) : first;
            const olderPending = writer.save(settings(2));
            const latestPending = writer.save(settings(3));
            assert.equal(olderPending, latestPending);
            const pendingObserved = pendingFails
                ? assert.rejects(olderPending, error => error === nextError) : olderPending;
            const drain = writer.closeAndDrain();
            let drained = false;
            void drain.then(() => { drained = true; });
            if (activeFails) firstGate.reject(firstError); else firstGate.resolve();
            await firstObserved;
            await tick();
            assert.deepEqual(writes, [1, 3]);
            assert.equal(drained, false, "first write settlement forgot the pending native owner");
            if (pendingFails) nextGate.reject(nextError); else nextGate.resolve();
            await pendingObserved;
            await drain;
            assert.equal(drained, true, "drain reports actual settlement, not successful persistence");
        }
    }
}

async function snapshotsDetachAndInvalidSubmissionPreservesPriorPending(): Promise<void> {
    const writes: Settings[] = [];
    const firstGate = gate();
    const writer = new SerialSettingsWriter<Settings>(value => {
        writes.push(value);
        return writes.length === 1 ? firstGate.promise : Promise.resolve();
    });
    const firstInput = settings(1);
    const first = writer.save(firstInput);
    firstInput.nested.names.push("caller mutation");
    firstInput.revision = 500;
    const input = settings(2);
    const pending = writer.save(input);
    input.nested.names[0] = "later mutation";
    input.revision = 999;
    const invalid: any = settings(3);
    invalid.circular = invalid;
    await assert.rejects(writer.save(invalid), TypeError);
    assert.equal(writes.length, 1);
    firstGate.resolve();
    await first;
    await pending;
    assert.deepEqual(writes, [settings(1), settings(2)]);
    await writer.closeAndDrain();

    let received: Settings | undefined;
    let copies = 0;
    const custom = new SerialSettingsWriter<Settings>(async value => { received = value; }, value => {
        copies++;
        return { ...value, nested: { names: [...value.nested.names] } };
    });
    const value = settings(4);
    await custom.save(value);
    value.nested.names.length = 0;
    assert.equal(copies, 1);
    assert.deepEqual(received, settings(4));
    await custom.closeAndDrain();
}

async function synchronousFailuresAndReentrantAdapterAdmission(): Promise<void> {
    let first = true;
    const failure = new Error("synchronous adapter failure");
    const published: number[] = [];
    const writer = new SerialSettingsWriter<Settings>(value => {
        if (first) { first = false; throw failure; }
        published.push(value.revision);
        return Promise.resolve();
    });
    const failed = assert.rejects(writer.save(settings(1)), error => error === failure);
    const pending = writer.save(settings(2));
    const drain = writer.closeAndDrain();
    await failed;
    await pending;
    await drain;
    assert.deepEqual(published, [2]);

    const native = gate();
    let nested!: Promise<void>;
    let nestedDrain!: Promise<void>;
    const seen: number[] = [];
    let reentrant!: SerialSettingsWriter<Settings>;
    reentrant = new SerialSettingsWriter<Settings>(value => {
        seen.push(value.revision);
        if (value.revision === 1) {
            assert.equal(reentrant.snapshot().active, true);
            nested = reentrant.save(settings(2));
            nestedDrain = reentrant.closeAndDrain();
            return native.promise;
        }
        return Promise.resolve();
    });
    const outer = reentrant.save(settings(1));
    assert.deepEqual(seen, [1]);
    assert.equal(reentrant.closeAndDrain(), nestedDrain);
    native.resolve();
    await outer;
    await nested;
    await nestedDrain;
    assert.deepEqual(seen, [1, 2]);
}

async function closedAndEmptyStatesAreStable(): Promise<void> {
    let writes = 0;
    const writer = new SerialSettingsWriter<Settings>(async () => { writes++; });
    assert.deepEqual(writer.snapshot(), { closed: false, active: false, pending: false, drained: false });
    const snapshot = writer.snapshot();
    snapshot.closed = true;
    assert.equal(writer.snapshot().closed, false);
    const drain = writer.closeAndDrain();
    await assert.rejects(writer.save(settings(1)), error => error instanceof SettingsWriterClosedError &&
        error.code === "SETTINGS_WRITER_CLOSED" && error.message === "Settings write admission is closed");
    await drain;
    assert.equal(writer.closeAndDrain(), drain);
    assert.equal(writes, 0);

    let duringCopy!: SerialSettingsWriter<Settings>;
    duringCopy = new SerialSettingsWriter<Settings>(async () => { writes++; }, value => {
        void duringCopy.closeAndDrain();
        return { ...value, nested: { names: [...value.nested.names] } };
    });
    await assert.rejects(duringCopy.save(settings(2)), SettingsWriterClosedError);
    await duringCopy.closeAndDrain();
    assert.equal(writes, 0, "snapshot-hook unload admitted a new native write after close");
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed) { console.error("settings persistence tests did not reach actual publication drain"); process.exitCode = 1; }
});
void (async () => {
    await thousandsOfSavesShareLatestPendingCut();
    await actualNativeCompletionPreservesPublicationOrder();
    await failuresDoNotForgetPendingOrMisreportDrain();
    await snapshotsDetachAndInvalidSubmissionPreservesPriorPending();
    await synchronousFailuresAndReentrantAdapterAdmission();
    await closedAndEmptyStatesAreStable();
    completed = true;
    console.log("settings persistence: 6 groups passed (10,000 submissions; 4 native-failure combinations)");
})().catch(error => { console.error(error); process.exitCode = 1; });
