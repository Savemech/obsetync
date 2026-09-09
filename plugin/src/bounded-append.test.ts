import { appendBinaryBounded, AppendPrefixChangedError, estimateAppendWorkset } from "./bounded-append";
import { ResourceBudget, ResourceBudgetOversizedError } from "./resource-budget";
import { reserveTransientScope } from "./transient-memory";

let assertions = 0;
const check = (condition: unknown, message: string): void => {
    assertions++;
    if (!condition) throw new Error(message);
};
const deferred = () => {
    let resolve!: () => void;
    const promise = new Promise<void>((done) => { resolve = done; });
    return { promise, resolve };
};
const flush = async () => { for (let i = 0; i < 16; i++) await Promise.resolve(); };
const bytes = new Uint8Array([3, 4]);

async function admissionBeforePrefixReadAndNativeSettlement(): Promise<void> {
    const workBytes = estimateAppendWorkset(2, 2);
    const budget = new ResourceBudget({ capacityBytes: workBytes + 2 });
    const memory = await reserveTransientScope({ ownerBytes: 2, workBytes }, { budget });
    const blocker = await memory.reserve(workBytes);
    const gate = deferred();
    let reads = 0;
    let writes = 0;
    const append = appendBinaryBounded({
        readBinary: async () => { reads++; return new Uint8Array([1, 2]).buffer; },
        writeBinary: async (_path, data) => {
            writes++;
            check(new Uint8Array(data).join() === "1,2,3,4", "bounded append lost prefix");
            await gate.promise;
        },
    }, async () => ({ size: 2, mtime: 1 }), "part", bytes, { memory });
    await flush();
    check(reads === 0 && writes === 0, "legacy prefix allocation started before admission");
    blocker.release();
    await flush();
    check(reads === 1 && writes === 1, "admitted append did not reach native write");
    memory.close();
    check(budget.snapshot().usedBytes === workBytes + 2, "close released unfinished native append");
    gate.resolve();
    await append;
    check(budget.snapshot().usedBytes === 0, "native append leaked parent reservation");
}

async function unsafeFallbackLeavesExistingPrefix(): Promise<void> {
    for (const failure of ["oversize", "read", "growth", "shrink"] as const) {
        const workBytes = estimateAppendWorkset(2, 2) - (failure === "oversize" ? 1 : 0);
        const budget = new ResourceBudget({ capacityBytes: workBytes + 2 });
        const memory = await reserveTransientScope({ ownerBytes: 2, workBytes }, { budget });
        let reads = 0;
        let writes = 0;
        let error: unknown;
        try {
            await appendBinaryBounded({
                readBinary: async () => {
                    reads++;
                    if (failure === "read") throw new Error("EIO");
                    return new Uint8Array(failure === "growth" ? 3 : 1).buffer;
                },
                writeBinary: async () => { writes++; },
            }, async () => ({ size: 2, mtime: 1 }), "private-staging-name", bytes, { memory });
        } catch (value) { error = value; }
        check(!!error && writes === 0, `${failure} fallback overwrote recoverable prefix`);
        if (failure === "oversize") {
            check(error instanceof ResourceBudgetOversizedError && reads === 0,
                "oversized prefix was read before rejection");
        }
        if (failure === "growth" || failure === "shrink") {
            check(error instanceof AppendPrefixChangedError, "prefix drift lost typed error");
        }
        check(!String(error).includes("private-staging-name"), "append error exposed path");
        memory.close();
        check(budget.snapshot().usedBytes === 0, `${failure} append leaked reservation`);
    }
}

async function cancellationDoesNotReleaseUnfinishedRead(): Promise<void> {
    const workBytes = estimateAppendWorkset(2, 2);
    const budget = new ResourceBudget({ capacityBytes: workBytes + 2 });
    const memory = await reserveTransientScope({ ownerBytes: 2, workBytes }, { budget });
    const signal = new AbortController();
    const readGate = deferred();
    let writes = 0;
    const append = appendBinaryBounded({
        readBinary: async () => { await readGate.promise; return new Uint8Array([1, 2]).buffer; },
        writeBinary: async () => { writes++; },
    }, async () => ({ size: 2, mtime: 1 }), "part", bytes, { memory, signal: signal.signal });
    const outcome = append.then(() => undefined, (error) => error);
    await flush();
    signal.abort();
    memory.close();
    check(budget.snapshot().usedBytes > 0, "abort released still-running native prefix read");
    readGate.resolve();
    const error = await outcome;
    check(error?.name === "AbortError" && writes === 0, "cancelled prefix read was written");
    check(budget.snapshot().usedBytes === 0, "cancelled prefix read leaked reservation");
}

async function queuedCancellationAndNativeCapability(): Promise<void> {
    const workBytes = estimateAppendWorkset(2);
    const budget = new ResourceBudget({ capacityBytes: workBytes + 2 });
    const memory = await reserveTransientScope({ ownerBytes: 2, workBytes }, { budget });
    const blocker = await memory.reserve(workBytes);
    const signal = new AbortController();
    let nativeCalls = 0;
    const adapter = {
        appendBinary: async () => { nativeCalls++; },
        readBinary: async (): Promise<ArrayBuffer> => { throw new Error("native append reread prefix"); },
        writeBinary: async () => { throw new Error("native append replaced prefix"); },
    };
    const stat = async (): Promise<never> => { throw new Error("native append applied legacy size cap"); };
    const pending = appendBinaryBounded(adapter, stat, "part", bytes, { memory, signal: signal.signal });
    const outcome = pending.then(() => undefined, (error) => error);
    await flush();
    signal.abort();
    check((await outcome)?.name === "AbortError", "queued append did not cancel");
    check(nativeCalls === 0, "queued cancellation started native allocation");
    blocker.release();
    await appendBinaryBounded(adapter, stat, "part", bytes, { memory });
    check(nativeCalls === 1, "actual append capability did not support arbitrarily large prefix");
    memory.close();
    check(budget.snapshot().usedBytes === 0, "native append leaked reservation");
}

async function stalePrefixMetadataFailsClosed(): Promise<void> {
    for (const change of ["queued", "reading", "appeared", "missing"] as const) {
        let metadata: { size: number; mtime: number } | null =
            change === "appeared" || change === "missing" ? null : { size: 2, mtime: 1 };
        const workBytes = estimateAppendWorkset(2, metadata?.size ?? 0);
        const budget = new ResourceBudget({ capacityBytes: workBytes + 2 });
        const memory = await reserveTransientScope({ ownerBytes: 2, workBytes }, { budget });
        const blocker = await memory.reserve(workBytes);
        const readGate = deferred();
        let reads = 0;
        let writes = 0;
        const append = appendBinaryBounded({
            readBinary: async () => {
                reads++;
                await readGate.promise;
                return new Uint8Array([1, 2]).buffer;
            },
            writeBinary: async (_path, value) => {
                writes++;
                check(new Uint8Array(value).join() === "3,4", "missing-prefix creation has wrong bytes");
            },
        }, async () => metadata, "part", bytes, { memory });
        const outcome = append.then(() => undefined, (error) => error);
        await flush();
        if (change === "queued" || change === "appeared") metadata = { size: 2, mtime: 2 };
        blocker.release();
        await flush();
        if (change === "reading") {
            check(reads === 1, "read-time metadata test did not enter native read");
            metadata = { size: 2, mtime: 2 };
        }
        readGate.resolve();
        const error = await outcome;
        if (change === "missing") {
            check(error === undefined && reads === 0 && writes === 1,
                "legitimate missing-prefix creation was rejected");
        } else {
            check(error instanceof AppendPrefixChangedError && writes === 0,
                `${change} prefix change reached replacement write`);
            if (change !== "reading") check(reads === 0, "stale queued prefix was reread");
        }
        memory.close();
        check(budget.snapshot().usedBytes === 0, `${change} metadata fence leaked admission`);
    }
}

void admissionBeforePrefixReadAndNativeSettlement()
    .then(unsafeFallbackLeavesExistingPrefix)
    .then(cancellationDoesNotReleaseUnfinishedRead)
    .then(queuedCancellationAndNativeCapability)
    .then(stalePrefixMetadataFailsClosed)
    .then(() => console.log(`bounded-append.test: ${assertions} assertions passed`))
    .catch((error) => { console.error(error); process.exitCode = 1; });
