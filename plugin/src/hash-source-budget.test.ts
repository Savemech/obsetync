import {
    estimateHashSourceWorkset,
    estimateStreamingHashWorkset,
    HashSourceBudget,
    HashSourceGrowthError,
    withHashSource,
    withStreamingHashSource,
} from "./hash-source-budget";
import {
    ResourceBudget,
    ResourceBudgetClosedError,
    ResourceBudgetOversizedError,
} from "./resource-budget";
import { hashAdmittedBytesWithBrowserWorker, hashFileStreaming, streamingHash,
    type WasmModule } from "./push";
import { BrowserHashWorkerError, type BrowserHashWorkerRuntime } from "./browser-hash-workers";
import type { PlatformIO } from "./platform";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

let assertions = 0;
const check = (value: unknown, message: string) => {
    assertions++;
    if (!value) throw new Error(message);
};
const rejected = (promise: Promise<unknown>): Promise<unknown> => promise.then(
    () => { throw new Error("expected source admission to reject"); },
    (error: unknown) => error,
);
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
async function microtasks(): Promise<void> {
    for (let index = 0; index < 6; index++) await Promise.resolve();
}
const SOURCE_BYTES = 8;
const FEED_BYTES = 4;
const WORKSET = estimateHashSourceWorkset(SOURCE_BYTES, FEED_BYTES);

async function admissionOwnsReadAndConsumer(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: WORKSET });
    const firstRead = deferred<Uint8Array>();
    const firstHash = deferred<string>();
    let reads = 0;
    let frees = 0;
    const first = withHashSource({
        sourceBytes: SOURCE_BYTES, feedBytes: FEED_BYTES, budget,
        read: () => { reads++; return firstRead.promise; },
        consume: async () => {
            try { return await firstHash.promise; }
            finally {
                check(budget.snapshot().usedBytes === WORKSET, "source released before hasher cleanup");
                frees++;
            }
        },
    });
    const second = withHashSource({
        sourceBytes: SOURCE_BYTES, feedBytes: FEED_BYTES, budget,
        read: async () => { reads++; return new Uint8Array(SOURCE_BYTES); },
        consume: () => "second",
    });
    check(reads === 0 && budget.snapshot().usedBytes === WORKSET,
        "native read began before synchronous admission");
    await microtasks();
    check(reads === 1 && budget.snapshot().queuedRequests === 1, "queued source allocated native bytes");
    firstRead.resolve(new Uint8Array(SOURCE_BYTES));
    await microtasks();
    check(reads === 1 && budget.snapshot().usedBytes === WORKSET,
        "read completion released bytes while its hash consumer was alive");
    firstHash.resolve("first");
    check(await first === "first" && await second === "second", "source digest changed");
    check(frees === 1 && reads === 2 && budget.snapshot().usedBytes === 0,
        "source lifetime leaked or released its successor incorrectly");
    budget.close();
}

async function errorsAndGrowth(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: WORKSET });
    let reads = 0;
    let consumes = 0;
    const oversized = await rejected(withHashSource({
        sourceBytes: SOURCE_BYTES + 1, feedBytes: FEED_BYTES, budget,
        read: async () => { reads++; return new Uint8Array(SOURCE_BYTES + 1); },
        consume: () => { consumes++; return "bad"; },
    }));
    check(oversized instanceof ResourceBudgetOversizedError && reads === 0,
        "oversized source entered its native allocator");
    for (const data of [new Uint8Array(SOURCE_BYTES + 1), new Uint8Array(SOURCE_BYTES + 1).subarray(0, SOURCE_BYTES)]) {
        const growth = await rejected(withHashSource({
            sourceBytes: SOURCE_BYTES, feedBytes: FEED_BYTES, budget,
            read: async () => { reads++; return data; },
            consume: () => { consumes++; return "bad"; },
        }));
        check(growth instanceof HashSourceGrowthError && growth.observedBytes === SOURCE_BYTES + 1,
            "source/backing allocation growth was accepted");
        check(budget.snapshot().usedBytes === 0, "growth rejection retained its reservation");
    }
    check(consumes === 0, "unadmitted growth reached WASM");
    const readFailure = new Error("native read failed");
    check(await rejected(withHashSource({
        sourceBytes: SOURCE_BYTES, feedBytes: FEED_BYTES, budget,
        read: async () => { throw readFailure; }, consume: () => "bad",
    })) === readFailure, "read failure was replaced");
    check(budget.snapshot().usedBytes === 0, "read failure leaked source bytes");
    const hashFailure = new Error("hasher failed");
    check(await rejected(withHashSource({
        sourceBytes: SOURCE_BYTES, feedBytes: FEED_BYTES, budget,
        read: async () => new Uint8Array(SOURCE_BYTES), consume: () => { throw hashFailure; },
    })) === hashFailure, "consumer failure was replaced");
    check(budget.snapshot().usedBytes === 0, "consumer failure leaked source bytes");
    check(await withHashSource({
        sourceBytes: 0, feedBytes: FEED_BYTES, budget,
        read: async () => new Uint8Array(), consume: (data) => String(data.length),
    }) === "0", "empty file admission failed");
    check(estimateHashSourceWorkset(0, FEED_BYTES) > 0, "empty files had no workset overhead");
    budget.close();
}

async function cancellationDoesNotReleaseUnfinishedReads(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: WORKSET });
    const held = await budget.reserve(WORKSET);
    const queuedAbort = new AbortController();
    let reads = 0;
    const queued = rejected(withHashSource({
        sourceBytes: SOURCE_BYTES, feedBytes: FEED_BYTES, budget, signal: queuedAbort.signal,
        read: async () => { reads++; return new Uint8Array(SOURCE_BYTES); }, consume: () => "bad",
    }));
    queuedAbort.abort();
    check((await queued as Error).name === "AbortError" && reads === 0,
        "cancelled queued source allocated data");
    held.release();

    const readAbort = new AbortController();
    const pendingRead = deferred<Uint8Array>();
    let consumed = false;
    let settled = false;
    const pending = rejected(withHashSource({
        sourceBytes: SOURCE_BYTES, feedBytes: FEED_BYTES, budget, signal: readAbort.signal,
        read: () => pendingRead.promise,
        consume: () => { consumed = true; return "bad"; },
    })).then((error) => { settled = true; return error; });
    await microtasks();
    readAbort.abort();
    await microtasks();
    check(!settled && budget.snapshot().usedBytes === WORKSET,
        "abort raced and forgot an uncancellable native read");
    pendingRead.resolve(new Uint8Array(SOURCE_BYTES));
    check((await pending as Error).name === "AbortError" && !consumed,
        "aborted native read still entered the hash consumer");
    check(budget.snapshot().usedBytes === 0, "settled cancelled read retained accounting");

    const consumeAbort = new AbortController();
    const consumeEnd = deferred<string>();
    const consuming = rejected(withHashSource({
        sourceBytes: SOURCE_BYTES, feedBytes: FEED_BYTES, budget, signal: consumeAbort.signal,
        read: async () => new Uint8Array(SOURCE_BYTES), consume: () => consumeEnd.promise,
    }));
    await microtasks();
    consumeAbort.abort();
    check(budget.snapshot().usedBytes === WORKSET, "consumer abort released retained source data");
    consumeEnd.resolve("ignored-after-abort");
    check((await consuming as Error).name === "AbortError", "aborted consumer result was published");
    check(budget.snapshot().usedBytes === 0, "cancelled consumer retained accounting");
    budget.close();
}

async function siblingFailureKeepsItsOwnLease(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 2 * WORKSET });
    const read = deferred<Uint8Array>();
    const good = withHashSource({
        sourceBytes: SOURCE_BYTES, feedBytes: FEED_BYTES, budget,
        read: () => read.promise, consume: () => "good",
    });
    const failure = new Error("sibling failed");
    const bad = withHashSource({
        sourceBytes: SOURCE_BYTES, feedBytes: FEED_BYTES, budget,
        read: async () => { throw failure; }, consume: () => "bad",
    });
    check(await rejected(Promise.all([good, bad])) === failure, "sibling failure was swallowed");
    check(budget.snapshot().usedBytes === WORKSET && budget.snapshot().activeReservations === 1,
        "Promise.all rejection released a sibling's still-active native read");
    read.resolve(new Uint8Array(SOURCE_BYTES));
    check(await good === "good" && budget.snapshot().usedBytes === 0, "live sibling could not finish cleanly");
    budget.close();
}

async function streamingAndPolicyLifetimes(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: estimateStreamingHashWorkset(FEED_BYTES) });
    const nativeClosed = deferred<string>();
    let opened = false;
    const stream = withStreamingHashSource({
        feedBytes: FEED_BYTES, budget,
        consume: () => { opened = true; return nativeClosed.promise; },
    });
    check(!opened && budget.snapshot().usedBytes > 0, "stream opened before its feed reservation");
    await microtasks();
    check(opened && budget.snapshot().usedBytes === estimateStreamingHashWorkset(FEED_BYTES),
        "stream feed workset was not retained");
    budget.close();
    check(budget.snapshot().usedBytes > 0, "stop forgot an unclosed native stream");
    nativeClosed.resolve("stream hash");
    check(await stream === "stream hash" && budget.snapshot().usedBytes === 0,
        "stream completion failed to release its bounded workset");

    const policy = new HashSourceBudget();
    const mib = 1024 * 1024;
    policy.configure({ runtime: "mobile", transientBudgetBytes: 100 * mib });
    check(policy.snapshot().capacityBytes === 32 * mib, "mobile workset ceiling was bypassed");
    policy.configure({ runtime: "desktop", transientBudgetBytes: 500 * mib });
    check(policy.snapshot().capacityBytes === 128 * mib, "desktop workset ceiling was bypassed");
    policy.configure({ runtime: "unknown", transientBudgetBytes: 500 * mib });
    check(policy.snapshot().capacityBytes === 32 * mib, "unknown runtime lacked a conservative ceiling");
    policy.configure({ runtime: "mobile", transientBudgetBytes: WORKSET });
    const grant = await policy.reserve(WORKSET);
    policy.configure({ runtime: "mobile", transientBudgetBytes: WORKSET - 1 });
    check(policy.snapshot().usedBytes === WORKSET, "reconfiguration forgot an old lease");
    policy.close();
    let reopened: unknown;
    try { policy.configure({ runtime: "desktop", transientBudgetBytes: WORKSET }); }
    catch (error) { reopened = error; }
    check(reopened instanceof ResourceBudgetClosedError && policy.snapshot().usedBytes === WORKSET,
        "reset reopened budget while old native owners were alive");
    grant.release();
    policy.configure({ runtime: "mobile", transientBudgetBytes: WORKSET });
    check(!policy.snapshot().closed && policy.snapshot().usedBytes === 0,
        "drained closed pool could not start a new epoch");
    policy.close();
}

async function actualFallbackRejectsBeforeRead(): Promise<void> {
    let reads = 0;
    let constructors = 0;
    const io = {
        getAbsolutePath: () => null,
        stat: async () => ({ size: 500 * 1024 * 1024, mtime: 1 }),
        readFile: async () => { reads++; return new Uint8Array(); },
    } as unknown as PlatformIO;
    const wasm = { Hasher: class { constructor() { constructors++; } } } as unknown as WasmModule;
    check(await rejected(hashFileStreaming("large.bin", io, wasm)) instanceof ResourceBudgetOversizedError,
        "actual fallback did not enforce admission before the native read");
    check(reads === 0 && constructors === 0, "oversized fallback allocated or created a hasher");
    const missing = { ...io, stat: async () => null } as PlatformIO;
    const error = await rejected(hashFileStreaming("missing", missing, wasm));
    check((error as { code?: string }).code === "ENOENT" && reads === 0,
        "missing stat reached readFile instead of remaining a source error");
}

async function actualNativeStreamClosesOnHasherFailureAndAbort(): Promise<void> {
    const directory = await mkdtemp(join(tmpdir(), "obsetync-hash-source-"));
    const path = join(directory, "source.bin");
    const bytes = new Uint8Array(8195).fill(3);
    try {
        await writeFile(path, bytes);
        let frees = 0;
        let failUpdate = false;
        let abort: AbortController | undefined;
        const failure = new Error("stream hasher failed");
        class Hasher {
            total = 0;
            update(data: Uint8Array): void {
                if (failUpdate) throw failure;
                for (const byte of data) this.total += byte;
                abort?.abort();
            }
            finalize(): string { return String(this.total); }
            free(): void { frees++; }
        }
        const wasm = { Hasher } as unknown as WasmModule;
        const io = {
            getAbsolutePath: () => path,
            stat: async () => { throw new Error("bounded native stream must not need whole-file stat admission"); },
            readFile: async () => { throw new Error("native stream fell back to a whole-file allocation"); },
        } as unknown as PlatformIO;
        check(await hashFileStreaming("source.bin", io, wasm) === String(bytes.length * 3),
            "bounded native stream changed file bytes");
        check(frees === 1, "native success leaked its hasher");
        failUpdate = true;
        check(await rejected(hashFileStreaming("source.bin", io, wasm)) === failure,
            "native data-handler failure did not reject through the source lifetime");
        check(frees === 2, "native hasher failure skipped cleanup");
        failUpdate = false;
        abort = new AbortController();
        check((await rejected(hashFileStreaming("source.bin", io, wasm, undefined, 1, abort.signal)) as Error).name === "AbortError",
            "native in-flight cancellation did not reject");
        check(frees === 3, "native cancellation skipped hasher cleanup");
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}

async function productionBrowserWorkerHashingAndFallbackFences(): Promise<void> {
    const source = Uint8Array.from([3, 1, 4, 1, 5, 9]);
    let rendererConstructions = 0;
    class Hasher {
        total = 0;
        constructor() { rendererConstructions++; }
        update(data: Uint8Array): void { for (const byte of data) this.total += byte; }
        finalize(): string { return String(this.total); }
        free(): void {}
    }
    const wasm = { Hasher } as unknown as WasmModule;
    const expected = streamingHash(wasm, source);
    rendererConstructions = 0;
    const makeIo = () => {
        let reads = 0;
        const io = {
            getAbsolutePath: () => null,
            stat: async () => ({ size: source.byteLength, mtime: 7 }),
            readFile: async () => { reads++; return source.slice(); },
        } as unknown as PlatformIO;
        return { io, reads: () => reads };
    };
    const readyStats = { state: "ready", wasmMode: "simd", fallbackSafe: true };
    const runtime = (run: (data: Uint8Array, options: unknown) => Promise<any>,
        assertCurrent = () => {}): BrowserHashWorkerRuntime => ({
        assertCurrent,
        pool: {
            stats: () => readyStats,
            diagnostics: () => ({ unconfirmedReleaseBytes: 0 }),
            run,
        } as any,
    });
    const offload = runtime(async data => {
        const transferred = structuredClone(data.buffer, { transfer: [data.buffer] }) as ArrayBuffer;
        const bytes = new Uint8Array(transferred);
        return { hash: String(bytes.reduce((sum, byte) => sum + byte, 0)),
            returnedBytes: bytes, hashMs: 7 };
    });
    const phases: Array<[string, number]> = [];
    const first = makeIo();
    check(await hashFileStreaming("note.md", first.io, wasm,
        { addPhase: (name: string, ms: number) => phases.push([name, ms]) } as any,
        3, undefined, offload) === expected, "browser offload changed the renderer hash");
    check(rendererConstructions === 0 && first.reads() === 1,
        "successful browser hashing ran the renderer or reread its source");
    check(phases.some(([name, ms]) => name === "hash" && ms === 21),
        "weighted browser hash timing was not recorded exactly once");

    const admitted = source.slice();
    const returned = await hashAdmittedBytesWithBrowserWorker(
        wasm, admitted, undefined, undefined, 64 * 1024, offload,
    );
    check(returned.offloaded && returned.hash === expected &&
        returned.data.byteLength === source.byteLength && admitted.byteLength === 0,
    "push helper did not replace the detached input with returned worker bytes");

    const crashIo = makeIo();
    rendererConstructions = 0;
    const crash = runtime(async data => {
        structuredClone(data.buffer, { transfer: [data.buffer] });
        throw new BrowserHashWorkerError("synthetic crash", "CRASH", false, data.byteLength);
    });
    const crashError = await rejected(hashFileStreaming(
        "note.md", crashIo.io, wasm, undefined, 1, undefined, crash,
    ));
    check(crashError instanceof BrowserHashWorkerError && !crashError.fallbackSafe &&
        crashIo.reads() === 1 && rendererConstructions === 0,
    "post-transfer crash authorized an unsafe renderer fallback");

    const freshIo = makeIo();
    rendererConstructions = 0;
    const safeLost = runtime(async data => {
        structuredClone(data.buffer, { transfer: [data.buffer] });
        throw new BrowserHashWorkerError("synthetic returned-owner loss", "UNAVAILABLE", true);
    });
    check(await hashFileStreaming("note.md", freshIo.io, wasm, undefined, 1,
        undefined, safeLost) === expected && freshIo.reads() === 2 && rendererConstructions === 1,
    "safe detached failure did not re-stat and re-read under fresh renderer admission");

    const unsupportedIo = makeIo();
    rendererConstructions = 0;
    const unsupported = runtime(async () => { throw new Error("run must not be called"); });
    (unsupported.pool as any).stats = () => ({ state: "unavailable", wasmMode: "unavailable",
        fallbackSafe: true });
    check(await hashFileStreaming("note.md", unsupportedIo.io, wasm, undefined, 1,
        undefined, unsupported) === expected && rendererConstructions === 1,
    "unsupported qualified capability did not use the existing renderer path");

    const staleIo = makeIo();
    let fenceCalls = 0;
    const stale = runtime(async data => {
        const transferred = structuredClone(data.buffer, { transfer: [data.buffer] }) as ArrayBuffer;
        return { hash: expected, returnedBytes: new Uint8Array(transferred), hashMs: 1 };
    }, () => { if (++fenceCalls > 1) throw new Error("browser hash worker generation changed"); });
    const staleError = await rejected(hashFileStreaming("note.md", staleIo.io, wasm,
        undefined, 1, undefined, stale));
    check((staleError as Error).message.includes("generation changed") && staleIo.reads() === 1,
        "stale worker result crossed the captured runtime generation fence");

    const abortIo = makeIo(), abort = new AbortController();
    const aborted = runtime(async data => {
        structuredClone(data.buffer, { transfer: [data.buffer] });
        abort.abort();
        throw Object.assign(new Error("cancelled"), { name: "AbortError" });
    });
    check(((await rejected(hashFileStreaming("note.md", abortIo.io, wasm,
        undefined, 1, abort.signal, aborted))) as Error).name === "AbortError" && abortIo.reads() === 1,
    "browser worker abort entered renderer fallback");
}

void admissionOwnsReadAndConsumer()
    .then(errorsAndGrowth)
    .then(cancellationDoesNotReleaseUnfinishedReads)
    .then(siblingFailureKeepsItsOwnLease)
    .then(streamingAndPolicyLifetimes)
    .then(actualFallbackRejectsBeforeRead)
    .then(actualNativeStreamClosesOnHasherFailureAndAbort)
    .then(productionBrowserWorkerHashingAndFallbackFences)
    .then(() => console.log(`hash-source-budget.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
