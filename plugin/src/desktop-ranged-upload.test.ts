import { HashWorkerFileDriftError } from "./desktop-hash-workers";
import {
    DESKTOP_RANGE_QUEUE_BYTES,
    desktopFileFingerprintMatches,
    openDesktopRangeReader,
    uploadDesktopMissingRanges,
    type DesktopMissingRange,
    type DesktopRangeReader,
    type DesktopRangeSource,
} from "./desktop-ranged-upload";
import { mkdtemp, rename, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { ResourceBudget } from "./resource-budget";
import { reserveTransientScope } from "./transient-memory";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const hash = (digit: string): string => digit.repeat(64);
const digest = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const source = (size: number): DesktopRangeSource => ({
    absolutePath: "/vault/huge.bin",
    fingerprint: {
        size,
        mtime: 1_000,
        ctime: 2_000,
        device: 9,
        inode: 99,
    },
});

function fakeOpener(
    events: string[],
    options: { failFinalVerify?: boolean } = {},
) {
    return async (): Promise<DesktopRangeReader> => {
        let verifyCalls = 0;
        return {
            async verify(): Promise<void> {
                verifyCalls++;
                events.push(`verify:${verifyCalls}`);
                if (options.failFinalVerify && verifyCalls === 2) {
                    throw new HashWorkerFileDriftError("injected final drift");
                }
            },
            async read(offset, size): Promise<Uint8Array> {
                events.push(`read:${offset}:${size}`);
                return new Uint8Array(size);
            },
            async close(): Promise<void> {
                events.push("close");
            },
        };
    };
}

async function twentyGiBSourceKeepsAnEightMiBQueue(): Promise<void> {
    const gib = 1024 * 1024 * 1024;
    const mib = 1024 * 1024;
    const events: string[] = [];
    const ranges: DesktopMissingRange[] = [
        { hash: hash("a"), offset: 3 * gib, size: 4 * mib },
        { hash: hash("b"), offset: 17 * gib, size: 4 * mib },
    ];
    let calls = 0;
    let retainedAtCall = 0;
    const result = await uploadDesktopMissingRanges(
        source(20 * gib),
        ranges,
        async (records) => {
            calls++;
            retainedAtCall = Math.max(
                retainedAtCall,
                records.reduce((sum, record) => sum + record.data.byteLength, 0),
            );
            events.push(`put:${records.map((record) => record.hash[0]).join("")}`);
        },
        async () => { events.push("manifest"); },
        { openReader: fakeOpener(events) },
    );

    check(calls === 1, "two missing ranges were not packed together");
    check(result.uploadedBytes === 8 * mib, "uploaded byte count differs");
    check(result.uploadedRanges === 2, "uploaded range count differs");
    check(result.peakBufferedBytes === DESKTOP_RANGE_QUEUE_BYTES, "range queue peak differs");
    check(retainedAtCall === DESKTOP_RANGE_QUEUE_BYTES, "transport saw an unbounded range queue");
    check(
        events.filter((event) => event.startsWith("read:")).join(",") ===
            `read:${3 * gib}:${4 * mib},read:${17 * gib}:${4 * mib}`,
        "pass 2 read anything outside the missing bitmap",
    );
    check(events.indexOf("manifest") > events.indexOf("put:ab"), "manifest preceded chunk ACKs");
    check(events.at(-1) === "close", "reader was not closed after success");
}

async function retryReadsOnlyRangesStillMissingOnServer(): Promise<void> {
    const ranges: DesktopMissingRange[] = [
        { hash: hash("a"), offset: 0, size: 4 },
        { hash: hash("b"), offset: 4, size: 4 },
        { hash: hash("c"), offset: 8, size: 4 },
    ];
    const firstEvents: string[] = [];
    const accepted = new Set<string>();
    let firstPuts = 0;
    let failed = false;
    try {
        await uploadDesktopMissingRanges(
            source(12),
            ranges,
            async (records) => {
                firstPuts++;
                if (firstPuts === 2) throw new Error("injected disconnect");
                for (const record of records) accepted.add(record.hash);
            },
            async () => { throw new Error("manifest must not run after failed chunks"); },
            {
                maxBufferedBytes: 4,
                openReader: fakeOpener(firstEvents),
            },
        );
    } catch (error) {
        failed = (error as Error).message === "injected disconnect";
    }
    check(failed, "interrupted range upload unexpectedly succeeded");
    check(accepted.has(hash("a")) && accepted.size === 1, "ACKed prefix was not modeled");
    check(
        firstEvents.filter((event) => event.startsWith("read:")).join(",") ===
            "read:0:4,read:4:4",
        "first attempt read past the failed pack",
    );
    check(firstEvents.at(-1) === "close", "reader leaked after interruption");

    const retryEvents: string[] = [];
    const remaining = ranges.filter((range) => !accepted.has(range.hash));
    let finalized = false;
    const result = await uploadDesktopMissingRanges(
        source(12),
        remaining,
        async (records) => {
            for (const record of records) accepted.add(record.hash);
        },
        async () => { finalized = true; },
        {
            maxBufferedBytes: 4,
            openReader: fakeOpener(retryEvents),
        },
    );
    check(result.uploadedRanges === 2, "retry did not upload exactly the missing suffix");
    check(accepted.size === 3, "retry left a missing chunk");
    check(finalized, "retry did not finalize its manifest");
    check(
        retryEvents.filter((event) => event.startsWith("read:")).join(",") ===
            "read:4:4,read:8:4",
        "retry re-read a previously ACKed range",
    );
}

async function finalDriftFailsAfterManifestAndCloses(): Promise<void> {
    const events: string[] = [];
    let drifted = false;
    try {
        await uploadDesktopMissingRanges(
            source(4),
            [{ hash: hash("d"), offset: 0, size: 4 }],
            async () => { events.push("put"); },
            async () => { events.push("manifest"); },
            { openReader: fakeOpener(events, { failFinalVerify: true }) },
        );
    } catch (error) {
        drifted = error instanceof HashWorkerFileDriftError;
    }
    check(drifted, "final pathname drift was hidden");
    check(events.includes("manifest"), "final verification ran before manifest ACK");
    check(events.at(-1) === "close", "reader leaked after final drift");
}

async function sameMetadataByteDriftNeverUploadsUnverifiedBytes(): Promise<void> {
    // The injectable validator uses a real digest over synthetic bytes; the
    // production caller selects the protocol hash and admits its workspace.
    const original = [new Uint8Array([1, 1, 1, 1]), new Uint8Array([2, 2, 2, 2])];
    const hashes = original.map(digest);
    for (const maxBufferedBytes of [4, 8]) {
        const events: string[] = [];
        const accepted: string[] = [];
        const mismatch = new HashWorkerFileDriftError("injected same-metadata content drift");
        let finalized = false;
        let validations = 0;
        let failure: unknown;
        try {
            await uploadDesktopMissingRanges(
                source(8),
                hashes.map((value, index) => ({ hash: value, offset: index * 4, size: 4 })),
                async (records) => { accepted.push(...records.map((record) => record.hash)); },
                async () => { finalized = true; },
                {
                    maxBufferedBytes,
                    openReader: async () => ({
                        // Simulate an adapter whose metadata never reports the edit.
                        async verify() { events.push("metadata:unchanged"); },
                        async read(offset) {
                            events.push(`read:${offset}`);
                            return offset === 0 ? original[0].slice() : new Uint8Array([2, 2, 2, 3]);
                        },
                        async close() { events.push("close"); },
                    }),
                    async verifyRange(data, expectedHash) {
                        validations++;
                        if (digest(data) !== expectedHash) throw mismatch;
                    },
                },
            );
        } catch (error) { failure = error; }
        check(failure === mismatch, "content mismatch was hidden or replaced");
        check(validations === 2, "not every read was validated");
        check(events.filter((event) => event === "metadata:unchanged").length === 2,
            "fixture did not exercise matching metadata on both reads");
        check(accepted.length === (maxBufferedBytes === 4 ? 1 : 0),
            "failed validation uploaded its bytes or flushed the pending pack");
        check(!accepted.includes(hashes[1]), "modified bytes reached transport");
        check(!finalized, "manifest finalized after a failed range hash");
        check(events.at(-1) === "close" && events.filter((event) => event === "close").length === 1,
            "failed verification did not close the reader exactly once");
    }
}

async function asynchronousValidationPrecedesQueueAndNextRead(): Promise<void> {
    const started = deferred<void>();
    const release = deferred<void>();
    const events: string[] = [];
    let verifiedData: Uint8Array | undefined;
    let finished = false;
    const running = uploadDesktopMissingRanges(
        source(8),
        [
            { hash: hash("a"), offset: 0, size: 4 },
            { hash: hash("b"), offset: 4, size: 4 },
        ],
        async (records) => {
            check(records.length === 1 && records[0].data === verifiedData,
                "transport did not receive the exact verified view");
            events.push(`put:${records[0].hash[0]}`);
        },
        async () => { events.push("manifest"); },
        {
            maxBufferedBytes: 4,
            openReader: fakeOpener(events),
            async verifyRange(data, expectedHash) {
                events.push(`hash:start:${expectedHash[0]}`);
                if (expectedHash === hash("a")) {
                    started.resolve();
                    await release.promise;
                }
                verifiedData = data;
                events.push(`hash:done:${expectedHash[0]}`);
            },
        },
    ).then((result) => { finished = true; return result; });
    await started.promise;
    await Promise.resolve();
    check(!finished && !events.some((event) => event.startsWith("put:")),
        "upload completed or sent bytes before asynchronous verification");
    check(events.filter((event) => event.startsWith("read:")).length === 1,
        "next range was read while the validator still owned its input");
    check(!events.includes("close") && !events.includes("manifest"),
        "reader closed or manifest finalized while verification was active");
    release.resolve();
    const result = await running;
    check(result.uploadedRanges === 2 && result.peakBufferedBytes === 4,
        "verification changed the bounded queue or ACK counts");
    for (const digit of ["a", "b"]) {
        check(events.indexOf(`hash:done:${digit}`) < events.indexOf(`put:${digit}`),
            "range reached transport before its validator settled");
    }
    check(events.at(-1) === "close", "successful validation leaked the reader");
}

async function lateVerificationFailureRetainsReaderAndAdmittedWork(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 100 });
    const scope = await reserveTransientScope({ ownerBytes: 60, workBytes: 40 }, { budget });
    const entered = deferred<void>();
    const native = deferred<void>();
    const closeEntered = deferred<void>();
    const closeDone = deferred<void>();
    const failure = new HashWorkerFileDriftError("late hash failure");
    let closeCalls = 0;
    let putCalls = 0;
    let finalizeCalls = 0;
    let finished = false;
    const running = scope.track(() => uploadDesktopMissingRanges(
        source(4), [{ hash: hash("a"), offset: 0, size: 4 }],
        async () => { putCalls++; }, async () => { finalizeCalls++; },
        {
            openReader: async () => ({
                async verify() {},
                async read() { return new Uint8Array([1, 2, 3, 4]); },
                async close() {
                    closeCalls++;
                    closeEntered.resolve();
                    await closeDone.promise;
                },
            }),
            verifyRange: (data) => scope.run(40, async () => {
                check(data.byteLength === 4, "validator lost its borrowed input");
                entered.resolve();
                await native.promise;
            }),
        },
    )).finally(() => { scope.close(); finished = true; }).then(
        () => undefined, (error: unknown) => error,
    );
    await entered.promise;
    // Model teardown while the actual native validator is still consuming the
    // borrowed range. The caller's existing scope tracks that owner lifetime.
    scope.close();
    let replacementGranted = false;
    const replacement = budget.reserve(100).then((lease) => {
        replacementGranted = true;
        return lease;
    });
    await Promise.resolve();
    check(!finished && closeCalls === 0, "reader closed before native verification settled");
    check(scope.snapshot().work.usedBytes === 40 && budget.snapshot().usedBytes === 100,
        "teardown freed admitted native verification work prematurely");
    check(!replacementGranted, "replacement reused bytes still owned by validation");
    native.reject(failure);
    await closeEntered.promise;
    check(!finished && budget.snapshot().usedBytes === 100 && !replacementGranted,
        "failed validation released owner bytes before reader close settled");
    check(putCalls === 0 && finalizeCalls === 0, "late rejection leaked bytes or finalized");
    closeDone.resolve();
    check(await running === failure, "late native failure was not propagated");
    const lease = await replacement;
    check(closeCalls === 1 && scope.snapshot().parentReleased,
        "completed cleanup did not close once and release accounting");
    lease.release();
    check(budget.snapshot().usedBytes === 0, "verification failure leaked its admitted workset");
}

async function noMissingRangesNeverReadOrValidateBytes(): Promise<void> {
    const events: string[] = [];
    const result = await uploadDesktopMissingRanges(
        source(20 * 1024 * 1024 * 1024), [],
        async () => { throw new Error("no ranges must not reach transport"); },
        async () => { events.push("manifest"); },
        {
            openReader: async () => ({
                async verify() { events.push("metadata"); },
                async read() { throw new Error("no ranges must not allocate file bytes"); },
                async close() { events.push("close"); },
            }),
            async verifyRange() { throw new Error("no range bytes exist to validate"); },
        },
    );
    check(result.uploadedBytes === 0 && result.uploadedRanges === 0 &&
        result.peakBufferedBytes === 0 && result.readMs === 0,
    "empty missing bitmap retained/read a file-byte queue");
    check(events.join(",") === "metadata,manifest,metadata,close",
        "empty missing bitmap changed source/finalization guards");
}

async function hostileRangesAreRejectedBeforeOpening(): Promise<void> {
    let opened = 0;
    const openReader = async (): Promise<DesktopRangeReader> => {
        opened++;
        throw new Error("must not open");
    };
    const invalid: DesktopMissingRange[][] = [
        [
            { hash: hash("e"), offset: 4, size: 4 },
            { hash: hash("f"), offset: 6, size: 4 },
        ],
        [{ hash: hash("e"), offset: 0, size: 4 * 1024 * 1024 + 1 }],
        [
            { hash: hash("e"), offset: 0, size: 4 },
            { hash: hash("e"), offset: 4, size: 4 },
        ],
        [{ hash: "not-a-hash", offset: 0, size: 1 }],
    ];
    for (const ranges of invalid) {
        let rejected = false;
        try {
            await uploadDesktopMissingRanges(
                source(16 * 1024 * 1024),
                ranges,
                async () => {},
                async () => {},
                { openReader },
            );
        } catch (error) {
            rejected = error instanceof RangeError;
        }
        check(rejected, "hostile range metadata was accepted");
    }
    check(opened === 0, "invalid ranges reached the filesystem");

    const original = source(10).fingerprint;
    check(
        !desktopFileFingerprintMatches(original, { ...original, inode: original.inode + 1 }),
        "same size/mtime pathname replacement was accepted",
    );
    check(
        !desktopFileFingerprintMatches(original, { ...original, ctime: original.ctime + 2 }),
        "ctime drift was accepted",
    );
}

async function pathnameReplacementIsDetectedByTheRealReader(): Promise<void> {
    const directory = await mkdtemp(join(tmpdir(), "obsetync-range-drift-"));
    const path = join(directory, "source.bin");
    const replacement = join(directory, "replacement.bin");
    const movedOriginal = join(directory, "source.previous.bin");
    let reader: DesktopRangeReader | null = null;
    try {
        await writeFile(path, new Uint8Array([1, 2, 3, 4]));
        await writeFile(replacement, new Uint8Array([5, 6, 7, 8]));
        const before = await stat(path);
        const fingerprint = {
            size: Number(before.size),
            mtime: Number(before.mtimeMs),
            ctime: Number(before.ctimeMs),
            device: Number(before.dev),
            inode: Number(before.ino),
        };
        reader = await openDesktopRangeReader({ absolutePath: path, fingerprint });
        await reader.verify();

        await rename(path, movedOriginal);
        await rename(replacement, path);
        await utimes(path, before.atime, before.mtime);

        let drifted = false;
        try {
            await reader.verify();
        } catch (error) {
            drifted = error instanceof HashWorkerFileDriftError;
        }
        check(drifted, "real reader accepted an atomic pathname replacement");
    } finally {
        await reader?.close();
        await rm(directory, { recursive: true, force: true });
    }
}

// Keep unresolved async regressions from silently exiting as a passing suite.
const suiteDeadline = setTimeout(() => {
    console.error("desktop-ranged-upload.test: async suite did not finish");
    process.exitCode = 1;
}, 30_000);
void twentyGiBSourceKeepsAnEightMiBQueue()
    .then(retryReadsOnlyRangesStillMissingOnServer)
    .then(finalDriftFailsAfterManifestAndCloses)
    .then(sameMetadataByteDriftNeverUploadsUnverifiedBytes)
    .then(asynchronousValidationPrecedesQueueAndNextRead)
    .then(lateVerificationFailureRetainsReaderAndAdmittedWork)
    .then(noMissingRangesNeverReadOrValidateBytes)
    .then(hostileRangesAreRejectedBeforeOpening)
    .then(pathnameReplacementIsDetectedByTheRealReader)
    .then(() => {
        clearTimeout(suiteDeadline);
        console.log(`desktop-ranged-upload.test: ${assertions} assertions passed`);
    })
    .catch((error) => {
        clearTimeout(suiteDeadline);
        console.error(error);
        process.exitCode = 1;
    });
