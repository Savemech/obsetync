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

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const hash = (digit: string): string => digit.repeat(64);
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

void twentyGiBSourceKeepsAnEightMiBQueue()
    .then(retryReadsOnlyRangesStillMissingOnServer)
    .then(finalDriftFailsAfterManifestAndCloses)
    .then(hostileRangesAreRejectedBeforeOpening)
    .then(pathnameReplacementIsDetectedByTheRealReader)
    .then(() => console.log(`desktop-ranged-upload.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
