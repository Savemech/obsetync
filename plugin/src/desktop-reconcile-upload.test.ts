import { BulkObjectKind } from "./bulk-codec";
import { reconcileDesktopLargeFile } from "./desktop-reconcile-upload";
import type { DesktopRangeReader } from "./desktop-ranged-upload";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const expectedHash = "a".repeat(64);
const firstHash = "b".repeat(64);
const secondHash = "c".repeat(64);
const fingerprint = {
    size: 8,
    mtime: 10,
    ctime: 11,
    device: 1,
    inode: 2,
};

async function missingBitmapReadsOneRangeBeforeManifest(): Promise<void> {
    let workerInput: any;
    const worker = {
        run: async (input: any) => {
            workerInput = input;
            return {
                type: "result",
                job_id: "test",
                mode: "manifest",
                manifest: {
                    file_hash: expectedHash,
                    total_size: 8,
                    chunks: [
                        { hash: firstHash, offset: 0, size: 4 },
                        { hash: secondHash, offset: 4, size: 4 },
                    ],
                },
                size: 8,
                mtime: 10,
                fingerprint,
                read_ms: 2,
                hash_ms: 3,
            } as const;
        },
    };
    const events: string[] = [];
    const openReader = async (): Promise<DesktopRangeReader> => ({
        async verify() { events.push("verify"); },
        async read(offset, size) {
            events.push(`read:${offset}:${size}`);
            return new Uint8Array(size);
        },
        async close() { events.push("close"); },
    });
    const result = await reconcileDesktopLargeFile(
        worker as any,
        {
            absolutePath: "/vault/large.bin",
            expectedHash,
            expectedSize: 8,
            expectedMtime: 10,
            feedBytes: 64 * 1024,
        },
        async (hashes) => {
            check(hashes.join(",") === `${firstHash},${secondHash}`, "chunk check lost order");
            return [secondHash];
        },
        async (records) => {
            events.push(records[0].kind === BulkObjectKind.Manifest
                ? "manifest"
                : `content:${records.map((record) => record.hash[0]).join("")}`);
        },
        undefined,
        openReader,
    );

    check(workerInput.mode === "manifest" && !("data" in workerInput), "worker received bytes");
    check(result.status === "uploaded", "desktop reconcile did not upload");
    check(result.status === "uploaded" && result.neededBytes === 4, "needed bytes differ");
    check(result.status === "uploaded" && result.uploadedBytes === 4, "uploaded bytes differ");
    check(events.includes("read:4:4") && !events.includes("read:0:4"), "wrong range was read");
    check(events.indexOf("manifest") > events.indexOf("content:c"), "manifest preceded content ACK");
    check(events.at(-1) === "close", "range reader leaked");
}

async function changedLocalHashUploadsNothing(): Promise<void> {
    let checked = 0;
    let uploaded = 0;
    const worker = {
        run: async () => ({
            type: "result",
            job_id: "drift",
            mode: "manifest",
            manifest: {
                file_hash: "d".repeat(64),
                total_size: 8,
                chunks: [{ hash: firstHash, offset: 0, size: 8 }],
            },
            size: 8,
            mtime: 10,
            fingerprint,
            read_ms: 2,
            hash_ms: 3,
        } as const),
    };
    const result = await reconcileDesktopLargeFile(
        worker as any,
        {
            absolutePath: "/vault/drift.bin",
            expectedHash,
            expectedSize: 8,
            expectedMtime: 10,
            feedBytes: 64 * 1024,
        },
        async () => { checked++; return []; },
        async () => { uploaded++; },
    );
    check(result.status === "drifted", "changed sync-base content was not rejected");
    check(checked === 0 && uploaded === 0, "drifted content reached the server");
}

void missingBitmapReadsOneRangeBeforeManifest()
    .then(changedLocalHashUploadsNothing)
    .then(() => console.log(`desktop-reconcile-upload.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
