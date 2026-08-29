import { applyLargeFile, validateManifest } from "./pull";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const fileHash = "f".repeat(64);
const firstHash = "a".repeat(64);
const secondHash = "b".repeat(64);
const manifest = {
    file_hash: fileHash,
    total_size: 5,
    chunks: [
        { hash: firstHash, offset: 0, size: 2 },
        { hash: secondHash, offset: 2, size: 3 },
    ],
};

check(validateManifest(manifest, fileHash, 5).chunks.length === 2, "valid manifest rejected");
let malformedRejected = false;
try {
    validateManifest({ ...manifest, chunks: [{ ...manifest.chunks[0], offset: 1 }] }, fileHash, 5);
} catch {
    malformedRejected = true;
}
check(malformedRejected, "manifest hole/offset mismatch accepted");

async function resumeInterruptedDownload(): Promise<void> {
    const files = new Map<string, Uint8Array>();
    let failSecond = true;
    let requested: string[] = [];
    const chunks = new Map([
        [firstHash, new Uint8Array([1, 2])],
        [secondHash, new Uint8Array([3, 4, 5])],
    ]);
    const api = {
        getManifest: async () => manifest,
        getContentChunk: async (hash: string) => {
            requested.push(hash);
            if (hash === secondHash && failSecond) throw new Error("interrupted");
            return chunks.get(hash)!;
        },
    } as any;
    const io = {
        readFile: async (path: string) => {
            const value = files.get(path);
            if (!value) throw new Error("missing");
            return value;
        },
        writeFile: async (path: string, data: Uint8Array) => files.set(path, data.slice()),
        appendFile: async (path: string, data: Uint8Array) => {
            const previous = files.get(path) ?? new Uint8Array();
            const combined = new Uint8Array(previous.length + data.length);
            combined.set(previous);
            combined.set(data, previous.length);
            files.set(path, combined);
        },
        replaceFile: async (staging: string, target: string) => {
            files.set(target, files.get(staging)!.slice());
            files.delete(staging);
        },
        deleteFile: async (path: string) => { files.delete(path); },
        stat: async (path: string) => {
            const value = files.get(path);
            return value ? { mtime: 1, size: value.length } : null;
        },
    } as any;
    const wasm = {
        wasm_hash: (data: Uint8Array) => data[0] === 1 ? firstHash : secondHash,
        wasm_hash_batch: () => ["0".repeat(64)],
        Hasher: class {
            update(): void {}
            update_and_hash(data: Uint8Array): string {
                return data[0] === 1 ? firstHash : secondHash;
            }
            finalize(): string { return fileHash; }
            free(): void {}
        },
    } as any;

    let interrupted = false;
    try {
        await applyLargeFile(api, io, "media/video.bin", fileHash, 5, wasm);
    } catch {
        interrupted = true;
    }
    check(interrupted, "interrupted transfer unexpectedly completed");
    check(
        [...files.entries()].some(([path, bytes]) => path.endsWith(".part") && bytes.length === 2),
        "verified first chunk was not checkpointed",
    );

    failSecond = false;
    requested = [];
    await applyLargeFile(api, io, "media/video.bin", fileHash, 5, wasm);
    check(requested.length === 1 && requested[0] === secondHash, "resume downloaded a verified chunk again");
    check(files.get("media/video.bin")?.join(",") === "1,2,3,4,5", "resumed file is corrupt");
    check(
        ![...files.keys()].some((path) => path.endsWith(".checkpoint.json")),
        "completed transfer left its checkpoint behind",
    );
}

async function freshDownloadRejectsWrongWholeFileHash(): Promise<void> {
    const files = new Map<string, Uint8Array>();
    const chunks = new Map([
        [firstHash, new Uint8Array([1, 2])],
        [secondHash, new Uint8Array([3, 4, 5])],
    ]);
    const api = {
        getManifest: async () => manifest,
        getContentChunk: async (hash: string) => chunks.get(hash)!,
    } as any;
    const io = {
        readFile: async (path: string) => {
            const value = files.get(path);
            if (!value) throw new Error("missing");
            return value;
        },
        writeFile: async (path: string, data: Uint8Array) => files.set(path, data.slice()),
        appendFile: async (path: string, data: Uint8Array) => {
            const previous = files.get(path) ?? new Uint8Array();
            const combined = new Uint8Array(previous.length + data.length);
            combined.set(previous);
            combined.set(data, previous.length);
            files.set(path, combined);
        },
        replaceFile: async (staging: string, target: string) => {
            files.set(target, files.get(staging)!.slice());
            files.delete(staging);
        },
        deleteFile: async (path: string) => { files.delete(path); },
        stat: async (path: string) => {
            const value = files.get(path);
            return value ? { mtime: 1, size: value.length } : null;
        },
    } as any;
    const wasm = {
        wasm_hash: (data: Uint8Array) => data[0] === 1 ? firstHash : secondHash,
        Hasher: class {
            update(): void {}
            update_and_hash(data: Uint8Array): string {
                return data[0] === 1 ? firstHash : secondHash;
            }
            finalize(): string { return "0".repeat(64); }
            free(): void {}
        },
    } as any;

    let rejected = false;
    try {
        await applyLargeFile(api, io, "media/wrong.bin", fileHash, 5, wasm);
    } catch {
        rejected = true;
    }
    check(rejected, "wrong whole-file hash was accepted from valid individual chunks");
    check(!files.has("media/wrong.bin"), "wrong assembled file replaced the target");
    check(
        ![...files.keys()].some((path) => path.includes(".part") || path.includes(".checkpoint")),
        "failed whole-file verification left a resumable corrupt staging set",
    );
}

void resumeInterruptedDownload()
    .then(freshDownloadRejectsWrongWholeFileHash)
    .then(() => console.log(`large-transfer.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
