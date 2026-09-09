import {
    applyLargeFile,
    LARGE_TRANSFER_STAGING_LIMITS,
    LargeTransferStagingQuotaError,
    validateManifest,
} from "./pull";
import { BulkObjectKind } from "./bulk-codec";
import { appendBinaryBounded } from "./bounded-append";
import { ResourceBudget, ResourceBudgetOversizedError } from "./resource-budget";
import { estimateTransportWorkset, reserveTransientScope, type TransientWorkScope } from "./transient-memory";

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
    let transferDirectoryExists = false;
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
        mkdir: async () => { transferDirectoryExists = true; },
        listDirectory: async (path: string) => {
            if (!transferDirectoryExists) throw new Error("missing internal transfer directory");
            return {
                files: [...files.keys()].filter((item) => item.startsWith(`${path}/`)), folders: [],
            };
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
        listDirectory: async (path: string) => ({
            files: [...files.keys()].filter((item) => item.startsWith(`${path}/`)), folders: [],
        }),
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

async function ownedLegacyAppendDefersAndNativeResumeRetainsCheckpoint(): Promise<void> {
    const chunkSize = 1024 * 1024;
    const largeManifest = {
        file_hash: fileHash, total_size: 2 * chunkSize,
        chunks: [
            { hash: firstHash, offset: 0, size: chunkSize },
            { hash: secondHash, offset: chunkSize, size: chunkSize },
        ],
    };
    const ownerBytes = chunkSize;
    const workBytes = estimateTransportWorkset(chunkSize);
    const budget = new ResourceBudget({ capacityBytes: ownerBytes + workBytes });
    const files = new Map<string, Uint8Array>();
    let prefixReads = 0;
    let nativeAvailable = false;
    let nativeActive = false;
    let abort = false;
    let releaseNative!: () => void;
    const nativeGate = new Promise<void>((resolve) => { releaseNative = resolve; });
    let nativeEntered!: () => void;
    const nativeStarted = new Promise<void>((resolve) => { nativeEntered = resolve; });
    const requested: string[] = [];
    let released = 0;
    const api = {
        getObjectsOwned: async (kind: BulkObjectKind, hashes: string[]) => {
            check(budget.snapshot().usedBytes === 0, "next owned chunk nested under previous parent");
            const memory = await reserveTransientScope({ ownerBytes, workBytes }, { budget });
            const objects = new Map<string, Uint8Array>();
            for (const hash of hashes) {
                if (kind === BulkObjectKind.Manifest) {
                    objects.set(hash, new TextEncoder().encode(JSON.stringify(largeManifest)));
                } else {
                    requested.push(hash);
                    const data = new Uint8Array(chunkSize);
                    data[0] = hash === firstHash ? 1 : 2;
                    objects.set(hash, data);
                }
            }
            return { objects, memory, release() {
                check(!nativeActive, "owned chunk released while native append was unfinished");
                objects.clear(); memory.close(); released++;
            } };
        },
        getObjects: async () => { throw new Error("owned large transfer used naked map"); },
    } as any;
    const stat = async (path: string) => files.has(path) ? { size: files.get(path)!.length, mtime: 1 } : null;
    const io = {
        readFile: async (path: string) => {
            const data = files.get(path);
            if (!data) throw new Error("missing");
            return data;
        },
        stat,
        writeFile: async (path: string, data: Uint8Array) => {
            const checkpoint = path.endsWith(".checkpoint.json")
                ? JSON.parse(new TextDecoder().decode(data)) : null;
            if (checkpoint?.bytesWritten > 0 && checkpoint.completedHash === undefined) {
                check(budget.snapshot().usedBytes > 0, "chunk checkpoint lost owned lease");
            }
            files.set(path, data.slice());
        },
        deleteFile: async (path: string) => { files.delete(path); },
        replaceFile: async (source: string, target: string) => {
            files.set(target, files.get(source)!); files.delete(source);
        },
        appendFile: async () => { throw new Error("production append bypassed bounded helper"); },
        appendFileOwned: async (path: string, data: Uint8Array, memory: TransientWorkScope) => {
            await appendBinaryBounded({
                readBinary: async () => { prefixReads++; return files.get(path)!.slice().buffer; },
                writeBinary: async (_path, value) => { files.set(path, new Uint8Array(value)); },
                appendBinary: nativeAvailable ? async (_path, value) => {
                    nativeActive = true;
                    nativeEntered();
                    await nativeGate;
                    const previous = files.get(path)!;
                    const combined = new Uint8Array(previous.length + value.byteLength);
                    combined.set(previous); combined.set(new Uint8Array(value), previous.length);
                    files.set(path, combined);
                    nativeActive = false;
                } : undefined,
            }, stat, path, data, { memory });
        },
        listDirectory: async (path: string) => ({
            files: [...files.keys()].filter((item) => item.startsWith(`${path}/`)), folders: [],
        }),
    } as any;
    const wasm = {
        wasm_hash: (data: Uint8Array) => data[0] === 1 ? firstHash : secondHash,
        Hasher: class {
            update(): void {}
            update_and_hash(data: Uint8Array): string { return data[0] === 1 ? firstHash : secondHash; }
            finalize(): string { return fileHash; }
            free(): void {}
        },
    } as any;
    let error: unknown;
    try { await applyLargeFile(api, io, "large.bin", fileHash, 2 * chunkSize, wasm); }
    catch (value) { error = value; }
    check(error instanceof ResourceBudgetOversizedError, "unsafe legacy prefix was not deferred by byte admission");
    check(prefixReads === 1, "oversized second prefix was reread before admission");
    const partPath = [...files.keys()].find((path) => path.endsWith(".part"))!;
    const checkpointPath = [...files.keys()].find((path) => path.endsWith(".checkpoint.json"))!;
    check(files.get(partPath)?.length === chunkSize, "deferred legacy append erased recoverable prefix");
    check(JSON.parse(new TextDecoder().decode(files.get(checkpointPath))).nextChunk === 1,
        "failed append advanced or erased checkpoint");
    check(budget.snapshot().usedBytes === 0 && released === 3, "failed legacy download leaked owned buffers");

    nativeAvailable = true;
    requested.length = 0;
    const resuming = applyLargeFile(api, io, "large.bin", fileHash, 2 * chunkSize, wasm, () => abort);
    const outcome = resuming.then(() => undefined, (value) => value);
    await nativeStarted;
    abort = true;
    check(budget.snapshot().usedBytes > 0, "in-flight native append lost global accounting");
    releaseNative();
    check(!!await outcome, "local edit during native append did not defer promotion");
    check(!files.has("large.bin") && files.get(partPath)?.length === 2 * chunkSize,
        "local edit promoted target or lost verified staging");
    check(JSON.parse(new TextDecoder().decode(files.get(checkpointPath))).nextChunk === 2,
        "settled native append did not durably checkpoint before local deferral");
    check(requested.join() === secondHash && prefixReads === 1, "native resume reread/downloaded verified prefix");
    check(budget.snapshot().usedBytes === 0, "deferred native promotion leaked ownership");
    abort = false;
    requested.length = 0;
    await applyLargeFile(api, io, "large.bin", fileHash, 2 * chunkSize, wasm);
    check(requested.length === 0 && files.get("large.bin")?.length === 2 * chunkSize,
        "completed checkpoint did not promote without another download");
    check(!files.has(checkpointPath) && budget.snapshot().usedBytes === 0, "completed owned transfer leaked state");
}

async function scopedRestartRejectsStaleRootAndLocalGeneration(): Promise<void> {
    const transferDir = ".obsidian/plugins/obsetync/transfers";
    const target = "scoped-large.bin";
    const files = new Map<string, Uint8Array>();
    const mtimes = new Map<string, number>();
    const requested: string[] = [];
    let failSecond = true;
    const chunks = new Map([
        [firstHash, new Uint8Array([1, 2])],
        [secondHash, new Uint8Array([3, 4, 5])],
    ]);
    const api = {
        getManifest: async () => manifest,
        getContentChunk: async (hash: string) => {
            requested.push(hash);
            if (hash === secondHash && failSecond) throw new Error("kill boundary");
            return chunks.get(hash)!;
        },
    } as any;
    const io = {
        readFile: async (path: string) => {
            const value = files.get(path);
            if (!value) throw new Error("missing");
            return value;
        },
        writeFile: async (path: string, data: Uint8Array) => {
            files.set(path, data.slice()); mtimes.set(path, 1);
        },
        appendFile: async (path: string, data: Uint8Array) => {
            const previous = files.get(path) ?? new Uint8Array();
            const combined = new Uint8Array(previous.length + data.length);
            combined.set(previous); combined.set(data, previous.length);
            files.set(path, combined); mtimes.set(path, 1);
        },
        replaceFile: async (staging: string, destination: string) => {
            files.set(destination, files.get(staging)!.slice()); mtimes.set(destination, 1);
            files.delete(staging); mtimes.delete(staging);
        },
        renameFile: async (source: string, destination: string) => {
            files.set(destination, files.get(source)!.slice());
            mtimes.set(destination, mtimes.get(source) ?? 1);
            files.delete(source); mtimes.delete(source);
        },
        deleteFile: async (path: string) => { files.delete(path); mtimes.delete(path); },
        stat: async (path: string) => files.has(path)
            ? { size: files.get(path)!.length, mtime: mtimes.get(path) ?? 1 } : null,
        exists: async (path: string) => files.has(path),
        listDirectory: async () => ({
            files: [...files.keys()].filter(path => path.startsWith(`${transferDir}/`)), folders: [],
        }),
    } as any;
    const wasm = {
        wasm_hash: (data: Uint8Array) => data[0] === 1 ? firstHash : secondHash,
        Hasher: class {
            update(): void {}
            update_and_hash(data: Uint8Array): string { return data[0] === 1 ? firstHash : secondHash; }
            finalize(): string { return fileHash; }
            free(): void {}
        },
    } as any;
    const scopeA = { vaultId: "vault-a", rootScope: "1".repeat(64),
        fileGeneration: "2".repeat(64), targetState: { present: false, size: 0, mtime: 0 } };
    const scopeB = { ...scopeA, rootScope: "3".repeat(64) };

    const staleKey = `${"c".repeat(64)}-${"d".repeat(16)}`;
    const otherKey = `${"d".repeat(64)}-${"e".repeat(16)}`;
    const staleCheckpoint = `${transferDir}/${staleKey}.checkpoint.json`;
    const stalePart = `${transferDir}/${staleKey}.part`;
    const otherCheckpoint = `${transferDir}/${otherKey}.checkpoint.json`;
    const otherPart = `${transferDir}/${otherKey}.part`;
    files.set(stalePart, new Uint8Array([7]));
    files.set(staleCheckpoint, new TextEncoder().encode(JSON.stringify({
        version: 2, ...scopeA, targetPath: target, fileHash: "c".repeat(64),
        manifestGeneration: "e".repeat(64), totalSize: 1, nextChunk: 1, bytesWritten: 1,
    })));
    files.set(otherPart, new Uint8Array([8]));
    files.set(otherCheckpoint, new TextEncoder().encode(JSON.stringify({
        version: 2, ...scopeA, targetPath: "other.bin", fileHash: "d".repeat(64),
        manifestGeneration: "f".repeat(64), totalSize: 1, nextChunk: 1, bytesWritten: 1,
    })));

    let interrupted = false;
    try { await applyLargeFile(api, io, target, fileHash, 5, wasm,
        undefined, false, undefined, undefined, scopeA); }
    catch { interrupted = true; }
    check(interrupted, "scoped fixture did not stop at its kill boundary");
    const checkpointPath = [...files.keys()].find(path => path.endsWith(".checkpoint.json") &&
        path !== otherCheckpoint)!;
    const partPath = checkpointPath.replace(".checkpoint.json", ".part");
    const checkpoint = JSON.parse(new TextDecoder().decode(files.get(checkpointPath)!));
    check(checkpoint.version === 2 && checkpoint.vaultId === scopeA.vaultId &&
        checkpoint.rootScope === scopeA.rootScope && checkpoint.nextChunk === 1 &&
        /^[0-9a-f]{64}$/.test(checkpoint.manifestGeneration),
    "kill checkpoint lost its vault/root/chunk generation scope");
    check(!files.has(stalePart) && !files.has(staleCheckpoint) &&
        files.has(otherPart) && files.has(otherCheckpoint),
    "bounded stale cleanup crossed target ownership or kept an obsolete pair");

    // Even identical file/root scope cannot mix a prefix with another chunk
    // layout generation. Corrupt the fence as a kill/restart stand-in and
    // require a chunk-zero restart (the injected second-chunk failure remains).
    checkpoint.manifestGeneration = "0".repeat(64);
    await io.writeFile(checkpointPath, new TextEncoder().encode(JSON.stringify(checkpoint)));
    requested.length = 0;
    try { await applyLargeFile(api, io, target, fileHash, 5, wasm,
        undefined, false, undefined, undefined, scopeA); } catch {}
    check(requested.join(",") === `${firstHash},${secondHash}` && files.get(partPath)?.length === 2,
        "manifest generation mismatch reused an incompatible prefix");

    // A local target appeared after the checkpoint. The old generation must
    // win immediately, before another manifest/chunk request or staging reset.
    files.set(target, new Uint8Array([9])); mtimes.set(target, 77);
    requested.length = 0;
    let localDeferred = false;
    try { await applyLargeFile(api, io, target, fileHash, 5, wasm,
        undefined, true, undefined, undefined, scopeB); }
    catch { localDeferred = true; }
    check(localDeferred && requested.length === 0 && files.get(target)?.[0] === 9 &&
        files.get(partPath)?.length === 2,
    "local generation lost or stale prefix was erased before deferral");

    // A fresh pull generation may preserve those local bytes, but cannot reuse
    // the old root's prefix: it must restart at chunk zero.
    failSecond = false;
    requested.length = 0;
    const currentScope = { ...scopeB, targetState: { present: true, size: 1, mtime: 77 } };
    await applyLargeFile(api, io, target, fileHash, 5, wasm,
        undefined, true, undefined, undefined, currentScope);
    check(requested.join(",") === `${firstHash},${secondHash}`,
        "different root/local generation reused the old verified prefix");
    check(files.get(target)?.join(",") === "1,2,3,4,5" &&
        [...files.keys()].some(path => path.includes("local-before-pull")),
    "fresh scoped restart lost local bytes or failed to promote verified content");
    check(!files.has(checkpointPath) && !files.has(partPath),
        "completed scoped restart left its staging pair behind");
}

async function stagingQuotaFailsClosedWithoutTouchingUnprovenEntries(): Promise<void> {
    const transferDir = ".obsidian/plugins/obsetync/transfers";
    const encoder = new TextEncoder();
    const scope = {
        vaultId: "quota-vault", rootScope: "1".repeat(64), fileGeneration: "2".repeat(64),
        targetState: { present: false, size: 0, mtime: 0 },
    };
    const checkpoint = (key: string, targetPath: string, totalSize: number, bytesWritten: number) =>
        encoder.encode(JSON.stringify({
            version: 2, ...scope, targetPath, fileHash: key.slice(0, 64),
            manifestGeneration: "3".repeat(64), totalSize,
            nextChunk: bytesWritten === 0 ? 0 : 1, bytesWritten,
        }));
    const run = async (
        setup: (files: Map<string, Uint8Array>, sizes: Map<string, number>) => string | undefined,
        succeeds = false,
        listingTransform?: (paths: string[], call: number) => string[],
    ) => {
        const files = new Map<string, Uint8Array>();
        const sizes = new Map<string, number>();
        const failDelete = setup(files, sizes);
        const before = new Map(files);
        let chunks = 0;
        let listingCalls = 0;
        const api = {
            getManifest: async () => manifest,
            getContentChunk: async (hash: string) => {
                chunks++;
                return hash === firstHash ? new Uint8Array([1, 2]) : new Uint8Array([3, 4, 5]);
            },
        } as any;
        const allPaths = () => new Set([...files.keys(), ...sizes.keys()]);
        const io = {
            readFile: async (path: string) => {
                const value = files.get(path);
                if (!value) throw new Error("missing");
                return value;
            },
            writeFile: async (path: string, data: Uint8Array) => { files.set(path, data.slice()); },
            appendFile: async (path: string, data: Uint8Array) => {
                const previous = files.get(path) ?? new Uint8Array();
                const combined = new Uint8Array(previous.byteLength + data.byteLength);
                combined.set(previous); combined.set(data, previous.byteLength); files.set(path, combined);
            },
            replaceFile: async (source: string, target: string) => {
                files.set(target, files.get(source)!.slice()); files.delete(source);
            },
            deleteFile: async (path: string) => {
                if (path === failDelete) throw new Error("injected cleanup failure");
                files.delete(path); sizes.delete(path);
            },
            stat: async (path: string) => {
                const size = sizes.get(path) ?? files.get(path)?.byteLength;
                return size === undefined ? null : { size, mtime: 1 };
            },
            listDirectory: async (path: string) => {
                check(path === transferDir, "quota maintenance enumerated outside transfer dir");
                const paths = [...allPaths()].filter((item) => item.startsWith(`${path}/`));
                return {
                    files: listingTransform
                        ? listingTransform(paths, ++listingCalls)
                        : paths,
                    folders: [],
                };
            },
        } as any;
        const wasm = {
            wasm_hash: (data: Uint8Array) => data[0] === 1 ? firstHash
                : data[0] === 3 ? secondHash : "4".repeat(64),
            Hasher: class {
                update(): void {}
                update_and_hash(data: Uint8Array): string {
                    return data[0] === 1 ? firstHash : secondHash;
                }
                finalize(): string { return fileHash; }
                free(): void {}
            },
        } as any;
        let error: unknown;
        try {
            await applyLargeFile(api, io, "quota-target.bin", fileHash, 5, wasm,
                undefined, false, undefined, undefined, scope);
        } catch (value) { error = value; }
        if (succeeds) {
            check(error === undefined, `bounded opaque staging entry blocked transfer: ${String(error)}`);
            check(chunks === 2 && files.get("quota-target.bin")?.byteLength === 5,
                "admitted transfer with opaque internal state did not complete");
        } else {
            check(error instanceof LargeTransferStagingQuotaError,
                `unproven/full staging state did not defer with a quota error: ${String(error)}`);
            check(chunks === 0, "quota refusal started content download");
        }
        if (failDelete) {
            const authority = failDelete.slice(0, -".part".length) + ".checkpoint.json";
            check(files.has(failDelete) && !files.has(authority),
                "failed stale-part cleanup left resumable checkpoint authority behind");
        } else for (const [path, bytes] of before) {
            check(files.get(path) === bytes, `quota refusal changed internal entry ${path}`);
        }
    };

    await run((files, sizes) => {
        const key = `${"5".repeat(64)}-${"6".repeat(16)}`;
        const part = `${transferDir}/${key}.part`;
        files.set(part, new Uint8Array());
        sizes.set(part, LARGE_TRANSFER_STAGING_LIMITS.bytes);
        files.set(`${transferDir}/${key}.checkpoint.json`,
            checkpoint(key, "other-large.bin", LARGE_TRANSFER_STAGING_LIMITS.bytes,
                LARGE_TRANSFER_STAGING_LIMITS.bytes));
        return undefined;
    });

    await run((files) => {
        files.set(`${transferDir}/unrelated.keep`, new Uint8Array([1]));
        return undefined;
    }, true);

    await run((files) => {
        const key = `${"7".repeat(64)}-${"8".repeat(16)}`;
        files.set(`${transferDir}/${key}.part`, new Uint8Array([1]));
        files.set(`${transferDir}/${key}.checkpoint.json`, encoder.encode("{malformed"));
        return undefined;
    }, true);

    await run((files) => {
        const currentKey = `${fileHash}-${"4".repeat(16)}`;
        files.set(`${transferDir}/${currentKey}.part`, new Uint8Array([1]));
        files.set(`${transferDir}/${currentKey}.checkpoint.json`, encoder.encode("{malformed-current"));
        return undefined;
    });

    await run((files) => {
        const currentKey = `${fileHash}-${"4".repeat(16)}`;
        files.set(`${transferDir}/${currentKey}.part`, new Uint8Array([1]));
        files.set(`${transferDir}/${currentKey}.checkpoint.json`,
            checkpoint(currentKey, "different-target.bin", 1, 1));
        return undefined;
    });

    await run((files) => {
        for (let index = 0; index < LARGE_TRANSFER_STAGING_LIMITS.files / 2; index++) {
            const hash = index.toString(16).padStart(64, "0");
            const key = `${hash}-${(index + 1).toString(16).padStart(16, "0")}`;
            files.set(`${transferDir}/${key}.part`, new Uint8Array());
            files.set(`${transferDir}/${key}.checkpoint.json`, checkpoint(key, `other-${index}.bin`, 0, 0));
        }
        return undefined;
    });

    await run((files) => {
        const key = `${"9".repeat(64)}-${"a".repeat(16)}`;
        const part = `${transferDir}/${key}.part`;
        files.set(part, new Uint8Array([9]));
        files.set(`${transferDir}/${key}.checkpoint.json`,
            checkpoint(key, "quota-target.bin", 1, 1));
        return part;
    });

    await run((files, sizes) => {
        const first = `${transferDir}/opaque-a`;
        const second = `${transferDir}/opaque-b`;
        files.set(first, new Uint8Array()); sizes.set(first, Number.MAX_SAFE_INTEGER);
        files.set(second, new Uint8Array()); sizes.set(second, 1);
        return undefined;
    });

    await run((files) => {
        files.set(`${transferDir}/duplicate.keep`, new Uint8Array([1]));
        return undefined;
    }, false, (paths) => [paths[0], paths[0]]);

    await run(() => undefined, false,
        () => [`${transferDir}/../outside-user-path`]);

    await run((files) => {
        files.set(`${transferDir}/stable.keep`, new Uint8Array([1]));
        return undefined;
    }, false, (paths, call) => call === 1
        ? paths
        : [...paths, `${transferDir}/late.keep`]);

    let lateCurrentFiles!: Map<string, Uint8Array>;
    const lateCurrentKey = `${fileHash}-${"4".repeat(16)}`;
    const latePart = `${transferDir}/${lateCurrentKey}.part`;
    const lateCheckpoint = `${transferDir}/${lateCurrentKey}.checkpoint.json`;
    await run((files) => {
        lateCurrentFiles = files;
        return undefined;
    }, false, (paths, call) => {
        if (call === 1) {
            lateCurrentFiles.set(latePart, new Uint8Array([1]));
            lateCurrentFiles.set(lateCheckpoint,
                checkpoint(lateCurrentKey, "quota-target.bin", 1, 1));
            return [...paths, latePart, lateCheckpoint];
        }
        return paths;
    });
    check(lateCurrentFiles.get(latePart)?.[0] === 1 && lateCurrentFiles.has(lateCheckpoint),
        "current staging pair appearing during admission was overwritten or deleted");

    let listed = false;
    let invalidPathError: unknown;
    try {
        await applyLargeFile({ getManifest: async () => manifest } as any, {
            stat: async () => null,
            listDirectory: async () => { listed = true; return { files: [], folders: [] }; },
        } as any, "bad-key.bin", fileHash, 5, {
            wasm_hash: () => "not-a-canonical-hash",
            Hasher: class {
                update(): void {}
                finalize(): string { return fileHash; }
                free(): void {}
            },
        } as any);
    } catch (error) { invalidPathError = error; }
    check(String(invalidPathError).includes("path generation") && !listed,
        "non-canonical path generation reached staging enumeration");
}

async function stagingLeaseReleasesAcrossPromotionFailureAndSuccess(): Promise<void> {
    const files = new Map<string, Uint8Array>();
    let failPromotion = true;
    let chunkRequests = 0;
    const scope = {
        vaultId: "lease-vault", rootScope: "1".repeat(64), fileGeneration: "2".repeat(64),
        targetState: { present: false, size: 0, mtime: 0 },
    };
    const api = {
        getManifest: async () => manifest,
        getContentChunk: async (hash: string) => {
            chunkRequests++;
            return hash === firstHash ? new Uint8Array([1, 2]) : new Uint8Array([3, 4, 5]);
        },
    } as any;
    const io = {
        readFile: async (path: string) => {
            const value = files.get(path); if (!value) throw new Error("missing"); return value;
        },
        writeFile: async (path: string, data: Uint8Array) => { files.set(path, data.slice()); },
        appendFile: async (path: string, data: Uint8Array) => {
            const previous = files.get(path) ?? new Uint8Array();
            const combined = new Uint8Array(previous.byteLength + data.byteLength);
            combined.set(previous); combined.set(data, previous.byteLength); files.set(path, combined);
        },
        replaceFile: async (source: string, target: string) => {
            if (failPromotion) throw new Error("injected promotion failure");
            files.set(target, files.get(source)!.slice()); files.delete(source);
        },
        deleteFile: async (path: string) => { files.delete(path); },
        stat: async (path: string) => files.has(path)
            ? { size: files.get(path)!.byteLength, mtime: 1 } : null,
        listDirectory: async (path: string) => ({
            files: [...files.keys()].filter((item) => item.startsWith(`${path}/`)), folders: [],
        }),
    } as any;
    const wasm = {
        wasm_hash: (data: Uint8Array) => data[0] === 1 ? firstHash
            : data[0] === 3 ? secondHash : "4".repeat(64),
        Hasher: class {
            update(): void {}
            update_and_hash(data: Uint8Array): string {
                return data[0] === 1 ? firstHash : secondHash;
            }
            finalize(): string { return fileHash; }
            free(): void {}
        },
    } as any;

    let failed = false;
    try {
        await applyLargeFile(api, io, "lease-target.bin", fileHash, 5, wasm,
            undefined, false, undefined, undefined, scope);
    } catch { failed = true; }
    check(failed && chunkRequests === 2, "promotion failure fixture did not reach its terminal branch");

    failPromotion = false;
    chunkRequests = 0;
    await applyLargeFile(api, io, "lease-target.bin", fileHash, 5, wasm,
        undefined, false, undefined, undefined, scope);
    check(chunkRequests === 0 && files.get("lease-target.bin")?.byteLength === 5,
        "promotion throw leaked lease or lost its completed resumable pair");

    chunkRequests = 0;
    await applyLargeFile(api, io, "lease-target.bin", fileHash, 5, wasm,
        undefined, false, undefined, undefined,
        { ...scope, targetState: { present: true, size: 5, mtime: 1 } });
    check(chunkRequests === 2 && files.get("lease-target.bin")?.byteLength === 5,
        "successful promotion retained its staging lease");

    const tornTarget = "torn-target.bin";
    const tornKey = `${fileHash}-${"4".repeat(16)}`;
    const transferDir = ".obsidian/plugins/obsetync/transfers";
    const tornPart = `${transferDir}/${tornKey}.part`;
    const tornCheckpoint = `${transferDir}/${tornKey}.checkpoint.json`;
    files.set(tornPart, new Uint8Array([1, 2, 3, 4, 5]));
    files.set(tornCheckpoint, new TextEncoder().encode(JSON.stringify({
        version: 2,
        ...scope,
        targetPath: tornTarget,
        fileHash,
        manifestGeneration: fileHash,
        totalSize: 5,
        nextChunk: 1,
        bytesWritten: 2,
    })));
    chunkRequests = 0;
    await applyLargeFile(api, io, tornTarget, fileHash, 5, wasm,
        undefined, false, undefined, undefined, scope);
    check(chunkRequests === 2 && files.get(tornTarget)?.join(",") === "1,2,3,4,5" &&
        !files.has(tornPart) && !files.has(tornCheckpoint),
    "uncheckpointed append tail was not safely restarted from its scoped pair");
}

void resumeInterruptedDownload()
    .then(freshDownloadRejectsWrongWholeFileHash)
    .then(ownedLegacyAppendDefersAndNativeResumeRetainsCheckpoint)
    .then(scopedRestartRejectsStaleRootAndLocalGeneration)
    .then(stagingQuotaFailsClosedWithoutTouchingUnprovenEntries)
    .then(stagingLeaseReleasesAcrossPromotionFailureAndSuccess)
    .then(() => console.log(`large-transfer.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
