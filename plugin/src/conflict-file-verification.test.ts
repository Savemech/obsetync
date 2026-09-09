import { strict as assert } from "node:assert";
import { blake3 } from "@noble/hashes/blake3";
import { link, mkdtemp, readFile, readdir, readlink, rm, stat, symlink, unlink, writeFile } from "node:fs/promises";
import { linkSync, renameSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { verifyConflictFile, CONFLICT_WHOLE_READ_LIMIT } from "./conflict-file-verification";
import type { PlatformIO } from "./platform";
import { ResourceBudget } from "./resource-budget";

let assertions = 0;
const check = (value: unknown, message: string) => { assertions++; assert.ok(value, message); };
const hash = (bytes: Uint8Array) => Buffer.from(blake3(bytes)).toString("hex");
function gate() { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; }
function instrumentedHasher(update?: (count: number) => void, options: { constructorError?: boolean } = {}) {
    const counts = { created: 0, freed: 0, largest: 0, updates: 0 };
    // Actual BLAKE3 oracle; this does not exercise wasm-bindgen allocation or
    // claim Electron/mobile qualification. Native ranged file IO is real.
    class Hasher {
        private state = blake3.create();
        constructor() { if (options.constructorError) throw new Error("injected constructor failure"); counts.created++; }
        update(bytes: Uint8Array) {
            counts.largest = Math.max(counts.largest, bytes.byteLength);
            this.state.update(bytes); update?.(++counts.updates);
        }
        update_and_hash(bytes: Uint8Array) { this.update(bytes); return hash(bytes); }
        finalize() { return Buffer.from(this.state.digest()).toString("hex"); }
        free() { counts.freed++; this.state.destroy(); }
    }
    return { Hasher, counts };
}
async function rejected(work: Promise<unknown>, message: string) {
    let failed = false; try { await work; } catch { failed = true; } check(failed, message);
}
async function openHandles(directory: string): Promise<number | null> {
    if (process.platform !== "linux") return null;
    const links = await Promise.all((await readdir("/proc/self/fd")).map(async name => {
        try { return await readlink(`/proc/self/fd/${name}`); } catch { return ""; }
    }));
    return links.filter(path => path.startsWith(`${directory}/`)).length;
}
async function nativeFiles() {
    const directory = await mkdtemp(join(tmpdir(), "obsetync-conflict-verify-"));
    const absolute = join(directory, "copy.bin"), replacement = join(directory, "replacement.bin");
    let wholeReads = 0;
    const io = {
        getAbsolutePath: () => absolute,
        stat: async () => { try { const s = await stat(absolute); return { mtime: s.mtimeMs, size: s.size }; }
            catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; } },
        readFile: async () => { wholeReads++; throw new Error("desktop verifier used a whole-file read"); },
    } as unknown as PlatformIO;
    try {
        for (const size of [0, 17, 3 * 1024 * 1024 + 1]) {
            const bytes = new Uint8Array(size); for (let i = 0; i < size; i++) bytes[i] = i % 251;
            await writeFile(absolute, bytes);
            const wasm = instrumentedHasher();
            check(await verifyConflictFile(io, wasm, "copy.bin", hash(bytes), size) === "verified", "native BLAKE3 ranged verification differs");
            check(wasm.counts.largest <= 65536 && wasm.counts.created === 1 && wasm.counts.freed === 1, "native verifier exceeded feed/handle bound");
            check(await verifyConflictFile(io, instrumentedHasher(), "copy.bin", "0".repeat(64), size) === "different", "native wrong expected hash was accepted");
        }
        const bytes = new Uint8Array(200000).fill(12); await writeFile(absolute, bytes);
        const changed = instrumentedHasher(count => {
            if (count === 1) { writeFileSync(replacement, bytes); renameSync(replacement, absolute); }
        });
        await rejected(verifyConflictFile(io, changed, "copy.bin", hash(bytes), bytes.length), "path replacement escaped descriptor/path fingerprint check");
        check(changed.counts.freed === 1, "path drift leaked a hasher");
        const controller = new AbortController();
        const aborted = instrumentedHasher(() => controller.abort());
        await rejected(verifyConflictFile(io, aborted, "copy.bin", hash(bytes), bytes.length, controller.signal), "abort during native feed was ignored");
        check(aborted.counts.updates === 1 && aborted.counts.freed === 1, "aborted feed continued or leaked its handle");
        const failing = instrumentedHasher(() => { throw new Error("injected feed failure"); });
        await rejected(verifyConflictFile(io, failing, "copy.bin", hash(bytes), bytes.length), "feed failure was hidden");
        check(failing.counts.freed === 1, "throwing feed leaked hasher");
        await rejected(verifyConflictFile(io, instrumentedHasher(undefined, { constructorError: true }), "copy.bin", hash(bytes), bytes.length), "constructor failure was hidden");
        const descriptors = await openHandles(directory);
        if (descriptors !== null) check(descriptors === 0, "native success/failure left actual file descriptors open");
        check(wholeReads === 0, "native verifier called adapter.readBinary");
        check((await readFile(absolute)).equals(Buffer.from(bytes)), "verification changed source bytes");
        await rm(absolute);
        check(await verifyConflictFile(io, instrumentedHasher(), "copy.bin", hash(bytes), bytes.length) === "missing", "confirmed missing native file differs");
    } finally { await rm(directory, { recursive: true, force: true }); }
}
async function portableReads() {
    const bytes = new Uint8Array(100000).fill(9); let reads = 0;
    let size = bytes.length, mtime = 1;
    const io = { getAbsolutePath: () => null, stat: async () => ({ size, mtime }),
        readFile: async () => { reads++; return Uint8Array.from(bytes); } } as unknown as PlatformIO;
    const wasm = instrumentedHasher();
    check(await verifyConflictFile(io, wasm, "copy.bin", hash(bytes), bytes.length) === "verified", "portable small BLAKE3 verification differs");
    check(reads === 1 && wasm.counts.largest <= 65536 && wasm.counts.freed === 1, "portable feed/cleanup differs");
    size = CONFLICT_WHOLE_READ_LIMIT + 1;
    check(await verifyConflictFile(io, instrumentedHasher(), "copy.bin", hash(bytes), size) === "reader-unavailable" && reads === 1, "large mobile ambiguity triggered native whole-file allocation");
    size = bytes.length;
    io.readFile = async () => new Uint8Array(bytes.length + 1).subarray(0, bytes.length);
    const backing = instrumentedHasher();
    await rejected(verifyConflictFile(io, backing, "copy.bin", hash(bytes), size), "oversized backing view escaped source admission");
    check(backing.counts.created === 0, "oversized backing was hashed before rejection");
    io.readFile = async () => { mtime++; return Uint8Array.from(bytes); };
    check(await verifyConflictFile(io, instrumentedHasher(), "copy.bin", hash(bytes), size) === "different", "post-read metadata drift was accepted");
    const entered = gate(), release = gate(), controller = new AbortController();
    io.readFile = async () => { entered.resolve(); await release.promise; return Uint8Array.from(bytes); };
    const delayed = instrumentedHasher(); let settled = false;
    const work = verifyConflictFile(io, delayed, "copy.bin", hash(bytes), size, controller.signal).finally(() => { settled = true; });
    const outcome = work.then(() => false, () => true);
    await entered.promise; controller.abort(); await Promise.resolve();
    check(!settled, "abort pretended uncancellable adapter read had finished");
    release.resolve(); check(await outcome, "aborted adapter read was reported as verified");
    check(delayed.counts.created === 0, "cancelled native result entered hashing");
}
async function nativeAliases() {
    const directory = await mkdtemp(join(tmpdir(), "obsetync-conflict-alias-"));
    const original = join(directory, "original.bin"), symbolic = join(directory, "symbolic.bin"), hard = join(directory, "hard.bin");
    const bytes = new Uint8Array(150000).fill(17);
    let wholeReads = 0;
    const ioFor = (absolute: string) => ({
        getAbsolutePath: () => absolute,
        stat: async () => { const value = await stat(absolute); return { size: value.size, mtime: value.mtimeMs }; },
        readFile: async () => { wholeReads++; throw new Error("alias verification used an unqualified whole-file fallback"); },
    }) as unknown as PlatformIO;
    try {
        await writeFile(original, bytes);
        await symlink(original, symbolic, "file");
        const symlinkHasher = instrumentedHasher();
        check(await verifyConflictFile(ioFor(symbolic), symlinkHasher, "symbolic.bin", hash(bytes), bytes.length) === "different",
            "a symlink to the original was accepted as an independent conflict copy");
        check(symlinkHasher.counts.created === 0, "known symlink entered hashing before rejection");
        await unlink(symbolic);
        await link(original, hard);
        const hardlinkHasher = instrumentedHasher();
        check(await verifyConflictFile(ioFor(hard), hardlinkHasher, "hard.bin", hash(bytes), bytes.length) === "different",
            "a hardlink to the original was accepted as an independent conflict copy");
        check(hardlinkHasher.counts.created === 0, "known hardlink entered hashing before rejection");
        await unlink(hard);
        const linkedDuringRead = instrumentedHasher(count => { if (count === 1) linkSync(original, hard); });
        await rejected(verifyConflictFile(ioFor(original), linkedDuringRead, "original.bin", hash(bytes), bytes.length),
            "a file acquiring a hardlink during hashing was accepted");
        check(linkedDuringRead.counts.created === 1 && linkedDuringRead.counts.freed === 1,
            "new hardlink rejection leaked a hasher");
        await unlink(hard);
        check(wholeReads === 0 && (await readFile(original)).equals(Buffer.from(bytes)), "alias verification read through fallback or changed original content");
        const descriptors = await openHandles(directory);
        if (descriptors !== null) check(descriptors === 0, "alias rejection leaked a native descriptor");
    } finally { await rm(directory, { recursive: true, force: true }); }
}
async function queuedPortableChanges() {
    for (const mode of ["growth", "mtime", "missing", "abort-in-fresh-stat"] as const) {
        const bytes = new Uint8Array(32).fill(9), pool = new ResourceBudget({ capacityBytes: 1024 * 1024 });
        const owner = await pool.reserve(1024 * 1024), queued = gate(), controller = new AbortController();
        let reads = 0, stats = 0, size = bytes.length, mtime = 1, missing = false;
        const io = { getAbsolutePath: () => null,
            stat: async () => {
                stats++;
                if (mode === "abort-in-fresh-stat" && stats === 2) controller.abort();
                return missing ? null : { size, mtime };
            },
            readFile: async () => { reads++; return new Uint8Array(size); },
        } as unknown as PlatformIO;
        const wasm = instrumentedHasher();
        const work = verifyConflictFile(io, wasm, "copy.bin", hash(bytes), bytes.length, controller.signal,
            { budget: { reserve: (count, options) => {
                const reservation = pool.reserve(count, options); queued.resolve(); return reservation;
            } } });
        const result = work.then(() => false, () => true);
        await queued.promise;
        check(pool.snapshot().queuedRequests === 1 && reads === 0, "portable verification allocated before admission");
        if (mode === "growth") size = CONFLICT_WHOLE_READ_LIMIT + 1;
        else if (mode === "mtime") mtime++;
        else if (mode === "missing") missing = true;
        owner.release();
        check(await result, `queued ${mode} change was accepted`);
        check(stats === 2 && reads === 0 && wasm.counts.created === 0,
            `queued ${mode} reached native whole read or hashing before its fresh metadata fence`);
        check(pool.snapshot().usedBytes === 0 && pool.snapshot().queuedRequests === 0,
            `queued ${mode} rejection leaked admission ownership`);
    }
}
void (async () => {
    await nativeFiles(); await portableReads(); await nativeAliases(); await queuedPortableChanges();
    console.log(`conflict-file-verification: ${assertions} assertions passed (native IO + BLAKE3 oracle)`);
})().catch(error => { console.error(error); process.exitCode = 1; });
