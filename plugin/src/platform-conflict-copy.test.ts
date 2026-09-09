import { strict as assert } from "node:assert";
import type { App } from "obsidian";
import { ObsetyncDesktopIO, ObsetyncMobileIO } from "./platform";

// Actual platform classes, synthetic adapter only. These checks establish
// delegation/ownership, not native host atomicity, fsync, or SDK correctness.
const source = ".obsidian/plugins/obsetync/staging/conflict.tmp";
const target = "notes/conflicts/note.md";
const directory = "notes/conflicts";
type Call = [string, ...unknown[]];
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
async function turns() { for (let count = 0; count < 12; count++) await Promise.resolve(); }
const errno = (code: string) => Object.assign(new Error(`synthetic ${code}`), { code });
const rejection = (promise: Promise<unknown>) => promise.then(
    () => { throw new Error("expected adapter failure"); }, (error: unknown) => error,
);

function fixture(Implementation: typeof ObsetyncDesktopIO) {
    const calls: Call[] = [];
    const files = new Map<string, Uint8Array>([[source, new Uint8Array([1, 2, 3])]]);
    let copy: (from: string, to: string) => Promise<void> = async (from, to) => {
        if (files.has(to)) throw errno("EEXIST");
        const data = files.get(from);
        if (!data) throw errno("ENOENT");
        files.set(to, data.slice());
    };
    let mkdir: (path: string) => Promise<void> = async () => undefined;
    let stat: (path: string) => Promise<unknown> = async () => null;
    let list: (path: string) => Promise<{ files: string[]; folders: string[] }> = async () => ({ files: [], folders: [] });
    const forbidden = (method: string) => (...args: unknown[]) => {
        calls.push([method, ...args]); throw new Error(`forbidden ${method} fallback`);
    };
    const adapter: Record<string, any> = {
        async copy(from: string, to: string) {
            assert.equal(this, adapter, "adapter.copy lost native receiver");
            calls.push(["copy", from, to]); await copy(from, to);
        },
        async mkdir(path: string) { calls.push(["mkdir", path]); await mkdir(path); },
        async stat(path: string) { calls.push(["stat", path]); return stat(path); },
        list(path: string) {
            assert.equal(this, adapter, "adapter.list lost native receiver");
            calls.push(["list", path]); return list(path);
        },
        appendBinary: undefined,
        exists: forbidden("exists"), readBinary: forbidden("readBinary"),
        writeBinary: forbidden("writeBinary"), write: forbidden("write"),
        rename: forbidden("rename"), remove: forbidden("remove"),
        getFullPath: forbidden("getFullPath"),
    };
    const app = { vault: { adapter, getFiles: forbidden("getFiles") } } as unknown as App;
    return {
        io: new Implementation(app), adapter, calls, files,
        setCopy(value: typeof copy) { copy = value; },
        setMkdir(value: typeof mkdir) { mkdir = value; },
        setStat(value: typeof stat) { stat = value; },
        setList(value: typeof list) { list = value; },
    };
}

async function delegatesOnlyAfterDirectoryReady(Implementation: typeof ObsetyncDesktopIO) {
    const f = fixture(Implementation), ready = deferred<void>();
    f.setMkdir(() => ready.promise);
    let settled = false;
    const pending = f.io.copyFileExclusive(source, target).then(() => { settled = true; });
    await turns();
    assert.equal(settled, false); assert.deepEqual(f.calls, [["mkdir", directory]]);
    assert.equal(f.files.has(target), false);
    ready.resolve(); await pending;
    assert.deepEqual(f.calls, [["mkdir", directory], ["copy", source, target]]);
    assert.deepEqual(f.files.get(target), new Uint8Array([1, 2, 3]));
    assert.deepEqual(f.files.get(source), new Uint8Array([1, 2, 3]), "publication removed staging source");
    const root = fixture(Implementation);
    await root.io.copyFileExclusive(source, "root-note.md");
    assert.deepEqual(root.calls, [["copy", source, "root-note.md"]], "root destination caused an invented directory operation");
}

async function realCopyPromiseIsJoined(Implementation: typeof ObsetyncDesktopIO) {
    for (const fails of [false, true]) {
        const f = fixture(Implementation), entered = deferred<void>(), native = deferred<void>();
        const error = errno("EIO");
        f.setCopy(async () => { entered.resolve(); await native.promise; });
        let settled = false;
        const result = f.io.copyFileExclusive(source, target).then(
            () => { settled = true; return undefined; }, failure => { settled = true; return failure; },
        );
        await entered.promise; await turns();
        assert.equal(settled, false, "copy completed while actual adapter promise was still pending");
        assert.deepEqual(f.calls, [["mkdir", directory], ["copy", source, target]]);
        if (fails) native.reject(error); else native.resolve();
        assert.equal(await result, fails ? error : undefined);
        assert.deepEqual(f.calls, [["mkdir", directory], ["copy", source, target]], "copy settlement added fallback IO");
    }
}

async function copyErrorsNeverReplaceOrCleanUp(Implementation: typeof ObsetyncDesktopIO) {
    const exists = fixture(Implementation), previous = new Uint8Array([8, 9]);
    exists.files.set(target, previous);
    const collision = errno("EEXIST");
    exists.setCopy(async () => { throw collision; });
    assert.equal(await rejection(exists.io.copyFileExclusive(source, target)), collision);
    assert.equal(exists.files.get(target), previous, "EEXIST changed the existing destination");
    assert.deepEqual(exists.calls, [["mkdir", directory], ["copy", source, target]]);
    assert(exists.files.has(source));

    const partial = fixture(Implementation), error = errno("EIO"), partialBytes = new Uint8Array([1]);
    partial.setCopy(async (_from, to) => { partial.files.set(to, partialBytes); throw error; });
    assert.equal(await rejection(partial.io.copyFileExclusive(source, target)), error);
    assert.equal(partial.files.get(target), partialBytes, "EIO removed/replaced a possibly partial native copy");
    assert.deepEqual(partial.calls, [["mkdir", directory], ["copy", source, target]]);
    assert(partial.files.has(source), "failed copy removed the verified stage");
}

async function missingCopyAndDirectoryErrors(Implementation: typeof ObsetyncDesktopIO) {
    for (const unsupported of [undefined, null, false, {}]) {
        const f = fixture(Implementation); f.adapter.copy = unsupported;
        await assert.rejects(f.io.copyFileExclusive(source, target), /exclusive file copy is unavailable/);
        assert.deepEqual(f.calls, [], "unsupported copy attempted a directory or replacement fallback");
    }
    const missing = fixture(Implementation); delete missing.adapter.copy;
    await assert.rejects(missing.io.copyFileExclusive(source, target), /exclusive file copy is unavailable/);
    assert.deepEqual(missing.calls, []);

    const exists = fixture(Implementation), collision = errno("EEXIST");
    exists.setMkdir(async () => { throw collision; });
    exists.setStat(async () => ({ type: "folder" }));
    await exists.io.copyFileExclusive(source, target);
    assert.deepEqual(exists.calls, [["mkdir", directory], ["stat", directory], ["copy", source, target]],
        "existing directory check inspected destination or added replacement fallback");
    for (const current of [null, { type: "file" }]) {
        const f = fixture(Implementation), failure = errno("EIO");
        f.setMkdir(async () => { throw failure; }); f.setStat(async () => current);
        assert.equal(await rejection(f.io.copyFileExclusive(source, target)), failure);
        assert.deepEqual(f.calls, [["mkdir", directory], ["stat", directory]], "copy ignored failed parent creation");
    }
}

async function capabilityAndListAreActualAdapterProbes(Implementation: typeof ObsetyncDesktopIO) {
    const f = fixture(Implementation);
    for (const unsupported of [undefined, null, false, 1, "appendBinary", {}]) {
        f.adapter.appendBinary = unsupported;
        assert.equal(f.io.supportsNativeAppend(), false);
    }
    let appendCalls = 0;
    f.adapter.appendBinary = () => { appendCalls++; throw new Error("must not call append during feature probe"); };
    assert.equal(f.io.supportsNativeAppend(), true); assert.equal(appendCalls, 0);
    delete f.adapter.appendBinary;
    assert.equal(f.io.supportsNativeAppend(), false, "feature result cached an unavailable native method");
    assert.deepEqual(f.calls, []);

    const native = deferred<{ files: string[]; folders: string[] }>();
    f.setList(() => native.promise);
    const pending = f.io.listDirectory(directory);
    assert.equal(pending, native.promise, "directory listing wrapped/replaced the actual adapter promise");
    const listing = { files: [`${directory}/a.md`], folders: [`${directory}/nested`] };
    native.resolve(listing);
    assert.equal(await pending, listing, "directory listing transformed the actual SDK result");
    assert.deepEqual(f.calls, [["list", directory]]);

    const error = errno("EIO"); f.setList(async () => { throw error; });
    assert.equal(await rejection(f.io.listDirectory(directory)), error);
    assert.deepEqual(f.calls, [["list", directory], ["list", directory]]);
}

async function run() {
    for (const Implementation of [ObsetyncDesktopIO, ObsetyncMobileIO]) {
        await delegatesOnlyAfterDirectoryReady(Implementation);
        await realCopyPromiseIsJoined(Implementation);
        await copyErrorsNeverReplaceOrCleanUp(Implementation);
        await missingCopyAndDirectoryErrors(Implementation);
        await capabilityAndListAreActualAdapterProbes(Implementation);
    }
    console.log("platform-conflict-copy.test: 10 desktop/mobile suites passed (actual classes, synthetic adapter; no native filesystem claims)");
}
void run().catch(error => { setTimeout(() => { throw error; }, 0); });
