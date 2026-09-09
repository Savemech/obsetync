import assert from "node:assert/strict";
import { test } from "node:test";
import * as fs from "node:fs/promises";
import { join } from "node:path";
import { createNativeRootIO } from "./lib/native-root-io.mjs";

async function fixture(work, options) {
    // Only this caller creates/deletes the exact known private fixture. The
    // adapter has no creation or broad/recursive cleanup authority.
    const directory = await fs.mkdtemp("/tmp/obsetync-native-root-");
    try { return await work(await createNativeRootIO(directory, options), directory); }
    finally { await fs.rm(directory, { recursive: true, force: true }); }
}
const code = expected => error => error?.code === expected;
function gate() {
    let resolve;
    const promise = new Promise(done => { resolve = done; });
    return { promise, resolve };
}
function idle(snapshot) {
    for (const group of Object.values(snapshot)) {
        assert.equal(group.inFlight, 0); assert.equal(group.native.inFlight, 0);
        for (const operation of Object.values(group.operations)) assert.equal(operation.inFlight, 0);
        for (const operation of Object.values(group.native.operations)) assert.equal(operation.inFlight, 0);
    }
}

test("requires an existing canonical caller-owned private temporary directory", async () => {
    for (const directory of ["/", "/tmp", "/tmp/obsetync-native-root-", "/tmp/../tmp/obsetync-native-root-a",
        "/tmp/other-a", "/tmp/obsetync-native-root-a/child", "relative"]) {
        await assert.rejects(createNativeRootIO(directory), code("EINVAL"));
    }
    await fixture(async (_, directory) => {
        await assert.rejects(createNativeRootIO(directory + "/"), code("EINVAL"));
        await fs.chmod(directory, 0o755);
        await assert.rejects(createNativeRootIO(directory), code("EACCES"));
        await fs.chmod(directory, 0o700);
        await fixture(async (_, link) => {
            await fs.rmdir(link); await fs.symlink(directory, link);
            await assert.rejects(createNativeRootIO(link), code("EACCES"));
        });
    });
    const removed = await fs.mkdtemp("/tmp/obsetync-native-root-"); await fs.rmdir(removed);
    await assert.rejects(createNativeRootIO(removed), code("ENOENT"));
});

test("real text/binary/append/stat/list/rename/remove methods satisfy adapter shapes", async () => {
    await fixture(async ({ adapter }, directory) => {
        await adapter.mkdir("notes/nested"); await adapter.mkdir("notes/nested");
        await adapter.write("notes/text.md", "ééééééé 🐈");
        assert.equal(await adapter.read("notes/text.md"), "ééééééé 🐈");
        const view = new Uint8Array([99, 1, 2, 3, 88]).subarray(1, 4);
        await adapter.writeBinary("notes/nested/raw.bin", view);
        await adapter.appendBinary("notes/nested/raw.bin", new Uint8Array([4, 5]).buffer);
        const data = await adapter.readBinary("notes/nested/raw.bin");
        assert(data instanceof ArrayBuffer); assert.equal(data.byteLength, 5);
        assert.deepEqual([...new Uint8Array(data)], [1, 2, 3, 4, 5]);
        const stat = await adapter.stat("notes/nested/raw.bin");
        assert.equal(stat.type, "file"); assert.equal(stat.size, 5);
        assert.equal(typeof stat.mtime, "number"); assert.equal(typeof stat.ctime, "number");
        assert.equal((await adapter.stat("notes")).type, "folder");
        assert.equal(await adapter.stat("missing/child"), null); assert.equal(await adapter.exists("missing"), false);
        assert.equal(await adapter.exists("notes"), true);
        assert.deepEqual(await adapter.list("notes"), { files: ["notes/text.md"], folders: ["notes/nested"] });
        assert.deepEqual(await adapter.list(""), { files: [], folders: ["notes"] });
        assert.equal(adapter.getFullPath("notes/text.md"), join(directory, "notes/text.md"));
        assert.equal(adapter.getFullPath(""), directory);
        await adapter.rename("notes/text.md", "notes/moved.md");
        assert.equal(await adapter.exists("notes/text.md"), false);
        await adapter.write("notes/moved.md", "x"); assert.equal(await adapter.read("notes/moved.md"), "x");
        await adapter.remove("notes/moved.md"); assert.equal(await adapter.exists("notes/moved.md"), false);
        // Fresh binary append creates a regular file, without rereading a prefix.
        await adapter.appendBinary("notes/new.bin", new ArrayBuffer(0));
        assert.equal((await adapter.stat("notes/new.bin")).size, 0);
    });
});

test("only actual ENOENT becomes absence; type/native failures stay failures", async () => {
    await fixture(async ({ adapter, resetCounters, snapshot }) => {
        await adapter.mkdir("folder"); await adapter.write("file", "preserved"); resetCounters();
        for (const operation of [() => adapter.stat("file/child"), () => adapter.exists("file/child"),
            () => adapter.mkdir("file/child"), () => adapter.list("file")]) {
            await assert.rejects(operation, code("ENOTDIR"));
        }
        for (const operation of [() => adapter.read("folder"), () => adapter.readBinary("folder"),
            () => adapter.write("folder", "bad"), () => adapter.remove("folder")]) {
            await assert.rejects(operation, code("EISDIR"));
        }
        for (const operation of [() => adapter.read("missing"), () => adapter.remove("missing"),
            () => adapter.rename("missing", "target"), () => adapter.copy("missing", "target"),
            () => adapter.write("missing/child", "bad"), () => adapter.list("missing")]) {
            await assert.rejects(operation, code("ENOENT"));
        }
        await assert.rejects(adapter.writeBinary("bad", "not bytes"), code("EINVAL"));
        await assert.rejects(adapter.write("bad", new Uint8Array(1)), code("EINVAL"));
        assert.equal(await adapter.read("file"), "preserved"); assert.equal((await adapter.stat("folder")).type, "folder");
        const result = snapshot(); idle(result);
        assert(result.total.failed >= 16); assert(result.total.native.operations.close.succeeded >= 3,
            "opened directory-read handles were not actually closed after type rejection");
    });
});

test("safe-relative checks reject escapes, aliases and root mutations", async () => {
    await fixture(async ({ adapter }) => {
        for (const path of ["../outside", "a/../../outside", "/tmp/outside", "a//b", "a/./b", "a/../b", "a/", "a\\b",
            "C:outside", "a\u0000b", "a\nb", "a\ud800b", "x".repeat(4097)]) {
            await assert.rejects(adapter.stat(path), code("EINVAL"));
            await assert.rejects(adapter.write(path, "bad"), code("EINVAL"));
            assert.throws(() => adapter.getFullPath(path), code("EINVAL"));
        }
        for (const operation of [() => adapter.remove(""), () => adapter.mkdir(""), () => adapter.write("", "bad"),
            () => adapter.rename("", "a"), () => adapter.rename("a", ""), () => adapter.copy("a", "")]) {
            await assert.rejects(operation, code("EINVAL"));
        }
        assert.equal((await adapter.stat("")).type, "folder");
    });
});

test("symlink ancestors/leaf and hard links never expose or overwrite another fixture", async () => {
    await fixture(async ({ adapter }, directory) => fixture(async ({ adapter: outside }, other) => {
        await outside.write("sentinel", "untouched"); await adapter.write("source", "local");
        await fs.symlink(other, join(directory, "escape"));
        await fs.symlink(join(other, "sentinel"), join(directory, "leaf"));
        for (const path of ["escape/sentinel", "escape/new", "leaf"]) {
            for (const operation of [() => adapter.stat(path), () => adapter.exists(path), () => adapter.read(path),
                () => adapter.write(path, "bad"), () => adapter.remove(path), () => adapter.copy("source", path)]) {
                await assert.rejects(operation, code("ELOOP"));
            }
            assert.throws(() => adapter.getFullPath(path), code("ELOOP"));
        }
        await assert.rejects(adapter.copy("leaf", "copy"), code("ELOOP"));
        await assert.rejects(adapter.rename("source", "escape/renamed"), code("ELOOP"));
        await assert.rejects(adapter.mkdir("escape/deeper"), code("ELOOP"));
        await assert.rejects(adapter.list(""), code("ELOOP"));
        await fs.link(join(other, "sentinel"), join(directory, "hard"));
        await assert.rejects(adapter.write("hard", "bad"), code("EMLINK"));
        await assert.rejects(adapter.read("hard"), code("EMLINK"));
        // Inspect with native IO: the outside adapter correctly rejects its
        // own now-multiply-linked sentinel until this link is removed.
        assert.equal(await fs.readFile(join(other, "sentinel"), "utf8"), "untouched");
        await fs.unlink(join(directory, "hard"));
        assert.equal(await outside.read("sentinel"), "untouched");
        assert.equal(await outside.exists("new"), false);
    }));
});

test("native exclusive copy refuses an existing or concurrently created target", async () => {
    await fixture(async ({ adapter, resetCounters, snapshot }) => {
        await adapter.write("source", "verified staging contents"); await adapter.write("target", "existing");
        await assert.rejects(adapter.copy("source", "target"), code("EEXIST"));
        assert.equal(await adapter.read("target"), "existing"); resetCounters();
        const results = await Promise.allSettled([adapter.copy("source", "race"), adapter.copy("source", "race")]);
        assert.equal(results.filter(value => value.status === "fulfilled").length, 1);
        assert.equal(results.find(value => value.status === "rejected").reason.code, "EEXIST");
        assert.equal(await adapter.read("race"), "verified staging contents");
        const result = snapshot(); idle(result);
        assert.equal(result.total.native.operations.copyFile.calls, 2);
        assert.equal(result.total.native.operations.copyFile.succeeded, 1);
        assert.equal(result.total.native.operations.copyFile.failed, 1);
        assert.equal(result.total.copyBytes, Buffer.byteLength("verified staging contents"));
        assert.equal(result.total.native.operations.writeFile.calls, 0, "exclusive copy used a write fallback");
    });
});

test("counters partition real logical bytes and return detached idle-reset snapshots", async () => {
    const prefixes = [".state"];
    await fixture(async ({ adapter, resetCounters, snapshot }) => {
        prefixes[0] = "notes"; // factory captured the original option.
        await adapter.mkdir(".state"); await adapter.mkdir("notes"); resetCounters();
        await adapter.write(".state/head", "hé"); await adapter.read(".state/head");
        await adapter.writeBinary("notes/raw", new Uint8Array([1, 2, 3])); await adapter.readBinary("notes/raw");
        await adapter.appendBinary("notes/raw", new Uint8Array([4, 5]));
        await adapter.copy(".state/head", "notes/copied");
        const result = snapshot(); idle(result);
        assert.equal(result.clientState.readBytes, 3); assert.equal(result.clientState.writeBytes, 3);
        assert.equal(result.vaultSource.readBytes, 3); assert.equal(result.vaultSource.writeBytes, 5);
        assert.equal(result.vaultSource.maxWriteBytes, 3); assert.equal(result.vaultSource.attemptedWriteBytes, 5);
        assert.equal(result.mixed.copyBytes, 3); assert.equal(result.total.writeBytes, 8);
        assert.equal(result.total.calls, result.clientState.calls + result.vaultSource.calls + result.mixed.calls);
        assert(result.total.native.calls > result.total.calls, "native safety/handle boundaries were hidden");
        assert.equal(result.total.native.operations.open.succeeded, result.total.native.operations.close.succeeded);
        assert.doesNotMatch(JSON.stringify(result), /\.state|notes|head|copied|\/tmp/);
        result.total.calls = 12345; result.clientState.operations.read.calls = 12345;
        assert.notEqual(snapshot().total.calls, 12345); assert.equal(snapshot().clientState.operations.read.calls, 1);
        resetCounters(); assert.equal(snapshot().total.calls, 0); assert.equal(snapshot().total.native.calls, 0);
    }, { clientStatePrefixes: prefixes });
});

test("before/after hooks retain actual native completion and prevent mid-flight counter reset", async () => {
    const before = gate(), beforeRelease = gate(), after = gate(), afterRelease = gate();
    await fixture(async ({ adapter, resetCounters, snapshot }, directory) => {
        let completed = false;
        const pending = adapter.write("note", "native complete").finally(() => { completed = true; });
        await before.promise;
        assert.equal(snapshot().total.inFlight, 1); assert.equal(snapshot().total.native.calls, 0);
        assert.throws(resetCounters, code("EBUSY"));
        await assert.rejects(fs.stat(join(directory, "note")), code("ENOENT"));
        beforeRelease.resolve(); await after.promise;
        assert.equal(completed, false); assert.equal(snapshot().total.inFlight, 1);
        assert.equal(snapshot().total.native.inFlight, 0);
        assert.equal(snapshot().total.native.operations.close.succeeded, 1);
        assert.equal(await fs.readFile(join(directory, "note"), "utf8"), "native complete");
        assert.throws(resetCounters, code("EBUSY"));
        afterRelease.resolve(); await pending; assert.equal(completed, true); idle(snapshot());
    }, { boundary: async event => {
        if (event.phase === "before") { before.resolve(); await beforeRelease.promise; }
        else { assert.equal(event.outcome, "succeeded"); after.resolve(); await afterRelease.promise; }
    } });
});

test("a native failure stays pending until the after boundary joins, then preserves errno", async () => {
    const entered = gate(), release = gate();
    await fixture(async ({ adapter, resetCounters, snapshot }) => {
        await adapter.write("source", "source"); await adapter.write("target", "untouched"); resetCounters();
        let completed = false;
        const pending = adapter.copy("source", "target").finally(() => { completed = true; });
        // Attach the rejection assertion before releasing the native-error tail.
        const rejected = assert.rejects(pending, code("EEXIST")); await entered.promise;
        assert.equal(completed, false); assert.equal(snapshot().total.inFlight, 1);
        assert.equal(snapshot().total.native.operations.copyFile.failed, 1);
        assert.equal(snapshot().total.copyBytes, 0); assert.throws(resetCounters, code("EBUSY"));
        release.resolve(); await rejected; assert.equal(completed, true); idle(snapshot());
        assert.equal(await adapter.read("target"), "untouched");
    }, { boundary: async event => {
        if (event.operation === "copy" && event.phase === "after") {
            assert.equal(event.outcome, "failed"); entered.resolve(); await release.promise;
        }
    } });
});

test("parallel native operations remain individually owned through after-hook failure", async () => {
    const both = gate(), release = gate(), injected = new Error("injected after native write"); let arrivals = 0;
    await fixture(async ({ adapter, snapshot }, directory) => {
        const first = adapter.write("first", "one"), second = adapter.write("second", "two");
        const all = Promise.allSettled([first, second]); await both.promise;
        assert.equal(snapshot().total.inFlight, 2); assert.equal(snapshot().total.peakInFlight, 2);
        assert.equal(snapshot().total.writeBytes, 6); assert.equal(snapshot().total.native.inFlight, 0);
        assert.equal(await fs.readFile(join(directory, "first"), "utf8"), "one");
        release.resolve(); const result = await all;
        assert.equal(result[0].status, "fulfilled"); assert.equal(result[1].reason, injected);
        assert.equal(snapshot().total.failed, 1); assert.equal(snapshot().total.native.failed, 2,
            "only two expected initial missing-leaf lstat calls should have failed natively");
        idle(snapshot());
    }, { boundary: async event => {
        if (event.phase !== "after") return;
        if (++arrivals === 2) both.resolve(); await release.promise;
        if (event.paths[0] === "second") throw injected;
    } });
});

test("replacing the fixture directory cannot transfer the old adapter's authority", async () => {
    await fixture(async ({ adapter }, directory) => fixture(async (_, holding) => {
        await adapter.write("original", "preserved");
        const saved = join(holding, "saved");
        await fs.rename(directory, saved); await fs.mkdir(directory, { mode: 0o700 });
        try {
            await assert.rejects(adapter.stat("missing"), code("ESTALE"));
            await assert.rejects(adapter.exists("missing"), code("ESTALE"));
            await assert.rejects(adapter.write("new", "must not publish"), code("ESTALE"));
            assert.throws(() => adapter.getFullPath("new"), code("ESTALE"));
            assert.deepEqual(await fs.readdir(directory), []);
            assert.equal(await fs.readFile(join(saved, "original"), "utf8"), "preserved");
        } finally {
            // Both directories are exact caller-created fixture identities;
            // restore the original before the enclosing owner cleans it up.
            await fs.rm(directory, { recursive: true, force: true }); await fs.rename(saved, directory);
        }
    }));
});

test("a queued before-boundary revalidates symlink ancestors before native writes", async () => {
    let directory, other, armed = false;
    await fixture(async ({ adapter }, root) => fixture(async ({ adapter: outside }, outsideRoot) => {
        directory = root; other = outsideRoot;
        await adapter.mkdir("queued"); await outside.write("sentinel", "untouched"); armed = true;
        await assert.rejects(adapter.write("queued/sentinel", "must not escape"), code("ELOOP"));
        assert.equal(await outside.read("sentinel"), "untouched");
        assert.deepEqual(await fs.readdir(other), ["sentinel"]);
    }), { boundary: async event => {
        if (armed && event.phase === "before" && event.operation === "write") {
            armed = false; await fs.rmdir(join(directory, "queued")); await fs.symlink(other, join(directory, "queued"));
        }
    } });
});

test("raw text read counters do not re-encode replacement characters as source bytes", async () => {
    await fixture(async ({ adapter, resetCounters, snapshot }, directory) => {
        await fs.writeFile(join(directory, "invalid-utf8"), new Uint8Array([0xff, 0x00])); resetCounters();
        const decoded = await adapter.read("invalid-utf8");
        assert.equal(Buffer.byteLength(decoded), 4); assert.equal(snapshot().total.readBytes, 2);
        assert.equal(snapshot().total.maxReadBytes, 2); idle(snapshot());
    });
});
