/** Native TEST/BENCH adapter, never a production or user-vault adapter.
 * Caller creates and owns one private /tmp/obsetync-native-root-* directory.
 * There is deliberately no fixture creation, recursive deletion or cleanup API.
 *
 * Completion means actual Node promises (including handle close) completed;
 * no fsync, power-loss durability, atomic copy, or hostile directory-replacement
 * sandbox is claimed. Ancestors are checked, not pinned with openat/dirfds.
 * The fixture must remain private and must not have an adversarial writer.
 * Binary input buffers are borrowed: caller keeps them stable until settlement.
 * Native streams/workers reached through getFullPath bypass these byte counters.
 */
import * as fs from "node:fs/promises";
import { constants, lstatSync } from "node:fs";
import { join } from "node:path";

const OPERATIONS = ["stat", "exists", "read", "write", "mkdir", "rename", "remove",
    "readBinary", "writeBinary", "appendBinary", "copy", "list", "getFullPath"];
const NATIVE_OPERATIONS = ["lstat", "open", "fstat", "readFile", "writeFile", "truncate", "close",
    "mkdir", "rename", "unlink", "copyFile", "readdir"];
const PARTITIONS = ["clientState", "vaultSource", "mixed"];
const failure = (code, message) => Object.assign(new Error(message), { code });
const missing = error => error?.code === "ENOENT";
const counts = () => ({ calls: 0, succeeded: 0, failed: 0, inFlight: 0, peakInFlight: 0 });
const table = names => Object.fromEntries(names.map(name => [name, counts()]));
const aggregate = () => ({ ...counts(), readBytes: 0, writeBytes: 0, copyBytes: 0,
    attemptedWriteBytes: 0, maxReadBytes: 0, maxWriteBytes: 0, maxCopyBytes: 0, maxAttemptedWriteBytes: 0,
    operations: table(OPERATIONS), native: { ...counts(), operations: table(NATIVE_OPERATIONS) } });
const metrics = () => ({ total: aggregate(), ...Object.fromEntries(PARTITIONS.map(name => [name, aggregate()])) });
function enter(counter) { counter.calls++; counter.inFlight++; counter.peakInFlight = Math.max(counter.peakInFlight, counter.inFlight); }
function leave(counter, succeeded) { counter.inFlight--; counter[succeeded ? "succeeded" : "failed"]++; }

function checkedRelative(value, allowRoot = false) {
    if (allowRoot && value === "") return value;
    if (typeof value !== "string" || !value || value.length > 4096 || /[\\:\x00-\x1f\x7f]/u.test(value) ||
        value.split("/").some(part => !part || part === "." || part === "..")) {
        throw failure("EINVAL", "native fixture requires a safe relative path");
    }
    // Node replaces lone surrogates when encoding paths. Reject aliases to a
    // different on-disk name instead of authorizing a lossy normalization.
    if (Buffer.from(value).toString("utf8") !== value) throw failure("EINVAL", "native fixture path is not valid Unicode");
    return value;
}

function checkRoot(stat, identity) {
    if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o777) !== 0o700 ||
        (typeof process.getuid === "function" && stat.uid !== process.getuid())) {
        throw failure("EACCES", "native fixture root must be an owned private directory");
    }
    if (identity && (stat.dev !== identity.dev || stat.ino !== identity.ino)) {
        throw failure("ESTALE", "native fixture directory identity changed");
    }
}
function checkEntry(stat, ancestor = false) {
    if (stat.isSymbolicLink()) throw failure("ELOOP", "native fixture refuses symbolic links");
    if (ancestor && !stat.isDirectory()) throw failure("ENOTDIR", "native fixture ancestor is not a directory");
    if (!stat.isFile() && !stat.isDirectory()) throw failure("ENOTSUP", "native fixture requires a regular file or directory");
    if (stat.isFile() && stat.nlink !== 1) throw failure("EMLINK", "native fixture refuses multiply-linked files");
    return stat;
}
function requireFile(stat) {
    checkEntry(stat);
    if (!stat.isFile()) throw failure("EISDIR", "native fixture file operation received a directory");
}
function binaryView(data) {
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data) && data.buffer instanceof ArrayBuffer) {
        return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    throw failure("EINVAL", "native fixture binary write requires ArrayBuffer bytes");
}

/** @param {string} directory Already-created canonical, private fixture root.
 * @param {{clientStatePrefixes?: readonly string[], boundary?: (event: {
 *   phase:'before'|'after', operation:string, paths:readonly string[],
 *   outcome?:'succeeded'|'failed'}) => Promise<void>|void}} options
 * @returns {Promise<{adapter:object, resetCounters:()=>void, snapshot:()=>object}>}
 * Counters are logical known-complete bytes, not kernel physical traffic.
 * Failed writes may have an unknown partial effect: attemptedWriteBytes is
 * separate; writeBytes increments only after the native write promise succeeds.
 * Regression hooks are never used by synchronous getFullPath. A hook failure
 * propagates; an after-hook cannot race or undo completed native effects.
 */
export async function createNativeRootIO(directory, options = {}) {
    if (typeof directory !== "string" || !/^\/tmp\/obsetync-native-root-[A-Za-z0-9_-]+$/u.test(directory)) {
        throw failure("EINVAL", "native fixture root is not an allowed canonical temporary directory");
    }
    const initial = await fs.lstat(directory);
    checkRoot(initial);
    if (await fs.realpath(directory) !== directory) throw failure("EINVAL", "native fixture root must already be canonical");
    const identity = { dev: initial.dev, ino: initial.ino };
    const prefixes = options.clientStatePrefixes ?? [".obsidian"];
    if (!Array.isArray(prefixes) || prefixes.length > 32) throw failure("EINVAL", "invalid native fixture state prefixes");
    const statePrefixes = prefixes.map(prefix => checkedRelative(prefix));
    const boundary = options.boundary;
    if (boundary !== undefined && typeof boundary !== "function") throw failure("EINVAL", "invalid native fixture boundary hook");
    let counters = metrics();

    const partition = paths => {
        const state = paths.map(path => typeof path === "string" && statePrefixes.some(prefix => path === prefix || path.startsWith(prefix + "/")));
        return state.every(Boolean) ? "clientState" : state.some(Boolean) ? "mixed" : "vaultSource";
    };
    const targets = group => [counters.total, counters[group]];
    const bytes = (group, kind, amount) => {
        for (const target of targets(group)) {
            target[kind + "Bytes"] += amount;
            target["max" + kind[0].toUpperCase() + kind.slice(1) + "Bytes"] = Math.max(
                target["max" + kind[0].toUpperCase() + kind.slice(1) + "Bytes"], amount);
        }
    };
    const native = async (group, operation, work) => {
        const active = targets(group).flatMap(target => [target.native, target.native.operations[operation]]);
        active.forEach(enter); let succeeded = false;
        try { const value = await work(); succeeded = true; return value; }
        finally { active.forEach(counter => leave(counter, succeeded)); }
    };
    const nativeSync = (group, operation, work) => {
        const active = targets(group).flatMap(target => [target.native, target.native.operations[operation]]);
        active.forEach(enter); let succeeded = false;
        try { const value = work(); succeeded = true; return value; }
        finally { active.forEach(counter => leave(counter, succeeded)); }
    };
    // Walk all existing ancestors. Missing suffixes are not inferred to be
    // folders; the requested native operation retains its real ENOENT error.
    const inspect = async (relative, group) => {
        const root = await native(group, "lstat", () => fs.lstat(directory)); checkRoot(root, identity);
        if (!relative) return root;
        const parts = relative.split("/"); let absolute = directory;
        for (let index = 0; index < parts.length; index++) {
            absolute = join(absolute, parts[index]);
            let stat;
            try { stat = await native(group, "lstat", () => fs.lstat(absolute)); }
            catch (error) { if (missing(error)) return null; throw error; }
            checkEntry(stat, index < parts.length - 1);
            if (index === parts.length - 1) return stat;
        }
    };
    const inspectSync = (relative, group) => {
        const root = nativeSync(group, "lstat", () => lstatSync(directory)); checkRoot(root, identity);
        if (!relative) return;
        const parts = relative.split("/"); let absolute = directory;
        for (let index = 0; index < parts.length; index++) {
            absolute = join(absolute, parts[index]);
            let stat;
            try { stat = nativeSync(group, "lstat", () => lstatSync(absolute)); }
            catch (error) { if (missing(error)) return; throw error; }
            checkEntry(stat, index < parts.length - 1);
        }
    };
    const run = async (operation, paths, work, allowRoot = false) => {
        const group = partition(paths), active = targets(group).flatMap(target => [target, target.operations[operation]]);
        active.forEach(enter); let succeeded = false, value, error, checked;
        try {
            try {
                checked = Object.freeze(paths.map(path => checkedRelative(path, allowRoot)));
                if (boundary) await boundary(Object.freeze({ phase: "before", operation, paths: checked }));
                value = await work(group); succeeded = true;
            } catch (caught) { error = caught; }
            if (boundary && checked) {
                try { await boundary(Object.freeze({ phase: "after", operation, paths: checked, outcome: succeeded ? "succeeded" : "failed" })); }
                catch (caught) { error = error === undefined ? caught : new AggregateError([error, caught], "native fixture operation and after-hook failed"); succeeded = false; }
            }
            if (!succeeded) throw error;
            return value;
        } finally { active.forEach(counter => leave(counter, succeeded)); }
    };
    const withFile = async (relative, group, flags, work) => {
        await inspect(relative, group);
        const handle = await native(group, "open", () => fs.open(join(directory, relative), flags | constants.O_NOFOLLOW, 0o600));
        let result, error;
        try { requireFile(await native(group, "fstat", () => handle.stat())); result = await work(handle); }
        catch (caught) { error = caught; }
        try { await native(group, "close", () => handle.close()); }
        catch (caught) { error = error === undefined ? caught : new AggregateError([error, caught], "native fixture operation and close failed"); }
        if (error !== undefined) throw error;
        return result;
    };
    const readBytes = (relative, group) => withFile(relative, group, constants.O_RDONLY, async handle => {
        const data = await native(group, "readFile", () => handle.readFile()); bytes(group, "read", data.byteLength); return data;
    });
    const writeBytes = async (relative, value, group, append) => {
        const data = typeof value === "string" ? value : binaryView(value);
        const length = typeof data === "string" ? Buffer.byteLength(data, "utf8") : data.byteLength;
        await withFile(relative, group, constants.O_WRONLY | constants.O_CREAT | (append ? constants.O_APPEND : 0), async handle => {
            // Do not truncate before verifying the opened regular file.
            if (!append) await native(group, "truncate", () => handle.truncate(0));
            bytes(group, "attemptedWrite", length);
            await native(group, "writeFile", () => handle.writeFile(data)); bytes(group, "write", length);
        });
    };
    const adapter = Object.freeze({
        stat: relative => run("stat", [relative], async group => {
            const stat = await inspect(relative, group);
            return stat ? { type: stat.isFile() ? "file" : "folder", size: stat.size, mtime: stat.mtimeMs, ctime: stat.ctimeMs } : null;
        }, true),
        exists: relative => run("exists", [relative], async group => (await inspect(relative, group)) !== null, true),
        read: relative => run("read", [relative], async group => (await readBytes(relative, group)).toString("utf8")),
        write: (relative, data) => run("write", [relative], group => {
            if (typeof data !== "string") throw failure("EINVAL", "native fixture text write requires a string");
            return writeBytes(relative, data, group, false);
        }),
        readBinary: relative => run("readBinary", [relative], async group => {
            const data = await readBytes(relative, group);
            return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
        }),
        writeBinary: (relative, data) => run("writeBinary", [relative], group => writeBytes(relative, binaryView(data), group, false)),
        appendBinary: (relative, data) => run("appendBinary", [relative], group => writeBytes(relative, binaryView(data), group, true)),
        mkdir: relative => run("mkdir", [relative], async group => {
            await inspect(relative, group);
            let absolute = directory;
            for (const part of relative.split("/")) {
                absolute = join(absolute, part);
                try { await native(group, "mkdir", () => fs.mkdir(absolute, { mode: 0o700 })); }
                catch (error) { if (error?.code !== "EEXIST") throw error; }
                checkEntry(await native(group, "lstat", () => fs.lstat(absolute)), true);
            }
        }),
        rename: (from, to) => run("rename", [from, to], async group => {
            await inspect(from, group); await inspect(to, group);
            await native(group, "rename", () => fs.rename(join(directory, from), join(directory, to)));
        }),
        remove: relative => run("remove", [relative], async group => {
            const stat = await inspect(relative, group); if (stat) requireFile(stat);
            await native(group, "unlink", () => fs.unlink(join(directory, relative)));
        }),
        copy: (from, to) => run("copy", [from, to], async group => {
            const source = await inspect(from, group); if (source) requireFile(source);
            await inspect(to, group);
            await native(group, "copyFile", () => fs.copyFile(join(directory, from), join(directory, to), constants.COPYFILE_EXCL));
            const stat = await inspect(to, group);
            if (!stat) throw failure("ENOENT", "native fixture copied destination disappeared");
            requireFile(stat); bytes(group, "copy", stat.size);
        }),
        list: relative => run("list", [relative], async group => {
            const stat = await inspect(relative, group);
            if (stat && !stat.isDirectory()) throw failure("ENOTDIR", "native fixture list requires a directory");
            const entries = await native(group, "readdir", () => fs.readdir(join(directory, relative), { withFileTypes: true }));
            const files = [], folders = [];
            for (const entry of entries) {
                const path = checkedRelative(relative ? relative + "/" + entry.name : entry.name);
                if (entry.isSymbolicLink()) throw failure("ELOOP", "native fixture refuses listing symbolic links");
                if (entry.isFile()) files.push(path);
                else if (entry.isDirectory()) folders.push(path);
                else throw failure("ENOTSUP", "native fixture refuses listing special files");
            }
            files.sort(); folders.sort(); return { files, folders };
        }, true),
        getFullPath: relative => {
            const group = partition([relative]), active = targets(group).flatMap(target => [target, target.operations.getFullPath]);
            active.forEach(enter); let succeeded = false;
            try { checkedRelative(relative, true); inspectSync(relative, group); succeeded = true; return join(directory, relative); }
            finally { active.forEach(counter => leave(counter, succeeded)); }
        },
    });
    return Object.freeze({ adapter,
        resetCounters() {
            if (counters.total.inFlight || counters.total.native.inFlight) throw failure("EBUSY", "cannot reset native fixture counters during active work");
            counters = metrics();
        },
        snapshot() { return structuredClone(counters); },
    });
}
