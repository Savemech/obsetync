import { strict as assert } from "node:assert";
import { HashWorkerFileDriftError } from "./desktop-hash-workers";
import {
    readDesktopFileIdentityVerified,
    type DesktopVerifiedReadHost,
} from "./desktop-verified-read";
import { PathIsDirectoryError } from "./file-safety";

let assertions = 0;
const check = (value: unknown, message: string) => { assertions++; assert.ok(value, message); };
const deferred = () => {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
};

function file(overrides: Record<string, unknown> = {}) {
    return {
        size: 3, mtimeMs: 10, ctimeMs: 20, dev: 1, ino: 2, nlink: 1,
        isFile: () => true, isDirectory: () => false, isSymbolicLink: () => false,
        ...overrides,
    };
}

function directory(overrides: Record<string, unknown> = {}) {
    return file({
        size: 0, ino: 1, nlink: 2, isFile: () => false, isDirectory: () => true,
        ...overrides,
    });
}

function fixture(options: {
    before?: ReturnType<typeof file>;
    after?: ReturnType<typeof file>;
    path?: ReturnType<typeof file>;
    ancestorBefore?: ReturnType<typeof file>;
    ancestorAfter?: ReturnType<typeof file>;
    ancestor?: (path: string, call: number) => ReturnType<typeof file>;
    read?: (buffer: Uint8Array, offset: number, length: number, position: number) => Promise<{ bytesRead: number }>;
    afterStat?: () => Promise<ReturnType<typeof file>>;
    pathLstat?: () => Promise<ReturnType<typeof file>>;
    openError?: Error;
    closeError?: Error;
} = {}) {
    const events: string[] = [];
    let stats = 0;
    const ancestorStats = new Map<string, number>();
    const bytes = Uint8Array.from([7, 8, 9]);
    const handle = {
        async stat() {
            events.push("fstat");
            if (++stats > 1 && options.afterStat) return options.afterStat();
            return stats === 1 ? options.before ?? file() : options.after ?? file();
        },
        async read(buffer: Uint8Array, offset: number, length: number, position: number) {
            events.push("read");
            if (options.read) return options.read(buffer, offset, length, position);
            const count = Math.min(2, length);
            buffer.set(bytes.subarray(position, position + count), offset);
            return { bytesRead: count };
        },
        async close() { events.push("close"); if (options.closeError) throw options.closeError; },
    };
    const host: DesktopVerifiedReadHost = {
        isAbsolute: path => path.startsWith("/"),
        async open(_path, flags) {
            events.push(`open:${flags}`);
            if (options.openError) throw options.openError;
            return handle;
        },
        async lstat(path) {
            if (!path.endsWith("/note.md")) {
                events.push(`ancestor:${path}`);
                const call = (ancestorStats.get(path) ?? 0) + 1;
                ancestorStats.set(path, call);
                if (options.ancestor) return options.ancestor(path, call);
                return call === 1 ? options.ancestorBefore ?? directory()
                    : options.ancestorAfter ?? options.ancestorBefore ?? directory();
            }
            events.push("lstat");
            if (options.pathLstat) return options.pathLstat();
            return options.path ?? file();
        },
        readOnlyNoFollow: 17,
    };
    return { host, events, bytes };
}

const source = () => ({
    path: "note.md", absolutePath: "/vault/note.md", ancestorPaths: ["/vault"],
    expected: { size: 3, mtime: 10 },
});

async function exactHandleReadAndIdentityProof(): Promise<void> {
    const f = fixture();
    const result = await readDesktopFileIdentityVerified(source(), undefined, f.host);
    check(result !== null && result.join(",") === f.bytes.join(","), "verified reader changed source bytes");
    check(result!.buffer.byteLength === 3, "verified reader returned an over-allocated backing buffer");
    check(f.events.join(",") === "ancestor:/vault,open:17,fstat,read,read,fstat,lstat,ancestor:/vault,close",
        "verified reader did not bind reads and final identity checks to one handle");

    const emptyStats = file({ size: 0 });
    const empty = fixture({ before: emptyStats, after: emptyStats, path: emptyStats });
    const emptyResult = await readDesktopFileIdentityVerified({
        ...source(), expected: { size: 0, mtime: 10 },
    }, undefined, empty.host);
    check(emptyResult?.byteLength === 0 && emptyResult.buffer.byteLength === 0,
        "verified reader changed an empty source allocation");
    check(!empty.events.includes("read") && empty.events.at(-1) === "close",
        "empty source issued a native read or leaked its handle");
}

async function driftAndDirectoryNeverReturnBytes(): Promise<void> {
    for (const [name, options] of [
        ["path replacement", { path: file({ ino: 99 }) }],
        ["in-place metadata drift", { after: file({ ctimeMs: 22 }) }],
        ["symlink replacement", { path: file({ isFile: () => false, isSymbolicLink: () => true }) }],
        ["hard-linked handle", { before: file({ nlink: 2 }) }],
        ["hard-linked pathname", { path: file({ nlink: 2 }) }],
        ["ancestor replacement", { ancestorAfter: directory({ ino: 99 }) }],
        ["ancestor symlink replacement", { ancestorAfter: directory({ isSymbolicLink: () => true }) }],
    ] as const) {
        const f = fixture(options);
        await assert.rejects(readDesktopFileIdentityVerified(source(), undefined, f.host), HashWorkerFileDriftError);
        assertions++;
        check(f.events.at(-1) === "close", `${name} leaked its file handle`);
    }
    const directorySource = fixture({ before: file({ isFile: () => false }) });
    await assert.rejects(readDesktopFileIdentityVerified(source(), undefined, directorySource.host), PathIsDirectoryError);
    assertions++;
    check(!directorySource.events.includes("read") && directorySource.events.at(-1) === "close",
        "directory source was read or leaked its handle");

    const linkedAncestor = fixture({ ancestorBefore: directory({ isSymbolicLink: () => true }) });
    await assert.rejects(readDesktopFileIdentityVerified(source(), undefined, linkedAncestor.host), HashWorkerFileDriftError);
    assertions++;
    check(linkedAncestor.events.join(",") === "ancestor:/vault",
        "symlinked source ancestor reached the native source handle");

    const nestedLinkedAncestor = fixture({ ancestor: path => directory({
        ino: path === "/vault/nested" ? 3 : 1,
        isSymbolicLink: () => path === "/vault/nested",
    }) });
    await assert.rejects(readDesktopFileIdentityVerified({
        ...source(),
        absolutePath: "/vault/nested/note.md",
        ancestorPaths: ["/vault", "/vault/nested"],
    }, undefined, nestedLinkedAncestor.host), HashWorkerFileDriftError);
    assertions++;
    check(nestedLinkedAncestor.events.join(",") === "ancestor:/vault,ancestor:/vault/nested",
        "nested symlink ancestor reached the native source handle");
}

async function cancellationJoinsNativeReadBeforeClose(): Promise<void> {
    const entered = deferred(), release = deferred();
    const f = fixture({ read: async (buffer, offset) => {
        entered.resolve();
        await release.promise;
        buffer[offset] = 7;
        return { bytesRead: 1 };
    } });
    const controller = new AbortController();
    let settled = false;
    const pending = readDesktopFileIdentityVerified(source(), controller.signal, f.host)
        .then(() => null, error => error).then(error => { settled = true; return error; });
    await entered.promise;
    controller.abort();
    await Promise.resolve();
    check(!settled && !f.events.includes("close"), "abort raced an unabortable handle read or closed it early");
    release.resolve();
    const error = await pending;
    check(error instanceof Error && error.name === "AbortError", "joined read lost cancellation identity");
    check(f.events.at(-1) === "close", "cancelled verified read leaked its file handle");
}

async function finalIdentityChecksJoinStartedSiblings(): Promise<void> {
    const entered = deferred(), release = deferred();
    const ioError = Object.assign(new Error("injected final handle failure"), { code: "EIO" });
    const f = fixture({
        afterStat: async () => { throw ioError; },
        pathLstat: async () => {
            entered.resolve();
            await release.promise;
            return file();
        },
    });
    let settled = false;
    const pending = readDesktopFileIdentityVerified(source(), undefined, f.host)
        .then(() => null, error => error).then(error => { settled = true; return error; });
    await entered.promise;
    await Promise.resolve();
    check(!settled && !f.events.includes("close"),
        "final identity rejection raced a started native sibling or closed its handle early");
    release.resolve();
    const error = await pending;
    check(error === ioError, "joined final identity checks changed the authoritative failure");
    check(f.events.at(-1) === "close", "joined final identity failure leaked its file handle");
}

async function nativeFailuresStayTypedAndClose(): Promise<void> {
    const ioError = Object.assign(new Error("injected source failure"), { code: "EIO" });
    const failed = fixture({ read: async () => { throw ioError; } });
    await assert.rejects(readDesktopFileIdentityVerified(source(), undefined, failed.host), error => error === ioError);
    assertions++;
    check(failed.events.at(-1) === "close", "source IO failure leaked its handle");

    const missing = fixture({ openError: Object.assign(new Error("missing"), { code: "ENOENT" }) });
    await assert.rejects(readDesktopFileIdentityVerified(source(), undefined, missing.host), HashWorkerFileDriftError);
    assertions++;
    check(!missing.events.includes("fstat") && !missing.events.includes("read"), "missing source reached handle work");

    const unsupported = fixture({ openError: Object.assign(new Error("unsupported no-follow"), { code: "EINVAL" }) });
    check(await readDesktopFileIdentityVerified(source(), undefined, unsupported.host) === null,
        "unsupported no-follow host did not preserve the portable fallback");
    check(unsupported.events.join(",") === "ancestor:/vault,open:17",
        "unsupported no-follow host advanced beyond bounded ancestor qualification and open");

    const invalid = fixture();
    await assert.rejects(readDesktopFileIdentityVerified({ ...source(), absolutePath: "relative" }, undefined, invalid.host), RangeError);
    assertions++;
    check(invalid.events.length === 0, "invalid source reached native open");
}

void exactHandleReadAndIdentityProof()
    .then(driftAndDirectoryNeverReturnBytes)
    .then(cancellationJoinsNativeReadBeforeClose)
    .then(finalIdentityChecksJoinStartedSiblings)
    .then(nativeFailuresStayTypedAndClose)
    .then(() => console.log(`desktop-verified-read.test: ${assertions} assertions passed`))
    .catch(error => { console.error(error); process.exitCode = 1; });
