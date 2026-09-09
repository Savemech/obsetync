import { HashWorkerFileDriftError } from "./desktop-hash-workers";
import { desktopFileFingerprintMatches } from "./desktop-ranged-upload";
import type { DesktopFileFingerprint } from "./hash-worker-protocol";
import { PathIsDirectoryError } from "./file-safety";
import { throwIfWorkAborted } from "./work-scheduler";

interface DesktopReadStats {
    size: number;
    mtimeMs: number;
    ctimeMs: number;
    dev: number;
    ino: number;
    nlink: number;
    isFile(): boolean;
    isDirectory(): boolean;
    isSymbolicLink?(): boolean;
}

interface DesktopReadHandle {
    stat(): Promise<DesktopReadStats>;
    read(buffer: Uint8Array, offset: number, length: number, position: number): Promise<{ bytesRead: number }>;
    close(): Promise<void>;
}

export interface DesktopVerifiedReadHost {
    isAbsolute(path: string): boolean;
    open(path: string, flags: number): Promise<DesktopReadHandle>;
    lstat(path: string): Promise<DesktopReadStats>;
    readOnlyNoFollow: number;
}

export interface DesktopVerifiedReadSource {
    path: string;
    absolutePath: string;
    /** Resolved vault root followed by each resolved parent directory. */
    ancestorPaths: readonly string[];
    expected: { size: number; mtime: number };
}

interface AncestorIdentity { device: number; inode: number }

function fingerprint(stats: DesktopReadStats): DesktopFileFingerprint {
    const value = {
        size: Number(stats.size),
        mtime: Number(stats.mtimeMs),
        ctime: Number(stats.ctimeMs),
        device: Number(stats.dev),
        inode: Number(stats.ino),
    };
    if (!Number.isSafeInteger(value.size) || value.size < 0 ||
        !Number.isFinite(value.mtime) || value.mtime < 0 ||
        !Number.isFinite(value.ctime) || value.ctime < 0 ||
        !Number.isInteger(value.device) || value.device < 0 ||
        !Number.isInteger(value.inode) || value.inode < 0) {
        throw new Error("desktop source returned invalid file metadata");
    }
    return value;
}

const expectedMatches = (expected: DesktopVerifiedReadSource["expected"], actual: DesktopFileFingerprint): boolean =>
    expected.size === actual.size && Math.abs(expected.mtime - actual.mtime) <= 1;

function nativeHost(): DesktopVerifiedReadHost | null {
    const requireNode = (typeof require === "function"
        ? require
        : (globalThis as typeof globalThis & { require?: NodeRequire }).require) as NodeRequire | undefined;
    if (!requireNode) return null;
    try {
        const fs = requireNode("node:fs/promises") as typeof import("node:fs/promises");
        const constants = (requireNode("node:fs") as typeof import("node:fs")).constants;
        const path = requireNode("node:path") as typeof import("node:path");
        if (typeof constants.O_NOFOLLOW !== "number" || constants.O_NOFOLLOW <= 0) return null;
        return {
            isAbsolute: value => path.isAbsolute(value),
            open: (value, flags) => fs.open(value, flags),
            lstat: value => fs.lstat(value),
            readOnlyNoFollow: constants.O_RDONLY | constants.O_NOFOLLOW,
        };
    } catch {
        return null;
    }
}

function code(error: unknown): string | undefined {
    return (error as NodeJS.ErrnoException | undefined)?.code;
}

function isVanished(error: unknown): boolean {
    return ["ENOENT", "ESTALE", "ELOOP"].includes(code(error) ?? "");
}

async function inspectAncestors(
    host: DesktopVerifiedReadHost,
    paths: readonly string[],
    signal?: AbortSignal,
): Promise<AncestorIdentity[]> {
    const identities: AncestorIdentity[] = [];
    for (const path of paths) {
        throwIfWorkAborted(signal);
        let stats: DesktopReadStats;
        try { stats = await host.lstat(path); }
        catch (error) {
            if (isVanished(error)) {
                throw new HashWorkerFileDriftError("desktop source ancestor disappeared or became linked");
            }
            throw error;
        }
        if (!stats.isDirectory() || stats.isSymbolicLink?.()) {
            throw new HashWorkerFileDriftError("desktop source ancestor is not a stable directory");
        }
        const device = Number(stats.dev), inode = Number(stats.ino);
        if (!Number.isInteger(device) || device < 0 || !Number.isInteger(inode) || inode < 0) {
            throw new Error("desktop source ancestor returned invalid identity");
        }
        identities.push({ device, inode });
    }
    return identities;
}

function sameAncestors(left: readonly AncestorIdentity[], right: readonly AncestorIdentity[]): boolean {
    return left.length === right.length && left.every((entry, index) =>
        entry.device === right[index].device && entry.inode === right[index].inode);
}

/** Read one desktop source through the same descriptor whose identity was
 * admitted. A final descriptor + pathname comparison detects in-place drift
 * and atomic replacement. Null means the runtime cannot provide the required
 * no-follow handle capability; callers must use their portable adapter path.
 * An observed unsafe or drifting pathname rejects instead of falling back. */
export async function readDesktopFileIdentityVerified(
    source: DesktopVerifiedReadSource,
    signal?: AbortSignal,
    injectedHost?: DesktopVerifiedReadHost,
): Promise<Uint8Array | null> {
    if (!source || typeof source.path !== "string" || !source.path ||
        typeof source.absolutePath !== "string" || source.absolutePath.includes("\0") ||
        !Array.isArray(source.ancestorPaths) || source.ancestorPaths.length === 0 ||
        !Number.isSafeInteger(source.expected?.size) || source.expected.size < 0 ||
        !Number.isFinite(source.expected?.mtime) || source.expected.mtime < 0) {
        throw new RangeError("invalid desktop verified-read source");
    }
    const host = injectedHost ?? nativeHost();
    if (!host) return null;
    if (!host.isAbsolute(source.absolutePath) || !Number.isInteger(host.readOnlyNoFollow) ||
        host.readOnlyNoFollow <= 0) {
        throw new RangeError("invalid desktop verified-read host");
    }
    if (source.ancestorPaths.some(path => typeof path !== "string" || path.includes("\0") ||
        !host.isAbsolute(path))) {
        throw new RangeError("invalid desktop verified-read ancestor");
    }
    throwIfWorkAborted(signal);

    const beforeAncestors = await inspectAncestors(host, source.ancestorPaths, signal);
    throwIfWorkAborted(signal);

    let handle: DesktopReadHandle;
    try {
        handle = await host.open(source.absolutePath, host.readOnlyNoFollow);
    } catch (error) {
        // Some Electron/Windows builds expose O_NOFOLLOW numerically but do
        // not accept it in open(). Refuse the accelerator without weakening
        // the flag set; the caller retains its adapter fallback.
        if (["EINVAL", "ENOTSUP", "EOPNOTSUPP"].includes(code(error) ?? "")) return null;
        if (code(error) === "EISDIR") throw new PathIsDirectoryError(source.path);
        if (["ENOENT", "ESTALE", "ELOOP"].includes(code(error) ?? "")) {
            throw new HashWorkerFileDriftError("desktop source disappeared, moved or became linked before read");
        }
        throw error;
    }

    let result: Uint8Array | undefined;
    let failure: unknown;
    try {
        throwIfWorkAborted(signal);
        const beforeStats = await handle.stat();
        if (!beforeStats.isFile()) throw new PathIsDirectoryError(source.path);
        if (Number(beforeStats.nlink) !== 1) {
            throw new HashWorkerFileDriftError("desktop source handle is not uniquely linked");
        }
        const before = fingerprint(beforeStats);
        if (!expectedMatches(source.expected, before)) {
            throw new HashWorkerFileDriftError("desktop source changed before admitted read");
        }

        // Allocation happens only after the handle itself proves the exact
        // admitted size and regular-file type. The caller already owns this
        // many transient source bytes.
        const output = new Uint8Array(before.size);
        let offset = 0;
        while (offset < output.length) {
            throwIfWorkAborted(signal);
            const { bytesRead } = await handle.read(output, offset, output.length - offset, offset);
            if (!Number.isSafeInteger(bytesRead) || bytesRead <= 0 || bytesRead > output.length - offset) {
                throw new HashWorkerFileDriftError("desktop source ended during admitted read");
            }
            offset += bytesRead;
        }
        throwIfWorkAborted(signal);

        const afterChecks = await Promise.allSettled([
            handle.stat(), host.lstat(source.absolutePath), inspectAncestors(host, source.ancestorPaths, signal),
        ]);
        const rejected = afterChecks.find((entry): entry is PromiseRejectedResult => entry.status === "rejected");
        if (rejected) throw rejected.reason;
        const [afterHandleStats, afterPathStats, afterAncestors] = afterChecks.map(entry =>
            (entry as PromiseFulfilledResult<DesktopReadStats | AncestorIdentity[]>).value) as
            [DesktopReadStats, DesktopReadStats, AncestorIdentity[]];
        if (!afterHandleStats.isFile() || !afterPathStats.isFile() || afterPathStats.isSymbolicLink?.()) {
            throw new HashWorkerFileDriftError("desktop source path changed type during admitted read");
        }
        if (Number(afterHandleStats.nlink) !== 1 || Number(afterPathStats.nlink) !== 1) {
            throw new HashWorkerFileDriftError("desktop source became multiply linked during admitted read");
        }
        if (!sameAncestors(beforeAncestors, afterAncestors)) {
            throw new HashWorkerFileDriftError("desktop source ancestor changed during admitted read");
        }
        const afterHandle = fingerprint(afterHandleStats);
        const afterPath = fingerprint(afterPathStats);
        if (!expectedMatches(source.expected, afterHandle) || !expectedMatches(source.expected, afterPath) ||
            !desktopFileFingerprintMatches(before, afterHandle) ||
            !desktopFileFingerprintMatches(before, afterPath) ||
            !desktopFileFingerprintMatches(afterHandle, afterPath)) {
            throw new HashWorkerFileDriftError("desktop source changed during admitted read");
        }
        throwIfWorkAborted(signal);
        result = output;
    } catch (error) {
        failure = isVanished(error)
            ? new HashWorkerFileDriftError("desktop source disappeared, moved or became linked during read")
            : error;
    }
    try {
        await handle.close();
    } catch (error) {
        // Preserve a typed drift/directory/cancellation error when close also
        // rejects; a close-only failure remains authoritative.
        if (failure === undefined) failure = error;
    }
    if (failure !== undefined) throw failure;
    return result!;
}
