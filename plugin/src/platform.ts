import { App, Platform } from "obsidian";
import { exactArrayBuffer } from "./binary";
import { runtimeForHost } from "./host-runtime";
import { appendBinaryBounded } from "./bounded-append";
import type { TransientWorkScope } from "./transient-memory";
import {
    browserMobileRangeHost,
    MOBILE_RESOURCE_RANGE_MAX_BYTES,
    MobileRangeReadError,
    openQualifiedMobileRangeReader,
    type MobileRangeHost,
    type MobileRangeReader,
} from "./mobile-ranged-source";
import {
    collectAdapterFileStats,
    isMissingPathError,
    PathIsDirectoryError,
    readFileStat,
    type FileStat,
} from "./file-safety";
import { isSafeVaultPath } from "./delta-validation";
import { readDesktopFileIdentityVerified } from "./desktop-verified-read";
export type { FileStat } from "./file-safety";

/** Mobile DataAdapter has no ranged reads. Refuse a single allocation large
 *  enough to plausibly trigger Jetsam on older 2 GB devices; large remote
 *  restores remain chunked and do not use readFile(). */
const MOBILE_WHOLE_FILE_LIMIT = 128 * 1024 * 1024;

export interface PlatformIO {
    readFile(path: string): Promise<Uint8Array>;
    /** Desktop-only whole read bound to one opened file identity. Null means
     * the host cannot safely provide it and the portable stat/read/stat path
     * must be used. The caller owns expected.size bytes before invocation. */
    readFileIdentityVerified?(path: string, expected: FileStat,
        signal?: AbortSignal): Promise<Uint8Array | null>;
    writeFile(path: string, data: Uint8Array): Promise<void>;
    /** Append one binary segment without reading the existing file. */
    appendFile(path: string, data: Uint8Array): Promise<void>;
    /** Reuse an owned download's work quota for append/copy lifetimes. */
    appendFileOwned?(path: string, data: Uint8Array, memory?: TransientWorkScope): Promise<void>;
    /** Move a completed staging file into place, replacing the target. */
    replaceFile(stagingPath: string, targetPath: string): Promise<void>;
    /** SDK copy contract: reject an existing destination. No overwrite/rename
     * fallback, atomic visibility or native fsync guarantee is implied. */
    copyFileExclusive?(stagingPath: string, targetPath: string): Promise<void>;
    supportsNativeAppend?(): boolean;
    listDirectory?(path: string): Promise<{ files: string[]; folders: string[] }>;
    deleteFile(path: string): Promise<void>;
    renameFile(oldPath: string, newPath: string): Promise<void>;
    /** Null means confirmed absence. Directories and IO failures reject. */
    stat(path: string): Promise<FileStat | null>;
    /** Return stats for all vault files from the in-memory cache — synchronous, no IPC. */
    statBulk(): Map<string, FileStat>;
    exists(path: string): Promise<boolean>;
    mkdir(path: string): Promise<void>;
    listFiles(): string[];
    /** OS-absolute path for a vault-relative path, used for Node.js fs streaming.
     *  Returns null on mobile (no Node.js fs). */
    getAbsolutePath(path: string): string | null;
    /** Optional, runtime-qualified mobile resource reader. Null means the
     * public host path failed closed and the file must remain pending. */
    openMobileRangeReader?(path: string, expected: FileStat,
        signal?: AbortSignal, owner?: TransientWorkScope): Promise<MobileRangeReader | null>;
    mobileRangeCapability?(): {
        state: "unprobed" | "qualified" | "rejected";
        reason: "UNAVAILABLE" | "PROTOCOL" | null;
        maxRangeBytes: number;
    };
    /** Stat all files inside .obsidian/ recursively.
     *  Used when syncObsidianConfig is enabled. */
    listObsidianConfig(): Promise<Map<string, FileStat>>;
}

/** Desktop implementation — uses Obsidian vault adapter (works on Electron). */
export class ObsetyncDesktopIO implements PlatformIO {
    private absoluteRoot: string | null | undefined;
    private verifiedReadAvailable = true;

    constructor(protected readonly app: App) {}

    async readFile(path: string): Promise<Uint8Array> {
        const buf = await this.app.vault.adapter.readBinary(path);
        return new Uint8Array(buf);
    }

    async readFileIdentityVerified(path: string, expected: FileStat,
        signal?: AbortSignal): Promise<Uint8Array | null> {
        if (!this.verifiedReadAvailable) return null;
        if (!isSafeVaultPath(path)) throw new RangeError("invalid desktop source path");
        const absolutePath = this.getAbsolutePath(path);
        if (!absolutePath) return null;
        if (this.absoluteRoot === undefined) this.absoluteRoot = this.getAbsolutePath("");
        if (!this.absoluteRoot) return null;
        const requireNode = typeof require === "function" ? require : (globalThis as any).require;
        let pathModule: typeof import("node:path");
        try { pathModule = requireNode?.("node:path"); }
        catch { return null; }
        if (!pathModule) return null;
        const root = pathModule.resolve(this.absoluteRoot);
        const source = pathModule.resolve(absolutePath);
        const relative = pathModule.relative(root, source);
        if (!relative || relative === ".." || relative.startsWith(`..${pathModule.sep}`) || pathModule.isAbsolute(relative)) {
            throw new Error("desktop source escaped the vault root");
        }
        const ancestorPaths = [root];
        let ancestor = root;
        for (const component of relative.split(pathModule.sep).slice(0, -1)) {
            ancestor = pathModule.join(ancestor, component);
            ancestorPaths.push(ancestor);
        }
        const data = await readDesktopFileIdentityVerified({
            path, absolutePath: source, ancestorPaths, expected,
        }, signal);
        if (data === null) this.verifiedReadAvailable = false;
        return data;
    }

    async writeFile(path: string, data: Uint8Array): Promise<void> {
        const dir = path.substring(0, path.lastIndexOf("/"));
        if (dir) await this.mkdir(dir);
        await this.app.vault.adapter.writeBinary(path, exactArrayBuffer(data));
    }

    async appendFile(path: string, data: Uint8Array): Promise<void> {
        await this.appendFileOwned(path, data);
    }

    async appendFileOwned(path: string, data: Uint8Array, memory?: TransientWorkScope): Promise<void> {
        const dir = path.substring(0, path.lastIndexOf("/"));
        if (dir) await this.mkdir(dir);
        await appendBinaryBounded(this.app.vault.adapter, (file) => this.stat(file), path, data, { memory });
    }

    async replaceFile(stagingPath: string, targetPath: string): Promise<void> {
        const dir = targetPath.substring(0, targetPath.lastIndexOf("/"));
        if (dir) await this.mkdir(dir);

        // Electron can use the OS atomic-replace primitive directly.
        const stagingAbsolute = this.getAbsolutePath(stagingPath);
        const targetAbsolute = this.getAbsolutePath(targetPath);
        const fs = (globalThis as any).require?.("fs") as typeof import("fs") | undefined;
        if (stagingAbsolute && targetAbsolute && fs?.renameSync) {
            try {
                // Atomic replacement on POSIX. Windows can reject rename over
                // an existing target, in which case the recoverable adapter
                // sequence below handles it instead.
                fs.renameSync(stagingAbsolute, targetAbsolute);
                return;
            } catch {
                // Fall through to target → backup → staging → target.
            }
        }

        // Mobile adapter fallback: keep the old target in the internal
        // staging area until the new file is in place, then remove it.
        const backupPath = `${stagingPath}.previous`;
        const adapter = this.app.vault.adapter;
        let backedUp = false;
        try {
            if (await adapter.exists(backupPath)) await adapter.remove(backupPath);
            if (await adapter.exists(targetPath)) {
                await adapter.rename(targetPath, backupPath);
                backedUp = true;
            }
            await adapter.rename(stagingPath, targetPath);
            if (backedUp) await adapter.remove(backupPath);
        } catch (error) {
            if (!(await adapter.exists(targetPath)) && backedUp && await adapter.exists(backupPath)) {
                try { await adapter.rename(backupPath, targetPath); } catch { /* preserve original error */ }
            }
            throw error;
        }
    }

    async copyFileExclusive(stagingPath: string, targetPath: string): Promise<void> {
        const adapter = this.app.vault.adapter;
        if (typeof adapter.copy !== "function") throw new Error("exclusive file copy is unavailable");
        const dir = targetPath.substring(0, targetPath.lastIndexOf("/"));
        if (dir) await this.mkdir(dir);
        // DataAdapter.copy explicitly fails if the destination already exists.
        // Keep its real native completion; do not emulate it with a TOCTOU
        // exists/write pair or a replacing rename, even on an adapter error.
        await adapter.copy(stagingPath, targetPath);
    }

    supportsNativeAppend(): boolean { return typeof this.app.vault.adapter.appendBinary === "function"; }
    listDirectory(path: string): Promise<{ files: string[]; folders: string[] }> {
        return this.app.vault.adapter.list(path);
    }

    async deleteFile(path: string): Promise<void> {
        try {
            await this.app.vault.adapter.remove(path);
        } catch (error) {
            if (isMissingPathError(error)) return;
            // Mobile adapters may omit errno. Preserve idempotent deletion
            // only when a fresh stat confirms absence, not by error text.
            if (await this.stat(path) !== null) throw error;
        }
    }

    async renameFile(oldPath: string, newPath: string): Promise<void> {
        const dir = newPath.substring(0, newPath.lastIndexOf("/"));
        if (dir) await this.mkdir(dir);
        await this.app.vault.adapter.rename(oldPath, newPath);
    }

    async stat(path: string): Promise<FileStat | null> {
        return readFileStat(path, (file) => this.app.vault.adapter.stat(file));
    }

    async exists(path: string): Promise<boolean> {
        return this.app.vault.adapter.exists(path);
    }

    async mkdir(path: string): Promise<void> {
        try {
            await this.app.vault.adapter.mkdir(path);
        } catch (error) {
            // Some adapters do not attach EEXIST. Verify the existing type
            // instead of swallowing permission/IO failures unconditionally.
            const existing = await this.app.vault.adapter.stat(path);
            if (existing?.type !== "folder") throw error;
        }
    }

    statBulk(): Map<string, FileStat> {
        const map = new Map<string, FileStat>();
        for (const f of this.app.vault.getFiles()) {
            map.set(f.path, { mtime: f.stat.mtime, size: f.stat.size });
        }
        return map;
    }

    listFiles(): string[] {
        return this.app.vault.getFiles().map((f) => f.path);
    }

    getAbsolutePath(path: string): string | null {
        try {
            // FileSystemAdapter exposes getFullPath() on desktop/Electron.
            return (this.app.vault.adapter as any).getFullPath?.(path) ?? null;
        } catch {
            return null;
        }
    }

    async listObsidianConfig(): Promise<Map<string, FileStat>> {
        const map = new Map<string, FileStat>();
        // Fast path on desktop: Node.js synchronous stat — no IPC per file.
        const absRoot = this.getAbsolutePath('.obsidian');
        if (absRoot) {
            const fs   = (globalThis as any).require?.('fs')   as typeof import('fs')   | undefined;
            const path = (globalThis as any).require?.('path') as typeof import('path') | undefined;
            if (fs && path) {
                let rootStat: import('fs').Stats;
                try {
                    rootStat = fs.statSync(absRoot);
                } catch (error) {
                    if (isMissingPathError(error)) return map;
                    throw error;
                }
                if (!rootStat.isDirectory()) {
                    throw new Error("expected a directory for configuration listing: .obsidian");
                }
                const recurse = (absDir: string, relDir: string) => {
                    const entries = fs.readdirSync(absDir, { withFileTypes: true });
                    for (const e of entries) {
                        const absChild = path.join(absDir, e.name);
                        const relChild = `${relDir}/${e.name}`;
                        if (e.isDirectory()) {
                            recurse(absChild, relChild);
                        } else if (e.isFile()) {
                            const s = fs.statSync(absChild);
                            if (s.isDirectory()) throw new PathIsDirectoryError(relChild);
                            if (!s.isFile()) throw new Error(`expected a regular file: ${relChild}`);
                            map.set(relChild, { mtime: s.mtimeMs, size: s.size });
                        }
                    }
                };
                recurse(absRoot, '.obsidian');
                return map;
            }
        }
        // Fallback (mobile / no Node.js): use Obsidian adapter recursively.
        return collectAdapterFileStats('.obsidian', this.app.vault.adapter);
    }
}


/** Mobile (iOS) implementation — same adapter, same code. */
export class ObsetyncMobileIO extends ObsetyncDesktopIO {
    private rangeState: "unprobed" | "qualified" | "rejected" = "unprobed";
    private rangeReason: "UNAVAILABLE" | "PROTOCOL" | null = null;

    constructor(app: App, private readonly rangeHost: MobileRangeHost = browserMobileRangeHost) {
        super(app);
    }

    async readFile(path: string): Promise<Uint8Array> {
        const stat = await this.stat(path);
        if (stat && stat.size > MOBILE_WHOLE_FILE_LIMIT) {
            throw new Error(
                `memory-safe mobile read limit exceeded for ${path} ` +
                `(${Math.ceil(stat.size / 1_048_576)} MiB > 128 MiB); ` +
                "upload this file from desktop or remove it from sync",
            );
        }
        return super.readFile(path);
    }

    /** Mobile retains the adapter whole-read/stat behavior; it never reaches
     * Electron file descriptors inherited from the desktop implementation. */
    async readFileIdentityVerified(_path: string, _expected: FileStat,
        _signal?: AbortSignal): Promise<Uint8Array | null> { return null; }

    // No Node.js fs on iOS — streaming hash not available.
    getAbsolutePath(_path: string): string | null { return null; }
    mobileRangeCapability() {
        return {
            state: this.rangeState,
            reason: this.rangeReason,
            maxRangeBytes: MOBILE_RESOURCE_RANGE_MAX_BYTES,
        } as const;
    }

    async openMobileRangeReader(path: string, expected: FileStat,
        signal?: AbortSignal, owner?: TransientWorkScope): Promise<MobileRangeReader | null> {
        if (this.rangeState === "rejected") return null;
        const adapter = this.app.vault.adapter;
        if (typeof adapter.getResourcePath !== "function") {
            this.rangeState = "rejected";
            this.rangeReason = "UNAVAILABLE";
            return null;
        }
        let resourceUrl: string;
        try { resourceUrl = adapter.getResourcePath(path); }
        catch {
            this.rangeState = "rejected";
            this.rangeReason = "UNAVAILABLE";
            return null;
        }
        try {
            const reader = await openQualifiedMobileRangeReader({
                resourceUrl,
                expected,
                stat: () => this.stat(path),
                currentResourceUrl: () => adapter.getResourcePath(path),
                host: this.rangeHost,
                signal,
                owner,
                onCapabilityFailure: (code) => {
                    this.rangeState = "rejected";
                    this.rangeReason = code;
                },
            });
            this.rangeState = "qualified";
            this.rangeReason = null;
            return reader;
        } catch (error) {
            if (error instanceof MobileRangeReadError &&
                (error.code === "UNAVAILABLE" || error.code === "PROTOCOL")) {
                this.rangeState = "rejected";
                this.rangeReason = error.code;
                return null;
            }
            throw error;
        }
    }
    // Falls back to adapter path in listObsidianConfig automatically (absRoot is null).
}

/** Create the appropriate PlatformIO for the current platform. */
export function createPlatformIO(app: App): PlatformIO {
    return runtimeForHost(Platform) === "mobile" ? new ObsetyncMobileIO(app) : new ObsetyncDesktopIO(app);
}
