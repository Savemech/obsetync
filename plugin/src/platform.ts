import { App, Platform } from "obsidian";
import { exactArrayBuffer } from "./binary";
import {
    collectAdapterFileStats,
    isMissingPathError,
    PathIsDirectoryError,
    readFileStat,
    type FileStat,
} from "./file-safety";
export type { FileStat } from "./file-safety";

const LEGACY_APPEND_LIMIT = 32 * 1024 * 1024;
/** Mobile DataAdapter has no ranged reads. Refuse a single allocation large
 *  enough to plausibly trigger Jetsam on older 2 GB devices; large remote
 *  restores remain chunked and do not use readFile(). */
const MOBILE_WHOLE_FILE_LIMIT = 128 * 1024 * 1024;

export interface PlatformIO {
    readFile(path: string): Promise<Uint8Array>;
    writeFile(path: string, data: Uint8Array): Promise<void>;
    /** Append one binary segment without reading the existing file. */
    appendFile(path: string, data: Uint8Array): Promise<void>;
    /** Move a completed staging file into place, replacing the target. */
    replaceFile(stagingPath: string, targetPath: string): Promise<void>;
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
    /** Stat all files inside .obsidian/ recursively.
     *  Used when syncObsidianConfig is enabled. */
    listObsidianConfig(): Promise<Map<string, FileStat>>;
}

/** Desktop implementation — uses Obsidian vault adapter (works on Electron). */
export class ObsetyncDesktopIO implements PlatformIO {
    constructor(private app: App) {}

    async readFile(path: string): Promise<Uint8Array> {
        const buf = await this.app.vault.adapter.readBinary(path);
        return new Uint8Array(buf);
    }

    async writeFile(path: string, data: Uint8Array): Promise<void> {
        const dir = path.substring(0, path.lastIndexOf("/"));
        if (dir) await this.mkdir(dir);
        await this.app.vault.adapter.writeBinary(path, exactArrayBuffer(data));
    }

    async appendFile(path: string, data: Uint8Array): Promise<void> {
        const dir = path.substring(0, path.lastIndexOf("/"));
        if (dir) await this.mkdir(dir);
        const adapter = this.app.vault.adapter;
        if (typeof adapter.appendBinary === "function") {
            await adapter.appendBinary(path, exactArrayBuffer(data));
            return;
        }

        // Compatibility fallback for old Obsidian adapters. Current mobile
        // builds expose appendBinary. The fallback must materialize both the
        // old partial and its replacement, so cap it before an old iPad turns
        // a missing API into an opaque Jetsam restart.
        const stat = await this.stat(path);
        if ((stat?.size ?? 0) + data.length > LEGACY_APPEND_LIMIT) {
            throw new Error(
                "Obsidian is too old for memory-safe large-file resume " +
                "(appendBinary unavailable); update Obsidian and retry",
            );
        }
        // A failed read of existing staging data must not turn into an empty
        // prefix and overwrite the recoverable partial download.
        const previous = stat
            ? new Uint8Array(await adapter.readBinary(path))
            : new Uint8Array();
        const combined = new Uint8Array(previous.length + data.length);
        combined.set(previous);
        combined.set(data, previous.length);
        await adapter.writeBinary(path, combined.buffer);
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

    // No Node.js fs on iOS — streaming hash not available.
    getAbsolutePath(_path: string): string | null { return null; }
    // Falls back to adapter path in listObsidianConfig automatically (absRoot is null).
}

/** Create the appropriate PlatformIO for the current platform. */
export function createPlatformIO(app: App): PlatformIO {
    return Platform.isMobile ? new ObsetyncMobileIO(app) : new ObsetyncDesktopIO(app);
}
