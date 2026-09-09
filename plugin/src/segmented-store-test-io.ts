import type { SegmentedStoreIO } from "./segmented-store";
import { checksumJournalUtf8 } from "./journal-format";

export interface StoreTestBoundary {
    index: number;
    method: "exists" | "read" | "write" | "mkdir" | "rename" | "remove" | "stat";
    phase: "before" | "after";
    path: string;
    to?: string;
    data?: string;
}

export interface StoreTestDisk {
    files: Map<string, string>;
    directories: Set<string>;
}

export const storeTestIOError = (code = "EIO"): Error => Object.assign(new Error(`injected ${code}`), { code });

/** Test-only, path-aware adapter. before/after hooks model rejection on either
 * side of an adapter completion; they do not claim fsync or native crash tests.
 * Immutable strings make cloned disks stable even while the original advances. */
export class MemorySegmentedIO implements SegmentedStoreIO {
    readonly files: Map<string, string>;
    readonly directories: Set<string>;
    readonly events: StoreTestBoundary[] = [];
    onBoundary?: (event: StoreTestBoundary, io: MemorySegmentedIO) => void | Promise<void>;

    constructor(disk?: StoreTestDisk) {
        this.files = new Map(disk?.files);
        this.directories = new Set(disk?.directories);
    }

    snapshot(): StoreTestDisk {
        return { files: new Map(this.files), directories: new Set(this.directories) };
    }

    clone(): MemorySegmentedIO { return new MemorySegmentedIO(this.snapshot()); }

    private async boundary(event: Omit<StoreTestBoundary, "index">): Promise<void> {
        const entry = { ...event, index: this.events.length };
        this.events.push(entry);
        await this.onBoundary?.(entry, this);
    }

    async exists(path: string): Promise<boolean> {
        await this.boundary({ method: "exists", phase: "before", path });
        const value = this.files.has(path) || this.directories.has(path);
        await this.boundary({ method: "exists", phase: "after", path });
        return value;
    }

    async read(path: string): Promise<string> {
        await this.boundary({ method: "read", phase: "before", path });
        if (!this.files.has(path)) throw storeTestIOError("ENOENT");
        const value = this.files.get(path)!;
        await this.boundary({ method: "read", phase: "after", path });
        return value;
    }

    async write(path: string, data: string): Promise<void> {
        await this.boundary({ method: "write", phase: "before", path, data });
        this.files.set(path, data);
        await this.boundary({ method: "write", phase: "after", path, data });
    }

    async mkdir(path: string): Promise<void> {
        await this.boundary({ method: "mkdir", phase: "before", path });
        this.directories.add(path);
        await this.boundary({ method: "mkdir", phase: "after", path });
    }

    async rename(path: string, to: string): Promise<void> {
        await this.boundary({ method: "rename", phase: "before", path, to });
        if (!this.files.has(path)) throw storeTestIOError("ENOENT");
        if (this.files.has(to)) throw storeTestIOError("EEXIST");
        this.files.set(to, this.files.get(path)!);
        this.files.delete(path);
        await this.boundary({ method: "rename", phase: "after", path, to });
    }

    async remove(path: string): Promise<void> {
        await this.boundary({ method: "remove", phase: "before", path });
        if (!this.files.delete(path)) throw storeTestIOError("ENOENT");
        await this.boundary({ method: "remove", phase: "after", path });
    }

    async stat(path: string): Promise<{ type: string; size: number } | null> {
        await this.boundary({ method: "stat", phase: "before", path });
        const value = this.files.get(path);
        const result = value === undefined ? null : { type: "file", size: new TextEncoder().encode(value).byteLength };
        await this.boundary({ method: "stat", phase: "after", path });
        return result;
    }
}

/** Intentional semantic corruption tests re-seal a frame: checksum validity
 * alone must not grant it a different epoch, sequence, or publication cut. */
export function sealStoreTestFrame(value: unknown): string {
    const payload = JSON.stringify(value);
    return JSON.stringify({ schema: 1, payload, ...checksumJournalUtf8(payload) });
}

export function readStoreTestFrame(io: MemorySegmentedIO, path: string): any {
    const raw = io.files.get(path);
    if (raw === undefined) throw new Error(`missing test frame: ${path}`);
    return JSON.parse(JSON.parse(raw).payload);
}
