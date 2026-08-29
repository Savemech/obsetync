import type { FileDelta } from "./api";

const MAX_DELTAS = 2_000_000;
const HASH = /^[0-9a-f]{64}$/i;

export function isSafeVaultPath(path: unknown): path is string {
    if (typeof path !== "string" || path.length === 0 || path.length > 4096) return false;
    if (path.startsWith("/") || path.includes("\\")) return false;
    if ([...path].some((char) => {
        const code = char.codePointAt(0)!;
        return code < 0x20 || code === 0x7f;
    })) return false;
    const segments = path.split("/");
    return segments.every((segment) => segment.length > 0 && segment !== "." && segment !== "..");
}

export function validateFileDeltas(value: unknown): FileDelta[] {
    if (!Array.isArray(value) || value.length > MAX_DELTAS) {
        throw new Error("server returned an invalid delta list");
    }

    const affectedPaths = new Set<string>();
    return value.map((candidate, index) => {
        if (!candidate || typeof candidate !== "object") {
            throw new Error(`server delta ${index} is not an object`);
        }
        const row = candidate as Record<string, unknown>;
        if (!isSafeVaultPath(row.path)) {
            throw new Error(`server delta ${index} has an unsafe vault path`);
        }
        const path = row.path;
        if (affectedPaths.has(path)) {
            throw new Error(`server delta list touches ${path} more than once`);
        }

        if (row.action === "deleted") {
            affectedPaths.add(path);
            return { action: "deleted", path };
        }
        if (row.action !== "added" && row.action !== "modified" && row.action !== "renamed") {
            throw new Error(`server delta ${index} has an unknown action`);
        }
        if (typeof row.hash !== "string" || !HASH.test(row.hash)) {
            throw new Error(`server delta ${index} has an invalid content hash`);
        }
        if (!Number.isSafeInteger(row.size) || (row.size as number) < 0) {
            throw new Error(`server delta ${index} has an invalid size`);
        }
        if (!Number.isSafeInteger(row.mtime_ms) || (row.mtime_ms as number) < 0) {
            throw new Error(`server delta ${index} has an invalid mtime`);
        }

        const base = {
            action: row.action,
            path,
            hash: row.hash.toLowerCase(),
            size: row.size as number,
            mtime_ms: row.mtime_ms as number,
        };
        if (row.action === "renamed") {
            if (!isSafeVaultPath(row.old_path) || row.old_path === path) {
                throw new Error(`server delta ${index} has an unsafe rename source`);
            }
            if (affectedPaths.has(row.old_path)) {
                throw new Error(`server delta list touches ${row.old_path} more than once`);
            }
            affectedPaths.add(row.old_path);
            affectedPaths.add(path);
            return { ...base, action: "renamed" as const, old_path: row.old_path };
        }
        affectedPaths.add(path);
        return { ...base, action: row.action } as FileDelta;
    });
}
