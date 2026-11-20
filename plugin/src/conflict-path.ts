/** Shared, filesystem-safe conflict-copy naming scheme.
 *
 * "notes/doc.md" → "notes/doc (conflict Laptop 2026-07-14 0132).md"
 */
export async function conflictCopyPath(path: string, device: string, when: Date): Promise<string> {
    const dot = path.lastIndexOf(".");
    const slash = path.lastIndexOf("/");
    const hasExt = dot > slash + 1;
    const stem = hasExt ? path.slice(0, dot) : path;
    const ext = hasExt ? path.slice(dot) : "";
    const dev = device.replace(/[\\/:*?"<>|()]/g, "-").trim() || "device";
    const pad = (n: number) => String(n).padStart(2, "0");
    const stamp =
        `${when.getFullYear()}-${pad(when.getMonth() + 1)}-${pad(when.getDate())} ` +
        `${pad(when.getHours())}${pad(when.getMinutes())}`;
    const result = `${stem} (conflict ${dev} ${stamp})${ext}`;
    const encoder = new TextEncoder();
    if (encoder.encode(result.slice(slash + 1)).length <= 255) return result;

    // A shortened stem cannot safely identify the original in the conflict UI.
    const identity = encoder.encode(JSON.stringify(["obsetync-long-conflict-v1", path, device, stamp]));
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", identity));
    return digestConflictCopyPath(path, Array.from(digest, byte => byte.toString(16).padStart(2, "0")).join(""));
}

export function digestConflictCopyPath(path: string, suffix: string): string {
    const slash = path.lastIndexOf("/"), parent = path.slice(0, slash + 1), name = path.slice(slash + 1);
    const dot = name.lastIndexOf(".");
    const extension = dot > 0 && /^\.[a-zA-Z0-9]{1,16}$/.test(name.slice(dot)) ? name.slice(dot) : "";
    const sourceStem = extension ? name.slice(0, dot) : name;
    const { read } = new TextEncoder().encodeInto(sourceStem, new Uint8Array(80));
    const stem = sourceStem.slice(0, read).replace(/[. ]+$/, "") || "file";
    return `${parent}${stem} (conflict sync ${suffix})${extension}`;
}
