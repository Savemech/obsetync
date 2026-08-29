/** Shared, filesystem-safe conflict-copy naming scheme.
 *
 * "notes/doc.md" → "notes/doc (conflict Laptop 2026-07-14 0132).md"
 */
export function conflictCopyPath(path: string, device: string, when: Date): string {
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
    return `${stem} (conflict ${dev} ${stamp})${ext}`;
}
