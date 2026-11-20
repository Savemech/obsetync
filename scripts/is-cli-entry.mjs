import { statSync } from "node:fs";

// Node may load the main module through a symlink or a differently cased mount path.
export function isCliEntry(moduleUrl) {
    if (!process.argv[1]) return false;
    const options = { bigint: true, throwIfNoEntry: false };
    const entry = statSync(process.argv[1], options);
    if (!entry?.isFile()) return false;
    const module = statSync(new URL(moduleUrl), options);
    return module?.dev === entry.dev && module.ino === entry.ino;
}
