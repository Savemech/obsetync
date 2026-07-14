import { App, Modal, Setting } from "obsidian";
import { PlatformIO } from "./platform";

interface ConflictInfo {
    original: string;
    preservedAs: string;
    winnerDevice?: string;
    preservedDevice?: string;
}

/**
 * Modal for resolving sync conflicts.
 * Shows a list of conflicted files and lets the user choose which to keep.
 */
export class ObsetyncConflictModal extends Modal {
    private conflicts: ConflictInfo[];
    private io: PlatformIO;
    private onResolved: () => void;

    constructor(
        app: App,
        io: PlatformIO,
        conflicts: ConflictInfo[],
        onResolved: () => void
    ) {
        super(app);
        this.io = io;
        this.conflicts = conflicts;
        this.onResolved = onResolved;
    }

    onOpen(): void {
        const { contentEl } = this;
        contentEl.empty();
        contentEl.createEl("h2", { text: "Sync Conflicts" });
        contentEl.createEl("p", {
            text: `${this.conflicts.length} file(s) have conflicting changes.`,
        });

        for (const conflict of this.conflicts) {
            const div = contentEl.createDiv({ cls: "obsetync-conflict" });
            div.createEl("strong", { text: conflict.original });

            new Setting(div)
                .setName("Resolution")
                .setDesc(
                    `Conflict copy saved as: ${conflict.preservedAs}`
                )
                .addButton((btn) =>
                    btn.setButtonText("Keep Remote").onClick(async () => {
                        // Delete the conflict copy, keep the winner.
                        await this.io.deleteFile(conflict.preservedAs);
                        this.removeConflict(conflict);
                    })
                )
                .addButton((btn) =>
                    btn.setButtonText("Keep Local").onClick(async () => {
                        // Replace winner with preserved copy.
                        const data = await this.io.readFile(conflict.preservedAs);
                        await this.io.writeFile(conflict.original, data);
                        await this.io.deleteFile(conflict.preservedAs);
                        this.removeConflict(conflict);
                    })
                )
                .addButton((btn) =>
                    btn.setButtonText("Keep Both").onClick(async () => {
                        // Just remove from conflict list, files stay as-is.
                        this.removeConflict(conflict);
                    })
                );
        }
    }

    private removeConflict(conflict: ConflictInfo): void {
        this.conflicts = this.conflicts.filter((c) => c !== conflict);
        if (this.conflicts.length === 0) {
            this.close();
            this.onResolved();
        } else {
            this.onOpen(); // Refresh.
        }
    }

    onClose(): void {
        this.contentEl.empty();
    }
}

/** The one true conflict-copy naming scheme. Built here and parsed by
 *  `findConflicts` below so the writer (sync engine) and the scanner can
 *  never drift apart again (the previous scanner searched for a pattern
 *  nothing ever produced).
 *
 *  "notes/doc.md" → "notes/doc (conflict Laptop 2026-07-14 0132).md"
 */
export function conflictCopyPath(path: string, device: string, when: Date): string {
    const dot = path.lastIndexOf(".");
    const slash = path.lastIndexOf("/");
    const hasExt = dot > slash + 1;
    const stem = hasExt ? path.slice(0, dot) : path;
    const ext = hasExt ? path.slice(dot) : "";
    // Keep the device name filesystem-safe across all platforms.
    const dev = device.replace(/[\\/:*?"<>|()]/g, "-").trim() || "device";
    const pad = (n: number) => String(n).padStart(2, "0");
    const stamp =
        `${when.getFullYear()}-${pad(when.getMonth() + 1)}-${pad(when.getDate())} ` +
        `${pad(when.getHours())}${pad(when.getMinutes())}`;
    return `${stem} (conflict ${dev} ${stamp})${ext}`;
}

const CONFLICT_COPY_RE = /^(.*) \(conflict [^)]+\)(\.[^./]+)?$/;

/** Scan the vault for conflict-copy files and return conflict info. */
export function findConflicts(io: PlatformIO): ConflictInfo[] {
    const files = io.listFiles();
    const conflicts: ConflictInfo[] = [];

    for (const path of files) {
        const parts = path.match(CONFLICT_COPY_RE);
        if (parts) {
            conflicts.push({
                original: parts[1] + (parts[2] ?? ""),
                preservedAs: path,
            });
        }
    }

    return conflicts;
}
