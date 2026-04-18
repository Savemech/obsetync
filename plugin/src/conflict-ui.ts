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
export class ConflictModal extends Modal {
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

/** Scan the vault for .sync-conflict files and return conflict info. */
export function findConflicts(io: PlatformIO): ConflictInfo[] {
    const files = io.listFiles();
    const conflicts: ConflictInfo[] = [];
    const conflictPattern = /\.conflict-\d{8}-\d{6}-/;

    for (const path of files) {
        if (conflictPattern.test(path)) {
            // Extract original filename by removing the conflict suffix.
            const parts = path.match(
                /^(.+)\.conflict-\d{8}-\d{6}-[^.]+(\.[^.]+)$/
            );
            if (parts) {
                conflicts.push({
                    original: parts[1] + parts[2],
                    preservedAs: path,
                });
            }
        }
    }

    return conflicts;
}
