import { App, Modal, Setting } from "obsidian";
import { PlatformIO } from "./platform";
export { conflictCopyPath } from "./conflict-path";

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
    private revoked = false;
    private activeHandlers = 0;
    private readonly resolving = new Set<ConflictInfo>();
    private drainWork: Promise<void> | null = null;
    private resolveDrain: (() => void) | null = null;

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
                    btn.setButtonText("Keep Remote").onClick(() => this.runResolution(conflict, async () => {
                        // Delete the conflict copy, keep the winner.
                        await this.io.deleteFile(conflict.preservedAs);
                    }))
                )
                .addButton((btn) =>
                    btn.setButtonText("Keep Local").onClick(() => this.runResolution(conflict, async () => {
                        // Replace winner with preserved copy.
                        const data = await this.io.readFile(conflict.preservedAs);
                        await this.io.writeFile(conflict.original, data);
                        await this.io.deleteFile(conflict.preservedAs);
                    }))
                )
                .addButton((btn) =>
                    btn.setButtonText("Keep Both").onClick(() => this.runResolution(conflict, async () => {
                        // Just remove from conflict list, files stay as-is.
                    }))
                );
        }
    }

    /** Close UI admission synchronously. Operations that entered before this
     * cut retain ownership of their complete read/write/delete sequence. */
    revoke(): void {
        if (this.revoked) return;
        this.revoked = true;
        this.close();
    }

    /** Repeated terminal owners join the same exact admitted-handler tail. */
    closeAndDrain(): Promise<void> {
        this.revoke();
        if (!this.drainWork) {
            this.drainWork = new Promise<void>(resolve => { this.resolveDrain = resolve; });
            this.completeDrainIfReady();
        }
        return this.drainWork;
    }

    private runResolution(conflict: ConflictInfo, operation: () => Promise<void>): Promise<void> {
        if (this.revoked || !this.conflicts.includes(conflict) || this.resolving.has(conflict)) {
            return Promise.resolve();
        }
        this.resolving.add(conflict);
        this.activeHandlers++;
        const release = () => {
            this.resolving.delete(conflict);
            this.activeHandlers--;
            this.completeDrainIfReady();
        };
        try {
            return Promise.resolve(operation()).then(() => {
                if (!this.revoked) this.removeConflict(conflict);
            }).finally(release);
        } catch (error) {
            release();
            return Promise.reject(error);
        }
    }

    private completeDrainIfReady(): void {
        if (!this.revoked || this.activeHandlers !== 0 || !this.resolveDrain) return;
        const resolve = this.resolveDrain;
        this.resolveDrain = null;
        resolve();
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
        // Obsidian's close button is also a synchronous admission cut.
        this.revoked = true;
        this.contentEl.empty();
        this.completeDrainIfReady();
    }
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
