import { App, Modal, Notice } from "obsidian";

/**
 * Scrollable modal for the "Show debug info" button.
 * Renders preformatted text and offers a one-tap copy button.
 */
export class ObsetyncDebugModal extends Modal {
    constructor(app: App, private text: string) {
        super(app);
    }

    onOpen(): void {
        const { contentEl } = this;
        contentEl.empty();

        contentEl.createEl("h2", { text: "ObsetyNC debug info" });

        contentEl.createEl("pre", { text: this.text, cls: "obsetync-debug-pre" });

        const actions = contentEl.createDiv({
            cls: "modal-button-container obsetync-modal-actions",
        });

        const copyBtn = actions.createEl("button", {
            text: "Copy to clipboard",
            cls: "mod-cta",
        });
        copyBtn.onclick = async () => {
            try {
                await navigator.clipboard.writeText(this.text);
                new Notice("Copied.");
            } catch {
                new Notice("Clipboard unavailable — select text manually.");
            }
        };

        const closeBtn = actions.createEl("button", { text: "Close" });
        closeBtn.onclick = () => this.close();
    }

    onClose(): void {
        this.contentEl.empty();
    }
}
