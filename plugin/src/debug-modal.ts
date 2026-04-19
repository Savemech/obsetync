import { App, Modal, Notice } from "obsidian";

/**
 * Scrollable modal for the "Show debug info" button.
 * Renders preformatted text and offers a one-tap copy button.
 */
export class DebugInfoModal extends Modal {
    constructor(app: App, private text: string) {
        super(app);
    }

    onOpen(): void {
        const { contentEl } = this;
        contentEl.empty();

        contentEl.createEl("h2", { text: "ObsetyNC debug info" });

        const pre = contentEl.createEl("pre", { text: this.text });
        pre.setAttribute(
            "style",
            "max-height: 60vh; overflow: auto; font-size: 11px; " +
                "white-space: pre-wrap; word-break: break-all; user-select: text; " +
                "border: 1px solid var(--background-modifier-border); padding: 12px; " +
                "background: var(--background-primary-alt); border-radius: 4px;"
        );

        const actions = contentEl.createDiv({ cls: "modal-button-container" });
        actions.setAttribute("style", "display: flex; gap: 8px; margin-top: 12px;");

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
