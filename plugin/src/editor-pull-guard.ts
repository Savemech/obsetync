import type { App, Editor, EventRef, MarkdownFileInfo, MarkdownView } from "obsidian";
import { isSafeVaultPath } from "./delta-validation";

const MAX_OPEN_EDITOR_PATHS = 64;
const MAX_EDITOR_BASELINE_BYTES = 1024 * 1024;

type EditorInfo = MarkdownView | MarkdownFileInfo;
type OpenEditor = {
    info: EditorInfo;
    editor: Editor;
};

function editorMatchesDisk(editor: Editor, disk: string): boolean {
    if (disk.length > MAX_EDITOR_BASELINE_BYTES || typeof editor.lineCount !== "function" ||
        typeof editor.getLine !== "function") return false;
    const count = editor.lineCount();
    if (!Number.isSafeInteger(count) || count < 1 || count > MAX_EDITOR_BASELINE_BYTES + 1) return false;
    let offset = 0;
    let observedUnits = 0;
    for (let line = 0; line < count; line++) {
        const nextNewline = disk.indexOf("\n", offset);
        const finalLine = line === count - 1;
        if ((!finalLine && nextNewline < 0) || (finalLine && nextNewline >= 0)) return false;
        const end = nextNewline < 0 ? disk.length : nextNewline;
        const value = editor.getLine(line);
        if (typeof value !== "string") return false;
        observedUnits += value.length + (finalLine ? 0 : 1);
        if (observedUnits > MAX_EDITOR_BASELINE_BYTES || value !== disk.slice(offset, end)) return false;
        offset = end + (nextNewline < 0 ? 0 : 1);
    }
    return offset === disk.length;
}

/**
 * Pull-scoped protection for editor bytes which have not reached the vault
 * adapter yet. The ordinary journal/dirty guard cannot see those bytes.
 *
 * The editor-change listener is attached before the baseline audit. Once a
 * path is protected it stays protected for the lifetime of this pull: a later
 * autosave may enqueue it, but cannot make an older remote delta applicable
 * again. Baseline reads are sequential and capped; an oversized or malformed
 * open-editor set fails closed for this pull rather than allocating without a
 * bound or overwriting an editor we could not classify.
 */
export class EditorPullGuard {
    private readonly protectedPaths = new Set<string>();
    private eventRef: EventRef | null = null;
    private protectAll = false;
    private closed = false;

    private constructor(
        private readonly app: Pick<App, "vault" | "workspace">,
        private readonly assertCurrent: () => void,
    ) {}

    static async open(
        app: Pick<App, "vault" | "workspace">,
        assertCurrent: () => void = () => {},
    ): Promise<EditorPullGuard> {
        const guard = new EditorPullGuard(app, assertCurrent);
        try {
            await guard.start();
            return guard;
        } catch (error) {
            guard.close();
            throw error;
        }
    }

    has(path: string): boolean {
        return this.protectAll || this.protectedPaths.has(path);
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;
        const ref = this.eventRef;
        this.eventRef = null;
        if (ref) {
            try { this.app.workspace.offref(ref); }
            catch (error) {
                console.warn("[obsetync] failed to detach pull editor guard:", error);
            }
        }
    }

    private async start(): Promise<void> {
        const workspace = this.app.workspace;
        // Narrow compatibility for old embeddings and structural engine tests.
        // Current Obsidian exposes both methods; a partially exposed capability
        // is unsafe and therefore protects every path for this pull.
        const canListen = typeof workspace.on === "function";
        const canEnumerate = typeof workspace.getLeavesOfType === "function";
        const canClose = typeof workspace.offref === "function";
        if (!canListen && !canEnumerate) return;
        if (!canListen || !canEnumerate || !canClose) {
            this.protectAll = true;
            return;
        }

        this.eventRef = workspace.on("editor-change", (_editor, info) => {
            if (this.closed) return;
            const path = info?.file?.path;
            if (typeof path === "string" && isSafeVaultPath(path)) {
                this.protectedPaths.add(path);
            }
        });
        this.assertOpenAndCurrent();

        let leaves: ReturnType<typeof workspace.getLeavesOfType>;
        try {
            leaves = workspace.getLeavesOfType("markdown");
        } catch {
            this.protectAll = true;
            return;
        }
        if (!Array.isArray(leaves) || leaves.length > MAX_OPEN_EDITOR_PATHS) {
            this.protectAll = true;
            return;
        }

        const byPath = new Map<string, OpenEditor[]>();
        for (const leaf of leaves) {
            const info = leaf?.view as EditorInfo | undefined;
            const path = info?.file?.path;
            const editor = info?.editor;
            if (typeof path !== "string" || !isSafeVaultPath(path)) continue;
            if (!editor || typeof editor.lineCount !== "function" || typeof editor.getLine !== "function") {
                this.protectedPaths.add(path);
                continue;
            }
            const existing = byPath.get(path);
            const opened = { info, editor };
            if (existing) existing.push(opened);
            else byPath.set(path, [opened]);
        }
        if (byPath.size > MAX_OPEN_EDITOR_PATHS) {
            this.protectAll = true;
            return;
        }

        for (const [path, editors] of byPath) {
            this.assertOpenAndCurrent();
            if (this.protectedPaths.has(path)) continue;
            const size = editors[0].info.file?.stat.size;
            if (!Number.isSafeInteger(size) || size! < 0 || size! > MAX_EDITOR_BASELINE_BYTES) {
                this.protectedPaths.add(path);
                continue;
            }
            let disk: string;
            try {
                disk = await this.app.vault.adapter.read(path);
            } catch {
                this.protectedPaths.add(path);
                this.assertOpenAndCurrent();
                continue;
            }
            this.assertOpenAndCurrent();
            if (this.protectedPaths.has(path)) continue;
            try {
                const stable = editors.every(({ info, editor }) =>
                    info.file?.path === path && editorMatchesDisk(editor, disk));
                if (!stable) {
                    this.protectedPaths.add(path);
                }
            } catch {
                this.protectedPaths.add(path);
            }
        }
    }

    private assertOpenAndCurrent(): void {
        if (this.closed) throw new Error("editor pull guard closed during baseline audit");
        this.assertCurrent();
        if (this.closed) throw new Error("editor pull guard closed during baseline audit");
    }
}
