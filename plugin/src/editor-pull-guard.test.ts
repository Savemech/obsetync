import { EditorPullGuard } from "./editor-pull-guard";

let assertions = 0;
const check = (condition: unknown, message: string): void => {
    assertions++;
    if (!condition) throw new Error(message);
};

function fixture(rows: Array<{ path: string; disk: string; editor: string; size?: number }>) {
    let listener: ((editor: unknown, info: any) => void) | null = null;
    let removed = 0;
    const disk = new Map(rows.map(row => [row.path, row.disk]));
    const views = rows.map(row => {
        const state = { value: row.editor };
        const lines = () => state.value.split("\n");
        return {
            state,
            leaf: { view: {
                file: { path: row.path, stat: { size: row.size ?? row.disk.length } },
                editor: { lineCount: () => lines().length, getLine: (line: number) => lines()[line] },
            } },
        };
    });
    const app = {
        vault: { adapter: { read: async (path: string) => {
            const value = disk.get(path);
            if (value === undefined) throw new Error("missing");
            return value;
        } } },
        workspace: {
            on: (name: string, callback: typeof listener) => {
                check(name === "editor-change", "guard subscribed to wrong workspace event");
                listener = callback;
                return { name };
            },
            offref: () => { removed++; listener = null; },
            getLeavesOfType: (kind: string) => {
                check(kind === "markdown", "guard enumerated the wrong view kind");
                return views.map(view => view.leaf);
            },
        },
    };
    return {
        app,
        views,
        fire(path: string) { listener?.({}, { file: { path } }); },
        removed: () => removed,
    };
}

async function detectsExistingUnsavedBuffers(): Promise<void> {
    const f = fixture([
        { path: "clean.md", disk: "same", editor: "same" },
        { path: "dirty.md", disk: "saved", editor: "unsaved" },
    ]);
    const guard = await EditorPullGuard.open(f.app as any);
    try {
        check(!guard.has("clean.md"), "clean open editor was deferred");
        check(guard.has("dirty.md"), "existing unsaved editor was not deferred");
    } finally { guard.close(); }
    check(f.removed() === 1, "editor listener was not retired exactly once");
}

async function catchesEditsAfterBaseline(): Promise<void> {
    const f = fixture([{ path: "live.md", disk: "saved", editor: "saved" }]);
    const guard = await EditorPullGuard.open(f.app as any);
    check(!guard.has("live.md"), "clean baseline started protected");
    f.views[0].state.value = "typing";
    f.fire("live.md");
    check(guard.has("live.md"), "mid-pull editor event did not close applicability");
    guard.close();
    f.fire("after-close.md");
    check(!guard.has("after-close.md"), "closed guard accepted new editor events");
}

async function catchesEditWhileBaselineReadIsPending(): Promise<void> {
    let release!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    let listener: ((editor: unknown, info: any) => void) | null = null;
    const state = { value: "saved" };
    const app = {
        vault: { adapter: { read: async () => { await blocked; return "saved"; } } },
        workspace: {
            on: (_name: string, callback: NonNullable<typeof listener>) => {
                listener = callback; return {};
            },
            offref: () => { listener = null; },
            getLeavesOfType: () => [{ view: {
                file: { path: "racing.md", stat: { size: 5 } },
                editor: {
                    lineCount: () => state.value.split("\n").length,
                    getLine: (line: number) => state.value.split("\n")[line],
                },
            } }],
        },
    };
    const opening = EditorPullGuard.open(app as any);
    await Promise.resolve();
    state.value = "typing";
    (listener as ((editor: unknown, info: any) => void) | null)?.(
        {}, { file: { path: "racing.md" } },
    );
    release();
    const guard = await opening;
    check(guard.has("racing.md"), "edit racing the baseline read was not deferred");
    guard.close();
}

async function failsClosedOnUnboundedOrUnreadableState(): Promise<void> {
    const oversized = fixture([{ path: "huge.md", disk: "x", editor: "x", size: 1024 * 1024 + 1 }]);
    const first = await EditorPullGuard.open(oversized.app as any);
    check(first.has("huge.md"), "oversized editor baseline was read optimistically");
    first.close();

    const many = fixture(Array.from({ length: 65 }, (_, index) => ({
        path: `many/${index}.md`, disk: "x", editor: "x",
    })));
    const second = await EditorPullGuard.open(many.app as any);
    check(second.has("unrelated.md"), "unbounded editor inventory did not fail closed");
    second.close();

    const partial = {
        vault: { adapter: { read: async () => "" } },
        workspace: { on: () => ({}) },
    };
    const third = await EditorPullGuard.open(partial as any);
    check(third.has("any.md"), "partial editor capability did not fail closed");
    third.close();
}

async function main(): Promise<void> {
    await detectsExistingUnsavedBuffers();
    await catchesEditsAfterBaseline();
    await catchesEditWhileBaselineReadIsPending();
    await failsClosedOnUnboundedOrUnreadableState();
    console.log(`editor pull guard tests passed (${assertions} assertions)`);
}

void main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
