import assert from "node:assert/strict";
import test from "node:test";
import { build } from "esbuild";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
let buttons = new Map();

class Element {
    empty() {}
    createDiv() { return new Element(); }
    createEl() { return new Element(); }
}

class Button {
    setButtonText(text) { this.text = text; return this; }
    onClick(callback) { buttons.set(this.text, callback); return this; }
}

const bundled = await build({
    absWorkingDir: root,
    entryPoints: ["src/conflict-ui.ts"],
    bundle: true,
    platform: "node",
    format: "esm",
    target: "node20",
    write: false,
    logLevel: "silent",
    plugins: [{
        name: "conflict-ui-fixture",
        setup(builder) {
            builder.onResolve({ filter: /^obsidian$/ }, () => ({ path: "obsidian", namespace: "fixture" }));
            builder.onLoad({ filter: /.*/, namespace: "fixture" }, () => ({ contents: `
                export class App {}
                export class Modal {
                    constructor(app) { this.app = app; this.contentEl = new globalThis.__conflictFence.Element(); }
                    open() { this.onOpen(); }
                    close() { this.onClose(); }
                }
                export class Setting {
                    setName() { return this; }
                    setDesc() { return this; }
                    addButton(callback) { callback(new globalThis.__conflictFence.Button()); return this; }
                }
            `, loader: "js" }));
        },
    }],
});

globalThis.__conflictFence = { Element, Button };
const source = Buffer.from(bundled.outputFiles[0].contents).toString("base64");
const { ObsetyncConflictModal } = await import(`data:text/javascript;base64,${source}`);

function deferred() {
    let resolve, reject;
    const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}

function fixture(overrides = {}) {
    buttons = new Map();
    const calls = { reads: 0, writes: 0, deletes: 0, resolved: 0 };
    const io = {
        async readFile() { calls.reads++; return new Uint8Array([1]); },
        async writeFile() { calls.writes++; },
        async deleteFile() { calls.deletes++; },
        ...overrides,
    };
    const modal = new ObsetyncConflictModal({}, io,
        [{ original: "note.md", preservedAs: "note (conflict device).md" }], () => { calls.resolved++; });
    modal.open();
    return { modal, calls };
}

test("revoke closes admission before stale buttons can touch the vault", async () => {
    const { modal, calls } = fixture();
    const staleRemote = buttons.get("Keep Remote"), staleLocal = buttons.get("Keep Local");
    assert(staleRemote && staleLocal);
    modal.revoke();
    await staleRemote(); await staleLocal();
    assert.deepEqual(calls, { reads: 0, writes: 0, deletes: 0, resolved: 0 });
    const first = modal.closeAndDrain();
    assert.equal(first, modal.closeAndDrain());
    await first;
});

test("closeAndDrain joins the full admitted read/write/delete sequence", async () => {
    const read = deferred();
    const { modal, calls } = fixture({ async readFile() { calls.reads++; await read.promise; return new Uint8Array([7]); } });
    const local = buttons.get("Keep Local");
    assert(local);
    const resolving = local();
    assert.equal(calls.reads, 1);
    const draining = modal.closeAndDrain();
    let drained = false;
    void draining.then(() => { drained = true; });
    await Promise.resolve();
    assert.equal(drained, false);
    await buttons.get("Keep Remote")();
    assert.equal(calls.deletes, 0, "post-revoke stale callback entered a second handler");
    read.resolve();
    await resolving;
    assert.deepEqual(calls, { reads: 1, writes: 1, deletes: 1, resolved: 0 });
    await draining;
    assert.equal(drained, true);
});

test("handler failure releases ownership and normal completion preserves UI behavior", async () => {
    const failure = new Error("delete failed");
    const failed = fixture({ async deleteFile() { failed.calls.deletes++; throw failure; } });
    await assert.rejects(buttons.get("Keep Remote")(), error => error === failure);
    await failed.modal.closeAndDrain();
    assert.equal(failed.calls.deletes, 1);

    const normal = fixture();
    await buttons.get("Keep Both")();
    assert.deepEqual(normal.calls, { reads: 0, writes: 0, deletes: 0, resolved: 1 });
    await normal.modal.closeAndDrain();
});

