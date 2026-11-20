import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { runInNewContext } from "node:vm";

test("explorer keeps the selected version, path and diff across late replies", async () => {
    const source = readFileSync(new URL("../crates/sync-server/src/admin.rs", import.meta.url), "utf8");
    const script = source.split("const EXPLORER_HTML:")[1].match(/<script>([\s\S]*?)<\/script>/)[1];
    const pane = () => ({ innerHTML: "", addEventListener() {}, classList: { add() {} } });
    const elements = { "vault-home": {getAttribute: () => "/admin/vaults/test"}, versions: pane(), files: pane(), diff: pane() };
    const replies = [];
    const ctx = {
        document: { getElementById: id => elements[id], querySelectorAll: () => [] },
        AbortController, clearTimeout,
        __VERSIONS_JSON__: [{hash: "first"}, {hash: "second"}],
        fetch: url => url.endsWith("/stats") ? Promise.resolve({ok: false})
            : new Promise(resolve => replies.push(json => resolve({ok: true, json: async () => json}))),
    };
    runInNewContext(script, ctx);
    const first = ctx.selectVersion(0, pane());
    const second = ctx.selectVersion(1, pane());
    const file = {path: "second.md", s: "M", old: "a", new: "b"};
    replies[1]({total: 1, changes: [file]});
    await second;
    replies[0]({total: 0});
    await first;
    assert.match(elements.files.innerHTML, /second\.md/);

    const stale = ctx.showDiff({...file, new: "c"}, pane());
    const selected = ctx.showDiff(file, pane());
    replies[3]({kind: "text", unified: "+selected"});
    await selected;
    replies[2]({kind: "text", unified: "+stale"});
    await stale;
    assert.match(elements.diff.innerHTML, /selected/);
    await ctx.showDiff({...file, path: "copy.md"}, pane());
    assert.match(elements.diff.innerHTML, /copy\.md/);
    assert.equal(replies.length, 4);
});
