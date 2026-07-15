/**
 * Unit tests for the ignore matcher. No test framework — plain assertions so it
 * runs through the already-installed esbuild:
 *
 *   npx esbuild src/ignore.test.ts --bundle --platform=node --format=cjs | node
 *
 * (also wired as `npm test`). A false NEGATIVE lets junk keep syncing; a false
 * POSITIVE silently stops a real note — the second set of cases guards that.
 */
import assert from "node:assert";
import { compileIgnore, DEFAULT_IGNORE_PATTERNS } from "./ignore";

let passed = 0;
function ok(name: string, cond: boolean) {
    assert.ok(cond, name);
    passed++;
}

const def = compileIgnore(DEFAULT_IGNORE_PATTERNS);

// --- defaults MUST ignore build/vcs/os junk at any depth ---------------------
ok("target/ nested", def.test("code/myapp/target/debug/deps/lib.rmeta"));
ok("target/ at root", def.test("target/debug/x.rlib"));
ok("node_modules nested", def.test("proj/node_modules/react/index.js"));
ok(".git nested", def.test("proj/.git/objects/ab/cdef"));
ok(".DS_Store basename", def.test("notes/.DS_Store"));
ok(".DS_Store at root", def.test(".DS_Store"));
ok("*.tmp basename", def.test("drafts/foo.tmp"));
ok("leading ./ normalized", def.test("./target/x"));
ok("leading / normalized", def.test("/target/x"));

// --- defaults MUST NOT ignore real notes (false-positive guards) -------------
ok("plain note", !def.test("notes/projects/design.md"));
ok("note named target.md", !def.test("projects/target.md"));
ok("file 'target' (not a dir)", !def.test("notes/target"));
ok("word containing target", !def.test("notes/targeting.md"));
ok("targets/ (not target/)", !def.test("data/targets/list.md"));
ok(".gitignore is not .git/", !def.test("repo/.gitignore"));
ok("tmp/ dir kept (only *.tmp)", !def.test("tmp/keep.md"));
ok("file ending path.tmpl", !def.test("templates/note.tmpl"));

// --- explicit pattern behaviours ---------------------------------------------
const g = compileIgnore([
    "build/",         // dir anywhere
    "*.log",          // glob basename
    "_attachments/big/", // anchored dir
    "# a comment",    // ignored
    "",               // blank, ignored
]);
ok("build/ subtree", g.test("app/build/out.js"));
ok("build file NOT ignored", !g.test("app/build")); // 'build' as a file, not dir
ok("*.log any depth", g.test("logs/2026/app.log"));
ok("anchored dir matches", g.test("_attachments/big/video.mp4"));
ok("anchored dir NOT matched elsewhere", !g.test("other/_attachments/big/x"));
ok("comment/blank contributed no rules", g.size === 3);

// --- ** across segments ------------------------------------------------------
const gg = compileIgnore(["docs/**/tmp/"]);
ok("** crosses segments", gg.test("docs/a/b/tmp/x.md"));
ok("** requires the tmp dir", !gg.test("docs/a/b/keep.md"));

// --- empty pattern set = fast path, ignores nothing --------------------------
const none = compileIgnore([]);
ok("empty set ignores nothing", !none.test("anything/at/all.md"));
ok("empty set size 0", none.size === 0);

console.log(`ignore.test: ${passed} assertions passed`);
