import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const runner = fileURLToPath(new URL("./run-tests.mjs", import.meta.url));

async function runFixture(source) {
    const directory = await mkdtemp(join(tmpdir(), "obsetync-runner-test-"));
    try {
        const entryPoint = join(directory, "fixture.ts");
        await writeFile(entryPoint, source);
        const result = spawnSync(process.execPath, [runner, entryPoint], {
            cwd: directory,
            encoding: "utf8",
            timeout: 15_000,
        });
        assert.ifError(result.error);
        assert.equal(result.signal, null, result.stderr);
        return result;
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}

test("test runner executes a successfully bundled suite", async () => {
    const result = await runFixture('console.log("fixture passed");');
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /fixture passed/);
});

test("test runner fails when bundling fails without running partial output", async () => {
    const result = await runFixture('console.log("must not run"); const broken = ;');
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /Build failed|Unexpected/);
    assert.doesNotMatch(result.stdout, /must not run/);
});

test("test runner propagates an assertion failure", async () => {
    const result = await runFixture(
        'import assert from "node:assert/strict"; assert.equal(1, 2, "fixture assertion");',
    );
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /fixture assertion/);
});

test("test runner preserves the suite exit code", async () => {
    const result = await runFixture("process.exitCode = 7;");
    assert.equal(result.status, 7, result.stderr);
});
