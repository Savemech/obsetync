import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const repository = fileURLToPath(new URL("../../", import.meta.url));
const buildScript = join(repository, "scripts/build-wasm.sh");

async function fakeBuild({ version = "117", optimizerName = "wasm-opt" } = {}) {
    const fixture = await mkdtemp(join(tmpdir(), "obsetync-build-wasm-contract-"));
    const tools = join(fixture, "tools");
    const optimizer = join(tools, optimizerName);
    await mkdir(tools);
    await writeFile(optimizer, `#!/bin/sh\necho 'wasm-opt version ${version}'\n`, { mode: 0o755 });
    await writeFile(join(tools, "wasm-pack"), "#!/bin/sh\nexit 73\n", { mode: 0o755 });
    await chmod(tools, 0o755);
    try {
        return spawnSync("bash", [buildScript, join(fixture, "output")], {
            cwd: repository,
            encoding: "utf8",
            env: {
                ...process.env,
                CARGO_TARGET_DIR: join(fixture, "target"),
                PATH: `${tools}:${process.env.PATH ?? ""}`,
                WASM_OPT: optimizer,
            },
        });
    } finally {
        await rm(fixture, { recursive: true, force: true });
    }
}

test("build-wasm rejects an optimizer older than the managed Binaryen contract", async () => {
    const result = await fakeBuild({ version: "116" });
    assert.ifError(result.error);
    assert.equal(result.status, 2);
    assert.match(result.stderr, /version 117 or newer required; found: wasm-opt version 116/);
});

test("build-wasm rejects an unparseable optimizer version", async () => {
    const result = await fakeBuild({ version: "development" });
    assert.ifError(result.error);
    assert.equal(result.status, 2);
    assert.match(result.stderr, /version 117 or newer required; found: wasm-opt version development/);
});

test("build-wasm rejects a custom optimizer basename that wasm-pack cannot discover", async () => {
    const result = await fakeBuild({ optimizerName: "custom-wasm-optimizer" });
    assert.ifError(result.error);
    assert.equal(result.status, 2);
    assert.match(result.stderr, /WASM_OPT must resolve to an executable named wasm-opt/);
});

test("build-wasm exposes an accepted optimizer to the managed wasm-pack build", async () => {
    const result = await fakeBuild();
    assert.ifError(result.error);
    assert.equal(result.status, 73, "the fake wasm-pack sentinel must own the post-validation exit");
    assert.doesNotMatch(result.stderr, /optimizer version|WASM_OPT must resolve/);
});
