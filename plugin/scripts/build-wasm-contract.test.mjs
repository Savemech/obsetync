import assert from "node:assert/strict";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const repository = fileURLToPath(new URL("../../", import.meta.url));
const buildScript = join(repository, "scripts/build-wasm.sh");

async function fakeBuild({ version = "117", optimizerName = "wasm-opt" } = {}) {
    const fixture = await mkdtemp(join(tmpdir(), "obsetync-build-wasm-contract-"));
    const tools = join(fixture, "tools with spaces");
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

async function fakeInstall(architecture, failChecksum = "") {
    const fixture = await mkdtemp(join(tmpdir(), "obsetync-install-wasm-"));
    const tools = join(fixture, "tools"), output = join(fixture, "installed");
    await mkdir(tools); await mkdir(output);
    for (const name of ["wasm-opt", "wasm-pack"]) await writeFile(join(output, name), "previous tool");
    const log = join(fixture, "calls.jsonl");
    await writeFile(log, "");
    // Fake downloads/checksums/extraction; install and error handling remain real.
    for (const name of ["curl", "sha256sum", "tar", "uname"]) {
        await writeFile(join(tools, name), `#!/usr/bin/env node
const fs = require("node:fs"), path = require("node:path");
const tool = path.basename(process.argv[1]), args = process.argv.slice(2);
const input = tool === "sha256sum" ? fs.readFileSync(0, "utf8").trim() : "";
fs.appendFileSync(process.env.TOOL_TEST_LOG, JSON.stringify({tool, args, input}) + "\\n");
if (tool === "uname") process.stdout.write("x86_64\\n");
if (tool === "curl") fs.writeFileSync(args[args.indexOf("-o") + 1], "download fixture");
if (tool === "sha256sum" && process.env.TOOL_TEST_FAIL && input.endsWith(process.env.TOOL_TEST_FAIL + ".tar.gz")) process.exit(41);
if (tool === "tar") fs.writeFileSync(path.join(args[args.indexOf("-C") + 1], path.basename(args.at(-1))), "verified fixture");
`, { mode: 0o755 });
    }
    try {
        const result = spawnSync("bash", [join(repository, "scripts/install-wasm-tools.sh"), architecture, output], {
            encoding: "utf8", timeout: 10_000,
            env: { ...process.env, PATH: `${tools}:${process.env.PATH ?? ""}`,
                TOOL_TEST_LOG: log, TOOL_TEST_FAIL: failChecksum, TMPDIR: fixture },
        });
        assert.ifError(result.error);
        return { ...result, calls: (await readFile(log, "utf8")).trim().split("\n").filter(Boolean).map(JSON.parse),
            installed: await Promise.all(["wasm-opt", "wasm-pack"].map(name => readFile(join(output, name), "utf8"))) };
    } finally { await rm(fixture, { recursive: true, force: true }); }
}

test("installer selects the pinned pair for native and Docker architecture names", async () => {
    for (const [aliases, arch, digests] of [
        [["amd64", "x86_64", ""], "x86_64", [
            "3dc677006555b355ea2da5e82602065a161d5e83eaefd3f759afa00b96e83212",
            "c539d91ccab2591a7e975bcf82c82e1911b03335c80aa83d67ad25ed2ad06539"]],
        [["arm64", "aarch64"], "aarch64", [
            "ad560204426015a815faa45693c83bef7d58677d38a39422c272a30ba4b6da2a",
            "2e65038769f8bbaa5fc237ad4bb523e692df99458cbd3e3d92525b89d8762379"]],
    ]) {
        for (const alias of aliases) {
            const result = await fakeInstall(alias);
            assert.equal(result.status, 0, result.stderr);
            const calls = result.calls.filter(call => call.tool !== "uname");
            assert.deepEqual(calls.map(call => call.tool), ["curl", "sha256sum", "curl", "sha256sum", "tar", "tar"]);
            assert.equal(calls[0].args[1], `https://github.com/WebAssembly/binaryen/releases/download/version_117/binaryen-version_117-${arch}-linux.tar.gz`);
            assert.equal(calls[2].args[1], `https://github.com/rustwasm/wasm-pack/releases/download/v0.13.1/wasm-pack-v0.13.1-${arch}-unknown-linux-musl.tar.gz`);
            assert.deepEqual([calls[1], calls[3]].map(call => call.input.split(" ")[0]), digests);
            assert.deepEqual(result.installed, ["verified fixture", "verified fixture"]);
        }
    }
});

test("installer refuses unsupported architectures before downloading", async () => {
    const result = await fakeInstall("riscv64");
    assert.equal(result.status, 2);
    assert.equal(result.calls.length, 0);
    assert.deepEqual(result.installed, ["previous tool", "previous tool"]);
});

test("a failed checksum never extracts archives or replaces existing tools", async () => {
    for (const name of ["binaryen", "wasm-pack"]) {
        const result = await fakeInstall("amd64", name);
        assert.equal(result.status, 41);
        assert(!result.calls.some(call => call.tool === "tar"));
        assert.deepEqual(result.installed, ["previous tool", "previous tool"]);
    }
});
