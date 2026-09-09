import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { lstat, mkdtemp, readFile, readdir, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const runner = fileURLToPath(new URL("./run-tests.mjs", import.meta.url));
const sourceDirectory = fileURLToPath(new URL("../src/", import.meta.url));
const productionManifest = join(sourceDirectory, "plugin.test.ts");
const discoveryAllowlist = new Set(["plugin.test.ts"]);

test("production manifest includes every sibling TypeScript test suite", async () => {
    const discovered = (await readdir(sourceDirectory, { withFileTypes: true }))
        .filter(entry => entry.name.endsWith(".test.ts") && !discoveryAllowlist.has(entry.name));
    for (const entry of discovered) {
        const path = join(sourceDirectory, entry.name);
        const info = await lstat(path);
        assert(entry.isFile() && info.isFile() && !info.isSymbolicLink(),
            `TypeScript test suite must be a regular sibling file: ${entry.name}`);
        assert.equal(await realpath(path), path,
            `TypeScript test suite must not escape the source directory: ${entry.name}`);
    }

    const source = await readFile(productionManifest, "utf8");
    const parsed = ts.createSourceFile(productionManifest, source,
        ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
    assert.equal(parsed.parseDiagnostics.length, 0, "production test manifest must parse");
    const imported = parsed.statements.map(statement => {
        assert(ts.isImportDeclaration(statement) && !statement.importClause &&
            !statement.attributes && !statement.modifiers?.length &&
            ts.isStringLiteral(statement.moduleSpecifier),
        "production test manifest must contain only side-effect imports");
        const specifier = statement.moduleSpecifier.text;
        assert.match(specifier, /^\.\/[A-Za-z0-9_-]+\.test(?:\.ts)?$/,
            "production test manifest import must name a sibling TypeScript test");
        return `${specifier.slice(2).replace(/\.ts$/, "")}.ts`;
    });
    assert.equal(new Set(imported).size, imported.length,
        "production test manifest must not contain duplicate suites");
    assert.deepEqual(imported.toSorted(), discovered.map(entry => entry.name).toSorted(),
        "production test manifest must include every sibling TypeScript test suite");
});

test("default npm test stays source-only and keeps generated/listener/crash gates opt-in", async () => {
    const manifest = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
    const stages = manifest.scripts.test.split(/\s*&&\s*/);
    assert.deepEqual(stages, [
        "node --test scripts/run-tests.test.mjs scripts/build-wasm-contract.test.mjs scripts/native-root-io.test.mjs scripts/native-root-metrics.test.mjs scripts/native-root-instrumentation.test.mjs scripts/native-root-child.test.mjs scripts/native-root-qualification.test.mjs scripts/native-root-auto-diagnostic.test.mjs scripts/native-root-audit-diagnostic.test.mjs scripts/settings-fence.test.mjs scripts/conflict-ui-lifecycle.test.mjs",
        "node scripts/run-tests.mjs",
        "node scripts/test-main-lifecycle.mjs",
        "node scripts/test-wasm-memory.mjs",
    ], "helper failures must gate the existing isolated suites without wildcard listener/benchmark discovery");
    assert.doesNotMatch(manifest.scripts.test,
        /browser-hash-worker\.test|test-native-root-server|test-native-root-crash-recovery|bench-/,
        "generated-artifact, listener, crash and benchmark gates must stay out of source-only npm test");
    assert.equal(manifest.scripts["test:browser-hash-worker"],
        "node --test scripts/browser-hash-worker.test.mjs",
        "the generated-WASM browser worker must retain an explicit artifact gate");
});

test("CI and release run the browser hash worker only after a fresh WASM build", async () => {
    for (const [label, url, jobName] of [
        ["CI", new URL("../../.github/workflows/ci.yml", import.meta.url), "plugin-wasm"],
        ["release", new URL("../../.github/workflows/release.yml", import.meta.url), "plugin"],
    ]) {
        const workflow = await readFile(url, "utf8");
        const lines = workflow.split("\n");
        const start = lines.findIndex(line => line === `  ${jobName}:`);
        const end = lines.findIndex((line, index) => index > start && /^  [A-Za-z0-9_-]+:$/.test(line));
        assert.ok(start >= 0, `${label} must retain the ${jobName} artifact job`);
        const job = lines.slice(start, end < 0 ? undefined : end).join("\n");
        const wasmBuild = job.indexOf("run: bash scripts/build-wasm.sh plugin/wasm");
        const dependencyInstall = job.indexOf("npm ci --no-audit --no-fund", wasmBuild);
        const workerGate = job.indexOf("npm run test:browser-hash-worker", dependencyInstall);
        assert.ok(wasmBuild >= 0, `${label} must build fresh packaged WASM`);
        assert.ok(dependencyInstall > wasmBuild, `${label} must install plugin dependencies after the WASM build`);
        assert.ok(workerGate > dependencyInstall,
            `${label} must run the packaged browser hash worker after build and dependency install`);
        assert.equal(workflow.match(/npm run test:browser-hash-worker/g)?.length, 1,
            `${label} must own exactly one packaged browser hash worker gate`);
        assert.match(job,
            /binaryen\/releases\/download\/version_117\/binaryen-version_117-x86_64-linux\.tar\.gz/,
            `${label} must pin the wasm-pack-compatible Binaryen release`);
        assert.match(job, /3dc677006555b355ea2da5e82602065a161d5e83eaefd3f759afa00b96e83212/,
            `${label} must pin the official Binaryen x86_64 digest`);
        assert.match(job, /c539d91ccab2591a7e975bcf82c82e1911b03335c80aa83d67ad25ed2ad06539/,
            `${label} must pin the official wasm-pack x86_64 digest`);
        assert.equal(job.match(/sha256sum -c -/g)?.length, 2,
            `${label} must verify both privileged tool downloads before extraction`);
    }
});

test("Docker WASM tools are checksum-pinned for every supported build architecture", async () => {
    const dockerfile = await readFile(new URL("../../Dockerfile", import.meta.url), "utf8");
    for (const digest of [
        "3dc677006555b355ea2da5e82602065a161d5e83eaefd3f759afa00b96e83212",
        "ad560204426015a815faa45693c83bef7d58677d38a39422c272a30ba4b6da2a",
        "c539d91ccab2591a7e975bcf82c82e1911b03335c80aa83d67ad25ed2ad06539",
        "2e65038769f8bbaa5fc237ad4bb523e692df99458cbd3e3d92525b89d8762379",
    ]) assert.match(dockerfile, new RegExp(digest));
    assert.match(dockerfile,
        /tool_arch="\$\{TARGETARCH\}";\s*\\\s*if \[ -z "\$\{tool_arch\}" \]; then\s*\\\s*tool_arch="\$\(dpkg --print-architecture\)";/,
        "an empty automatic TARGETARCH must fall back to the native builder architecture");
    assert.match(dockerfile, /case "\$\{tool_arch\}" in/,
        "explicit and host-derived architectures must share the same fail-closed allowlist");
    assert.match(dockerfile, /amd64\)[\s\S]*binaryen_arch=x86_64;[\s\S]*wasm_pack_arch=x86_64;/);
    assert.match(dockerfile, /arm64\)[\s\S]*binaryen_arch=aarch64;[\s\S]*wasm_pack_arch=aarch64;/);
    assert.match(dockerfile,
        /\*\) echo "unsupported Docker build TARGETARCH\/host architecture: \$\{tool_arch:-<empty>\}" >&2; exit 2 ;;/,
        "unsupported explicit and native builder architectures must fail closed");
    assert.equal(dockerfile.match(/sha256sum -c -/g)?.length, 2,
        "Docker must verify both architecture-selected tool archives");
});

test("Docker plugin builds carry the build-identity helper and exact release inputs", async () => {
    const dockerfile = await readFile(new URL("../../Dockerfile", import.meta.url), "utf8");
    const pluginBuilder = dockerfile.slice(dockerfile.indexOf("FROM node:20-bookworm-slim AS plugin-builder"),
        dockerfile.indexOf("FROM debian:bookworm-slim AS server"));
    assert.ok(pluginBuilder.length > 0, "Dockerfile must retain the plugin-builder stage");
    assert.match(pluginBuilder,
        /COPY plugin\/scripts\/build-identity-config\.mjs \.\/scripts\/build-identity-config\.mjs/,
        "the production esbuild configuration must receive its build-identity dependency");
    for (const name of [
        "OBSETYNC_BUILD_GIT_COMMIT",
        "OBSETYNC_BUILD_SOURCE_STATE",
        "OBSETYNC_BUILD_EXPECTED_COMMIT",
        "OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT",
        "OBSETYNC_BUILD_REQUIRE_CLEAN",
        "OBSETYNC_BUILD_EXPECTED_VERSION",
    ]) {
        assert.match(pluginBuilder, new RegExp(`ARG ${name}(?:\\n|$)`),
            `plugin-builder must redeclare ${name}`);
        assert.match(pluginBuilder, new RegExp(`${name}="\\$\\{${name}\\}"`),
            `production esbuild must receive ${name}`);
    }
    assert.match(pluginBuilder,
        /OBSETYNC_BUILD_ALLOW_LOCAL_UNKNOWN=1\s*\\\s*node esbuild\.config\.mjs production/,
        "documented local Docker builds must explicitly permit only the helper's local-unknown identity");
});

test("Nix WASM artifacts retain the optimizer and scalar fallback contract", async () => {
    const flake = await readFile(new URL("../../flake.nix", import.meta.url), "utf8");
    assert.match(flake, /wasm-opt[\s\S]*\$3 \+ 0 >= 117/,
        "Nix must reject an optimizer older than the packaged build contract");
    assert.match(flake, /wasm-opt -O[\s\S]*--disable-simd[\s\S]*bindings\/sync_core_bg\.wasm/,
        "Nix must reject SIMD instructions in the scalar fallback");
    assert.match(flake, /wasm-opt -O[\s\S]*--enable-simd[\s\S]*bindings\/sync_core_simd_bg\.wasm/,
        "Nix must admit SIMD only in the fast artifact");
    assert.match(flake, /inherit sync-server plugin sync-core-wasm;/,
        "nix flake check must build the packaged WASM derivation explicitly");
});

async function runFixture(source) {
    return runFiles({ "fixture.ts": source }, "fixture.ts");
}

async function runFiles(files, entry = "manifest.ts", manifest = false) {
    const directory = await mkdtemp(join(tmpdir(), "obsetync-runner-test-"));
    try {
        for (const [name, source] of Object.entries(files)) {
            assert.match(name, /^[A-Za-z0-9_.-]+$/, "fixture writes must remain inside their own temporary directory");
            await writeFile(join(directory, name), source.replaceAll("$FIXTURE_DIRECTORY", JSON.stringify(directory)));
        }
        const entryPoint = join(directory, entry);
        const result = spawnSync(process.execPath, [runner, ...(manifest ? ["--manifest"] : []), entryPoint], {
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

test("Obsidian test transport fails closed without an explicit local fake", async () => {
    const result = await runFixture(
        'import { requestUrl } from "obsidian"; requestUrl({ url: "http://must-not-contact" });',
    );
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /Obsidian requestUrl test stub is not installed/);
});

test("manifest suites use fresh globals and join the previous child's actual asynchronous tail", async () => {
    const result = await runFiles({
        "manifest.ts": '// Allowed comments do not become executable statements.\nimport "./first.test";\nimport "./second.test.ts";\nimport "./third.test";\n',
        "first.test.ts": `import { writeFileSync } from "node:fs";
            globalThis.fixtureTuning = { maxFiles: 2 };
            setTimeout(() => {
                writeFileSync($FIXTURE_DIRECTORY + "/completed", "actual tail completed");
                console.log("first isolated suite passed");
            }, 10);`,
        "second.test.ts": `import assert from "node:assert/strict";
            import { readFileSync } from "node:fs";
            assert.equal(globalThis.fixtureTuning, undefined, "prior suite leaked tuning");
            assert.equal(readFileSync($FIXTURE_DIRECTORY + "/completed", "utf8"), "actual tail completed");
            globalThis.fixtureTuning = { maxFiles: 1 };
            console.log("second isolated suite passed");`,
        "third.test.ts": 'import assert from "node:assert/strict"; assert.equal(globalThis.fixtureTuning, undefined); console.log("third isolated suite passed");',
    }, "manifest.ts", true);
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /first isolated suite passed\nsecond isolated suite passed\nthird isolated suite passed/);
});

test("each child failure is propagated and no later suite starts", async () => {
    for (let failed = 0; failed < 3; failed++) {
        const files = { "manifest.ts": 'import "./first.test"; import "./second.test"; import "./third.test";' };
        for (const [index, name] of ["first", "second", "third"].entries()) {
            files[`${name}.test.ts`] = `console.log("suite-${index} started"); ${index === failed ? `process.exitCode = ${7 + index};` : ""}`;
        }
        const result = await runFiles(files, "manifest.ts", true);
        assert.equal(result.status, 7 + failed, result.stderr);
        for (let index = 0; index < 3; index++) {
            assert.equal(result.stdout.includes(`suite-${index} started`), index <= failed, result.stdout);
        }
    }
});

test("a later suite build error prevents every child from starting", async () => {
    const result = await runFiles({
        "manifest.ts": 'import "./first.test"; import "./broken.test";',
        "first.test.ts": 'console.log("must not run");',
        "broken.test.ts": "const broken = ;",
    }, "manifest.ts", true);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /Build failed|Unexpected/);
    assert.doesNotMatch(result.stdout, /must not run/);
});

test("unsupported manifest declarations fail before any child executes", async () => {
    for (const unsupported of [
        'console.log("manifest must not run");',
        'import { fixture } from "./first.test";',
        'import("./first.test");',
        'export * from "./first.test";',
        'import "../outside.test";',
        'import "node:fs";',
        'import "./first.test" with { type: "json" };',
    ]) {
        const result = await runFiles({
            "manifest.ts": `import "./first.test";\n${unsupported}`,
            "first.test.ts": 'console.log("must not run"); export const fixture = 1;',
        }, "manifest.ts", true);
        assert.notEqual(result.status, 0);
        assert.match(result.stderr, /Invalid test manifest/);
        assert.doesNotMatch(result.stdout, /must not run/);
    }
});

test("empty, malformed, missing and duplicate manifest entries fail closed before execution", async () => {
    for (const [source, error] of [
        ["// no suites", /Invalid test manifest/],
        ['import "./first.test"; import ;', /Invalid test manifest/],
        ['import "./first.test"; import "./missing.test";', /ENOENT/],
        ['import "./first.test"; import "./first.test";', /duplicate test entry/],
        ['import "./first.test"; import "./first.test.ts";', /duplicate test entry/],
    ]) {
        const result = await runFiles({ "manifest.ts": source, "first.test.ts": 'console.log("must not run");' }, "manifest.ts", true);
        assert.notEqual(result.status, 0);
        assert.match(result.stderr, error);
        assert.doesNotMatch(result.stdout, /must not run/);
    }
});

test("delayed assertions and unhandled promise rejections fail isolated children", async () => {
    for (const source of [
        'setTimeout(() => { throw new Error("delayed fixture failure"); }, 5);',
        'void Promise.resolve().then(() => { throw new Error("delayed fixture failure"); });',
    ]) {
        const result = await runFiles({
            "manifest.ts": 'import "./failed.test"; import "./later.test";',
            "failed.test.ts": source,
            "later.test.ts": 'console.log("later suite must not run");',
        }, "manifest.ts", true);
        assert.notEqual(result.status, 0);
        assert.match(result.stderr, /delayed fixture failure/);
        assert.doesNotMatch(result.stdout, /later suite must not run/);
    }
});

test("child termination by signal is a failure, not a successful empty run", async () => {
    const result = await runFixture('process.kill(process.pid, "SIGTERM");');
    assert.notEqual(result.status, 0);
});

test("suite-owned pending completion guards remain authoritative", async () => {
    const result = await runFixture(`let completed = false;
        process.once("beforeExit", () => { if (!completed) { console.error("fixture tail remained pending"); process.exitCode = 9; } });
        void (async () => { await new Promise(() => {}); completed = true; })();`);
    assert.equal(result.status, 9, result.stderr);
    assert.match(result.stderr, /fixture tail remained pending/);
});
