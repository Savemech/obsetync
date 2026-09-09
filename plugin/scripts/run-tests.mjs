import { build } from "esbuild";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, realpath, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));

/** The default entry is a manifest, not a module to execute: importing async
 * self-running suites into one bundle overlaps their module/global state.
 * Only sibling .test modules are allowed; no executable manifest statements,
 * imports with bindings, attributes, duplicates or symlink escapes. */
async function manifestEntries(manifestPath) {
    const manifest = await realpath(manifestPath);
    const source = await readFile(manifest, "utf8");
    const parsed = ts.createSourceFile(manifest, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
    if (parsed.parseDiagnostics.length || parsed.statements.length === 0) {
        throw new Error("Invalid test manifest: expected static test imports");
    }
    const entries = [], seen = new Set();
    for (const statement of parsed.statements) {
        if (!ts.isImportDeclaration(statement) || statement.importClause || statement.attributes ||
            statement.modifiers?.length || !ts.isStringLiteral(statement.moduleSpecifier)) {
            throw new Error("Invalid test manifest: only side-effect test imports are supported");
        }
        const specifier = statement.moduleSpecifier.text;
        if (!/^\.\/[A-Za-z0-9_-]+\.test(?:\.ts)?$/.test(specifier)) {
            throw new Error("Invalid test manifest: expected a sibling .test module");
        }
        const entry = resolve(dirname(manifest), specifier.endsWith(".ts") ? specifier : `${specifier}.ts`);
        if (seen.has(entry)) throw new Error("Invalid test manifest: duplicate test entry");
        seen.add(entry);
        // Missing/inaccessible sources and aliases outside the manifest's
        // directory are errors, never a silently omitted test.
        if (await realpath(entry) !== entry || !(await stat(entry)).isFile()) {
            throw new Error("Invalid test manifest: source must be a regular sibling file");
        }
        entries.push(entry);
    }
    return entries;
}

// Positional entrypoints retain the focused/benchmark single-module behavior.
// --manifest is also used by fixture regressions without editing the real list.
const args = process.argv.slice(2);
let entries;
if (args.length === 0) entries = await manifestEntries(join(pluginDirectory, "src/plugin.test.ts"));
else if (args.length === 2 && args[0] === "--manifest") {
    entries = await manifestEntries(resolve(pluginDirectory, args[1]));
} else if (args.length === 1 && !args[0].startsWith("-")) entries = [resolve(pluginDirectory, args[0])];
else throw new Error("Usage: run-tests.mjs [entrypoint | --manifest path]");

const directory = await mkdtemp(join(tmpdir(), "obsetync-tests-"));
try {
    const bundles = entries.map((_, index) => join(directory, `suite-${index}.cjs`));
    // Compile EVERY selected suite before launching ANY child. A malformed
    // later suite must not leave a partly executed, apparently successful run.
    for (let index = 0; index < entries.length; index++) await build({
        absWorkingDir: pluginDirectory,
        entryPoints: [entries[index]],
        bundle: true,
        platform: "node",
        format: "cjs",
        target: "node20",
        outfile: bundles[index],
        logLevel: "warning",
        plugins: [{
            name: "test-obsidian-request-url",
            setup(builder) {
                builder.onResolve({ filter: /^obsidian$/ }, () => ({
                    path: "obsidian", namespace: "test-obsidian",
                }));
                builder.onLoad({ filter: /.*/, namespace: "test-obsidian" }, () => ({
                    contents: `export function requestUrl(params) {
                        const routes = globalThis.__obsetyncTestRequestUrlRoutes;
                        const fake = routes?.get(new URL(params.url).origin) ?? globalThis.__obsetyncTestRequestUrl;
                        if (typeof fake !== "function") {
                            throw new Error("Obsidian requestUrl test stub is not installed");
                        }
                        return fake(params);
                    }
                    export class TAbstractFile {}
                    export class TFile extends TAbstractFile {}
                    export const Platform = new Proxy({}, { get() {
                        throw new Error("Obsidian Platform test stub is not installed");
                    } });
                    export class Notice {
                        constructor(...args) {
                            const fake = globalThis.__obsetyncTestNotice;
                            if (typeof fake !== "function") throw new Error("Obsidian Notice test stub is not installed");
                            return fake(...args);
                        }
                    }
                    export function debounce(...args) {
                        const fake = globalThis.__obsetyncTestDebounce;
                        if (typeof fake !== "function") throw new Error("Obsidian debounce test stub is not installed");
                        return fake(...args);
                    }`,
                    loader: "js",
                }));
            },
        }],
    });
    for (const bundlePath of bundles) {
        const exitCode = await new Promise((resolve, reject) => {
            const child = spawn(process.execPath, ["--unhandled-rejections=strict", bundlePath], { stdio: "inherit" });
            child.once("error", reject);
            child.once("close", (code) => resolve(code ?? 1));
        });
        if (exitCode !== 0) { process.exitCode = exitCode; break; }
    }
    // Child exit joins native handles and preserves suite-owned beforeExit
    // completion guards. A bare unresolved Promise has no Node handle: without
    // a suite completion contract it cannot be distinguished from an
    // intentionally pending fixture. Do not claim to detect that here.
} finally {
    await rm(directory, { recursive: true, force: true });
}
