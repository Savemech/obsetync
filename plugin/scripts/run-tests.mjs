import { build } from "esbuild";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

// An optional entry point also lets the runner's regression tests exercise build
// and assertion failures without modifying the real suite.
const entryPoint = process.argv[2] ?? "src/plugin.test.ts";
const directory = await mkdtemp(join(tmpdir(), "obsetync-tests-"));
try {
    const bundlePath = join(directory, "tests.cjs");
    // Do not launch Node until bundling succeeds: a shell pipeline can turn a
    // failed build into an empty, successful test run.
    await build({
        absWorkingDir: fileURLToPath(new URL("../", import.meta.url)),
        entryPoints: [entryPoint],
        bundle: true,
        platform: "node",
        format: "cjs",
        target: "node20",
        outfile: bundlePath,
        logLevel: "warning",
    });
    const exitCode = await new Promise((resolve, reject) => {
        const child = spawn(process.execPath, [bundlePath], { stdio: "inherit" });
        child.once("error", reject);
        child.once("close", (code) => resolve(code ?? 1));
    });
    process.exitCode = exitCode;
} finally {
    await rm(directory, { recursive: true, force: true });
}
