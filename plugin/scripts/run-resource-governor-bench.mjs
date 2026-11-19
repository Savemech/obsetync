import esbuild from "esbuild";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";

const worker = await esbuild.build({
    entryPoints: ["src/hash-worker-entry.ts"],
    bundle: true,
    platform: "node",
    format: "cjs",
    target: "node18",
    loader: { ".wasm": "binary" },
    write: false,
    minify: true,
    keepNames: true,
    logLevel: "warning",
});
const workerSource = worker.outputFiles[0]?.text;
if (!workerSource) throw new Error("hash worker bundle produced no output");

const virtualWorker = {
    name: "obsetync-hash-worker-source",
    setup(build) {
        build.onResolve(
            { filter: /^obsetync-hash-worker-source$/ },
            () => ({ path: "obsetync-hash-worker-source", namespace: "worker-source" }),
        );
        build.onLoad(
            { filter: /.*/, namespace: "worker-source" },
            () => ({ contents: workerSource, loader: "text" }),
        );
    },
};

const directory = await mkdtemp(join(tmpdir(), "obsetync-governor-runner-"));
const bundlePath = join(directory, "benchmark.cjs");
const resultPath = join(directory, "result.json");
try {
    await esbuild.build({
        entryPoints: ["src/resource-governor.bench.ts"],
        bundle: true,
        plugins: [virtualWorker],
        platform: "node",
        format: "cjs",
        target: "node18",
        outfile: bundlePath,
        minify: true,
        keepNames: true,
        logLevel: "warning",
    });
    const exitCode = await new Promise((resolveResult, reject) => {
        const child = spawn(process.execPath, [bundlePath, "--output", resultPath], {
            stdio: ["ignore", "inherit", "inherit"],
        });
        child.on("error", reject);
        child.on("close", (code) => resolveResult(code));
    });
    if (exitCode !== 0) {
        process.exitCode = exitCode ?? 1;
        throw new Error("governor benchmark gates failed; refusing to record evidence");
    }
    const result = await readFile(resultPath, "utf8");
    const report = JSON.parse(result);
    if (process.argv.includes("--record")) {
        const outputDir = await mkdtemp(join(tmpdir(), "obsetync-governor-report-"));
        const output = join(outputDir, "governor.json");
        await writeFile(output, result);
        console.log(`Recorded ${report.profile_family} benchmark at ${output}`);
    }
} finally {
    await rm(directory, { recursive: true, force: true });
}
