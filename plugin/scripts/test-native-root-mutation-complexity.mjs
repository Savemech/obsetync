import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import Module from "node:module";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build } from "esbuild";

const pluginDirectory = fileURLToPath(new URL("../", import.meta.url));
const args = process.argv.slice(2);
const options = { check: false, count: 25_000, profile: "full" };
if (args.length === 1 && args[0] === "--help") {
    process.stdout.write(`Usage: node scripts/test-native-root-mutation-complexity.mjs [--check|--medium]

Runs one deterministic Tree-v1 candidate mutation through the packaged SIMD
WASM and production TypeScript driver. The full corpus contains 25,000 existing
and changed files under one directory prefix; --medium uses 4,096. Assertions
bind actual native work units to 64-unit WASM calls and real cooperative host
turns without wall-clock duration assertions. --check only bundles the driver.
`);
    process.exit(0);
}
assert(args.length <= 1, "use at most one complexity-test option");
if (args[0] === "--check") options.check = true;
else if (args[0] === "--medium") { options.count = 4096; options.profile = "medium"; }
else if (args.length) throw new Error("unsupported complexity-test option; use --help");

const result = await build({ absWorkingDir: pluginDirectory, bundle: true, platform: "node",
    format: "cjs", target: "node20", write: false, logLevel: "silent", stdin: {
        resolveDir: pluginDirectory, sourcefile: "native-root-mutation-complexity.ts", loader: "ts", contents: `
            export { applyTreeCandidateMutation, TREE_CANDIDATE_MUTATION_STEP_UNITS,
                TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN }
                from "./src/tree-candidate-mutation-job";
            export { beginTreeCandidate } from "./src/tree-candidate-job";
            export { yieldWork, disposeWorkScheduler, workSchedulerSnapshot } from "./src/work-scheduler";
        `,
    } });
assert.equal(result.outputFiles.length, 1);
if (options.check) {
    console.log("native-root mutation complexity bundle check passed; no WASM tree was created");
    process.exit(0);
}

const filename = join(pluginDirectory, "native-root-mutation-complexity-in-memory.cjs");
const module = new Module(filename); module.filename = filename;
module.paths = Module._nodeModulePaths(pluginDirectory);
module._compile(result.outputFiles[0].text, filename);
const driver = module.exports;
assert.equal(driver.TREE_CANDIDATE_MUTATION_STEP_UNITS, 64);
assert.equal(driver.TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN, 32);

const wasmPath = join(pluginDirectory, "wasm/sync_core_simd_bg.wasm");
const wasmBytes = await readFile(wasmPath);
const bindings = await import(pathToFileURL(join(pluginDirectory, "wasm/sync_core_simd.js")).href);
const native = await bindings.default({ module_or_path: wasmBytes });
assert(native.memory instanceof WebAssembly.Memory);

const encoder = new TextEncoder();
const baselineHash = bindings.wasm_hash(encoder.encode("native-root-complexity:baseline"));
const changedHash = bindings.wasm_hash(encoder.encode("native-root-complexity:changed"));
const pathAt = index => `one-prefix/n-${String(index).padStart(5, "0")}.md`;
const row = (index, hash, generation) => ({ path: pathAt(index), hash,
    mtime_ms: generation * 1_000 + index, size: generation * 10 + index });
const baseline = Array.from({ length: options.count }, (_, index) => row(index, baselineHash, 1));
const changed = Array.from({ length: options.count }, (_, index) => row(index, changedHash, 2));
const tree = new bindings.WasmTree("native-root-complexity", "synthetic-device");

let mutationSteps = 0, mutationUnits = 0, planBarriers = 0, doneSteps = 0;
let maxRequestedUnits = 0, previousCompleted = 0;
let hostYieldsRequested = 0, hostYieldsCompleted = 0;
let retirementYieldsRequested = 0, retirementYieldsCompleted = 0;
const returnedUnits = [];
try {
    tree.rebuild_from_entries_in_version(1, JSON.stringify(baseline));
    const baselineRoot = tree.root_hash_hex();
    assert(/^[0-9a-f]{64}$/.test(baselineRoot)); assert.equal(tree.total_files(), options.count);
    let opened = 0;
    await driver.beginTreeCandidate(tree, {
        cooperate: () => driver.yieldWork({ lane: "bulk" }),
        cooperateRetirement: () => driver.yieldWork({ lane: "interactive", deadlineMs: 50 }),
        onCandidateOpened: reachable => { opened++; assert(reachable > 0); },
    });
    assert.equal(opened, 1); assert.equal(tree.has_candidate(), true);

    const nativeStep = tree.step_candidate_mutation_output_memory_v1_job.bind(tree);
    tree.step_candidate_mutation_output_memory_v1_job = (token, maxUnits) => {
        mutationSteps++; maxRequestedUnits = Math.max(maxRequestedUnits, maxUnits);
        const progress = nativeStep(token, maxUnits);
        assert(progress && typeof progress === "object");
        assert(Number.isSafeInteger(progress.units) && progress.units >= 0 && progress.units <= maxUnits);
        assert(Number.isSafeInteger(progress.completed) && progress.completed === previousCompleted + progress.units);
        previousCompleted = progress.completed; mutationUnits += progress.units;
        if (progress.phase === "plan ready") { planBarriers++; assert.equal(progress.units, 0); }
        else returnedUnits.push(progress.units);
        if (progress.done) doneSteps++;
        return progress;
    };
    const cooperate = async () => {
        hostYieldsRequested++;
        await driver.yieldWork({ lane: "bulk" });
        hostYieldsCompleted++;
    };
    const cooperateRetirement = async () => {
        retirementYieldsRequested++;
        await driver.yieldWork({ lane: "interactive", deadlineMs: 50 });
        retirementYieldsCompleted++;
    };
    let mutations = 0;
    await driver.applyTreeCandidateMutation(tree, "update", JSON.stringify(changed), {
        cooperate, cooperateRetirement,
        legacy: () => { throw new Error("packaged tree used legacy candidate mutation"); },
        onCandidateMutated: () => { mutations++; },
    });

    const dataSteps = mutationSteps - planBarriers;
    assert.equal(planBarriers, 1); assert.equal(doneSteps, 1); assert.equal(mutations, 1);
    assert.equal(maxRequestedUnits, driver.TREE_CANDIDATE_MUTATION_STEP_UNITS);
    const packedDataSteps = Math.ceil(mutationUnits / driver.TREE_CANDIDATE_MUTATION_STEP_UNITS);
    // Canonical V1 leaves contain at most 1,000 rows. Shape validation forces
    // an external return after each leaf so a 64-unit bridge call cannot hide
    // tens of thousands of path comparisons from the host clock.
    // The final ValidateShape -> Search transition is also reported as one
    // bounded native step after the last leaf has been accepted.
    const shapeBoundaryAllowance = Math.ceil(options.count / 1_000) + 1;
    assert(dataSteps >= packedDataSteps && dataSteps <= packedDataSteps + shapeBoundaryAllowance,
        `shape-safe native call count escaped its bound: ${dataSteps}/${packedDataSteps}/${shapeBoundaryAllowance}`);
    assert(returnedUnits.length > 1); assert(returnedUnits.every(units =>
        units > 0 && units <= driver.TREE_CANDIDATE_MUTATION_STEP_UNITS));
    const shapeLeafWitnessMinimum = Math.ceil(options.count / 1_000);
    const shapeBoundaryWitnesses = returnedUnits.filter(units => units === 1).length;
    assert(shapeBoundaryWitnesses >= shapeLeafWitnessMinimum,
        `canonical leaf-validation boundaries were not observed: ${shapeBoundaryWitnesses}/${shapeLeafWitnessMinimum}`);
    // One pre-begin turn is unconditional. Even if the monotonic clock never
    // reaches 4ms, the reliable-clock step cap requires another host turn
    // after each complete group of 32 loop calls (including PlanReady/Done).
    const minimumLoopYields = Math.floor(
        mutationSteps / driver.TREE_CANDIDATE_MUTATION_MAX_RELIABLE_STEPS_PER_TURN,
    );
    assert(hostYieldsRequested >= 1 + minimumLoopYields && hostYieldsRequested <= mutationSteps + 1,
        `mutation loop cooperation outside bounds: ${hostYieldsRequested}/${minimumLoopYields}`);
    assert.equal(hostYieldsCompleted, hostYieldsRequested);
    assert.equal(retirementYieldsCompleted, retirementYieldsRequested);

    // Fixed-corpus complexity fence. It intentionally counts native logical
    // units rather than time: a return to one-unit bridge calls violates the
    // exact quotient above, while accidental repeated-prefix work violates
    // this conservative linear ceiling.
    const logicalUnitLimit = 96 * options.count + 4096;
    assert(mutationUnits <= logicalUnitLimit,
        `same-prefix mutation exceeded deterministic unit ceiling: ${mutationUnits}/${logicalUnitLimit}`);
    assert(mutationSteps <= Math.ceil(logicalUnitLimit / driver.TREE_CANDIDATE_MUTATION_STEP_UNITS) +
        shapeBoundaryAllowance + 2);
    assert.equal(tree.total_files(), options.count); assert.equal(tree.candidate_total_files(), options.count);
    assert.equal(tree.has_candidate(), true);
    assert.notEqual(tree.candidate_root_hash_hex(), baselineRoot);

    const scheduler = driver.workSchedulerSnapshot();
    assert.equal(scheduler.pendingJobs, 0);
    process.stdout.write(JSON.stringify({ test: "native-root-mutation-complexity", passed: true,
        profile: options.profile, files: options.count, prefixCount: 1,
        mutationSteps, mutationUnits, planBarriers, hostYields: hostYieldsCompleted,
        retirementYields: retirementYieldsCompleted, quantum: driver.TREE_CANDIDATE_MUTATION_STEP_UNITS,
        wasmBytes: wasmBytes.length, wasmSha256: createHash("sha256").update(wasmBytes).digest("hex") }) + "\n");
} finally {
    try { if (tree.has_candidate()) tree.abort_candidate(); }
    finally { tree.free(); driver.disposeWorkScheduler(); }
}
