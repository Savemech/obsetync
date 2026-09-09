import { strict as assert } from "node:assert";
import { ResourceBudget } from "./resource-budget";
import { exportTreeRoot, estimateTreeRootExportWorkset, TREE_ROOT_EXPORT_PAGE_BYTES as PAGE, TREE_ROOT_EXPORT_MAX_STEP_UNITS,
    TREE_ROOT_EXPORT_MAX_STEP_BYTES, TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES,
    type ExportTreeRootOptions, type TreeRootExportJobTree, type TreeRootExportMode } from "./tree-root-export-job";

const HASH = "ab".repeat(32);
const CHEAP_CHARGE = 256; // Scheduled work/CPU proxy, not native bytes touched.
const payload = (length: number) => Uint8Array.from({ length }, (_, index) => (index * 31 + (index >>> 8)) & 255);
function gate() { let release!: () => void; const promise = new Promise<void>(resolve => { release = resolve; }); return { promise, release }; }
async function until(predicate: () => boolean) {
    for (let turn = 0; turn < 100; turn++) { if (predicate()) return; await Promise.resolve(); }
    assert(predicate(), "deterministic fixture gate did not open");
}
async function rejects(work: Promise<unknown>, pattern: RegExp) { await assert.rejects(work, pattern); }

class Fixture implements TreeRootExportJobTree {
    readonly source: Uint8Array;
    readonly budget = new ResourceBudget({ capacityBytes: 16 * 1024 * 1024 });
    readonly events: string[] = [];
    readonly reads: Array<{ offset: number; maxBytes: number }> = [];
    readonly pages: Uint8Array[] = [];
    readonly steps: Array<{ phase: "plan" | "build"; requested: number; requestedBytes: number;
        units: number; bytes: number; completed: number; processed: number }> = [];
    job = false;
    token = 37;
    arenaCap?: number;
    mode?: TreeRootExportMode;
    phase: "plan" | "build" = "plan";
    completed = 0;
    processed = 0;
    offset = 0;
    planSteps = 3;
    buildSteps = 2;
    costs?: { plan: readonly number[]; build: readonly number[] };
    maxLength: number;
    offsetBytes = 128;
    version: 1 | 2 = 1;
    legacyCalls = 0;
    parses = 0;
    yields = 0;
    cancels = 0;
    finishes = 0;
    buildStarts = 0;
    afterRead?: () => void;
    afterFinish?: () => void;
    constructor(length = PAGE + 1) { this.source = payload(length); this.maxLength = length + 100; }
    private begin(mode: TreeRootExportMode, maxArenaBytes: number): number {
        assert.equal(this.job, false); assert(Number.isInteger(maxArenaBytes) && maxArenaBytes >= 1 && maxArenaBytes <= 0xffff_ffff);
        this.arenaCap = maxArenaBytes; this.job = true; this.mode = mode; this.events.push(`begin:${mode}`); return this.token;
    }
    begin_candidate_root_export_job(maxArenaBytes: number): unknown { return this.begin("candidate", maxArenaBytes); }
    begin_committed_root_export_job(maxArenaBytes: number): unknown { return this.begin("committed", maxArenaBytes); }
    step_tree_job(): unknown { throw new Error("root exporter used the shared unit-only step"); }
    step_root_export_job(token: number, maxUnits: number, maxBytes: number): unknown {
        assert.equal(token, this.token); assert(Number.isInteger(maxUnits) && maxUnits >= 1 && maxUnits <= TREE_ROOT_EXPORT_MAX_STEP_UNITS);
        assert(Number.isInteger(maxBytes) && maxBytes >= 1 && maxBytes <= TREE_ROOT_EXPORT_MAX_STEP_BYTES); assert(this.job);
        const total = this.phase === "plan" ? this.planSteps : this.buildSteps;
        assert(this.completed < total); let units = 0, bytes = 0;
        while (units < maxUnits && this.completed < total) {
            const charge = this.costs?.[this.phase][this.completed] ?? CHEAP_CHARGE;
            assert(Number.isInteger(charge) && charge >= CHEAP_CHARGE && charge <= TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES);
            if (units > 0 && bytes + charge > maxBytes) break;
            bytes += charge; units++; this.completed++;
            if (bytes >= maxBytes) break;
        }
        this.processed += bytes; this.events.push(`step:${this.phase}`);
        this.steps.push({ phase: this.phase, requested: maxUnits, requestedBytes: maxBytes, units, bytes,
            completed: this.completed, processed: this.processed });
        return { done: this.completed === total, units, bytes, completed: this.completed, processed: this.processed };
    }
    root_export_workset(token: number): unknown {
        assert.equal(token, this.token); assert.equal(this.phase, "plan"); assert.equal(this.completed, this.planSteps);
        this.events.push("workset"); return { max_length: this.maxLength, offset_bytes: this.offsetBytes };
    }
    start_root_export_build_job(token: number): void {
        assert.equal(token, this.token); assert.equal(this.phase, "plan"); assert(this.budget.snapshot().usedBytes > 0);
        this.phase = "build"; this.completed = 0; this.processed = 0; this.buildStarts++; this.events.push("start");
    }
    root_export_info(token: number): unknown {
        assert.equal(token, this.token); assert.equal(this.completed, this.buildSteps); this.events.push("info");
        return { length: this.source.length, version: this.version, hash: HASH };
    }
    read_root_export_job(token: number, offset: number, maxBytes: number): unknown {
        assert.equal(token, this.token); assert.equal(offset, this.offset); assert.equal(maxBytes, PAGE); assert(this.job);
        this.reads.push({ offset, maxBytes }); this.events.push("read");
        const page = this.source.slice(offset, offset + maxBytes); this.offset += page.length; this.pages.push(page);
        this.afterRead?.(); return page;
    }
    finish_root_export_job(token: number): void {
        assert.equal(token, this.token); assert.equal(this.offset, this.source.length); assert(this.job);
        this.job = false; this.finishes++; this.events.push("finish"); this.afterFinish?.();
    }
    cancel_tree_job(token: number): void {
        assert.equal(token, this.token); assert(this.job); this.job = false; this.cancels++; this.events.push("cancel");
    }
    options(extra: Partial<ExportTreeRootOptions> = {}): ExportTreeRootOptions {
        return { cooperate: async () => { this.yields++; this.events.push("yield"); }, stepUnits: 1, expectedHash: HASH,
            expectedVersion: this.version, budget: this.budget,
            parse: data => { assert.equal(this.job, false, "parser ran while native export still retained its full output");
                this.parses++; assert.deepEqual(data, this.source); return { hash: HASH, version: this.version }; },
            legacy: () => { this.legacyCalls++; throw new Error("unexpected legacy getter"); }, ...extra };
    }
    export(extra: Partial<ExportTreeRootOptions> = {}, mode: TreeRootExportMode = "candidate") {
        return exportTreeRoot(this, mode, this.options(extra));
    }
}

async function exactModesPhasesPagesAndOwnedResult() {
    for (const mode of ["candidate", "committed"] as const) for (const version of [1, 2] as const) {
        for (const length of [1, 63 * 1024, PAGE, PAGE + 1, 2 * PAGE + 7]) {
            const f = new Fixture(length); f.version = version;
            const result = await f.export({}, mode);
            assert.equal(f.mode, mode); assert.deepEqual(result.bytes, f.source); assert.notEqual(result.bytes, f.source);
            assert.equal(f.arenaCap, 0xffff_ffff);
            assert.equal(result.bytes.byteOffset, 0); assert.equal(result.bytes.buffer.byteLength, length);
            assert.equal(f.yields, 1 + f.planSteps + f.buildSteps + Math.ceil(length / PAGE) - 1);
            assert.equal(f.parses, 1); assert.equal(f.legacyCalls, 0); assert.equal(f.finishes, 1); assert.equal(f.cancels, 0);
            const steps = f.events.map((event, index) => ({ event, index })).filter(row => row.event.startsWith("step:"));
            for (const row of steps) assert.equal(f.events[row.index - 1], "yield", "native step lacked a host yield");
            assert.deepEqual(f.reads, Array.from({ length: Math.ceil(length / PAGE) }, (_, index) => ({ offset: index * PAGE, maxBytes: PAGE })));
            const estimate = estimateTreeRootExportWorkset(f.maxLength, f.offsetBytes);
            assert.equal(f.budget.snapshot().usedBytes, estimate.ownerBytes + estimate.workBytes);
            for (const page of f.pages) page.fill(0);
            assert.deepEqual(result.bytes, f.source, "result borrowed a page");
            result.release(); result.release(); assert.equal(f.budget.snapshot().usedBytes, 0);
            assert.throws(() => result.bytes, /released/);
        }
    }
}

async function capturedMultiUnitStepsReduceYieldsWithoutChangingPhaseWork() {
    assert.equal(TREE_ROOT_EXPORT_MAX_STEP_UNITS, 4096);
    const yields = new Map<number, number>();
    for (const stepUnits of [1, 4, 256]) {
        const f = new Fixture(PAGE + 1); f.planSteps = 259; f.buildSteps = 514;
        const options = f.options({ stepUnits }); const cooperate = options.cooperate;
        options.cooperate = async () => { options.stepUnits = stepUnits === 1 ? 256 : 1; await cooperate(); };
        const result = await exportTreeRoot(f, "committed", options);
        assert.deepEqual(result.bytes, f.source);
        const expected = (phase: "plan" | "build", total: number) => Array.from({ length: Math.ceil(total / stepUnits) }, (_, index) => ({
            phase, requested: stepUnits, requestedBytes: PAGE, units: Math.min(stepUnits, total - index * stepUnits),
            bytes: CHEAP_CHARGE * Math.min(stepUnits, total - index * stepUnits), completed: Math.min(total, (index + 1) * stepUnits),
            processed: CHEAP_CHARGE * Math.min(total, (index + 1) * stepUnits),
        }));
        assert.deepEqual(f.steps, [...expected("plan", f.planSteps), ...expected("build", f.buildSteps)]);
        assert.equal(f.yields, 1 + Math.ceil(f.planSteps / stepUnits) + Math.ceil(f.buildSteps / stepUnits) + Math.ceil(f.source.length / PAGE) - 1);
        assert.equal(f.steps.filter(step => step.phase === "build")[0].completed, stepUnits,
            "Build inherited Plan's completed counter");
        assert.equal(f.finishes, 1); assert.equal(f.cancels, 0); yields.set(stepUnits, f.yields);
        result.release(); assert.equal(f.budget.snapshot().usedBytes, 0);
    }
    assert(yields.get(4)! < yields.get(1)!); assert(yields.get(256)! < yields.get(4)!);
    assert.equal(yields.get(1), 775); assert.equal(yields.get(256), 7);

    for (const phase of ["plan", "build"] as const) {
        const overBudget = new Fixture(), original = overBudget.step_root_export_job.bind(overBudget);
        overBudget.step_root_export_job = (token, units, bytes) => {
            const row = original(token, units, bytes) as any;
            return overBudget.phase === phase ? { ...row, units: units + 1, completed: units + 1 } : row;
        };
        await rejects(overBudget.export({ stepUnits: 4 }), /inconsistent/);
        assert.equal(overBudget.finishes, 0); assert.equal(overBudget.cancels, 1); assert.equal(overBudget.budget.snapshot().usedBytes, 0);

        const dropped = new Fixture(), run = dropped.step_root_export_job.bind(dropped), reason = new Error(`multi-unit ${phase} execution dropped job`);
        dropped.step_root_export_job = (token, units, bytes) => {
            if (dropped.phase === phase) { dropped.job = false; throw reason; }
            return run(token, units, bytes);
        };
        assert.equal(await dropped.export({ stepUnits: 4 }).then(() => null, error => error), reason);
        assert.equal(dropped.cancels, 0); assert.equal(dropped.finishes, 0); assert.equal(dropped.budget.snapshot().usedBytes, 0);
    }
    for (const stepUnits of [0, -1, 4097, 1.5, Number.NaN, Infinity, null, "4"]) {
        const f = new Fixture(); await rejects(f.export({ stepUnits } as any), /stepUnits/);
        assert.equal(f.job, false); assert.equal(f.yields, 0); assert.equal(f.legacyCalls, 0); assert.equal(f.budget.snapshot().peakUsedBytes, 0);
    }
}

async function twentyFiveThousandCheapPrimitivesRespectBothCapturedBudgets() {
    assert.equal(TREE_ROOT_EXPORT_MAX_STEP_BYTES, 16 * 1024 * 1024);
    assert.equal(TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES, PAGE + 128);
    const cases = [
        { stepUnits: undefined, stepBytes: undefined, units: 4096, bytes: PAGE, calls: 98 },
        { stepUnits: 4096, stepBytes: 256 * 1024, units: 4096, bytes: 256 * 1024, calls: 25 },
        { stepUnits: 4096, stepBytes: TREE_ROOT_EXPORT_MAX_STEP_BYTES, units: 4096, bytes: TREE_ROOT_EXPORT_MAX_STEP_BYTES, calls: 7 },
        { stepUnits: 64, stepBytes: 256 * 1024, units: 64, bytes: 256 * 1024, calls: 391 },
    ];
    for (const selected of cases) {
        const f = new Fixture(1); f.planSteps = 25_000; f.buildSteps = 25_000;
        const options = f.options({ stepUnits: selected.stepUnits, stepBytes: selected.stepBytes }), cooperate = options.cooperate;
        options.cooperate = async () => { options.stepUnits = 1; options.stepBytes = 1; await cooperate(); };
        const result = await exportTreeRoot(f, "committed", options);
        assert.deepEqual(result.bytes, f.source);
        assert.equal(f.yields, 1 + 2 * selected.calls);
        for (const phase of ["plan", "build"] as const) {
            const actual = f.steps.filter(row => row.phase === phase);
            assert.equal(actual.length, selected.calls);
            const expectedRows = [], perCall = Math.min(selected.units, Math.floor(selected.bytes / CHEAP_CHARGE));
            for (let completed = 0; completed < 25_000;) {
                const units = Math.min(perCall, 25_000 - completed); completed += units;
                expectedRows.push({ phase, requested: selected.units, requestedBytes: selected.bytes,
                    units, bytes: units * CHEAP_CHARGE, completed, processed: completed * CHEAP_CHARGE });
            }
            assert.deepEqual(actual, expectedRows, "byte/unit guard or phase reset changed exact charged work");
            assert.equal(actual.at(-1)?.completed, 25_000); assert.equal(actual.at(-1)?.processed, 6_400_000);
        }
        for (let index = 0; index < f.events.length; index++) {
            if (f.events[index].startsWith("step:")) assert.equal(f.events[index - 1], "yield");
        }
        assert.equal(f.finishes, 1); assert.equal(f.cancels, 0); result.release(); assert.equal(f.budget.snapshot().usedBytes, 0);
    }
    // This is deterministic synthetic ABI/work-count evidence: 25 turns per
    // phase at 4096/256KiB, not a native throughput or elapsed-time claim.
}

async function largestAtomicStringIsIsolatedWhenItExceedsTheByteBudget() {
    const plan = [CHEAP_CHARGE, TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES, CHEAP_CHARGE, PAGE, CHEAP_CHARGE];
    const build = [TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES, CHEAP_CHARGE];
    for (const stepBytes of [1, PAGE, 256 * 1024]) {
        const f = new Fixture(1); f.planSteps = plan.length; f.buildSteps = build.length; f.costs = { plan, build };
        const result = await f.export({ stepUnits: 4096, stepBytes });
        assert.deepEqual(result.bytes, f.source);
        for (const phase of ["plan", "build"] as const) {
            const actual = f.steps.filter(row => row.phase === phase), costs = phase === "plan" ? plan : build;
            let offset = 0, processed = 0;
            for (const row of actual) {
                const exactBytes = costs.slice(offset, offset + row.units).reduce((sum, cost) => sum + cost, 0);
                offset += row.units; processed += exactBytes;
                assert.equal(row.bytes, exactBytes); assert.equal(row.completed, offset); assert.equal(row.processed, processed);
                if (row.bytes > stepBytes) { assert.equal(row.units, 1); assert(row.bytes <= TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES); }
            }
            assert.equal(offset, costs.length); assert.equal(processed, costs.reduce((sum, cost) => sum + cost, 0));
            if (stepBytes < TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES) {
                assert(actual.some(row => row.units === 1 && row.bytes === TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES));
            }
        }
        if (stepBytes === PAGE) assert.deepEqual(f.steps.filter(row => row.phase === "plan").map(row => row.bytes), plan);
        if (stepBytes === 256 * 1024) assert.equal(f.steps.length, 2);
        result.release(); assert.equal(f.budget.snapshot().usedBytes, 0);
    }

    for (const phase of ["plan", "build"] as const) {
        const f = new Fixture(1), held = gate(), entered = gate(), controller = new AbortController();
        f.planSteps = f.buildSteps = 2;
        f.costs = { plan: [TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES, CHEAP_CHARGE], build: [TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES, CHEAP_CHARGE] };
        const reason = new Error(`cancel after atomic ${phase}`); let settled = false;
        const pending = f.export({ stepUnits: 4096, stepBytes: 1, signal: controller.signal,
            cooperate: async () => { if (f.phase === phase && f.completed === 1) { entered.release(); await held.promise; } } });
        const observed = pending.then(() => undefined, error => error).finally(() => { settled = true; });
        await entered.promise; controller.abort(reason); await Promise.resolve();
        assert.equal(settled, false); assert.equal(f.job, true); assert.equal(f.cancels, 0);
        assert.equal(f.budget.snapshot().usedBytes > 0, phase === "build");
        held.release(); assert.equal(await observed, reason); assert.equal(f.cancels, 1); assert.equal(f.finishes, 0);
        assert.equal(f.steps.filter(row => row.phase === phase).length, 1); assert.equal(f.budget.snapshot().usedBytes, 0);
    }
}

async function invalidByteBudgetAndProgressFailWithoutFallback() {
    for (const stepBytes of [0, -1, TREE_ROOT_EXPORT_MAX_STEP_BYTES + 1, 1.5, Number.NaN, Infinity, null, "65536"]) {
        const f = new Fixture(); await rejects(f.export({ stepBytes } as any), /stepBytes/);
        assert.equal(f.job, false); assert.equal(f.yields, 0); assert.equal(f.legacyCalls, 0); assert.equal(f.budget.snapshot().peakUsedBytes, 0);
    }
    for (const phase of ["plan", "build"] as const) for (const corrupt of [
        // Over-budget work cannot hide multiple cheap ops in the first-atomic exception.
        (row: any) => ({ ...row, units: 2, bytes: 2 * CHEAP_CHARGE, completed: 2, processed: 2 * CHEAP_CHARGE }),
        (row: any) => ({ ...row, units: 1, bytes: TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES + 1,
            completed: 1, processed: TREE_ROOT_EXPORT_MAX_ATOMIC_BYTES + 1 }),
        (row: any) => ({ ...row, units: 0, bytes: 0, completed: 0, processed: 0, done: false }),
        (row: any) => ({ ...row, processed: row.processed + CHEAP_CHARGE }),
        (row: any) => ({ ...row, bytes: 1.5 }),
        (row: any) => ({ ...row, processed: Number.MAX_SAFE_INTEGER + 1 }),
    ]) {
        const f = new Fixture(), native = f.step_root_export_job.bind(f);
        f.step_root_export_job = (token, units, bytes) => {
            const row = native(token, units, bytes); return f.phase === phase ? corrupt(row) : row;
        };
        await rejects(f.export({ stepUnits: 4096, stepBytes: 1 }), /progress/);
        assert.equal(f.finishes, 0); assert.equal(f.cancels, 1); assert.equal(f.legacyCalls, 0); assert.equal(f.budget.snapshot().usedBytes, 0);
    }
    for (const corrupt of [
        (row: any) => ({ ...row, units: 1, bytes: TREE_ROOT_EXPORT_MAX_STEP_BYTES,
            completed: 1, processed: TREE_ROOT_EXPORT_MAX_STEP_BYTES }),
        (row: any) => ({ ...row, units: TREE_ROOT_EXPORT_MAX_STEP_UNITS, bytes: 1,
            completed: TREE_ROOT_EXPORT_MAX_STEP_UNITS, processed: 1 }),
    ]) {
        const f = new Fixture(), native = f.step_root_export_job.bind(f);
        f.step_root_export_job = (token, units, bytes) => corrupt(native(token, units, bytes));
        await rejects(f.export({ stepUnits: TREE_ROOT_EXPORT_MAX_STEP_UNITS,
            stepBytes: TREE_ROOT_EXPORT_MAX_STEP_BYTES }), /progress/);
        assert.equal(f.finishes, 0); assert.equal(f.cancels, 1); assert.equal(f.legacyCalls, 0);
        assert.equal(f.budget.snapshot().usedBytes, 0);
    }
}

async function capturedArenaCeilingRejectsDuringPlanBeforeAdmission() {
    for (const mode of ["candidate", "committed"] as const) {
        const f = new Fixture(1); f.planSteps = 25_000;
        const original = f.step_root_export_job.bind(f), reason = new Error("native Plan arena ceiling exceeded");
        f.step_root_export_job = (token, units, bytes) => {
            const row = original(token, units, bytes);
            // Synthetic incremental admission witness: the second planned
            // descriptor grows a 512-byte arena above the captured ceiling.
            if (f.phase === "plan" && f.completed === 2 && f.arenaCap! < 640) { f.job = false; throw reason; }
            return row;
        };
        const options = f.options({ maxArenaBytes: 512, workBytes: () => { throw new Error("early Plan reached memory admission"); } });
        const cooperate = options.cooperate;
        options.cooperate = async () => { options.maxArenaBytes = 0xffff_ffff; await cooperate(); };
        assert.equal(await exportTreeRoot(f, mode, options).then(() => null, error => error), reason);
        assert.equal(f.mode, mode); assert.equal(f.arenaCap, 512); assert.equal(f.completed, 2);
        assert.equal(f.buildStarts, 0); assert.equal(f.reads.length, 0); assert.equal(f.parses, 0); assert.equal(f.cancels, 0);
        assert.equal(f.job, false); assert.equal(f.budget.snapshot().peakUsedBytes, 0); assert.equal(f.budget.snapshot().queuedRequests, 0);
    }
    const fits = new Fixture(17);
    const result = await fits.export({ maxArenaBytes: fits.maxLength, maxOutputBytes: fits.source.length });
    assert(fits.maxLength > fits.source.length, "fixture did not separate arena and output caps");
    assert.equal(fits.arenaCap, fits.maxLength); assert.deepEqual(result.bytes, fits.source); result.release();
    const exceedsOutput = new Fixture(17);
    await rejects(exceedsOutput.export({ maxArenaBytes: exceedsOutput.maxLength, maxOutputBytes: 16 }), /sealed length/);
    assert.equal(exceedsOutput.buildStarts, 1); assert.equal(exceedsOutput.reads.length, 0); assert.equal(exceedsOutput.cancels, 1);
    assert.equal(exceedsOutput.budget.snapshot().usedBytes, 0);
    for (const maxArenaBytes of [0, -1, 0x1_0000_0000, 1.5, Number.NaN, Infinity, null, "512"]) {
        const invalid = new Fixture(); await rejects(invalid.export({ maxArenaBytes } as any), /requires/);
        assert.equal(invalid.job, false); assert.equal(invalid.yields, 0); assert.equal(invalid.legacyCalls, 0);
    }
}

async function admissionQueuesAfterPlanAndBeforeBuild() {
    const f = new Fixture(), occupied = await f.budget.reserve(f.budget.snapshot().capacityBytes);
    const work = f.export(); await until(() => f.budget.snapshot().queuedRequests === 1);
    assert.equal(f.completed, f.planSteps); assert.equal(f.buildStarts, 0); assert.equal(f.reads.length, 0);
    assert.equal(f.parses, 0); assert(f.job); assert.equal(f.budget.snapshot().activeReservations, 1);
    occupied.release(); const result = await work; assert.equal(f.buildStarts, 1); result.release();
    assert.equal(f.budget.snapshot().usedBytes, 0);

    for (const failure of ["abort", "close"] as const) {
        const waiting = new Fixture(), controller = new AbortController();
        const blocker = await waiting.budget.reserve(waiting.budget.snapshot().capacityBytes);
        const pending = waiting.export({ signal: controller.signal });
        const observed = pending.then(() => { throw new Error("queued export unexpectedly succeeded"); }, error => error);
        await until(() => waiting.budget.snapshot().queuedRequests === 1);
        if (failure === "abort") controller.abort(new Error("queued stop")); else waiting.budget.close();
        assert.match(String(await observed), failure === "abort" ? /queued stop/ : /closed/);
        assert.equal(waiting.buildStarts, 0); assert.equal(waiting.reads.length, 0); assert.equal(waiting.cancels, 1);
        blocker.release(); assert.equal(waiting.budget.snapshot().usedBytes, 0);
    }
}

async function heldPageCancellationAndOwnerLoss() {
    for (const failure of ["abort", "owner", "yield"] as const) {
        const f = new Fixture(2 * PAGE + 1), held = gate(), entered = gate(), controller = new AbortController();
        let current = true, settled = false;
        const reason = new Error(`page ${failure}`);
        const pending = f.export({ signal: controller.signal, assertCurrent: () => { if (!current) throw reason; },
            cooperate: async () => { if (f.reads.length === 1) { entered.release(); await held.promise; if (failure === "yield") throw reason; } } });
        const observed = pending.then(() => undefined, error => error).finally(() => { settled = true; });
        await entered.promise;
        if (failure === "abort") controller.abort(reason); else if (failure === "owner") current = false;
        await Promise.resolve(); assert.equal(settled, false); assert(f.job); assert(f.budget.snapshot().usedBytes > 0);
        assert.equal(f.cancels, 0); held.release(); assert.equal(await observed, reason);
        assert.equal(f.reads.length, 1); assert.equal(f.finishes, 0); assert.equal(f.cancels, 1);
        assert.equal(f.budget.snapshot().usedBytes, 0);
    }
}

async function borrowedNativePagesDoNotEscapeAcrossYield() {
    const f = new Fixture(2 * PAGE + 7);
    const result = await f.export({ cooperate: async () => {
        // Synthetic native-memory replacement: invalidate every previous page
        // before subsequent native work. This is not a packaged WASM proof.
        for (const page of f.pages) if (page.buffer.byteLength) structuredClone(page.buffer, { transfer: [page.buffer] });
    } });
    assert.deepEqual(result.bytes, f.source); result.release();
}

async function malformedProgressAndPhaseReset() {
    const variants: Array<(row: any) => unknown> = [
        () => null, row => ({ ...row, extra: 1 }), row => ({ ...row, units: 2 }),
        row => ({ ...row, completed: row.completed + 1 }), row => ({ ...row, processed: row.processed + 1 }),
        row => ({ ...row, bytes: -1 }), row => ({ ...row, done: "yes" }),
        row => ({ ...row, units: 0 }), row => ({ ...row, bytes: 0 }),
        row => ({ ...row, completed: Number.MAX_SAFE_INTEGER + 1 }),
        row => ({ ...row, processed: Number.MAX_SAFE_INTEGER + 1 }),
        row => ({ done: row.done, units: row.units, completed: row.completed, remaining: 1, reachable: 0 }),
    ];
    for (const phase of ["plan", "build"] as const) for (const corrupt of variants) {
        const f = new Fixture(), original = f.step_root_export_job.bind(f);
        f.step_root_export_job = (token, units, bytes) => { const row = original(token, units, bytes); return f.phase === phase ? corrupt(row) : row; };
        await rejects(f.export(), /progress|descriptor/); assert.equal(f.cancels, 1); assert.equal(f.finishes, 0);
        assert.equal(f.budget.snapshot().usedBytes, 0); if (phase === "plan") assert.equal(f.buildStarts, 0);
    }
    const stale = new Fixture(), original = stale.step_root_export_job.bind(stale);
    stale.step_root_export_job = (token, units, bytes) => {
        const row = original(token, units, bytes) as any;
        return stale.phase === "build" ? { ...row, completed: row.completed + stale.planSteps } : row;
    };
    await rejects(stale.export(), /inconsistent/); assert.equal(stale.reads.length, 0);
    const staleBytes = new Fixture(), step = staleBytes.step_root_export_job.bind(staleBytes);
    staleBytes.step_root_export_job = (token, units, bytes) => {
        const row = step(token, units, bytes) as any;
        return staleBytes.phase === "build" ? { ...row, processed: row.processed + staleBytes.planSteps * CHEAP_CHARGE } : row;
    };
    await rejects(staleBytes.export(), /inconsistent/); assert.equal(staleBytes.reads.length, 0);
}

async function malformedWorksetsInfoAndPages() {
    for (const descriptor of [null, {}, { max_length: 0, offset_bytes: 0 }, { max_length: 100, offset_bytes: -1 },
        { max_length: 100, offset_bytes: 0, extra: 1 }, { max_length: 2 ** 32, offset_bytes: 0 },
        { get max_length() { throw new Error("descriptor getter ran"); }, offset_bytes: 0 }]) {
        const f = new Fixture(); f.root_export_workset = () => descriptor;
        await rejects(f.export(), /descriptor|byte bound/); assert.equal(f.buildStarts, 0); assert.equal(f.cancels, 1);
    }
    const capped = new Fixture(); await rejects(capped.export({ maxArenaBytes: capped.maxLength - 1 }), /byte bound/);
    assert.equal(capped.buildStarts, 0); assert.equal(capped.budget.snapshot().peakUsedBytes, 0);
    for (const descriptor of [null, { length: 0, version: 1, hash: HASH }, { length: 999999, version: 1, hash: HASH },
        { length: 1, version: 2, hash: HASH }, { length: 1, version: 1, hash: "cd".repeat(32) },
        { length: 1, version: 1, hash: HASH, extra: 1 }]) {
        const f = new Fixture(); f.root_export_info = () => descriptor;
        await rejects(f.export(), /descriptor|identity/); assert.equal(f.reads.length, 0); assert.equal(f.cancels, 1);
        assert.equal(f.budget.snapshot().usedBytes, 0);
    }
    for (const make of [() => null, () => new Uint8Array(0), () => new Uint8Array(PAGE - 1), () => new Uint8Array(PAGE + 1),
        () => new Uint8Array(PAGE + 1).subarray(1), () => new Uint8Array(new SharedArrayBuffer(PAGE)),
        () => { const value = new Uint8Array(PAGE); structuredClone(value.buffer, { transfer: [value.buffer] }); return value; },
        () => { const value = new Uint8Array(PAGE + 1); Object.defineProperty(value, "byteLength", { value: PAGE }); return value; }]) {
        const f = new Fixture(); f.read_root_export_job = () => make();
        await rejects(f.export(), /invalid byte/); assert.equal(f.cancels, 1); assert.equal(f.finishes, 0);
        assert.equal(f.budget.snapshot().usedBytes, 0);
    }
}

async function partialApiInvalidArgumentsAndNoSpeculativeCancel() {
    const names: Array<keyof TreeRootExportJobTree> = ["begin_candidate_root_export_job", "begin_committed_root_export_job", "step_root_export_job",
        "cancel_tree_job", "root_export_workset", "start_root_export_build_job", "root_export_info", "read_root_export_job", "finish_root_export_job"];
    for (const name of names) for (const invalid of [undefined, null, 17]) {
        const f = new Fixture(); (f as any)[name] = invalid;
        await rejects(f.export(), /incomplete/); assert.equal(f.job, false); assert.equal(f.legacyCalls, 0); assert.equal(f.yields, 0);
    }
    const onlyStep = new Fixture();
    await rejects(exportTreeRoot({ step_root_export_job: onlyStep.step_root_export_job.bind(onlyStep) }, "committed", onlyStep.options()), /incomplete/);
    assert.equal(onlyStep.yields, 0); assert.equal(onlyStep.legacyCalls, 0);
    for (const token of [null, 0, -1, 1.5, Number.NaN, 2 ** 32]) {
        const f = new Fixture(); f.begin_candidate_root_export_job = () => token;
        await rejects(f.export(), /invalid token/); assert.equal(f.cancels, 0);
    }
    for (const extra of [{ expectedHash: "AB".repeat(32) }, { expectedVersion: 3 }, { maxOutputBytes: 0 },
        { maxOutputBytes: 2 ** 32 }, { workBytes: 0 }, { workBytes: 1.5 }, { cooperate: null }, { parse: null }, { assertCurrent: 1 }]) {
        const f = new Fixture(); await rejects(f.export(extra as any), /requires/); assert.equal(f.job, false);
    }
    const f = new Fixture(); await rejects(f.export({}, "other" as any), /requires/);
    const cancelled = new AbortController(); cancelled.abort(new Error("before begin"));
    await rejects(f.export({ signal: cancelled.signal }), /before begin/); assert.equal(f.job, false);
}

async function nativeErrorsParserErrorsAndCleanupPreservePrimary() {
    for (const name of ["step_root_export_job", "start_root_export_build_job", "root_export_info", "read_root_export_job", "finish_root_export_job"] as const) {
        const f = new Fixture(), primary = new Error(`native ${name}`); (f as any)[name] = () => { throw primary; };
        assert.equal(await f.export().then(() => null, error => error), primary);
        assert.equal(f.cancels, name === "step_root_export_job" ? 0 : 1);
        assert.equal(f.finishes, 0); assert.equal(f.budget.snapshot().usedBytes, 0);
    }
    for (const parse of [() => ({ hash: "cd".repeat(32), version: 1 }), () => ({ hash: HASH, version: 2 }),
        () => { throw new Error("parser failed"); }]) {
        const f = new Fixture(); await rejects(f.export({ parse }), /identity|parser failed/);
        assert.equal(f.cancels, 0); assert.equal(f.finishes, 1); assert.equal(f.budget.snapshot().usedBytes, 0);
    }
    const oldLog = console.error;
    try {
        console.error = () => { throw new Error("logger failed"); };
        for (const frozen of [false, true]) {
            const f = new Fixture(), original = new Error("native finish failed"), primary = frozen ? Object.freeze(original) : original;
            const cleanup = new Error("synthetic cancellation failed");
            f.finish_root_export_job = () => { throw primary; };
            f.cancel_tree_job = () => { f.cancels++; throw cleanup; };
            assert.equal(await f.export().then(() => null, error => error), primary);
            if (!frozen) assert.deepEqual((primary as any).treeCandidateCleanupErrors, [cleanup]);
            assert.equal(f.cancels, 1); assert.equal(f.budget.snapshot().usedBytes, 0);
        }
        const stale = new Fixture(), primary = new Error("stale root export token");
        const originalToken = stale.token;
        stale.read_root_export_job = () => { stale.token++; throw primary; };
        let replacementCancelled = false;
        stale.cancel_tree_job = token => {
            assert.equal(token, originalToken, "cleanup guessed the replacement owner token");
            if (token === stale.token) replacementCancelled = true;
            throw new Error("matching old job no longer exists");
        };
        assert.equal(await stale.export().then(() => null, error => error), primary);
        assert.equal(replacementCancelled, false); assert.equal(stale.budget.snapshot().usedBytes, 0);

        const dropped = new Fixture(), stepError = new Error("native step dropped poisoned job");
        dropped.step_root_export_job = () => { dropped.job = false; throw stepError; };
        assert.equal(await dropped.export().then(() => null, error => error), stepError);
        assert.equal(dropped.cancels, 0);
        assert.equal(dropped.budget.snapshot().usedBytes, 0);
    } finally { console.error = oldLog; }
}

async function scopeOwnsActualConsumerAndBorrowedQuota() {
    const f = new Fixture(), result = await f.export({ workBytes: 128 }), io = gate();
    const owner = result.memory.track(async () => { const bytes = result.bytes; await io.promise; assert.deepEqual(bytes, f.source); });
    const child = await result.memory.reserve(128);
    assert.equal(result.memory.snapshot().work.capacityBytes, 128);
    result.release(); assert(result.memory.snapshot().ownerClosed); assert(f.budget.snapshot().usedBytes > 0);
    io.release(); await owner; assert(f.budget.snapshot().usedBytes > 0);
    child.release(); assert.equal(f.budget.snapshot().usedBytes, 0);
    await assert.rejects(result.memory.track(async () => {}), /closed/);

    const failed = new Fixture(), owned = await failed.export(), held = gate(), reason = new Error("native sink failed");
    const pending = owned.memory.track(async () => { await held.promise; throw reason; });
    const observed = pending.catch(error => error); owned.release(); assert(failed.budget.snapshot().usedBytes > 0);
    held.release(); assert.equal(await observed, reason); assert.equal(failed.budget.snapshot().usedBytes, 0);
}

async function consumerQuotaUsesValidatedPlanAndCapturedCallback() {
    const f = new Fixture(); let calls = 0;
    const options = f.options({ workBytes: maximum => {
        calls++; assert.equal(maximum, f.maxLength); assert(f.events.includes("workset"));
        assert.equal(f.buildStarts, 0); assert.equal(f.budget.snapshot().usedBytes, 0); return maximum + PAGE;
    } });
    options.cooperate = async () => { options.workBytes = () => { throw new Error("replaced quota callback"); }; };
    const result = await exportTreeRoot(f, "committed", options);
    assert.equal(calls, 1); assert.equal(result.memory.snapshot().work.capacityBytes, f.maxLength + PAGE);
    await result.memory.run(result.bytes.length + PAGE, async () => { assert.deepEqual(result.bytes, f.source); });
    result.release(); assert.equal(f.budget.snapshot().usedBytes, 0);

    for (const returned of [0, -1, 0.5, Number.NaN, Number.MAX_SAFE_INTEGER + 1, Number.MAX_SAFE_INTEGER]) {
        const invalid = new Fixture();
        await rejects(invalid.export({ workBytes: () => returned }), /quota|workset/);
        assert.equal(invalid.buildStarts, 0); assert.equal(invalid.cancels, 1); assert.equal(invalid.budget.snapshot().peakUsedBytes, 0);
    }
    const stopped = new Fixture(), controller = new AbortController();
    await rejects(stopped.export({ signal: controller.signal, workBytes: () => {
        controller.abort(new Error("quota owner stopped")); return 1;
    } }), /quota owner stopped/);
    assert.equal(stopped.buildStarts, 0); assert.equal(stopped.budget.snapshot().peakUsedBytes, 0);
    const invalidPlan = new Fixture(); invalidPlan.root_export_workset = () => ({ max_length: -1, offset_bytes: 0 });
    await rejects(invalidPlan.export({ workBytes: () => { throw new Error("quota ran before plan validation"); } }), /byte bound/);

    const legacy = new Fixture(17); let legacyQuota = 0;
    const owned = await exportTreeRoot({}, "committed", legacy.options({ legacy: () => legacy.source.slice(),
        workBytes: length => { legacyQuota++; assert.equal(length, 17); return length + PAGE; } }));
    assert.equal(legacyQuota, 1); assert.equal(owned.memory.snapshot().work.capacityBytes, 17 + PAGE); owned.release();
}

async function guardsCaptureMethodsAndCatchReentrantStop() {
    const f = new Fixture(), options = f.options(), original = options.cooperate;
    let first = true;
    options.cooperate = async () => {
        await original();
        if (first) { first = false; f.read_root_export_job = () => { throw new Error("replaced method"); };
            options.parse = () => { throw new Error("replaced parser"); }; options.expectedHash = "cd".repeat(32); }
    };
    const result = await exportTreeRoot(f, "candidate", options); assert.deepEqual(result.bytes, f.source); result.release();
    for (const point of ["initial", "plan", "admitted", "last-page", "finish"] as const) {
        const stopped = new Fixture(1), controller = new AbortController(), reason = new Error(`stop ${point}`);
        if (point === "last-page") stopped.afterRead = () => controller.abort(reason);
        if (point === "finish") stopped.afterFinish = () => controller.abort(reason);
        const work = stopped.export({ signal: controller.signal, assertCurrent: () => {
            if (point === "initial" || point === "plan" && stopped.completed === 1 ||
                point === "admitted" && stopped.budget.snapshot().usedBytes > 0) controller.abort(reason);
        } });
        assert.equal(await work.then(() => null, error => error), reason); assert.equal(stopped.budget.snapshot().usedBytes, 0);
        if (point === "initial") assert.equal(stopped.job, false);
        else assert.equal(stopped.cancels, point === "finish" ? 0 : 1);
    }
}

async function legacyOnceExplicitModeAndLateAdmission() {
    for (const mode of ["candidate", "committed"] as const) {
        const f = new Fixture(), legacyBytes = f.source.slice(), held = gate(), entered = gate(); let calls = 0;
        const sharedOnly = { step_tree_job() { throw new Error("unexpected shared step"); }, cancel_tree_job() { throw new Error("unexpected shared cancel"); } };
        const work = exportTreeRoot(sharedOnly, mode, f.options({
            cooperate: async () => { entered.release(); await held.promise; },
            legacy: selected => { assert.equal(selected, mode); assert.equal(f.budget.snapshot().usedBytes, 0); calls++; return legacyBytes; },
        }));
        await entered.promise; assert.equal(calls, 0); held.release(); const result = await work;
        assert.equal(calls, 1); assert.equal(result.bytes, legacyBytes); assert.equal(f.parses, 1);
        assert.equal(f.budget.snapshot().usedBytes, 2 * legacyBytes.length + 1); result.release();
    }
    for (const make of [() => null, () => new Uint8Array(0), () => new Uint8Array(5).subarray(1), () => new Uint8Array(9)]) {
        const f = new Fixture(4); let calls = 0;
        await rejects(exportTreeRoot({}, "committed", f.options({ maxOutputBytes: 4, legacy: () => { calls++; return make(); } })), /invalid byte|byte bound/);
        assert.equal(calls, 1); assert.equal(f.budget.snapshot().usedBytes, 0);
    }
    const f = new Fixture(), controller = new AbortController();
    await rejects(exportTreeRoot({}, "candidate", f.options({ signal: controller.signal, legacy: () => {
        controller.abort(new Error("legacy stop")); return f.source.slice();
    } })), /legacy stop/); assert.equal(f.budget.snapshot().usedBytes, 0); assert.equal(f.parses, 0);
}

void (async () => {
    const tests = [exactModesPhasesPagesAndOwnedResult, capturedMultiUnitStepsReduceYieldsWithoutChangingPhaseWork,
        twentyFiveThousandCheapPrimitivesRespectBothCapturedBudgets, largestAtomicStringIsIsolatedWhenItExceedsTheByteBudget,
        invalidByteBudgetAndProgressFailWithoutFallback, capturedArenaCeilingRejectsDuringPlanBeforeAdmission, admissionQueuesAfterPlanAndBeforeBuild,
        heldPageCancellationAndOwnerLoss, borrowedNativePagesDoNotEscapeAcrossYield, malformedProgressAndPhaseReset,
        malformedWorksetsInfoAndPages, partialApiInvalidArgumentsAndNoSpeculativeCancel,
        nativeErrorsParserErrorsAndCleanupPreservePrimary, scopeOwnsActualConsumerAndBorrowedQuota,
        consumerQuotaUsesValidatedPlanAndCapturedCallback, guardsCaptureMethodsAndCatchReentrantStop, legacyOnceExplicitModeAndLateAdmission];
    for (const test of tests) await test();
    console.log(`tree-root-export-job: ${tests.length} suites passed`);
})().catch(error => { console.error(error); process.exitCode = 1; });
