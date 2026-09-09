import { strict as assert } from "node:assert";
import { exportTreeChunk, TREE_CHUNK_EXPORT_PAGE_BYTES as PAGE,
    type TreeChunkExportJobTree, type ExportTreeChunkOptions } from "./tree-chunk-export-job";

const HASH = "ab".repeat(32);
const payload = (length: number) => Uint8Array.from({ length }, (_, index) => (index * 17 + (index >>> 8)) & 255);
function gate() { let release!: () => void; const promise = new Promise<void>(resolve => { release = resolve; }); return { promise, release }; }

class ExportFixture implements TreeChunkExportJobTree {
    candidate = true;
    readonly candidateRevision = 1;
    readonly committedRevision = 1;
    job = false;
    token = 37;
    offset = 0;
    begins = 0;
    finishes = 0;
    cancels = 0;
    legacyCalls = 0;
    yields = 0;
    readonly events: string[] = [];
    readonly reads: Array<{ token: number; offset: number; maxBytes: number }> = [];
    pages: Uint8Array[] = [];
    afterRead?: () => void;
    afterFinish?: () => void;
    constructor(readonly source: Uint8Array) {}
    has_candidate(): boolean { return this.candidate; }
    begin_tree_chunk_export_job(hash: string): unknown {
        assert.equal(hash, HASH); assert.equal(this.job, false);
        this.begins++; this.job = true; this.offset = 0; this.events.push("begin");
        return { token: this.token, length: this.source.length };
    }
    read_tree_chunk_export_job(token: number, expectedOffset: number, maxBytes: number): unknown {
        assert.equal(token, this.token); assert.equal(this.job, true); assert.equal(expectedOffset, this.offset);
        assert.equal(maxBytes, PAGE); assert(expectedOffset < this.source.length, "unexpected read at EOF");
        this.reads.push({ token, offset: expectedOffset, maxBytes }); this.events.push("read");
        const result = this.source.slice(expectedOffset, expectedOffset + maxBytes);
        this.offset += result.length; this.pages.push(result); this.afterRead?.(); return result;
    }
    finish_tree_chunk_export_job(token: number): void {
        assert.equal(token, this.token); assert.equal(this.job, true); assert.equal(this.offset, this.source.length);
        this.finishes++; this.job = false; this.events.push("finish"); this.afterFinish?.();
    }
    cancel_tree_job(token: number): void {
        assert.equal(token, this.token); assert.equal(this.job, true);
        this.cancels++; this.job = false; this.events.push("cancel");
    }
    options(extra: Partial<ExportTreeChunkOptions> = {}): ExportTreeChunkOptions {
        return { cooperate: async () => { this.yields++; this.events.push("yield"); },
            legacy: () => { this.legacyCalls++; throw new Error("unexpected legacy getter"); }, ...extra };
    }
    export(extra: Partial<ExportTreeChunkOptions> = {}) {
        return exportTreeChunk(this, HASH, this.source.length, this.options(extra));
    }
}

async function exactPagesAndNoInitialOrTerminalYield() {
    for (const length of [0, 1, 63 * 1024, PAGE, PAGE + 1, 2 * PAGE, 2 * PAGE + 17]) {
        const tree = new ExportFixture(payload(length));
        const work = tree.export();
        assert.equal(tree.begins, 1, "helper added an initial async yield");
        assert.equal(tree.reads.length, Math.min(1, Math.ceil(length / PAGE)));
        const result = await work;
        assert.deepEqual(result, tree.source); assert.notEqual(result, tree.source);
        assert.equal(result.buffer.byteLength, length); assert.equal(result.byteOffset, 0);
        assert.deepEqual(tree.reads, Array.from({ length: Math.ceil(length / PAGE) }, (_, index) =>
            ({ token: tree.token, offset: index * PAGE, maxBytes: PAGE })));
        assert.equal(tree.yields, Math.max(0, Math.ceil(length / PAGE) - 1));
        assert.equal(tree.finishes, 1); assert.equal(tree.cancels, 0); assert.equal(tree.job, false);
        assert.equal(tree.legacyCalls, 0); assert.equal(tree.candidate, true);
        assert.equal(tree.candidateRevision, 1); assert.equal(tree.committedRevision, 1);
        for (const page of tree.pages) page.fill(0);
        assert.deepEqual(result, tree.source, "returned buffer aliases a native page");
    }
    const committedOnly = new ExportFixture(payload(PAGE + 1)); committedOnly.candidate = false;
    assert.deepEqual(await committedOnly.export(), committedOnly.source);
    assert.equal(committedOnly.candidate, false); assert.equal(committedOnly.finishes, 1);
}

async function heldCancellationDoesNotDropNativeOwnershipEarly() {
    const tree = new ExportFixture(payload(PAGE + 1)), controller = new AbortController(), entered = gate(), held = gate();
    const failure = new Error("export stopped between pages"); let settled = false;
    const work = tree.export({ signal: controller.signal, cooperate: async () => { entered.release(); await held.promise; } });
    const outcome = work.then(() => undefined, error => error).finally(() => { settled = true; });
    try {
        await entered.promise; controller.abort(failure); await Promise.resolve(); await Promise.resolve();
        assert.equal(settled, false); assert.equal(tree.job, true); assert.equal(tree.cancels, 0);
        assert.equal(tree.reads.length, 1); assert.equal(tree.finishes, 0);
    } finally { held.release(); }
    assert.equal(await outcome, failure); assert.equal(tree.cancels, 1); assert.equal(tree.job, false);
    assert.equal(tree.finishes, 0); assert.equal(tree.candidate, true); assert.equal(tree.reads.length, 1);
}

async function pagesAreCopiedBeforeMemoryChangesAtYield() {
    const tree = new ExportFixture(payload(2 * PAGE + 1)); let yields = 0;
    const result = await tree.export({ cooperate: async () => {
        yields++;
        for (const page of tree.pages) {
            page.fill(0); // Native memory reuse analog before the next read.
            structuredClone(page.buffer, { transfer: [page.buffer] }); // memory.grow/detach analog.
            assert.equal(page.buffer.byteLength, 0);
        }
        tree.pages = [];
    } });
    assert.equal(yields, 2); assert.deepEqual(result, tree.source);
    tree.pages.at(-1)!.fill(0); assert.deepEqual(result, tree.source);
    result.fill(255); assert.notDeepEqual(result, tree.source);
}

async function malformedPagesAndNativeFailuresRetainCancelOwner() {
    const wrongPages: unknown[] = [null, undefined, [], new Uint16Array(1), new Uint8Array(0),
        new Uint8Array(PAGE - 1), new Uint8Array(PAGE + 1), new Uint8Array(PAGE + 1).subarray(1),
        new Uint8Array(PAGE + 1).subarray(0, PAGE), new Uint8Array(new SharedArrayBuffer(PAGE))];
    const hiddenBacking = new Uint8Array(PAGE + 1).subarray(0, PAGE);
    Object.defineProperty(hiddenBacking, "buffer", { value: new ArrayBuffer(PAGE) }); wrongPages.push(hiddenBacking);
    const detached = new Uint8Array(PAGE); structuredClone(detached.buffer, { transfer: [detached.buffer] }); wrongPages.push(detached);
    for (const page of wrongPages) {
        const tree = new ExportFixture(payload(PAGE + 1)); tree.read_tree_chunk_export_job = () => page;
        await assert.rejects(tree.export(), /invalid byte/);
        assert.equal(tree.cancels, 1); assert.equal(tree.finishes, 0); assert.equal(tree.job, false);
        assert.equal(tree.legacyCalls, 0); assert.equal(tree.candidate, true);
    }
    for (const stage of ["read", "last-read", "finish", "yield"] as const) {
        const tree = new ExportFixture(payload(PAGE + 1)), failure = new Error(`retained ${stage} failure`);
        const read = tree.read_tree_chunk_export_job.bind(tree);
        if (stage === "read" || stage === "last-read") tree.read_tree_chunk_export_job = (token, offset, cap) => {
            if (stage === "read" || offset > 0) throw failure; return read(token, offset, cap);
        };
        if (stage === "finish") tree.finish_tree_chunk_export_job = () => { throw failure; };
        await assert.rejects(tree.export({ cooperate: async () => { if (stage === "yield") throw failure; } }), error => error === failure);
        assert.equal(tree.job, false); assert.equal(tree.cancels, 1); assert.equal(tree.candidate, true);
        assert.equal(tree.legacyCalls, 0);
    }
}

async function malformedStartAndInputsFailClosed() {
    for (const start of [{ token: 37, length: 2 }, { token: 37, length: "1" },
        { token: 37, length: Number.NaN }, { token: 37 }, { token: 37, length: 1, extra: true },
        { token: 37, get length() { throw new Error("must not invoke native response accessor"); } }]) {
        const tree = new ExportFixture(payload(1));
        tree.begin_tree_chunk_export_job = () => { tree.job = true; tree.begins++; return start; };
        await assert.rejects(tree.export(), /invalid start/);
        assert.equal(tree.cancels, 1); assert.equal(tree.job, false); assert.equal(tree.reads.length, 0);
        assert.equal(tree.finishes, 0); assert.equal(tree.legacyCalls, 0);
    }
    for (const start of [null, [], 37, {}, { token: 0, length: 1 }, { token: -1, length: 1 },
        { token: 0x1_0000_0000, length: 1 }, { token: 1.5, length: 1 }, { token: "37", length: 1 }]) {
        const tree = new ExportFixture(payload(1)); tree.begin_tree_chunk_export_job = () => start;
        await assert.rejects(tree.export(), /invalid start/);
        assert.equal(tree.cancels, 0, "invalid token was guessed/coerced into another native owner");
        assert.equal(tree.reads.length, 0); assert.equal(tree.legacyCalls, 0);
    }
    for (const hash of ["", "A".repeat(64), "ab".repeat(31), `${HASH}0`, "g".repeat(64)]) {
        const tree = new ExportFixture(payload(1));
        await assert.rejects(exportTreeChunk(tree, hash, 1, tree.options()), /canonical hash/);
        assert.equal(tree.begins, 0);
    }
    for (const size of [-1, NaN, Infinity, .5, 0x1_0000_0000, "1"] as unknown[]) {
        const tree = new ExportFixture(payload(1));
        await assert.rejects(exportTreeChunk(tree, HASH, size as number, tree.options()), /u32 length/);
        assert.equal(tree.begins, 0);
    }
}

async function partialApiNeverFallsBack() {
    for (const name of ["begin_tree_chunk_export_job", "read_tree_chunk_export_job",
        "finish_tree_chunk_export_job", "cancel_tree_job"] as const) for (const value of [undefined, null, 42]) {
        const tree = new ExportFixture(payload(1)); Object.defineProperty(tree, name, { value });
        await assert.rejects(tree.export(), /incomplete tree chunk export/);
        assert.equal(tree.begins, 0); assert.equal(tree.legacyCalls, 0); assert.equal(tree.candidate, true);
    }
    const tree = new ExportFixture(payload(1)), failure = new Error("begin failed before token ownership");
    tree.begin_tree_chunk_export_job = () => { throw failure; };
    await assert.rejects(tree.export(), error => error === failure); assert.equal(tree.cancels, 0);
}

async function guardsCoverBeginReadFinishAndLegacy() {
    for (const phase of ["before", "begin", "read", "last-read", "yield", "finish", "candidate", "hook-abort"] as const) {
        const tree = new ExportFixture(payload(PAGE + 1)), controller = new AbortController(), failure = new Error(`stale ${phase}`);
        let current = phase !== "before";
        const begin = tree.begin_tree_chunk_export_job.bind(tree);
        tree.begin_tree_chunk_export_job = hash => { const value = begin(hash); if (phase === "begin") current = false; return value; };
        tree.afterRead = () => {
            if (phase === "read" || phase === "last-read" && tree.offset === tree.source.length) current = false;
            if (phase === "candidate") tree.candidate = false;
        };
        tree.afterFinish = () => { if (phase === "finish") current = false; };
        await assert.rejects(tree.export({ signal: controller.signal,
            assertCurrent: () => {
                if (phase === "hook-abort") controller.abort(failure); if (!current) throw failure;
                if (!tree.candidate) throw new Error("captured candidate disappeared");
            },
            cooperate: async () => { if (phase === "yield") current = false; },
        }), phase === "candidate" ? /candidate disappeared/ : error => error === failure);
        assert.equal(tree.begins, Number(phase !== "before" && phase !== "hook-abort"));
        assert.equal(tree.cancels, Number(!["before", "hook-abort", "finish"].includes(phase)));
        assert.equal(tree.finishes, Number(phase === "finish")); assert.equal(tree.job, false);
    }
    for (const phase of ["success", "pre-abort", "post-abort", "scope", "candidate", "malformed"] as const) {
        let calls = 0, current = true, candidate = true; const controller = new AbortController(), failure = new Error(phase);
        const source = payload(17), legacyBytes = source.slice(), tree: TreeChunkExportJobTree = {
            cancel_tree_job: () => { throw new Error("shared cancel does not claim export API"); },
        };
        if (phase === "pre-abort") controller.abort(failure);
        const work = exportTreeChunk(tree, HASH, source.length, { signal: controller.signal,
            cooperate: async () => { throw new Error("legacy added an initial yield"); },
            assertCurrent: () => { if (!current) throw failure; if (!candidate) throw new Error("captured candidate disappeared"); }, legacy: () => {
                calls++; if (phase === "post-abort") controller.abort(failure);
                if (phase === "scope") current = false; if (phase === "candidate") candidate = false;
                return phase === "malformed" ? new Uint8Array(0) : legacyBytes;
            },
        });
        assert.equal(calls, Number(phase !== "pre-abort"), "legacy getter did not execute synchronously exactly once");
        if (phase === "success") {
            const result = await work; assert.deepEqual(result, source);
            assert.equal(result, legacyBytes, "helper copied the legacy getter's already-owned return value");
            source.fill(0); assert.deepEqual(result, payload(17));
        } else await assert.rejects(work, phase === "candidate" ? /candidate disappeared/ :
            phase === "malformed" ? /invalid byte/ : error => error === failure);
    }
}

async function exactOffsetsRejectStaleNativeState() {
    const previous = console.error;
    try {
        for (const phase of ["offset", "token"] as const) {
            const tree = new ExportFixture(payload(PAGE + 1)), read = tree.read_tree_chunk_export_job.bind(tree);
            let primary: unknown, diagnostics = 0;
            console.error = () => { diagnostics++; };
            tree.read_tree_chunk_export_job = (token, offset, cap) => {
                try { return read(token, offset, cap); } catch (error) { primary = error; throw error; }
            };
            await assert.rejects(tree.export({ cooperate: async () => {
                if (phase === "offset") tree.offset++; else tree.token++;
            } }), error => error === primary);
            assert(primary);
            // A changed native token is not ours to cancel; cleanup must retain
            // the primary failure instead of cancelling the replacement owner.
            assert.equal(tree.finishes, 0); assert.equal(tree.legacyCalls, 0);
            assert.equal(diagnostics, Number(phase === "token"));
            if (phase === "offset") { assert.equal(tree.cancels, 1); assert.equal(tree.job, false); }
            else { assert.equal(tree.cancels, 0); assert.equal(tree.job, true); }
        }
    } finally { console.error = previous; }
}

async function cleanupFailureNeverMasksPrimary() {
    const previous = console.error;
    try {
        for (const frozen of [false, true]) for (const dropped of [false, true]) {
            const tree = new ExportFixture(payload(1)), primary = new Error("read retained job"), cleanup = new Error("cancel failed");
            if (frozen) Object.freeze(primary);
            tree.read_tree_chunk_export_job = () => { throw primary; };
            const cancel = tree.cancel_tree_job.bind(tree); tree.cancel_tree_job = token => { if (dropped) cancel(token); throw cleanup; };
            let diagnostics = 0; console.error = () => { diagnostics++; if (frozen) throw new Error("logger failed"); };
            await assert.rejects(tree.export(), error => error === primary);
            if (!frozen) assert.deepEqual((primary as Error & { treeCandidateCleanupErrors?: unknown[] }).treeCandidateCleanupErrors, [cleanup]);
            assert.equal(diagnostics, 1); assert.equal(tree.cancels, Number(dropped)); assert.equal(tree.job, !dropped);
        }
    } finally { console.error = previous; }
}

async function capturedOptionsAndMethodsStayStableAcrossYield() {
    const tree = new ExportFixture(payload(PAGE + 1)), signal = new AbortController(), replacement = new AbortController();
    replacement.abort(new Error("new signal must not replace captured owner"));
    const options = tree.options({ signal: signal.signal, cooperate: async () => {
        options.signal = replacement.signal; options.assertCurrent = () => { throw new Error("new guard"); };
        options.cooperate = async () => { throw new Error("new cooperate"); };
        tree.read_tree_chunk_export_job = () => { throw new Error("new read"); };
        tree.finish_tree_chunk_export_job = () => { throw new Error("new finish"); };
        tree.cancel_tree_job = () => { throw new Error("new cancel"); };
    } });
    assert.deepEqual(await exportTreeChunk(tree, HASH, tree.source.length, options), tree.source);
    assert.equal(tree.finishes, 1); assert.equal(tree.cancels, 0);
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed && !process.exitCode) { console.error("tree-chunk-export-job suite did not finish"); process.exitCode = 1; }
});
void (async () => {
    const suites = [exactPagesAndNoInitialOrTerminalYield, heldCancellationDoesNotDropNativeOwnershipEarly,
        pagesAreCopiedBeforeMemoryChangesAtYield, malformedPagesAndNativeFailuresRetainCancelOwner,
        malformedStartAndInputsFailClosed, partialApiNeverFallsBack, guardsCoverBeginReadFinishAndLegacy,
        exactOffsetsRejectStaleNativeState, cleanupFailureNeverMasksPrimary, capturedOptionsAndMethodsStayStableAcrossYield];
    for (const suite of suites) await suite();
    completed = true;
    console.log(`tree-chunk-export-job: ${suites.length} host-driver suites passed (synthetic native ports)`);
})().catch(error => { console.error(error); process.exitCode = 1; });
