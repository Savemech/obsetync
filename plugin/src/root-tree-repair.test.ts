import { strict as assert } from "node:assert";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { ObsetyncSyncBase, type CapturedBaseTreeEntries, type CapturedBaseTreeEntry } from "./sync-base";
import { MemorySegmentedIO, storeTestIOError } from "./segmented-store-test-io";
import { ResourceBudget, ResourceBudgetOversizedError } from "./resource-budget";
import { rebuildRootTreeFromBase, estimateRootTreeRepairWorkset, ROOT_TREE_REPAIR_LIMITS,
    RootTreeRepairError } from "./root-tree-repair";

let assertions = 0;
function check(value: unknown, message: string): void { assertions++; assert.ok(value, message); }
const hash = (digit: string) => digit.repeat(64);
const cooperate = async () => {};
const pool = (capacityBytes = 16 * 1024 * 1024) => new ResourceBudget({ capacityBytes });
function gate() { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; }
async function rejected(work: Promise<unknown>, code?: RootTreeRepairError["code"]): Promise<unknown> {
    let failure: unknown;
    try { await work; } catch (error) { failure = error; }
    check(failure !== undefined, "root rebuild unexpectedly succeeded");
    if (code) check(failure instanceof RootTreeRepairError && failure.code === code, `unexpected root rebuild error, expected ${code}`);
    return failure;
}
async function fixture(count = 3) {
    const io = new MemorySegmentedIO(), base = new ObsetyncSyncBase({ vault: { adapter: io } } as any);
    await base.load();
    for (let i = 0; i < count; i++) base.setEntry(`dir/note-${i.toString().padStart(5, "0")}.md`, hash("a"), 20 + i, i, 10 + i);
    base.setTreeBaseRoot(hash("b")); await base.checkpoint(); io.events.length = 0;
    return { io, base };
}
function fakeTree(onRebuild?: (json: string) => void) {
    let version = 1, rows: CapturedBaseTreeEntry[] = [{ path: "old.md", hash: hash("0"), mtime_ms: 0, size: 0 }];
    let root = hash("0"), calls = 0;
    return {
        rebuild_from_entries_in_version(next: number, json: string) {
            calls++; onRebuild?.(json); const parsed = JSON.parse(json); version = next; rows = parsed; root = hash("c");
        },
        root_hash_hex: () => root,
        tree_version: () => version,
        total_files: () => rows.length,
        get calls() { return calls; },
        get rows() { return rows; },
    };
}
function syntheticBase(rows: readonly CapturedBaseTreeEntry[], declaredCount = rows.length,
    callbacks: { iteration?: (pass: number) => void; current?: () => boolean } = {}) {
    let pass = 0;
    return { captureTreeEntries(): CapturedBaseTreeEntries {
        return { entryCount: declaredCount, treeBaseRoot: null,
            *entries() { callbacks.iteration?.(++pass); for (const row of rows) yield { ...row }; },
            isCurrent: callbacks.current ?? (() => true) };
    } };
}

async function capturesAreDetachedAndInvalidated() {
    const { base, io } = await fixture();
    const captured = base.captureTreeEntries(), first = captured.entries().next().value!;
    first.hash = hash("f"); first.path = "changed.md";
    check(base.getHash("dir/note-00000.md") === hash("a"), "capture row aliases the persistent base value");
    check(captured.entries().next().value!.mtime_ms === 10, "capture replaced committed tree mtime with local stat mtime");
    check(captured.isCurrent(), "detached row edits changed the witness");
    base.setEntry("later.md", hash("d"), 40, 4);
    check(!captured.isCurrent() && [...captured.entries()].length === 3, "captured traversal mixed in a newer entry");
    check(base.captureTreeEntries().entryCount === 4, "new capture omitted the visible validated working entry");
    await base.checkpoint();
    const beforeReload = base.captureTreeEntries(); await base.load();
    check(!beforeReload.isCurrent(), "reload reused a pre-reload witness after mutation version reset");
    const beforeRoot = base.captureTreeEntries(); base.setTreeBaseRoot(hash("e"));
    check(!beforeRoot.isCurrent(), "changed honest parent retained the old witness");
    await base.checkpoint();
    const beforePoison = base.captureTreeEntries();
    base.setLastSyncTimestamp(1);
    io.onBoundary = event => { if (event.method === "write") throw storeTestIOError(); };
    await rejected(base.checkpoint());
    check(!beforePoison.isCurrent(), "ambiguous write preserved a usable tree witness");
    let blocked = false; try { base.captureTreeEntries(); } catch { blocked = true; }
    check(blocked, "poisoned base allowed a new graph capture");
    const unloaded = new ObsetyncSyncBase({ vault: { adapter: new MemorySegmentedIO() } } as any);
    blocked = false; try { unloaded.captureTreeEntries(); } catch { blocked = true; }
    check(blocked, "unloaded base was inferred empty");
}

async function wholeCurrentGraphAndHonestParent() {
    const { base, io } = await fixture(257), budget = pool();
    // This models a persisted partial pull: semantic entries advanced while
    // ancestry intentionally remains the older completely known parent.
    base.setEntry("partial-page.md", hash("d"), 60, 6); await base.checkpoint(); io.events.length = 0;
    (base as any).allPaths = () => { throw new Error("root repair materialized allPaths"); };
    let yields = 0;
    const tree = fakeTree(json => {
        check(budget.snapshot().usedBytes === estimateRootTreeRepairWorkset(Buffer.byteLength(json)), "native input was not covered by the complete shared lease");
    });
    const result = await rebuildRootTreeFromBase(base, tree, 2, undefined, { budget, cooperate: async () => { yields++; } });
    check(result.rebuiltRoot === hash("c") && result.capturedBaseRoot === hash("b"), "semantic repair incorrectly advanced the honest parent");
    check(tree.calls === 1 && tree.tree_version() === 2 && tree.total_files() === 258, "repair did not make one complete native replacement");
    check(tree.rows[0].mtime_ms === 10 && tree.rows.at(-1)?.path === "partial-page.md", "repair lost current committed metadata");
    check(result.serializedBytes === Buffer.byteLength(JSON.stringify(tree.rows)), "reported JSON bound differs from actual input");
    check(yields === 2 && budget.snapshot().usedBytes === 0, "two bounded passes failed to yield or leaked their memory ownership");
    check(io.events.length === 0 && base.treeBaseRoot === hash("b"), "tree repair wrote persistence or fabricated ancestry");
    const empty = await fixture(0); empty.base.setTreeBaseRoot(null);
    const emptyTree = fakeTree();
    const emptyResult = await rebuildRootTreeFromBase(empty.base, emptyTree, 1, undefined, { budget, cooperate });
    check(emptyResult.entryCount === 0 && emptyResult.serializedBytes === 2 && emptyResult.capturedBaseRoot === null && emptyTree.calls === 1,
        "proven empty base did not rebuild an actual empty graph");
}

async function driftAndCancellationBeforeNative() {
    for (const mode of ["first-yield", "second-yield", "after-native", "cancel-first-yield", "cancel-after-native"] as const) {
        const { base } = await fixture(256), budget = pool(), controller = new AbortController(); let yields = 0;
        const tree = fakeTree(() => {
            if (mode === "after-native") base.setEntry("newer.md", hash("e"), 1, 1);
            if (mode === "cancel-after-native") controller.abort();
        });
        await rejected(rebuildRootTreeFromBase(base, tree, 1, controller.signal, { budget, cooperate: async () => {
            yields++;
            if (mode === "first-yield" && yields === 1 || mode === "second-yield" && yields === 2) base.removeEntry("dir/note-00000.md");
            if (mode === "cancel-first-yield") controller.abort();
        } }), mode.startsWith("cancel") ? undefined : "BASE_CHANGED");
        check(tree.calls === (mode.endsWith("after-native") || mode === "after-native" ? 1 : 0), "stale/aborted preparation entered native rebuild");
        check(budget.snapshot().usedBytes === 0, "drift/cancellation released ownership before completion or leaked it");
    }
    const { base } = await fixture(), budget = pool(), controller = new AbortController(); controller.abort();
    const tree = fakeTree();
    await rejected(rebuildRootTreeFromBase(base, tree, 1, controller.signal, { budget, cooperate }));
    check(tree.calls === 0 && budget.snapshot().peakUsedBytes === 0, "already aborted repair admitted work");
}

async function queuedAdmissionNeverMaterializesEarly() {
    for (const mode of ["drift", "cancel", "shrink", "success"] as const) {
        const { base } = await fixture(), budget = pool(), controller = new AbortController();
        const queued = gate(); let blocker: Awaited<ReturnType<ResourceBudget["reserve"]>> | undefined;
        let admissions = 0, iterations = 0;
        const port = { captureTreeEntries() {
            const captured = base.captureTreeEntries();
            return { ...captured, entries() { iterations++; return captured.entries(); } };
        } };
        const tree = fakeTree();
        const work = rebuildRootTreeFromBase(port, tree, 1, controller.signal, { cooperate,
            budget: { reserve: async (bytes, options) => {
                if (++admissions === 2) {
                    blocker = await budget.reserve(budget.snapshot().capacityBytes);
                    const waiting = budget.reserve(bytes, options); queued.resolve(); return waiting;
                }
                return budget.reserve(bytes, options);
            } } });
        const outcome = work.then(() => null, error => error);
        await queued.promise;
        check(iterations === 1 && tree.calls === 0 && budget.snapshot().queuedRequests === 1, "second-pass metadata was allocated before admission");
        if (mode === "drift") base.removeEntry("dir/note-00000.md");
        if (mode === "cancel") controller.abort();
        if (mode === "shrink") budget.setCapacity(ROOT_TREE_REPAIR_LIMITS.scratchBytes);
        blocker!.release(); const error = await outcome;
        if (mode === "success") check(error === null && iterations === 2 && tree.calls === 1, "complete workset nested admission deadlocked or failed");
        else check(error !== null && tree.calls === 0 && iterations === 1, "queued stale/aborted/oversized work reached the allocating pass");
        check(budget.snapshot().usedBytes === 0 && budget.snapshot().queuedRequests === 0, "queued failure leaked memory ownership");
    }
}

async function boundsAndAtomicFailure() {
    const budget = pool(), row = { path: "a.md", hash: hash("a"), mtime_ms: 1, size: 1 };
    let traversed = 0;
    const tooMany = syntheticBase([], ROOT_TREE_REPAIR_LIMITS.entries + 1, { iteration: () => { traversed++; } });
    const tree = fakeTree(); await rejected(rebuildRootTreeFromBase(tooMany, tree, 1, undefined, { budget, cooperate }), "LIMIT");
    check(traversed === 0 && budget.snapshot().peakUsedBytes === 0, "oversized count traversed or allocated the backlog");
    const longRows = Array.from({ length: 2200 }, (_, index) => ({ ...row, path: `${index.toString().padStart(4, "0")}-${"x".repeat(4091)}` }));
    let passes = 0;
    await rejected(rebuildRootTreeFromBase(syntheticBase(longRows, longRows.length, { iteration: () => { passes++; } }),
        tree, 1, undefined, { budget, cooperate }), "LIMIT");
    check(passes === 1 && tree.calls === 0 && budget.snapshot().usedBytes === 0, "oversized serialized JSON was fully materialized or published");
    for (const bad of [ { ...row, mtime_ms: 0.5 }, { ...row, mtime_ms: -1 }, { ...row, hash: "invalid" },
        { ...row, size: Number.MAX_SAFE_INTEGER + 1 }, { ...row, path: "../unsafe.md" } ]) {
        await rejected(rebuildRootTreeFromBase(syntheticBase([bad]), tree, 1, undefined, { budget, cooperate }), "INVALID_BASE");
    }
    await rejected(rebuildRootTreeFromBase(syntheticBase([row], 0), tree, 1, undefined, { budget, cooperate }), "INVALID_BASE");
    await rejected(rebuildRootTreeFromBase(syntheticBase([], 1), tree, 1, undefined, { budget, cooperate }), "INVALID_BASE");
    await rejected(rebuildRootTreeFromBase(syntheticBase([row, row]), tree, 1, undefined, { budget, cooperate }), "INVALID_BASE");
    await rejected(rebuildRootTreeFromBase(syntheticBase([row]), tree, 3, undefined, { budget, cooperate }), "INVALID_TREE");
    const tiny = pool(ROOT_TREE_REPAIR_LIMITS.scratchBytes);
    check(await rejected(rebuildRootTreeFromBase(syntheticBase([row]), tree, 1, undefined, { budget: tiny, cooperate })) instanceof ResourceBudgetOversizedError,
        "budget oversize was hidden as an empty graph");
    check(tree.calls === 0 && tiny.snapshot().usedBytes === 0, "oversized workset entered native code or leaked the counting lease");
    const failed = fakeTree(() => { throw new Error("injected atomic native failure"); });
    await rejected(rebuildRootTreeFromBase(syntheticBase([row]), failed, 2, undefined, { budget, cooperate }));
    check(failed.root_hash_hex() === hash("0") && failed.tree_version() === 1 && failed.total_files() === 1 && budget.snapshot().usedBytes === 0,
        "native failure replaced the previous tree or leaked input admission");
    const texts = ["quote\".md", "é-😀.md", "isolated-\ud800.md"].sort();
    const unicodeTree = fakeTree(); const unicode = await rebuildRootTreeFromBase(syntheticBase(texts.map(path => ({ ...row, path }))),
        unicodeTree, 1, undefined, { budget, cooperate });
    check(unicode.serializedBytes === Buffer.byteLength(JSON.stringify(unicodeTree.rows)), "Unicode/JSON escaping undercounted native input");
}

async function queuedPublicationInvalidatesCapture() {
    const { base } = await fixture(), captured = base.captureTreeEntries();
    const work = base.commitRootPublication({ identity: { scopeHash: hash("1"), sequence: 1, mutationId: "2".repeat(32), requestHash: hash("3") },
        candidateRoot: hash("4"), committedAt: 1, entries: [{ action: "upsert", path: "published.md", hash: hash("5"), mtime: 1, size: 1 }] });
    check(!captured.isCurrent(), "queued root publication did not invalidate old capture in its submission turn");
    let blocked = false; try { base.captureTreeEntries(); } catch { blocked = true; }
    check(blocked, "unverified root publication allowed capture of its earlier visible cut");
    await work;
    check(base.captureTreeEntries().entryCount === 4 && !captured.isCurrent(), "verified publication did not expose the complete newer base cut");
}

async function packagedWasmGraphClosure() {
    // Optional additional qualification: source-only JS CI runs before its
    // WASM build. When packaged artifacts exist, exercise the ACTUAL atomic
    // implementation and root-only missing-child failure, not a fake graph.
    let glue: string, binary: Buffer;
    try {
        glue = await readFile(join(process.cwd(), "wasm", "sync_core.js"), "utf8");
        binary = await readFile(join(process.cwd(), "wasm", "sync_core_bg.wasm"));
    } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        console.log("root-tree-repair: packaged WASM qualification skipped (source-only checkout)"); return;
    }
    const importRuntime = new Function("specifier", "return import(specifier)") as (specifier: string) => Promise<any>;
    const wasm = await importRuntime(`data:text/javascript;base64,${Buffer.from(glue).toString("base64")}`);
    wasm.initSync({ module: binary });
    const { base, io } = await fixture(300), budget = pool();
    for (const version of [1, 2]) {
        const tree = new wasm.WasmTree("tree-repair-test", "device"), rootOnly = new wasm.WasmTree("tree-repair-test", "device");
        try {
            const result = await rebuildRootTreeFromBase(base, tree, version, undefined, { budget, cooperate });
            check(result.entryCount === 300 && tree.total_files() === 300 && result.capturedBaseRoot === hash("b"), "packaged native rebuild lost base entries/ancestry");
            const hashes: string[] = wasm.wasm_tree_committed_chunk_hashes(tree);
            check(hashes.length > 0 && hashes.every(value => wasm.wasm_tree_get_chunk(tree, value) !== undefined), "packaged graph omitted resident immutable children");
            rootOnly.load_root(tree.root_bytes());
            let missingChildren = false; try { rootOnly.begin_candidate(); } catch { missingChildren = true; }
            check(missingChildren, "fixture failed to reproduce root-only missing-child graph");
            await rebuildRootTreeFromBase(base, rootOnly, version, undefined, { budget, cooperate });
            rootOnly.begin_candidate(); check(rootOnly.has_candidate(), "rebuilt graph could not create a complete candidate"); rootOnly.abort_candidate();
            const rootBefore = rootOnly.root_hash_hex();
            let invalidRejected = false;
            try { rootOnly.rebuild_from_entries_in_version(version, '[{"path":"bad","hash":"bad","mtime_ms":0,"size":0}]'); }
            catch { invalidRejected = true; }
            check(invalidRejected && rootOnly.root_hash_hex() === rootBefore && rootOnly.total_files() === 300, "packaged failed rebuild published a partial replacement");
            rootOnly.begin_candidate(); rootOnly.abort_candidate();
        } finally { rootOnly.free(); tree.free(); }
    }
    check(io.events.length === 0 && budget.snapshot().usedBytes === 0, "packaged graph repair wrote base state or retained input admission");
    console.log("root-tree-repair: actual packaged scalar WASM v1/v2 closure qualification passed");
}

void (async () => {
    await capturesAreDetachedAndInvalidated(); await wholeCurrentGraphAndHonestParent(); await driftAndCancellationBeforeNative();
    await queuedAdmissionNeverMaterializesEarly(); await boundsAndAtomicFailure(); await queuedPublicationInvalidatesCapture();
    await packagedWasmGraphClosure();
    console.log(`root-tree-repair: ${assertions} assertions passed`);
})().catch(error => { console.error(error); process.exitCode = 1; });
