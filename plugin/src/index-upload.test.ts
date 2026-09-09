import { strict as assert } from "node:assert";
import { reserveCompactIndexHashSpool, uploadIndexChunks } from "./index-upload";
import { ResourceBudgetOversizedError } from "./resource-budget";
import { reserveTransientWorkset, transientMemorySnapshot, type TransientWorkScope } from "./transient-memory";
import type { BulkUploadRecord } from "./bulk-codec";
import { configureHashTuning, getHashTuning } from "./hash-runtime";
import type { TreeChunkExportJobTree } from "./tree-chunk-export-job";

function gate(): { promise: Promise<void>; resolve(): void } {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
}

function pagedTree(sources: ReadonlyMap<string, Uint8Array>) {
    let active: { token: number; hash: string; bytes: Uint8Array; offset: number } | undefined;
    let serial = 0;
    const events: string[] = [];
    let pages = 0, finishes = 0, cancellations = 0;
    const tree: TreeChunkExportJobTree = {
        begin_tree_chunk_export_job(hash: string) {
            assert.ok(transientMemorySnapshot().activeReservations > 0, "export began before pack admission");
            assert.equal(active, undefined, "overlapping native export jobs");
            const bytes = sources.get(hash);
            assert.ok(bytes);
            active = { token: ++serial, hash, bytes, offset: 0 };
            events.push(`begin:${hash}`);
            return { token: serial, length: bytes.byteLength };
        },
        read_tree_chunk_export_job(token: number, offset: number, maxBytes: number) {
            assert.ok(transientMemorySnapshot().activeReservations > 0, "page copied before pack admission");
            assert.ok(active);
            assert.equal(token, active.token);
            assert.equal(offset, active.offset);
            assert.ok(Number.isInteger(maxBytes) && maxBytes > 0 && maxBytes <= 64 * 1024);
            const output = active.bytes.slice(offset, offset + maxBytes);
            active.offset += output.byteLength;
            pages++;
            events.push(`page:${active.hash}:${offset}`);
            return output;
        },
        finish_tree_chunk_export_job(token: number) {
            assert.ok(active);
            assert.equal(token, active.token);
            assert.equal(active.offset, active.bytes.byteLength);
            finishes++;
            events.push(`finish:${active.hash}`);
            active = undefined;
        },
        cancel_tree_job(token: number) {
            assert.ok(active);
            assert.equal(token, active.token);
            cancellations++;
            active = undefined;
        },
    };
    const wasm = {
        wasm_tree_chunk_byte_length: (_tree: unknown, hash: string) => sources.get(hash)?.byteLength,
        wasm_tree_get_chunk: (): Uint8Array | null => { throw new Error("paged tree fell back to whole export"); },
    };
    return { tree, wasm, events, snapshot: () => ({ pages, finishes, cancellations, active: !!active }) };
}

async function boundedIndexCopies(): Promise<void> {
    const hashes = Array.from({ length: 600 }, (_, i) => i.toString(16).padStart(64, "0"));
    let copied = 0;
    let acknowledged = 0;
    let packs = 0;
    let previous: TransientWorkScope | undefined;
    const wasm = {
        wasm_tree_chunk_byte_length: () => 4,
        wasm_tree_get_chunk: () => {
            assert.ok(transientMemorySnapshot().activeReservations > 0, "WASM copied index before admission");
            copied++;
            return new Uint8Array(4);
        },
    };
    const api = {
        putObjects: async (records: readonly BulkUploadRecord[], _perf: unknown, memory?: TransientWorkScope) => {
            assert.ok(memory);
            assert.ok(!memory.snapshot().ownerClosed);
            assert.ok(memory.snapshot().ownerBytes >= 2 * records.length * 4);
            assert.equal(copied - acknowledged, records.length, "all index bytes were copied ahead of bounded ACK");
            assert.deepEqual(records.map(record => record.hash), hashes.slice(acknowledged, copied));
            if (previous) assert.ok(previous.snapshot().parentReleased, "prior ACK scope was retained into next pack");
            previous = memory;
            await Promise.resolve();
            assert.ok(!memory.snapshot().parentReleased, "index scope released before actual ACK");
            acknowledged += records.length;
            packs++;
        },
    };
    await uploadIndexChunks(api, wasm, {}, hashes);
    assert.equal(copied, 600);
    assert.equal(acknowledged, 600);
    assert.ok(packs > 1, "index upload gathered the entire vault-sized copy array");
    assert.ok(previous?.snapshot().parentReleased);
}

async function rejectedIndexNeverAllocates(): Promise<void> {
    let copies = 0;
    let uploads = 0;
    const wasm = {
        wasm_tree_chunk_byte_length: (): number | undefined => 512 * 1024 * 1024,
        wasm_tree_get_chunk: () => { copies++; return new Uint8Array(1); },
    };
    const api = { putObjects: async () => { uploads++; } };
    await assert.rejects(uploadIndexChunks(api, wasm, {}, ["a".repeat(64)]), ResourceBudgetOversizedError);
    assert.equal(copies, 0);
    assert.equal(uploads, 0);
    wasm.wasm_tree_chunk_byte_length = () => undefined;
    await assert.rejects(uploadIndexChunks(api, wasm, {}, ["a".repeat(64)]), /absent/);
    assert.equal(copies, 0);
    wasm.wasm_tree_chunk_byte_length = () => 2;
    await assert.rejects(uploadIndexChunks(api, wasm, {}, ["a".repeat(64)]), /changed|length/);
    assert.equal(copies, 1);
    assert.equal(uploads, 0);
}

async function pagedExactBytesAndPackLifetime(): Promise<void> {
    const original = getHashTuning();
    configureHashTuning({ ...original, maxBatchFiles: 2 });
    const hashes = ["a".repeat(64), "b".repeat(64), "c".repeat(64)];
    const sources = new Map(hashes.map((hash, i) => [hash,
        Uint8Array.from({ length: i === 0 ? 2 * 64 * 1024 + 7 : 13 + i }, (_, n) => (n * 17 + i) % 256)]));
    const fixture = pagedTree(sources);
    const entered = gate(), ack = gate();
    const scopes: TransientWorkScope[] = [];
    let copiedAtFirstAck = 0, calls = 0, cooperation = 0;
    const api = { putObjects: async (records: readonly BulkUploadRecord[], _perf: unknown, memory?: TransientWorkScope) => {
        assert.ok(memory);
        scopes.push(memory);
        assert.ok(!memory.snapshot().ownerClosed);
        assert.deepEqual(records.map(record => record.hash), calls === 0 ? hashes.slice(0, 2) : hashes.slice(2));
        for (const record of records) assert.deepEqual(record.data, sources.get(record.hash));
        calls++;
        if (calls === 1) {
            copiedAtFirstAck = fixture.snapshot().pages;
            entered.resolve();
            await ack.promise;
            assert.equal(fixture.snapshot().pages, copiedAtFirstAck, "next pack read before real ACK");
            assert.ok(!memory.snapshot().parentReleased, "bytes released while ACK still pending");
        }
    } };
    try {
        const upload = uploadIndexChunks(api, fixture.wasm, fixture.tree, hashes, undefined,
            async () => { cooperation++; });
        await entered.promise;
        assert.equal(fixture.snapshot().finishes, 2);
        assert.equal(fixture.snapshot().pages, 4);
        assert.equal(calls, 1);
        assert.ok(transientMemorySnapshot().usedBytes > 0);
        // Pack start, two between-page turns and the between-object turn.
        assert.equal(cooperation, 4);
        ack.resolve();
        await upload;
        assert.equal(calls, 2);
        assert.equal(fixture.snapshot().pages, 5);
        assert.equal(fixture.snapshot().finishes, 3);
        assert.equal(fixture.snapshot().cancellations, 0);
        assert.equal(cooperation, 5);
        assert.ok(scopes.every(scope => scope.snapshot().parentReleased));
        assert.equal(transientMemorySnapshot().usedBytes, 0);
    } finally {
        ack.resolve();
        configureHashTuning(original);
    }
}

async function cancelBetweenPagesAndObjects(): Promise<void> {
    for (const pages of [true, false]) {
        const hashes = ["a".repeat(64), "b".repeat(64)];
        const sources = new Map(hashes.map(hash => [hash, new Uint8Array(pages ? 64 * 1024 + 1 : 3)]));
        const fixture = pagedTree(sources);
        const controller = new AbortController();
        const reason = new Error("cancel owned index export");
        let cooperation = 0, uploads = 0;
        const usedBefore = transientMemorySnapshot().usedBytes;
        await assert.rejects(uploadIndexChunks({ putObjects: async () => { uploads++; } },
            fixture.wasm, fixture.tree, hashes, undefined, async () => {
                if (++cooperation === 2) controller.abort(reason);
            }, controller.signal), error => error === reason);
        assert.equal(fixture.snapshot().pages, 1);
        assert.equal(fixture.snapshot().finishes, pages ? 0 : 1);
        assert.equal(fixture.snapshot().cancellations, pages ? 1 : 0);
        assert.equal(fixture.snapshot().active, false);
        assert.equal(uploads, 0);
        assert.equal(transientMemorySnapshot().usedBytes, usedBefore);
    }
}

async function queuedAdmissionAndOwnershipFence(): Promise<void> {
    const hash = "a".repeat(64);
    const fixture = pagedTree(new Map([[hash, new Uint8Array(5)]]));
    const blocker = await reserveTransientWorkset(transientMemorySnapshot().capacityBytes);
    const requested = gate();
    const originalLength = fixture.wasm.wasm_tree_chunk_byte_length;
    fixture.wasm.wasm_tree_chunk_byte_length = (tree, value) => {
        requested.resolve();
        return originalLength(tree, value);
    };
    let current = true, uploads = 0;
    const stale = new Error("index owner changed during admission");
    const result = uploadIndexChunks({ putObjects: async () => { uploads++; } }, fixture.wasm,
        fixture.tree, [hash], undefined, undefined, undefined, () => { if (!current) throw stale; });
    const rejected = assert.rejects(result, error => error === stale);
    try {
        await requested.promise;
        assert.equal(fixture.events.length, 0, "native begin happened while reservation queued");
        assert.equal(transientMemorySnapshot().queuedRequests, 1);
        current = false;
        blocker.release();
        await rejected;
        assert.equal(fixture.events.length, 0);
        assert.equal(uploads, 0);
        assert.equal(transientMemorySnapshot().usedBytes, 0);
    } finally { blocker.release(); }
}

async function cancelledAckStillOwnsBytes(): Promise<void> {
    const original = getHashTuning();
    configureHashTuning({ ...original, maxBatchFiles: 1 });
    const hashes = ["a".repeat(64), "b".repeat(64)];
    const fixture = pagedTree(new Map(hashes.map(hash => [hash, new Uint8Array(2)])));
    const controller = new AbortController(), entered = gate(), ack = gate();
    const reason = new Error("stopped during native ACK");
    let scope: TransientWorkScope | undefined, calls = 0, returned = false;
    const upload = uploadIndexChunks({ putObjects: async (_records, _perf, memory) => {
        scope = memory;
        calls++;
        entered.resolve();
        await ack.promise;
    } }, fixture.wasm, fixture.tree, hashes, undefined, undefined, controller.signal);
    const outcome = assert.rejects(upload, error => error === reason).finally(() => { returned = true; });
    try {
        await entered.promise;
        controller.abort(reason);
        await Promise.resolve();
        assert.equal(returned, false, "abort raced actual ACK completion");
        assert.ok(scope && !scope.snapshot().ownerClosed);
        assert.ok(transientMemorySnapshot().usedBytes > 0);
        assert.equal(fixture.snapshot().finishes, 1);
        ack.resolve();
        await outcome;
        assert.equal(calls, 1);
        assert.equal(fixture.snapshot().finishes, 1, "new pack exported after cancellation");
        assert.ok(scope.snapshot().parentReleased);
        assert.equal(transientMemorySnapshot().usedBytes, 0);
    } finally { ack.resolve(); configureHashTuning(original); }
}

async function partialApiAndDetachedHashSlots(): Promise<void> {
    const hash = "a".repeat(64), changed = "b".repeat(64);
    let copied = 0, uploaded = 0;
    const wasm = { wasm_tree_chunk_byte_length: () => 1,
        wasm_tree_get_chunk: () => { copied++; return new Uint8Array([7]); } };
    await assert.rejects(uploadIndexChunks({ putObjects: async () => { uploaded++; } }, wasm,
        { begin_tree_chunk_export_job: () => ({ token: 1, length: 1 }) }, [hash]), /incomplete/);
    assert.equal(copied, 0);
    assert.equal(uploaded, 0);
    assert.equal(transientMemorySnapshot().usedBytes, 0);

    const hashes = [hash];
    const bytes = new Uint8Array(64 * 1024 + 3);
    const fixture = pagedTree(new Map([[hash, bytes]]));
    let turns = 0;
    await uploadIndexChunks({ putObjects: async records => {
        assert.equal(records.length, 1);
        assert.equal(records[0].hash, hash, "mutable caller hash paired with old exported bytes");
        assert.deepEqual(records[0].data, bytes);
        uploaded++;
    } }, fixture.wasm, fixture.tree, hashes, undefined, async () => {
        if (++turns === 2) hashes[0] = changed;
    });
    assert.equal(uploaded, 1);
    assert.equal(fixture.snapshot().finishes, 1);
}

async function failedNativeOrAckReleasesOnlyOwnedScope(): Promise<void> {
    const hash = "a".repeat(64);
    for (const failureAt of ["read", "finish", "ack"] as const) {
        const fixture = pagedTree(new Map([[hash, new Uint8Array(3)]]));
        const failure = new Error(`failed owned ${failureAt}`);
        let scope: TransientWorkScope | undefined, uploads = 0;
        if (failureAt === "read") fixture.tree.read_tree_chunk_export_job = () => { throw failure; };
        if (failureAt === "finish") fixture.tree.finish_tree_chunk_export_job = () => { throw failure; };
        await assert.rejects(uploadIndexChunks({ putObjects: async (_records, _perf, memory) => {
            uploads++;
            scope = memory;
            assert.ok(scope && !scope.snapshot().parentReleased);
            await Promise.resolve();
            throw failure;
        } }, fixture.wasm, fixture.tree, [hash]), error => error === failure);
        assert.equal(uploads, failureAt === "ack" ? 1 : 0);
        assert.equal(fixture.snapshot().cancellations, failureAt === "ack" ? 0 : 1);
        assert.equal(fixture.snapshot().active, false);
        if (scope) assert.ok(scope.snapshot().parentReleased);
        assert.equal(transientMemorySnapshot().usedBytes, 0);
    }

    const fixture = pagedTree(new Map([[hash, new Uint8Array(1)]]));
    fixture.wasm.wasm_tree_chunk_byte_length = () => 512 * 1024 * 1024;
    await assert.rejects(uploadIndexChunks({ putObjects: async () => { assert.fail("oversized export reached upload"); } },
        fixture.wasm, fixture.tree, [hash]), ResourceBudgetOversizedError);
    assert.deepEqual(fixture.events, [], "oversized export began a native job before refusal");
    assert.equal(transientMemorySnapshot().usedBytes, 0);
}

async function compactSpoolAdmissionAndLazyUpload(): Promise<void> {
    const hashes = ["0".repeat(64), "1".repeat(64), "f".repeat(64)];
    const sources = new Map(hashes.map((hash, index) => [hash, new Uint8Array([index + 1])]));
    const before = transientMemorySnapshot().usedBytes;
    const admitted = await reserveCompactIndexHashSpool(hashes.length, hashes.length * 64);
    assert.equal(admitted.spoolBytes, 96);
    assert.equal(admitted.peakAdmissionBytes, 288);
    assert.equal(transientMemorySnapshot().usedBytes, before + 288,
        "sort and compact spool were not admitted atomically");
    for (const hash of hashes) admitted.spool.append(hash);
    assert.throws(() => admitted.spool.hashAt(0), /sealed/);
    admitted.coOwner.release();
    admitted.coOwner.release();
    assert.equal(transientMemorySnapshot().usedBytes, before + 96,
        "retired sort workspace stayed charged beside the compact spool");
    admitted.spool.seal();
    assert.deepEqual(hashes.map((_, index) => admitted.spool.hashAt(index)), hashes);
    const fixture = pagedTree(sources);
    let uploads = 0;
    try {
        await uploadIndexChunks({ putObjects: async records => {
            uploads++;
            assert.deepEqual(records.map(record => record.hash), hashes);
            assert.ok(transientMemorySnapshot().usedBytes > before + admitted.spoolBytes,
                "compact backing was released before the real upload ACK");
        } }, fixture.wasm, fixture.tree, admitted.spool, undefined, undefined, undefined, undefined,
        transientMemorySnapshot().capacityBytes - admitted.spoolBytes);
        assert.equal(uploads, 1);
        assert.equal(transientMemorySnapshot().usedBytes, before + admitted.spoolBytes,
            "upload pack leaked beside the compact backing");
    } finally {
        admitted.spool.close();
    }
    assert.equal(transientMemorySnapshot().usedBytes, before);
    assert.throws(() => admitted.spool.hashAt(0), /sealed|closed/);
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed) { console.error("index-upload.test: async suite did not complete"); process.exitCode = 1; }
});
void boundedIndexCopies().then(rejectedIndexNeverAllocates)
    .then(pagedExactBytesAndPackLifetime)
    .then(cancelBetweenPagesAndObjects)
    .then(queuedAdmissionAndOwnershipFence)
    .then(cancelledAckStillOwnsBytes)
    .then(partialApiAndDetachedHashSlots)
    .then(failedNativeOrAckReleasesOnlyOwnedScope)
    .then(compactSpoolAdmissionAndLazyUpload)
    .then(() => {
        completed = true;
        console.log("index-upload.test: 9 suites passed (paged bytes, compact spool admission, ACK ownership, cancellation and legacy safety)");
    })
    .catch(error => { completed = true; console.error(error); process.exitCode = 1; });
