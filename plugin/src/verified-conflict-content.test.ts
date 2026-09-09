import { strict as assert } from "node:assert";
import { blake3 } from "@noble/hashes/blake3";
import type { OwnedObjects } from "./api";
import { BulkObjectKind } from "./bulk-codec";
import { ResourceBudget } from "./resource-budget";
import { estimateTransportWorkset, reserveTransientScope, type TransientWorkScope } from "./transient-memory";
import {
    CONFLICT_CHUNK_THRESHOLD, CONFLICT_MAX_CHUNK_BYTES, CONFLICT_MAX_CHUNKS,
    CONFLICT_MAX_MANIFEST_BYTES, verifyConflictContent, type VerifyConflictContentOptions,
} from "./verified-conflict-content";

// Instrumented portable BLAKE3 test double, not native WASM/Obsidian evidence.
// The real admission scope is used; the API port only returns synthetic data.
const hash = (data: Uint8Array) => Buffer.from(blake3(data)).toString("hex");
const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
const raw = (text: string) => new TextEncoder().encode(text);
const MIB = 1024 * 1024;
function bytes(size: number, seed = 1): Uint8Array {
    const data = new Uint8Array(size);
    for (let index = 0; index < size; index++) data[index] = (index * 13 + seed * 31 + (index >>> 8)) & 255;
    return data;
}
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const rejection = (promise: Promise<unknown>) => promise.then(
    () => { throw new Error("expected verifier to fail"); }, (error: unknown) => error,
);
async function turns() { for (let count = 0; count < 20; count++) await Promise.resolve(); }

function fixture() {
    const objects = new Map<string, Uint8Array>();
    const budget = new ResourceBudget({ capacityBytes: 32 * MIB });
    const calls: { kind: BulkObjectKind; hash: string }[] = [];
    const owners: TransientWorkScope[] = [];
    const states: { updates: number[]; frees: number; finalized: number }[] = [];
    const emitted: { data: Uint8Array; offset: number }[] = [];
    let active = 0, releases = 0, yields = 0, batches = 0;
    let afterOwned: ((owned: OwnedObjects) => Promise<void>) | undefined;
    let replaceMap: ((map: Map<string, Uint8Array>) => Map<string, Uint8Array>) | undefined;
    let hashFailure: "update" | "finalize" | undefined;
    let failHasherAt = -1;
    class Hasher {
        readonly state = { updates: [] as number[], frees: 0, finalized: 0 };
        private readonly inner = blake3.create({});
        constructor() {
            if (states.length === failHasherAt) throw new Error("hasher constructor failed");
            states.push(this.state);
        }
        update(data: Uint8Array) {
            assert.equal(this.state.frees, 0);
            this.state.updates.push(data.length);
            if (hashFailure === "update") throw new Error("hash update failed");
            this.inner.update(data);
        }
        finalize() {
            assert.equal(this.state.frees, 0);
            this.state.finalized++;
            if (hashFailure === "finalize") throw new Error("hash finalize failed");
            return Buffer.from(this.inner.digest()).toString("hex");
        }
        free() { assert.equal(++this.state.frees, 1, "WASM owner freed more than once"); this.inner.destroy(); }
    }
    const api: VerifyConflictContentOptions["api"] = {
        async getObjectsOwned(kind, hashes) {
            assert.equal(hashes.length, 1, "download was not a bounded single-object pack");
            assert.equal(active, 0, "new global admission while earlier download owner still retained");
            calls.push({ kind, hash: hashes[0] });
            const data = objects.get(`${kind}:${hashes[0]}`);
            const ownerBytes = data?.byteLength ?? 0;
            const workBytes = estimateTransportWorkset(ownerBytes);
            const memory = await reserveTransientScope({ ownerBytes, workBytes }, { budget });
            owners.push(memory); active++;
            let map = new Map<string, Uint8Array>(data ? [[hashes[0], data]] : []);
            map = replaceMap ? replaceMap(map) : map;
            let released = false;
            const owned: OwnedObjects = {
                objects: map, memory,
                release() {
                    assert.equal(released, false, "download released more than once");
                    released = true; releases++; active--; map.clear(); memory.close();
                },
            };
            await afterOwned?.(owned);
            return owned;
        },
    };
    const defaults = {
        api, wasm: { Hasher },
        async consume(data: Uint8Array, offset: number, memory: TransientWorkScope) {
            assert.equal(memory.snapshot().ownerClosed, false);
            assert.equal(memory.snapshot().work.usedBytes, 0, "hash child still reserved during consumer allocation");
            await memory.run(2 * data.length + 64 * 1024, async () => {
                // Fixture borrows for assertions only; do not retain API buffers.
                emitted.push({ data: data.slice(), offset });
            });
        },
        async yieldControl() { yields++; },
        async beforeBatch() { batches++; },
    };
    return {
        objects, budget, calls, owners, states, emitted, defaults,
        get active() { return active; }, get releases() { return releases; },
        get yields() { return yields; }, get batches() { return batches; },
        setAfterOwned(hook: typeof afterOwned) { afterOwned = hook; },
        setMap(hook: typeof replaceMap) { replaceMap = hook; },
        setHashFailure(value: typeof hashFailure) { hashFailure = value; },
        setConstructorFailure(index: number) { failHasherAt = index; },
        put(kind: BulkObjectKind, digest: string, data: Uint8Array) { objects.set(`${kind}:${digest}`, data); },
        run(expectedHash: string, expectedSize: number, options: Partial<VerifyConflictContentOptions> = {}) {
            return verifyConflictContent({ ...defaults, expectedHash, expectedSize, ...options });
        },
        drained() {
            assert.equal(active, 0); assert.equal(budget.snapshot().usedBytes, 0);
            assert.equal(budget.snapshot().queuedRequests, 0);
            assert.equal(releases, calls.length);
            for (const state of states) assert.equal(state.frees, 1, "WASM verifier leaked on a terminal path");
            for (const owner of owners) assert.equal(owner.snapshot().parentReleased, true);
        },
    };
}
type Fixture = ReturnType<typeof fixture>;
function installLarge(f: Fixture, parts: Uint8Array[], expectedHash?: string) {
    const whole = blake3.create({});
    for (const part of parts) whole.update(part);
    const actualHash = Buffer.from(whole.digest()).toString("hex"); whole.destroy();
    const fileHash = expectedHash ?? actualHash;
    let offset = 0;
    const chunks = parts.map(part => {
        const row = { hash: hash(part), offset, size: part.length }; offset += part.length;
        f.put(BulkObjectKind.ContentChunk, row.hash, part); return row;
    });
    const manifest = { file_hash: fileHash, total_size: offset, chunks };
    f.put(BulkObjectKind.Manifest, fileHash, encode(manifest));
    return { fileHash, actualHash, size: offset, manifest };
}

async function validThresholdsAndFeedBounds() {
    for (const length of [0, 1, 64 * 1024, CONFLICT_CHUNK_THRESHOLD - 1]) {
        const f = fixture(), data = bytes(length), digest = hash(data);
        f.put(BulkObjectKind.Content, digest, data);
        assert.deepEqual(await f.run(digest, length), { hash: digest, size: length });
        assert.deepEqual(f.calls, [{ kind: BulkObjectKind.Content, hash: digest }]);
        assert.equal(f.emitted.length, 1); assert.equal(f.emitted[0].offset, 0);
        assert.deepEqual(f.emitted[0].data, data);
        assert.equal(f.states.length, 1); assert.equal(f.states[0].finalized, 1);
        assert(f.states[0].updates.every(size => size <= 64 * 1024));
        assert.equal(f.yields, Math.max(0, Math.ceil(length / (64 * 1024)) - 1));
        f.drained();
    }
    for (const parts of [[bytes(MIB / 2), bytes(MIB / 2, 2)], [bytes(CONFLICT_MAX_CHUNK_BYTES)]]) {
        const f = fixture(), installed = installLarge(f, parts);
        assert.deepEqual(await f.run(installed.fileHash, installed.size), { hash: installed.fileHash, size: installed.size });
        assert.equal(f.calls[0].kind, BulkObjectKind.Manifest);
        assert.equal(f.calls.length, parts.length + 1); assert.equal(f.batches, f.calls.length);
        assert.deepEqual(f.emitted.map(row => row.offset), installed.manifest.chunks.map(row => row.offset));
        f.emitted.forEach((row, index) => assert.deepEqual(row.data, parts[index]));
        assert.equal(f.states.length, parts.length + 1);
        for (const state of f.states) assert(state.updates.every(size => size <= 64 * 1024));
        assert(f.yields >= installed.size / (64 * 1024) - parts.length);
        f.drained();
    }
}

async function invalidIdentityAndObjects() {
    for (const size of [undefined, -0, -1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER, CONFLICT_MAX_CHUNKS * CONFLICT_MAX_CHUNK_BYTES + 1]) {
        const f = fixture();
        await assert.rejects(f.run("a".repeat(64), size as number), /exact size/);
        assert.equal(f.calls.length, 0); assert.equal(f.states.length, 0); f.drained();
    }
    for (const digest of ["", "A".repeat(64), "a".repeat(63), "a".repeat(65), "z".repeat(64)]) {
        const f = fixture(); await assert.rejects(f.run(digest, 0), /exact size/); f.drained();
    }
    for (const mode of ["missing", "foreign", "extra", "not-bytes", "size", "hash", "empty-hash"] as const) {
        const f = fixture(), data = mode === "empty-hash" ? bytes(0) : bytes(10);
        const digest = mode === "hash" || mode === "empty-hash" ? "a".repeat(64) : hash(data);
        if (mode !== "missing") f.put(BulkObjectKind.Content, digest, data);
        if (mode === "foreign") f.setMap(() => new Map([["b".repeat(64), data]]));
        if (mode === "extra") f.setMap(map => new Map([...map, ["b".repeat(64), data]]));
        if (mode === "not-bytes") f.setMap(() => new Map([[digest, [] as unknown as Uint8Array]]));
        await assert.rejects(f.run(digest, data.length + (mode === "size" ? 1 : 0)), /verification:/);
        assert.equal(f.emitted.length, 0, `${mode} emitted unverified bytes`); f.drained();
    }
}

async function malformedManifests() {
    const part = bytes(MIB), digest = hash(part), chunkHash = hash(part);
    const base = { file_hash: digest, total_size: MIB, chunks: [{ hash: chunkHash, offset: 0, size: MIB }] };
    const bad: Uint8Array[] = [
        bytes(0), new Uint8Array(CONFLICT_MAX_MANIFEST_BYTES + 1), new Uint8Array([0xff]),
        raw("{}"), raw("[]"), raw("null"), raw(JSON.stringify(base) + " false"),
        raw(JSON.stringify(base).replace('"chunks":', '"chunks":[],"chunks":')),
        raw(JSON.stringify(base).replace('"size":', '"size":1,"size":')),
        raw(JSON.stringify(base).replace('"offset":0', '"offset":00')),
        raw(JSON.stringify(base).replace('"offset":0', '"offset":0.0')),
        raw(JSON.stringify(base).replace('"offset":0', '"offset":-0')),
        raw(JSON.stringify(base).replace('"size":1048576', '"size":1e6')),
        raw(JSON.stringify(base).replace('"hash":', '"hash":{} ,"ignore":')),
        encode({ ...base, file_hash: "b".repeat(64) }), encode({ ...base, total_size: 0 }),
        encode({ ...base, chunks: [] }), encode({ ...base, extra: true }),
        encode({ ...base, chunks: [{ ...base.chunks[0], extra: true }] }),
        encode({ ...base, chunks: [{ ...base.chunks[0], hash: chunkHash.toUpperCase() }] }),
        encode({ ...base, chunks: [{ ...base.chunks[0], offset: 1 }] }),
        encode({ ...base, chunks: [{ ...base.chunks[0], size: 0 }] }),
        encode({ ...base, chunks: [{ ...base.chunks[0], size: MIB - 1 }] }),
        encode({ ...base, chunks: [{ ...base.chunks[0], size: CONFLICT_MAX_CHUNK_BYTES + 1 }] }),
        encode({ ...base, chunks: [{ hash: chunkHash, offset: 0, size: MIB / 2 }, { hash: chunkHash, offset: 0, size: MIB / 2 }] }),
        raw(JSON.stringify(base).replace('"chunks":[', '"chunks":[[')),
        raw(JSON.stringify(base).slice(0, -1)), raw("\ufeff" + JSON.stringify(base)),
        raw(JSON.stringify(base).replace('"total_size"', '"total_size":1048576,"\\u0074otal_size"')),
    ];
    for (let index = 0; index < bad.length; index++) {
        const f = fixture(); f.put(BulkObjectKind.Manifest, digest, bad[index]);
        await assert.rejects(f.run(digest, MIB), /verification:/, `invalid manifest ${index}`);
        assert.equal(f.calls.length, 1); assert.equal(f.emitted.length, 0); f.drained();
    }
    // Reordered/escaped schema strings are valid JSON and preserve exact data.
    const f = fixture();
    f.put(BulkObjectKind.Manifest, digest, raw(` { "chunks": [{"size":${MIB},"offset":0,"ha\\u0073h":"${chunkHash}"}], "total_size":${MIB}, "file_hash":"${digest}" } `));
    f.put(BulkObjectKind.ContentChunk, chunkHash, part);
    await f.run(digest, MIB); assert.equal(f.emitted.length, 1); f.drained();
}

async function metadataCountBoundAndYield() {
    const digest = "a".repeat(64), chunkHash = "b".repeat(64);
    for (const count of [CONFLICT_MAX_CHUNKS, CONFLICT_MAX_CHUNKS + 1]) {
        const f = fixture();
        const data = encode({ file_hash: digest, total_size: count * 64,
            chunks: Array.from({ length: count }, (_, index) => ({ hash: chunkHash, offset: index * 64, size: 64 })) });
        assert(data.length < CONFLICT_MAX_MANIFEST_BYTES, "fixture must exercise count, not raw byte cap");
        f.put(BulkObjectKind.Manifest, digest, data);
        const stop = new Error("stop before synthetic chunk download");
        const failure = await rejection(f.run(digest, count * 64, { beforeBatch: async () => {
            if (f.calls.length) throw stop;
        } }));
        if (count === CONFLICT_MAX_CHUNKS) assert.equal(failure, stop, "legal metadata count rejected");
        else assert.match(String(failure), /chunk limit/);
        assert.equal(f.yields, CONFLICT_MAX_CHUNKS / 256, "metadata parser monopolized the renderer");
        assert.equal(f.calls.length, 1); f.drained();
    }
}

async function individuallyValidIsNotWholeFileProof() {
    const f = fixture();
    const installed = installLarge(f, [bytes(MIB / 2), bytes(MIB / 2, 2)], "a".repeat(64));
    assert.notEqual(installed.actualHash, installed.fileHash);
    await assert.rejects(f.run(installed.fileHash, installed.size), /whole concatenation/);
    assert.equal(f.emitted.length, 2, "individually verified staging data unexpectedly omitted");
    assert(f.states.every(state => state.finalized === 1)); f.drained();
    for (const mode of ["missing", "wrong-hash", "wrong-size"] as const) {
        const g = fixture(), item = installLarge(g, [bytes(MIB / 2), bytes(MIB / 2, 3)]);
        const second = item.manifest.chunks[1];
        if (mode === "missing") g.objects.delete(`${BulkObjectKind.ContentChunk}:${second.hash}`);
        else g.put(BulkObjectKind.ContentChunk, second.hash, bytes(second.size + (mode === "wrong-size" ? 1 : 0), 44));
        await assert.rejects(g.run(item.fileHash, item.size), /verification:/);
        assert.equal(g.emitted.length, 1, `${mode} chunk reached consumer`); g.drained();
    }
}

async function actualConsumerOwnershipAndCancellation() {
    for (const cancel of [false, true]) {
        const f = fixture(), data = bytes(128 * 1024), digest = hash(data);
        f.put(BulkObjectKind.Content, digest, data);
        const entered = deferred<void>(), native = deferred<void>(), controller = new AbortController();
        const failure = new Error(cancel ? "consumer cancelled" : "native write failed");
        let settled = false;
        const result = rejection(f.run(digest, data.length, {
            signal: controller.signal,
            consume: async (view, offset, memory) => {
                assert.equal(view, data); assert.equal(offset, 0);
                await memory.run(2 * data.length, async context => {
                    entered.resolve(); await context.track(() => native.promise);
                });
            },
        })).then(error => { settled = true; return error; });
        await entered.promise;
        if (cancel) controller.abort(failure);
        f.budget.close();
        await turns();
        assert.equal(settled, false, "abort/close impersonated actual consumer completion");
        assert(f.budget.snapshot().usedBytes > 0); assert.equal(f.active, 1);
        assert.equal(f.states[0].frees, 0); assert(f.owners[0].snapshot().work.usedBytes > 0);
        if (cancel) native.resolve(); else native.reject(failure);
        assert.equal(await result, failure); f.drained();
    }
    const f = fixture(), item = installLarge(f, [bytes(MIB / 2), bytes(MIB / 2, 5)]);
    const failed = new Error("first chunk destination write failed");
    assert.equal(await rejection(f.run(item.fileHash, item.size, { consume: async () => { throw failed; } })), failed);
    assert.equal(f.calls.length, 2, "continued downloading after failed stage write"); f.drained();
}

async function failedConsumerRetainsAlreadyStartedNativeTask() {
    const f = fixture(), data = bytes(100), digest = hash(data);
    f.put(BulkObjectKind.Content, digest, data);
    const native = deferred<void>(), failed = new Error("consumer failed after dispatch");
    let actualTask: Promise<void> | undefined;
    const error = await rejection(f.run(digest, data.length, {
        consume: async (_view, _offset, memory) => memory.run(256, context => {
            // A synchronous downstream error is possible after dispatch. The
            // native owner must still be explicitly joined/accounted by scope.
            actualTask = context.track(() => native.promise);
            throw failed;
        }),
    }));
    assert.equal(error, failed);
    assert.equal(f.active, 0); assert.equal(f.releases, 1);
    assert.equal(f.states[0].frees, 1, "unused verifier was not freed on consumer error");
    assert.equal(f.owners[0].snapshot().ownerClosed, true);
    assert.equal(f.owners[0].snapshot().parentReleased, false);
    assert.equal(f.owners[0].snapshot().work.usedBytes, 256);
    assert(f.budget.snapshot().usedBytes > 0, "consumer error freed still-running native IO buffers");
    native.resolve(); await actualTask; await turns(); f.drained();
}

async function downloadAndFeedCancellation() {
    const f = fixture(), data = bytes(20), digest = hash(data);
    f.put(BulkObjectKind.Content, digest, data);
    const ready = deferred<void>(), native = deferred<void>(), controller = new AbortController();
    f.setAfterOwned(async () => { ready.resolve(); await native.promise; });
    let settled = false;
    const result = rejection(f.run(digest, data.length, { signal: controller.signal })).then(error => { settled = true; return error; });
    await ready.promise; const stop = new Error("download cancelled"); controller.abort(stop);
    await turns(); assert.equal(settled, false); assert.equal(f.active, 1); assert.equal(f.states[0].frees, 0);
    native.resolve(); assert.equal(await result, stop); assert.equal(f.emitted.length, 0); f.drained();

    const g = fixture(), item = installLarge(g, [bytes(CONFLICT_MAX_CHUNK_BYTES)]);
    const feedController = new AbortController(), feedStop = new Error("feed cancelled");
    assert.equal(await rejection(g.run(item.fileHash, item.size, {
        signal: feedController.signal, yieldControl: async () => feedController.abort(feedStop),
    })), feedStop);
    assert.equal(g.states[0].updates.length, 1); assert.equal(g.states[1].updates.length, 1);
    assert.equal(g.emitted.length, 0); g.drained();
    const h = fixture(), early = new AbortController(); early.abort(stop);
    assert.equal(await rejection(h.run(hash(bytes(0)), 0, { signal: early.signal })), stop);
    assert.equal(h.calls.length, 0); assert.equal(h.states.length, 0); h.drained();

    const metadata = fixture(), metadataController = new AbortController();
    const metaHash = "a".repeat(64), count = 256;
    metadata.put(BulkObjectKind.Manifest, metaHash, encode({ file_hash: metaHash, total_size: MIB,
        chunks: Array.from({ length: count }, (_, index) => ({ hash: "b".repeat(64), offset: index * 4096, size: 4096 })) }));
    assert.equal(await rejection(metadata.run(metaHash, MIB, { signal: metadataController.signal,
        yieldControl: async () => metadataController.abort(stop) })), stop);
    assert.equal(metadata.calls.length, 1); assert.equal(metadata.emitted.length, 0); metadata.drained();
}

async function verifierAndSchedulerFailures() {
    for (const failure of ["update", "finalize"] as const) {
        const f = fixture(), item = installLarge(f, [bytes(MIB)]); f.setHashFailure(failure);
        await assert.rejects(f.run(item.fileHash, item.size), /hash .* failed/);
        assert.equal(f.emitted.length, 0); f.drained();
    }
    for (const at of [0, 1]) {
        const f = fixture(), item = installLarge(f, [bytes(MIB)]); f.setConstructorFailure(at);
        await assert.rejects(f.run(item.fileHash, item.size), /constructor failed/); f.drained();
    }
    for (const at of ["before", "yield"] as const) {
        const f = fixture(), item = installLarge(f, [bytes(MIB)]), stop = new Error("scheduler stopped");
        assert.equal(await rejection(f.run(item.fileHash, item.size, at === "before"
            ? { beforeBatch: async () => { throw stop; } } : { yieldControl: async () => { throw stop; } })), stop);
        assert.equal(f.emitted.length, 0); f.drained();
    }
}

async function run() {
    await validThresholdsAndFeedBounds();
    await invalidIdentityAndObjects();
    await malformedManifests();
    await metadataCountBoundAndYield();
    await individuallyValidIsNotWholeFileProof();
    await actualConsumerOwnershipAndCancellation();
    await failedConsumerRetainsAlreadyStartedNativeTask();
    await downloadAndFeedCancellation();
    await verifierAndSchedulerFailures();
    console.log("verified-conflict-content.test: 9 bounded suites passed (portable BLAKE3 test double; owned memory, no native host)");
}
void run().catch(error => { setTimeout(() => { throw error; }, 0); });
