import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { MAX_FASTCDC_CHUNK_BYTES, type HashWorkerHashResult, type HashWorkerManifestResult } from "./hash-worker-protocol";
import { PreparedDesktopManifestError, preparedManifestCloneBytes, resolvePreparedDesktopManifest,
    type PreparedDesktopManifestOptions, type PreparedDesktopManifestStage } from "./prepared-desktop-manifest";
import { SyncMemoryArbiter } from "./sync-memory-arbiter";

// Abstract worker fixture: SHA-256 supplies deterministic distinct identifiers;
// these tests validate orchestration, not production BLAKE3/FastCDC parity.
const hashBytes = (value: string) => createHash("sha256").update(value).digest("hex");
const mtime = 1234;
function fullResult(bytes = "aaaa"): HashWorkerManifestResult {
    return { type: "result", mode: "manifest", job_id: "fresh-manifest", size: bytes.length, mtime,
        fingerprint: { size: bytes.length, mtime, ctime: 44, device: 2, inode: 99 },
        read_ms: 7, hash_ms: 11, manifest: { file_hash: hashBytes(bytes), total_size: bytes.length,
            chunks: bytes.length ? [{ hash: hashBytes(bytes), offset: 0, size: bytes.length }] : [] } };
}
function hashResult(bytes = "aaaa"): HashWorkerHashResult {
    const { manifest, ...common } = fullResult(bytes);
    return { ...common, mode: "hash", job_id: "fresh-verification", hash: manifest.file_hash, read_ms: 3, hash_ms: 5 };
}
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
function fixture() {
    const calls: string[] = [];
    const retained: HashWorkerManifestResult[] = [];
    const hint = { mutationId: 41, manifest: fullResult().manifest };
    const options: PreparedDesktopManifestOptions = {
        expectedSize: 4, expectedMtime: mtime,
        loadHint: async () => { calls.push("load"); return hint; },
        discardHint: async id => { calls.push(`discard:${id}`); },
        retain: async value => { calls.push("retain"); retained.push(value); },
        run: async mode => { calls.push(mode); return mode === "hash" ? hashResult() : fullResult(); },
        assertApplicable: () => {},
    };
    return { options, calls, retained, hint };
}
async function rejectsStage(work: Promise<unknown>, stage: PreparedDesktopManifestStage, cause?: unknown) {
    await assert.rejects(work, error => error instanceof PreparedDesktopManifestError && error.stage === stage &&
        (cause === undefined || error.cause === cause));
}

async function alwaysVerifyEvenWhenOldObjectsExist(): Promise<void> {
    for (const changed of [false, true]) {
        const { options, calls, hint, retained } = fixture();
        const oldChunkHash = hint.manifest.chunks[0].hash;
        const allOldObjectsPresentOnServer = new Set([oldChunkHash, hint.manifest.file_hash]);
        let source = "aaaa";
        let reads = 0;
        options.run = async mode => {
            calls.push(mode);
            reads++;
            return mode === "hash" ? hashResult(source) : fullResult(source);
        };
        if (changed) source = "bbbb"; // Same bytes length AND unchanged mtime/fingerprint.
        assert(allOldObjectsPresentOnServer.has(oldChunkHash));
        const result = await resolvePreparedDesktopManifest(options);
        assert(reads > 0, "prepared hash/server object existence cannot license a no-read path");
        assert.equal(result.result.manifest.file_hash, hashBytes(source));
        assert.equal(result.reusedPrepared, !changed);
        assert.deepEqual(calls, changed ? ["load", "hash", "discard:41", "manifest", "retain"] : ["load", "hash"]);
        assert.equal(result.validationReadMs, 3);
        assert.equal(result.validationHashMs, 5);
        assert.equal(result.result.read_ms, changed ? 7 : 0);
        assert.equal(result.result.hash_ms, changed ? 11 : 0);
        assert.equal(retained.length, changed ? 1 : 0);
    }
}

async function reuseUsesFreshFingerprintAndDetachedOwnership(): Promise<void> {
    const { options, hint } = fixture();
    const gate = deferred<HashWorkerHashResult>();
    const entered = deferred<void>();
    options.run = () => { entered.resolve(); return gate.promise; };
    const resolving = resolvePreparedDesktopManifest(options);
    await entered.promise;
    hint.manifest.chunks[0].hash = "f".repeat(64);
    hint.manifest.file_hash = "f".repeat(64);
    const verified = hashResult();
    verified.fingerprint.inode = 777;
    verified.fingerprint.ctime = 999;
    gate.resolve(verified);
    const result = await resolving;
    assert.equal(result.result.manifest.file_hash, hashBytes("aaaa"));
    assert.equal(result.result.manifest.chunks[0].hash, hashBytes("aaaa"));
    assert.equal(result.result.fingerprint.inode, 777);
    assert.equal(result.result.fingerprint.ctime, 999);
    assert.equal(result.result.job_id, "fresh-verification");
    verified.fingerprint.inode = 888;
    assert.equal(result.result.fingerprint.inode, 777);
    result.result.manifest.chunks[0].size = 1;
    assert.equal(hint.manifest.chunks[0].size, 4);
}

async function acceptsOpaqueWindowsFileIdsBeyondSafeInteger(): Promise<void> {
    const { options } = fixture();
    const opaqueId = Number.MAX_SAFE_INTEGER + 2048;
    options.loadHint = async () => null;
    options.run = async () => {
        const value = fullResult();
        value.fingerprint.device = opaqueId;
        value.fingerprint.inode = opaqueId + 2;
        return value;
    };
    const resolved = await resolvePreparedDesktopManifest(options);
    assert.equal(resolved.result.fingerprint.device, opaqueId);
    assert.equal(resolved.result.fingerprint.inode, opaqueId + 2);
}

async function missingAndSizeMismatchingHintsPrepareFresh(): Promise<void> {
    for (const missing of [true, false]) {
        const { options, calls } = fixture();
        options.loadHint = async () => { calls.push("load"); return missing ? null : { mutationId: 42, manifest: fullResult("old").manifest }; };
        const result = await resolvePreparedDesktopManifest(options);
        assert.deepEqual(calls, missing ? ["load", "manifest", "retain"] : ["load", "discard:42", "manifest", "retain"]);
        assert.equal(result.reusedPrepared, false);
        assert.equal(result.validationReadMs, 0);
        assert.equal(result.validationHashMs, 0);
        assert.equal(result.result.hash_ms, 11);
    }
    const { options } = fixture();
    options.expectedSize = 0;
    options.loadHint = async () => ({ mutationId: 1, manifest: fullResult("").manifest });
    options.run = async () => hashResult("");
    assert.equal((await resolvePreparedDesktopManifest(options)).result.manifest.chunks.length, 0);

    const cas = fixture();
    let currentHint = cas.hint;
    cas.options.run = async mode => {
        // A newer prepared-cache record is distinct from the journal owner.
        // Retiring the previously loaded candidate must carry its exact ID.
        if (mode === "hash") currentHint = { mutationId: 42, manifest: fullResult("bbbb").manifest };
        return mode === "hash" ? hashResult("bbbb") : fullResult("bbbb");
    };
    cas.options.discardHint = async id => {
        assert.equal(id, 41);
        assert.notEqual(id, currentHint.mutationId);
    };
    cas.options.retain = async () => ({ kind: "limit" }); // No durable-retention claim on capacity skip.
    assert.equal((await resolvePreparedDesktopManifest(cas.options)).reusedPrepared, false);
    assert.equal(currentHint.mutationId, 42);
}

async function freshPreparationWaitsForSaveAndKeepsSeparateOwners(): Promise<void> {
    const { options } = fixture();
    options.loadHint = async () => null;
    const gate = deferred<void>();
    const entered = deferred<void>();
    const workerResult = fullResult();
    options.run = async () => workerResult;
    options.retain = async value => {
        value.manifest.chunks[0].hash = "e".repeat(64);
        value.fingerprint.inode = 456;
        entered.resolve();
        await gate.promise;
    };
    let settled = false;
    const resolving = resolvePreparedDesktopManifest(options).then(value => { settled = true; return value; });
    await entered.promise;
    workerResult.manifest.file_hash = "e".repeat(64);
    workerResult.fingerprint.inode = 654;
    await Promise.resolve();
    assert.equal(settled, false, "fresh manifest escaped before the save publication barrier");
    gate.resolve();
    const result = await resolving;
    assert.equal(result.result.manifest.file_hash, hashBytes("aaaa"));
    assert.equal(result.result.manifest.chunks[0].hash, hashBytes("aaaa"));
    assert.equal(result.result.fingerprint.inode, 99);
}

type Boundary = "load" | "verify" | "discard" | "prepare" | "retain";
async function lateInvalidationOrAbortAwaitsEveryNativeBoundary(): Promise<void> {
    for (const boundary of ["load", "verify", "discard", "prepare", "retain"] as Boundary[]) {
        for (const cancel of [false, true]) {
            const { options, calls, hint } = fixture();
            const entered = deferred<void>();
            const gate = deferred<void>();
            const controller = new AbortController();
            const stale = new Error("generation advanced");
            let generation = 9;
            const savedGenerations: number[] = [];
            const owner = generation;
            options.signal = controller.signal;
            options.assertApplicable = () => { if (generation !== owner) throw stale; };
            const block = async () => { entered.resolve(); await gate.promise; };
            options.loadHint = async () => {
                calls.push("load");
                if (boundary === "load") await block();
                return boundary === "prepare" || boundary === "retain" ? null : hint;
            };
            options.run = async mode => {
                calls.push(mode);
                if ((mode === "hash" && boundary === "verify") || (mode === "manifest" && boundary === "prepare")) await block();
                return mode === "hash" ? hashResult(boundary === "discard" ? "bbbb" : "aaaa") : fullResult();
            };
            options.discardHint = async id => { calls.push(`discard:${id}`); if (boundary === "discard") await block(); };
            options.retain = async () => {
                calls.push("retain");
                if (boundary === "retain") await block();
                // Like the caller's durable CAS: native save completion may
                // retain a hint for its OLD owner, not a usable new generation.
                savedGenerations.push(owner);
            };
            let settled = false;
            const resolving = resolvePreparedDesktopManifest(options);
            const observed = resolving.then(() => { settled = true; }, () => { settled = true; });
            await entered.promise;
            if (cancel) controller.abort(); else generation++;
            await Promise.resolve();
            await Promise.resolve();
            assert.equal(settled, false, `${boundary} released before native settlement`);
            const callsAtInvalidation = [...calls];
            gate.resolve();
            if (cancel) await assert.rejects(resolving, error => (error as Error)?.name === "AbortError");
            else await rejectsStage(resolving, "applicability", stale);
            await observed;
            assert.deepEqual(calls, callsAtInvalidation, "new work began after invalidation");
            assert.deepEqual(savedGenerations, boundary === "retain" ? [owner] : []);
            if (!cancel) assert(!savedGenerations.includes(generation));
        }
    }
}

async function corruptionIsNotAMissOrWorkerFailure(): Promise<void> {
    const boundary = fixture();
    const sized = fullResult();
    sized.size = sized.fingerprint.size = sized.manifest.total_size = 2 * MAX_FASTCDC_CHUNK_BYTES;
    sized.manifest.chunks = [0, MAX_FASTCDC_CHUNK_BYTES].map(offset => ({
        hash: hashBytes("fixture chunk"), offset, size: MAX_FASTCDC_CHUNK_BYTES,
    }));
    boundary.options.expectedSize = sized.size;
    boundary.options.loadHint = async () => null;
    boundary.options.run = async () => sized;
    assert.equal((await resolvePreparedDesktopManifest(boundary.options)).result.manifest.chunks.length, 2);

    const minimum = 256 * 1024; // Versioned local producer invariant, not generic wire validation.
    for (const canonical of [false, true]) {
        const checked = fixture();
        const value = fullResult();
        value.size = value.fingerprint.size = value.manifest.total_size = minimum + 1;
        const firstSize = canonical ? minimum : minimum - 1;
        value.manifest.chunks = [{ hash: hashBytes("first"), offset: 0, size: firstSize },
            { hash: hashBytes("tail"), offset: firstSize, size: value.size - firstSize }];
        checked.options.expectedSize = value.size;
        checked.options.loadHint = async () => null;
        checked.options.run = async () => value;
        if (canonical) assert.equal((await resolvePreparedDesktopManifest(checked.options)).result.manifest.chunks[1].size, 1);
        else await rejectsStage(resolvePreparedDesktopManifest(checked.options), "validation");
    }
    const expansion = fixture();
    let inspectedChunks = 0;
    expansion.hint.manifest.chunks = new Proxy(new Array(2), {
        get(target, property, receiver) {
            if (typeof property === "string" && /^\d+$/.test(property)) inspectedChunks++;
            return Reflect.get(target, property, receiver);
        },
    });
    await rejectsStage(resolvePreparedDesktopManifest(expansion.options), "validation");
    assert.equal(inspectedChunks, 0, "impossible chunk-count expansion was copied/walked before rejection");

    const corruptHints: Array<(hint: any) => void> = [
        value => { value.mutationId = 0; }, value => { value.mutationId = Number.MAX_SAFE_INTEGER + 1; },
        value => { value.manifest.file_hash = "invalid"; }, value => { value.manifest.total_size = -1; },
        value => { value.manifest.chunks[0].hash = "A".repeat(64); },
        value => { value.manifest.chunks[0].offset = 1; }, value => { value.manifest.chunks[0].size = 3; },
        value => { value.manifest.chunks[0].size = 0; }, value => { value.manifest.chunks.push({ ...value.manifest.chunks[0] }); },
        value => { value.manifest.total_size = MAX_FASTCDC_CHUNK_BYTES + 1; value.manifest.chunks[0].size = MAX_FASTCDC_CHUNK_BYTES + 1; },
        value => { value.manifest.chunks = {}; },
    ];
    for (const corrupt of corruptHints) {
        const { options, calls, hint } = fixture();
        corrupt(hint);
        await rejectsStage(resolvePreparedDesktopManifest(options), "validation");
        assert.deepEqual(calls, ["load"]);
    }
    const { options, calls } = fixture();
    options.loadHint = async () => undefined as any;
    await rejectsStage(resolvePreparedDesktopManifest(options), "validation");
    assert.deepEqual(calls, []);

    const corruptResults: Array<(value: any) => void> = [
        value => { value.type = "unknown"; }, value => { value.mode = "unknown"; }, value => { value.job_id = ""; },
        value => { value.size = 3; }, value => { value.mtime += 2; }, value => { value.read_ms = -1; },
        value => { value.hash_ms = Infinity; }, value => { value.fingerprint.inode = -1; },
        value => { value.fingerprint.device = 0.5; }, value => { value.fingerprint.mtime += 2; },
        value => { value.fingerprint.ctime = NaN; }, value => { value.fingerprint.size++; },
        value => { value.manifest.chunks[0].offset = 1; }, value => { value.manifest.file_hash = "bad"; },
    ];
    for (const corrupt of corruptResults) {
        const { options, retained } = fixture();
        options.loadHint = async () => null;
        const value = fullResult();
        corrupt(value);
        options.run = async () => value;
        await rejectsStage(resolvePreparedDesktopManifest(options), "validation");
        assert.equal(retained.length, 0);
    }
    const hashFixture = fixture();
    hashFixture.options.run = async () => ({ ...hashResult(), hash: "bad" });
    await rejectsStage(resolvePreparedDesktopManifest(hashFixture.options), "validation");
    assert.deepEqual(hashFixture.calls, ["load"]);
}

async function errorTagsPreserveWorkerCleanupAndFailClosedIO(): Promise<void> {
    for (const stage of ["load", "discard", "retain"] as const) {
        const { options } = fixture();
        const ioError = Object.assign(new Error("unavailable fixture IO"), { code: "EIO" });
        if (stage === "load") options.loadHint = async () => { throw ioError; };
        if (stage === "discard") {
            options.run = async () => hashResult("bbbb");
            options.discardHint = async () => { throw ioError; };
        }
        if (stage === "retain") {
            options.loadHint = async () => null;
            options.retain = async () => { throw ioError; };
        }
        await rejectsStage(resolvePreparedDesktopManifest(options), stage, ioError);
    }
    for (const mode of ["hash", "manifest"] as const) {
        const { options } = fixture();
        if (mode === "manifest") options.loadHint = async () => null;
        const nativeError = new Error("native lifetime cleanup is attached to this identity");
        options.run = async () => { throw nativeError; };
        await assert.rejects(resolvePreparedDesktopManifest(options), error => error === nativeError);
    }
    const { options, calls } = fixture();
    const controller = new AbortController();
    controller.abort();
    options.signal = controller.signal;
    await assert.rejects(resolvePreparedDesktopManifest(options), error => (error as Error)?.name === "AbortError");
    assert.deepEqual(calls, []);
    for (const [size, time] of [[-1, mtime], [4.5, mtime], [4, NaN], [4, -1]]) {
        const invalidOptions = fixture();
        invalidOptions.options.expectedSize = size;
        invalidOptions.options.expectedMtime = time;
        await rejectsStage(resolvePreparedDesktopManifest(invalidOptions.options), "validation");
        assert.deepEqual(invalidOptions.calls, []);
    }
}

async function detachedCloneAdmissionIsExactAndRAII(): Promise<void> {
    const bytes = preparedManifestCloneBytes(1);
    const denied = fixture();
    denied.options.memoryArbiter = new SyncMemoryArbiter({ capacityBytes: bytes - 1 });
    let rowReads = 0;
    const chunks: any[] = [];
    Object.defineProperty(chunks, "0", { enumerable: true, configurable: true,
        get() { rowReads++; return denied.hint.manifest.chunks[0]; } });
    chunks.length = 1;
    denied.hint.manifest.chunks = chunks;
    await assert.rejects(resolvePreparedDesktopManifest(denied.options), /exceeds capacity/);
    assert.equal(rowReads, 0, "chunk getter ran before detached-clone admission");

    const reused = fixture();
    const reuseArbiter = new SyncMemoryArbiter({ capacityBytes: bytes });
    reused.options.memoryArbiter = reuseArbiter;
    const reusedResult = await resolvePreparedDesktopManifest(reused.options);
    assert.equal(reusedResult.memoryOwner?.bytes, bytes);
    assert.equal(reuseArbiter.snapshot().usedBytes, bytes, "returned hint clone lost its lifetime lease");
    reusedResult.memoryOwner?.release(); reusedResult.memoryOwner?.release();
    assert.equal(reuseArbiter.snapshot().usedBytes, 0);

    const discarded = fixture();
    const discardArbiter = new SyncMemoryArbiter({ capacityBytes: bytes * 2 });
    discarded.options.memoryArbiter = discardArbiter;
    discarded.options.run = async mode => mode === "hash" ? hashResult("bbbb") : fullResult("bbbb");
    discarded.options.discardHint = async () => {
        assert.equal(discardArbiter.snapshot().usedBytes, 0,
            "rejected prepared clone remained admitted across discard IO");
    };
    const discardedResult = await resolvePreparedDesktopManifest(discarded.options);
    discardedResult.memoryOwner?.release();
    assert.equal(discardArbiter.snapshot().usedBytes, 0);

    const fresh = fixture();
    const freshArbiter = new SyncMemoryArbiter({ capacityBytes: bytes * 2 });
    fresh.options.memoryArbiter = freshArbiter;
    fresh.options.loadHint = async () => null;
    fresh.options.retain = async () => {
        assert.equal(freshArbiter.snapshot().usedBytes, bytes * 2,
            "retain argument and returned clone were not independently admitted");
    };
    const freshResult = await resolvePreparedDesktopManifest(fresh.options);
    assert.equal(freshArbiter.snapshot().usedBytes, bytes, "temporary retain clone lease escaped settlement");
    freshResult.memoryOwner?.release();
    assert.equal(freshArbiter.snapshot().usedBytes, 0);

    const nested = fixture();
    const nestedArbiter = new SyncMemoryArbiter({ capacityBytes: bytes });
    nested.options.memoryArbiter = nestedArbiter; nested.options.loadHint = async () => null;
    await assert.rejects(resolvePreparedDesktopManifest(nested.options), /currently unavailable/,
        "retain clone waited on the returned clone's own lease");
    assert.equal(nestedArbiter.snapshot().usedBytes, 0, "failed nested clone admission leaked its first lease");

    const failed = fixture();
    const failedArbiter = new SyncMemoryArbiter({ capacityBytes: bytes * 2 });
    failed.options.memoryArbiter = failedArbiter;
    failed.options.loadHint = async () => null;
    failed.options.retain = async () => { throw new Error("save failed"); };
    await rejectsStage(resolvePreparedDesktopManifest(failed.options), "retain");
    assert.equal(failedArbiter.snapshot().usedBytes, 0, "failed retain leaked a detached clone lease");

    const queued = fixture(), controller = new AbortController();
    const queuedArbiter = new SyncMemoryArbiter({ capacityBytes: bytes });
    const blocker = await queuedArbiter.reserve("transfer", bytes);
    queued.options.memoryArbiter = queuedArbiter; queued.options.signal = controller.signal;
    const pending = resolvePreparedDesktopManifest(queued.options);
    controller.abort();
    await assert.rejects(pending, error => (error as Error)?.name === "AbortError");
    assert.equal(queuedArbiter.snapshot().queuedRequests, 0);
    blocker.release();
    assert.equal(queuedArbiter.snapshot().usedBytes, 0, "aborted admission leaked capacity");
}

void (async () => {
    await alwaysVerifyEvenWhenOldObjectsExist();
    await reuseUsesFreshFingerprintAndDetachedOwnership();
    await acceptsOpaqueWindowsFileIdsBeyondSafeInteger();
    await missingAndSizeMismatchingHintsPrepareFresh();
    await freshPreparationWaitsForSaveAndKeepsSeparateOwners();
    await lateInvalidationOrAbortAwaitsEveryNativeBoundary();
    await corruptionIsNotAMissOrWorkerFailure();
    await errorTagsPreserveWorkerCleanupAndFailClosedIO();
    await detachedCloneAdmissionIsExactAndRAII();
    console.log("prepared desktop manifest: 8 groups passed (10 gated invalidation/cancellation boundaries)");
})().catch(error => { console.error(error); process.exitCode = 1; });
