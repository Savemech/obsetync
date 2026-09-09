import { strict as assert } from "node:assert";
import { resolvePreparedMobileManifest } from "./prepared-mobile-manifest";
import { MemorySegmentedIO } from "./segmented-store-test-io";
import { SyncMemoryArbiter } from "./sync-memory-arbiter";
import { MOBILE_PREPARED_TRANSFER_PATH, PREPARED_STORE_OPERATION_BYTES, PREPARED_TRANSFER_LIMITS, PREPARED_TRANSFER_PATH,
    MobilePreparedTransferPlan, PreparedTransferPlan,
    type PreparedMobileSource } from "./transfer-plan";

const digest = (value: number) => value.toString(16).padStart(64, "0");
const scope = digest(1), path = "mobile.bin", baseRoot = digest(2);
const manifest = (file = 50, total = 6) => ({ file_hash: digest(file), total_size: total,
    chunks: total === 6 ? [{ hash: digest(10), offset: 0, size: 3 }, { hash: digest(11), offset: 3, size: 3 }]
        : [{ hash: digest(10), offset: 0, size: total }] });
const source = (version = 20, size = 6, mtime = 10): PreparedMobileSource => ({ size, mtime,
    fingerprint: { kind: "mobile-resource-v1", size, mtime, resourceVersion: digest(version) } });
const arbiter = () => new SyncMemoryArbiter({ capacityBytes:
    PREPARED_TRANSFER_LIMITS.retainedBytes + PREPARED_STORE_OPERATION_BYTES + 1024 * 1024 });

async function resolve(plan: MobilePreparedTransferPlan, options: { generation?: number; scopeHash?: string;
    current?: PreparedMobileSource; hash?: string; prepare?: () => ReturnType<typeof manifest>;
    root?: string | null; signal?: AbortSignal; assertApplicable?: () => void; hashWork?: () => void } = {}) {
    let hashes = 0, preparations = 0;
    const result = await resolvePreparedMobileManifest({ plan, scopeHash: options.scopeHash ?? scope, path,
        journalThroughId: options.generation ?? 1, baseRoot: "root" in options ? options.root : baseRoot,
        source: options.current ?? source(),
        hashCurrent: async () => { hashes++; options.hashWork?.(); return options.hash ?? digest(50); },
        prepareCurrent: async () => { preparations++; return options.prepare?.() ?? manifest(); },
        assertApplicable: options.assertApplicable ?? (() => {}), signal: options.signal });
    return { result, hashes, preparations };
}

async function restartReusesOnlyAfterCurrentFullHash(): Promise<void> {
    const io = new MemorySegmentedIO();
    const firstArbiter = arbiter();
    const first = new MobilePreparedTransferPlan(io, {}, firstArbiter); await first.load();
    assert.equal(first.snapshot().managedResidentBytes, PREPARED_TRANSFER_LIMITS.retainedBytes,
        "idle loaded store charged transient queue ceiling as resident memory");
    assert.throws(() => first.lookup(scope, path), /requires admitted ownership/,
        "arbitrated instance exposed the legacy unadmitted clone path");
    const prepared = await resolve(first);
    assert.equal(prepared.hashes, 0, "fresh current chunking redundantly ran the reuse verifier");
    assert.equal(prepared.preparations, 1);
    assert.equal(prepared.result.reusedPrepared, false);
    prepared.result.memoryOwner?.release();
    await first.compact(); await first.closeAndDrain();
    assert.equal(firstArbiter.snapshot().usedBytes, 0, "close did not return idle resident admission to baseline");

    const restartedArbiter = arbiter();
    const restarted = new MobilePreparedTransferPlan(io, {}, restartedArbiter); await restarted.load();
    const reused = await resolve(restarted);
    assert.equal(reused.hashes, 1, "restart reuse skipped current-file full hashing");
    assert.equal(reused.preparations, 0, "unchanged restart source was rechunked");
    assert.equal(reused.result.reusedPrepared, true);
    assert.deepEqual(reused.result.manifest, manifest());
    assert(restartedArbiter.snapshot().usedBytes > PREPARED_TRANSFER_LIMITS.retainedBytes,
        "decoded store and detached result were not co-admitted");
    reused.result.memoryOwner?.release(); await restarted.closeAndDrain();
}

async function everyScopeAndGenerationFenceFailsClosed(): Promise<void> {
    const io = new MemorySegmentedIO(), plan = new MobilePreparedTransferPlan(io, {}, arbiter()); await plan.load();
    const initial = await resolve(plan); initial.result.memoryOwner?.release();

    const generation = await resolve(plan, { generation: 2 });
    assert.equal(generation.hashes, 0); assert.equal(generation.preparations, 1);
    generation.result.memoryOwner?.release();

    const rootFence = await resolve(plan, { generation: 2, root: digest(3) });
    assert.equal(rootFence.hashes, 0); assert.equal(rootFence.preparations, 1,
        "base-root mismatch reused a prior prepared generation");
    rootFence.result.memoryOwner?.release();

    const mtime = await resolve(plan, { generation: 2, current: source(20, 6, 11) });
    assert.equal(mtime.hashes, 0); assert.equal(mtime.preparations, 1);
    mtime.result.memoryOwner?.release();

    const fingerprint = await resolve(plan, { generation: 2, current: source(21, 6, 11) });
    assert.equal(fingerprint.hashes, 0); assert.equal(fingerprint.preparations, 1,
        "qualified resource URL identity churn did not become a safe prepared-cache miss");
    fingerprint.result.memoryOwner?.release();

    const size = await resolve(plan, { generation: 2, current: source(21, 7, 11), prepare: () => manifest(50, 7) });
    assert.equal(size.hashes, 0); assert.equal(size.preparations, 1);
    size.result.memoryOwner?.release();

    const changedScope = await resolve(plan, { generation: 2, scopeHash: digest(99), current: source(21, 7, 11),
        prepare: () => manifest(50, 7) });
    assert.equal(changedScope.hashes, 0); assert.equal(changedScope.preparations, 1,
        "settings/ignore scope reused a record from another scope");
    changedScope.result.memoryOwner?.release();

    const hashDrift = await resolve(plan, { generation: 2, current: source(21, 7, 11), hash: digest(999),
        prepare: () => manifest(999, 7) });
    assert.equal(hashDrift.hashes, 1); assert.equal(hashDrift.preparations, 1,
        "same metadata/resource identity treated persisted chunks as byte authority");
    assert.equal(hashDrift.result.manifest.file_hash, digest(999));
    hashDrift.result.memoryOwner?.release(); await plan.closeAndDrain();
}

async function abortAndApplicabilityReleaseDetachedOwners(): Promise<void> {
    const io = new MemorySegmentedIO(), memory = arbiter(), plan = new MobilePreparedTransferPlan(io, {}, memory); await plan.load();
    const seeded = await resolve(plan); seeded.result.memoryOwner?.release();
    const baseline = memory.snapshot().usedBytes;
    const controller = new AbortController();
    await assert.rejects(resolve(plan, { signal: controller.signal, hashWork: () => controller.abort() }),
        error => (error as Error)?.name === "AbortError");
    assert.equal(memory.snapshot().usedBytes, baseline, "abort leaked admitted lookup clone");

    let checks = 0;
    const stale = new Error("generation changed");
    await assert.rejects(resolve(plan, { assertApplicable: () => { if (++checks >= 3) throw stale; } }),
        error => error instanceof Error && error.name === "PreparedMobileManifestError" && (error as any).cause === stale);
    assert.equal(memory.snapshot().usedBytes, baseline, "applicability failure leaked admitted lookup clone");
    await plan.closeAndDrain();
}

async function decodeAdmissionFailurePreservesForensicStore(): Promise<void> {
    const io = new MemorySegmentedIO(), seed = new MobilePreparedTransferPlan(io); await seed.load();
    const retained = await seed.retain({ scopeHash: scope, path, journalThroughId: 1, baseRoot,
        source: source(), manifest: manifest(), expectedMutationId: null });
    assert(retained.retained); await seed.compact();
    const before = io.snapshot();
    const constrainedArbiter = new SyncMemoryArbiter({ capacityBytes: PREPARED_TRANSFER_LIMITS.retainedBytes - 1 });
    const constrained = new MobilePreparedTransferPlan(io, {}, constrainedArbiter);
    await assert.rejects(constrained.load(), /exceeds capacity/);
    assert.deepEqual(io.snapshot(), before, "decode admission failure rewrote or deleted recovery evidence");
    assert.equal(constrainedArbiter.snapshot().usedBytes, 0);
}

async function mobileStoreIsDowngradeIsolated(): Promise<void> {
    const io = new MemorySegmentedIO();
    const desktop = new PreparedTransferPlan(io); await desktop.load();
    const desktopRetained = await desktop.retain({ scopeHash: scope, path: "desktop.bin", journalThroughId: 1, baseRoot,
        source: { size: 6, mtime: 10, fingerprint: { kind: "desktop-v1", size: 6, mtime: 10,
            ctime: 9, device: 1, inode: 2 } }, manifest: manifest(), expectedMutationId: null });
    assert(desktopRetained.retained); await desktop.compact();
    const oldBytes = new Map([...io.files].filter(([name]) => name.startsWith(PREPARED_TRANSFER_PATH)));

    const mobile = new MobilePreparedTransferPlan(io); await mobile.load();
    const mobileRetained = await mobile.retain({ scopeHash: scope, path, journalThroughId: 1, baseRoot,
        source: source(), manifest: manifest(), expectedMutationId: null });
    assert(mobileRetained.retained); await mobile.compact();
    assert([...io.files.keys()].some(name => name.startsWith(MOBILE_PREPARED_TRANSFER_PATH)),
        "mobile persistence did not use its forward-additive path");
    assert.deepEqual(new Map([...io.files].filter(([name]) => name.startsWith(PREPARED_TRANSFER_PATH))), oldBytes,
        "mobile persistence changed bytes under the released desktop store path");

    // This class is the released desktop-only reader model: it never opens or
    // parses the additive mobile domain.
    const legacy = new PreparedTransferPlan(io); await legacy.load();
    assert.equal(legacy.lookup(scope, "desktop.bin")?.source.fingerprint.kind, "desktop-v1");
    assert.deepEqual(new Map([...io.files].filter(([name]) => name.startsWith(PREPARED_TRANSFER_PATH))), oldBytes,
        "legacy desktop reload rewrote its byte-stable store after mobile persistence");
    const mobileRestart = new MobilePreparedTransferPlan(io); await mobileRestart.load();
    assert.equal(mobileRestart.lookup(scope, path)?.source.fingerprint.kind, "mobile-resource-v1",
        "downgrade-compatible layout lost mobile forensic evidence");
}

void (async () => {
    await restartReusesOnlyAfterCurrentFullHash();
    await everyScopeAndGenerationFenceFailsClosed();
    await abortAndApplicabilityReleaseDetachedOwners();
    await decodeAdmissionFailurePreservesForensicStore();
    await mobileStoreIsDowngradeIsolated();
    console.log("prepared mobile manifest: restart reuse and scope/generation/hash fences passed");
})().catch(error => { console.error(error); process.exitCode = 1; });
