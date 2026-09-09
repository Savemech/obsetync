import { strict as assert } from "node:assert";
import { blake3 } from "@noble/hashes/blake3";
import {
    ROOT_INTENT_HASH_FEED_BYTES, estimateRootIntentPreparationWorkset, rootPublicationArenaLimit,
} from "./root-publication-memory";
import {
    ROOT_MAX_BYTES, computeRootRequestHash, createRootCommitIntent, rootCommitRootByteLength,
    type RootCommitRequest,
} from "./root-outcome";
import { estimateTreeRootExportWorkset } from "./tree-root-export-job";
import { ResourceBudget, ResourceBudgetOversizedError } from "./resource-budget";

const FIXED_BYTES = 256n * 1024n + 512n;
const HASH_WORK_BYTES = 7n * 65536n;
const PAGE_BYTES = 65536;
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
const MAX_ARENA = 0xffff_ffff;

function exactPreparation(rootBytes: number) {
    const root = BigInt(rootBytes), base64Chars = 4n * ((root + 2n) / 3n);
    const ownerBytes = 6n * root + 2n * base64Chars + FIXED_BYTES;
    return { ownerBytes, workBytes: HASH_WORK_BYTES, totalBytes: ownerBytes + HASH_WORK_BYTES };
}

function preparationFormulaAndOverflow(): void {
    assert.equal(ROOT_INTENT_HASH_FEED_BYTES, 65536);
    for (const root of [1, 2, 3, 4, 5, 6, 16383, 16384, 16385, 65535, 65536, 65537,
        ROOT_MAX_BYTES - 2, ROOT_MAX_BYTES - 1, ROOT_MAX_BYTES, ROOT_MAX_BYTES + 1]) {
        const expected = exactPreparation(root);
        assert.deepEqual(estimateRootIntentPreparationWorkset(root), {
            ownerBytes: Number(expected.ownerBytes), workBytes: Number(expected.workBytes),
            totalBytes: Number(expected.totalBytes),
        }, `preparation copy allowance differs for ${root} bytes`);
    }
    assert.deepEqual(estimateRootIntentPreparationWorkset(ROOT_MAX_BYTES), {
        ownerBytes: 4_806_488, workBytes: 458_752, totalBytes: 5_265_240,
    });
    // The estimator is arithmetic, not permission to bypass the codec's root
    // cap. Its upper numeric boundary must reject total overflow too, even
    // when the owner portion by itself still fits a safe integer.
    let low = 1n, high = MAX_SAFE;
    while (low < high) {
        const middle = (low + high + 1n) / 2n;
        if (exactPreparation(Number(middle)).totalBytes <= MAX_SAFE) low = middle;
        else high = middle - 1n;
    }
    for (const root of [Number(low - 2n), Number(low - 1n), Number(low)]) {
        const expected = exactPreparation(root), actual = estimateRootIntentPreparationWorkset(root);
        assert.equal(BigInt(actual.ownerBytes), expected.ownerBytes);
        assert.equal(BigInt(actual.totalBytes), expected.totalBytes);
    }
    assert(exactPreparation(Number(low + 1n)).ownerBytes <= MAX_SAFE);
    assert.throws(() => estimateRootIntentPreparationWorkset(Number(low + 1n)), RangeError);
    for (const invalid of [0, -0, -1, 0.5, NaN, Infinity, -Infinity,
        Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER + 1]) {
        assert.throws(() => estimateRootIntentPreparationWorkset(invalid), RangeError);
    }
    const detached = estimateRootIntentPreparationWorkset(1);
    detached.ownerBytes = 0;
    assert.equal(estimateRootIntentPreparationWorkset(1).ownerBytes, Number(exactPreparation(1).ownerBytes));
}

function exactArena(capacity: number, preparation: number): number {
    const remaining = BigInt(capacity) - 2n * BigInt(PAGE_BYTES) - BigInt(preparation);
    const numerator = 32n * remaining + 512n;
    if (numerator < 65n * 512n) return 512;
    const limit = numerator / 65n;
    return Number(limit > BigInt(MAX_ARENA) ? BigInt(MAX_ARENA) : limit);
}

async function arenaBoundsAndMinimumAdmission(): Promise<void> {
    const preparation = estimateRootIntentPreparationWorkset(ROOT_MAX_BYTES).totalBytes;
    const minimumTotal = preparation + 2 * PAGE_BYTES + 2 * 512;
    for (const capacity of [1, minimumTotal - 1, minimumTotal, minimumTotal + 1,
        8 * 1024 * 1024, 32 * 1024 * 1024, 128 * 1024 * 1024,
        8_724_152_832, Number.MAX_SAFE_INTEGER]) {
        const arena = rootPublicationArenaLimit(capacity, preparation);
        assert.equal(arena, exactArena(capacity, preparation));
        assert(Number.isSafeInteger(arena) && arena >= 512 && arena <= MAX_ARENA);
        if (capacity >= minimumTotal && arena < MAX_ARENA) {
            const available = BigInt(capacity - preparation - 2 * PAGE_BYTES) * 32n;
            assert(65n * BigInt(arena) - 512n <= available,
                "selected arena exceeds the conservative native-offset allowance");
            assert(65n * BigInt(arena + 1) - 512n > available,
                "arena formula unnecessarily lost a complete byte at the fit boundary");
        }
    }
    for (const capacity of [1, 512, 32 * 1024 * 1024, Number.MAX_SAFE_INTEGER]) {
        for (const quota of [1, 512, preparation, Number.MAX_SAFE_INTEGER]) {
            assert.equal(rootPublicationArenaLimit(capacity, quota), exactArena(capacity, quota));
        }
    }
    for (const invalid of [0, -0, -1, 0.5, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1]) {
        assert.throws(() => rootPublicationArenaLimit(invalid, preparation), RangeError);
        assert.throws(() => rootPublicationArenaLimit(32 * 1024 * 1024, invalid), RangeError);
    }
    // 512 is a minimum native Plan ceiling, not a reservation grant. An
    // undersized pool must still reject the actual minimum owner before Build.
    const lowPool = new ResourceBudget({ capacityBytes: minimumTotal - 1 });
    const floor = rootPublicationArenaLimit(minimumTotal - 1, preparation);
    const estimate = estimateTreeRootExportWorkset(floor, 0, preparation);
    await assert.rejects(lowPool.reserve(estimate.ownerBytes + estimate.workBytes), ResourceBudgetOversizedError);
    assert.equal(lowPool.snapshot().usedBytes, 0);
    assert.equal(lowPool.snapshot().queuedRequests, 0);
    lowPool.close();
    const exactPool = new ResourceBudget({ capacityBytes: minimumTotal });
    const lease = await exactPool.reserve(estimate.ownerBytes + estimate.workBytes);
    assert.equal(exactPool.snapshot().usedBytes, minimumTotal);
    lease.release();
    assert.equal(exactPool.snapshot().usedBytes, 0);
    exactPool.close();
}

function makeRequest(root: Uint8Array): RootCommitRequest {
    return { protocol_version: 1, server_incarnation: "a".repeat(64),
        sequence: Number.MAX_SAFE_INTEGER, mutation_id: "b".repeat(32),
        parent_root: "c".repeat(64), root: Buffer.from(root).toString("base64") };
}

/** Independent Node framing oracle: production uses DataView plus decoded
 * root buffers. No production framing helper constructs this expected input. */
function expectedPreimage(vault: string, device: string, request: RootCommitRequest, root: Uint8Array): Buffer {
    const u64 = (value: number) => {
        const result = Buffer.alloc(8); result.writeBigUInt64LE(BigInt(value)); return result;
    };
    const fields = [Buffer.from(vault), Buffer.from(device), Buffer.from(request.server_incarnation),
        Buffer.from(request.mutation_id), Buffer.from(request.parent_root), Buffer.from(root)];
    const chunks = [Buffer.from("obsetync.root-commit.v1\0"), u64(request.sequence)];
    for (const field of fields) chunks.push(u64(field.length), field);
    return Buffer.concat(chunks);
}

async function maximumRootFixedIndexDecode(): Promise<void> {
    const root = new Uint8Array(ROOT_MAX_BYTES);
    for (let index = 0; index < root.length; index++) root[index] = (index * 73 + 19) & 255;
    const vault = "v".repeat(128), device = "d".repeat(128), request = makeRequest(root);
    assert.equal(rootCommitRootByteLength(request), ROOT_MAX_BYTES);
    const preimage = expectedPreimage(vault, device, request, root);
    assert.equal(preimage.length, ROOT_MAX_BYTES + 496);
    const expectedDigest = Buffer.from(blake3(preimage)).toString("hex");
    let settle!: (digest: string) => void;
    const hashing = new Promise<string>(resolve => { settle = resolve; });
    let borrowed: Uint8Array | undefined, calls = 0;
    const iteratorDescriptor = Object.getOwnPropertyDescriptor(String.prototype, Symbol.iterator)!;
    const fromDescriptor = Object.getOwnPropertyDescriptor(Uint8Array, "from");
    let creating!: ReturnType<typeof createRootCommitIntent>;
    try {
        // This patch exists only until the synchronous decoder reaches its
        // hash callback; no await or asynchronous suite overlaps the hooks.
        Object.defineProperty(String.prototype, Symbol.iterator, { configurable: true,
            value() { throw new Error("root decoder materialized a character iterator"); } });
        Object.defineProperty(Uint8Array, "from", { configurable: true,
            value() { throw new Error("root decoder called TypedArray.from"); } });
        creating = createRootCommitIntent(vault, device, request, bytes => {
            calls++; borrowed = bytes; return hashing;
        });
    } finally {
        Object.defineProperty(String.prototype, Symbol.iterator, iteratorDescriptor);
        if (fromDescriptor) Object.defineProperty(Uint8Array, "from", fromDescriptor);
        else delete (Uint8Array as unknown as { from?: unknown }).from;
    }
    assert.equal(calls, 1, "maximum decoder did not reach the admitted hash consumer");
    assert(borrowed);
    assert.equal(borrowed.byteOffset, 0);
    assert.equal(borrowed.buffer.byteLength, preimage.byteLength);
    assert.deepEqual(Buffer.from(borrowed), preimage);
    const original = { ...request };
    request.root = "AA=="; request.sequence = 1;
    root.fill(0);
    await Promise.resolve();
    assert.deepEqual(Buffer.from(borrowed), preimage, "deferred hashing lost exact captured source bytes");
    settle(expectedDigest);
    assert.deepEqual(await creating, { ...original, request_hash: expectedDigest });
}

async function rootSizeAndFailureGates(): Promise<void> {
    for (const size of [1, 2, 3, 4, ROOT_MAX_BYTES - 2, ROOT_MAX_BYTES - 1, ROOT_MAX_BYTES]) {
        const raw = new Uint8Array(size).fill(255), request = makeRequest(raw);
        assert.equal(rootCommitRootByteLength(request), size, "base64 padding changed the byte bound");
        const expected = expectedPreimage("vault", "device", request, raw);
        assert.equal(await computeRootRequestHash("vault", "device", request, input => {
            assert.deepEqual(Buffer.from(input), expected);
            return Buffer.from(blake3(input)).toString("hex");
        }), Buffer.from(blake3(expected)).toString("hex"));
    }
    let hashes = 0;
    for (const root of ["", "AB==", "AA==\n", Buffer.alloc(ROOT_MAX_BYTES + 1).toString("base64")]) {
        const request = { ...makeRequest(new Uint8Array([0])), root };
        assert.throws(() => rootCommitRootByteLength(request));
        await assert.rejects(computeRootRequestHash("vault", "device", request, () => { hashes++; return "a".repeat(64); }));
    }
    assert.equal(hashes, 0, "invalid or oversized input reached hashing");
    const failure = new Error("actual hash work failed");
    await assert.rejects(computeRootRequestHash("vault", "device", makeRequest(new Uint8Array([0, 128, 255])),
        async () => { await Promise.resolve(); throw failure; }), error => error === failure);
}

async function run(): Promise<void> {
    preparationFormulaAndOverflow();
    await arenaBoundsAndMinimumAdmission();
    await maximumRootFixedIndexDecode();
    await rootSizeAndFailureGates();
    console.log("root-publication-memory.test: 4 suites passed (exact estimates/overflow, arena admission, max-root fixed-index framing, codec/failure gates)");
}
let completed = false;
process.once("beforeExit", () => { if (!completed) throw new Error("root publication memory test left an unsettled consumer"); });
void run().then(() => { completed = true; }, error => { completed = true; setTimeout(() => { throw error; }, 0); });
