import { strict as assert } from "node:assert";
import { MemorySegmentedIO, storeTestIOError, type StoreTestBoundary } from "./segmented-store-test-io";
import { OBJECT_CONFIRMATION_PATH, ObjectConfirmationStore, ObjectConfirmationStoreError,
    type ConfirmedObject } from "./object-confirmation-store";

let assertions = 0;
const same = (actual: unknown, expected: unknown, message: string) => {
    assertions++; assert.deepEqual(actual, expected, message);
};
const check = (value: unknown, message: string) => { assertions++; assert.ok(value, message); };
const h = (value: number) => value.toString(16).padStart(64, "0");
const SCOPE = h(1), GENERATION = h(2);
const content = (value: number): ConfirmedObject => ({ kind: "content", hash: h(value) });
const chunk = (value: number): ConfirmedObject => ({ kind: "content-chunk", hash: h(value) });
const index = (value: number): ConfirmedObject => ({ kind: "index-chunk", hash: h(value) });

async function opened(io = new MemorySegmentedIO(), limits?: ConstructorParameters<typeof ObjectConfirmationStore>[1]) {
    const store = new ObjectConfirmationStore(io, limits); await store.load(); return { io, store };
}
async function rejects(work: Promise<unknown>, code: ObjectConfirmationStoreError["code"]): Promise<void> {
    let error: unknown;
    try { await work; } catch (caught) { error = caught; }
    check(error instanceof ObjectConfirmationStoreError && error.code === code,
        `expected ${code}, received ${(error as any)?.code}`);
}

async function restartAndGenerationFence(): Promise<void> {
    const { io, store } = await opened();
    check(!store.has(SCOPE, GENERATION, content(10)), "absent confirmation was reported present");
    check(await store.retain(SCOPE, GENERATION, [content(10), chunk(11), index(12), content(10)]),
        "bounded confirmation batch was not retained");
    check(store.has(SCOPE, GENERATION, content(10)) && store.has(SCOPE, GENERATION, chunk(11)) &&
        store.has(SCOPE, GENERATION, index(12)),
        "accepted confirmations were not visible");
    check(!store.has(SCOPE, GENERATION, chunk(10)), "object kind was not part of confirmation identity");
    check(!store.has(h(3), GENERATION, content(10)) && !store.has(SCOPE, h(4), content(10)),
        "scope or server generation did not fence a positive hint");

    const cold = await opened(new MemorySegmentedIO(io.snapshot()));
    check(cold.store.has(SCOPE, GENERATION, content(10)) && cold.store.has(SCOPE, GENERATION, chunk(11)),
        "cold recovery lost confirmed object presence");
    check(await cold.store.retain(SCOPE, h(5), [content(12)]), "new server generation was not admitted");
    check(!cold.store.has(SCOPE, GENERATION, content(10)) && cold.store.has(SCOPE, h(5), content(12)),
        "generation replacement retained stale positive authority");
    const restarted = await opened(new MemorySegmentedIO(cold.io.snapshot()));
    same(restarted.store.snapshot().records, 1, "generation replacement was not one atomic durable cut");
    check(restarted.store.has(SCOPE, h(5), content(12)), "replacement generation did not survive restart");
    check(await restarted.store.invalidate(SCOPE, h(5)), "matching failed-publication fence did not clear hints");
    check(!restarted.store.has(SCOPE, h(5), content(12)), "invalidated positive hint remained usable");
    same(await restarted.store.invalidate(SCOPE, h(5)), false, "empty invalidation was not idempotent");
    const afterInvalidate = await opened(new MemorySegmentedIO(restarted.io.snapshot()));
    same(afterInvalidate.store.snapshot().records, 0, "invalidation did not survive restart");
}

async function boundedFailureIsOnlyAnOptimizationLoss(): Promise<void> {
    const { io, store } = await opened(undefined, { records: 2, retainedBytes: 512, batchRecords: 2,
        queuedBytes: 1024, queuedRequests: 2 });
    check(await store.retain(SCOPE, GENERATION, [content(1), content(2)]), "store did not accept exact record ceiling");
    const before = io.snapshot();
    same(await store.retain(SCOPE, GENERATION, [content(3)]), false,
        "full positive-hint cache did not degrade without mutation");
    same(io.snapshot(), before, "refused hint retention changed durable confirmation state");
    check(store.has(SCOPE, GENERATION, content(1)) && !store.has(SCOPE, GENERATION, content(3)),
        "refused retention evicted existing confirmations");
    same(await store.retain(SCOPE, GENERATION, [content(1)]), true,
        "idempotent already-confirmed retention failed at capacity");
    same(await store.retain(SCOPE, GENERATION, [content(4), content(5), content(6)]), false,
        "oversized caller batch was copied or queued");
}

async function publicationFailureRequiresValidatedReload(): Promise<void> {
    const { io, store } = await opened();
    let failed = false;
    io.onBoundary = (event: StoreTestBoundary) => {
        if (!failed && event.method === "rename" && event.phase === "before" &&
            event.to === `${OBJECT_CONFIRMATION_PATH}/head.json`) {
            failed = true; throw storeTestIOError("ambiguous confirmation publish");
        }
    };
    let publishError: unknown;
    try { await store.retain(SCOPE, GENERATION, [content(20)]); } catch (error) { publishError = error; }
    check(publishError instanceof Error, "injected ambiguous publication unexpectedly succeeded");
    check(!store.snapshot().ready, "ambiguous persistence left positive hints usable");
    let lookupError: unknown;
    try { store.has(SCOPE, GENERATION, content(20)); } catch (error) { lookupError = error; }
    check(lookupError instanceof ObjectConfirmationStoreError && lookupError.code === "RECOVERY_REQUIRED",
        "poisoned store exposed an unverified confirmation");
    io.onBoundary = undefined;
    await store.load();
    check(store.snapshot().ready, "validated reload did not recover confirmation store");
}

async function closureRejectsCorruptionAndCloseStopsAdmission(): Promise<void> {
    const { io, store } = await opened();
    await store.retain(SCOPE, GENERATION, [content(30)]);
    const cold = await opened(new MemorySegmentedIO(io.snapshot()));
    const closing = cold.store.closeAndDrain();
    await rejects(cold.store.retain(SCOPE, GENERATION, [content(31)]), "CLOSED");
    let error: unknown;
    try { cold.store.has(SCOPE, GENERATION, content(30)); } catch (caught) { error = caught; }
    check(error instanceof ObjectConfirmationStoreError && error.code === "CLOSED",
        "closed store continued serving durable authority");
    await closing;
}

async function main(): Promise<void> {
    await restartAndGenerationFence();
    await boundedFailureIsOnlyAnOptimizationLoss();
    await publicationFailureRequiresValidatedReload();
    await closureRejectsCorruptionAndCloseStopsAdmission();
    console.log(`object-confirmation-store.test: ${assertions} assertions passed`);
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
