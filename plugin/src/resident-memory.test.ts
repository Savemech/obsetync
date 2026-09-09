import { strict as assert } from "node:assert";
import {
    NativeResidentLedger,
    NativeResidentLeaseOwnershipError,
    NativeResidentLeaseReleasedError,
    type NativeResidentLease,
} from "./resident-memory";

function lease(ledger: NativeResidentLedger, bytes: number): NativeResidentLease {
    const result = ledger.tryReserve(bytes);
    assert(result, `expected ${bytes} resident bytes to be admitted`);
    return result;
}

function validationIsExactAndCallbackFree(): void {
    const invalid = [0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1];
    for (const value of invalid) {
        assert.throws(() => new NativeResidentLedger({ capacityBytes: value }), RangeError);
    }
    const ledger = new NativeResidentLedger({ capacityBytes: 100 });
    for (const value of invalid) {
        assert.throws(() => ledger.tryReserve(value), RangeError);
        assert.throws(() => ledger.setCapacity(value), RangeError);
    }
    const owner = lease(ledger, 20);
    for (const value of invalid) {
        assert.throws(() => ledger.tryGrow(owner, value), RangeError);
        assert.throws(() => ledger.tryResize(owner, value), RangeError);
        assert.throws(() => ledger.split(owner, value), RangeError);
    }
    let coercions = 0;
    const hostile = { [Symbol.toPrimitive]() { coercions++; return 1; } } as unknown as number;
    assert.throws(() => ledger.tryReserve(hostile), RangeError);
    assert.throws(() => ledger.tryGrow(owner, hostile), RangeError);
    assert.equal(coercions, 0, "numeric validation invoked caller coercion");
    assert.deepEqual(ledger.snapshot(), {
        capacityBytes: 100, usedBytes: 20, peakUsedBytes: 20, availableBytes: 80,
        overcommittedBytes: 0, activeLeases: 1, closed: false,
        refusedReservations: 0, refusedGrowths: 0,
    });
    owner.release();
}

function failFastReservationAndAtomicResize(): void {
    const ledger = new NativeResidentLedger({ capacityBytes: 100 });
    const oldResident = lease(ledger, 60);
    const refused = ledger.tryReserve(50);
    assert(!(refused instanceof Promise), "resident admission unexpectedly returned asynchronous work");
    assert.equal(refused, null, "replacement waited or overcommitted instead of failing immediately");
    assert.deepEqual(ledger.snapshot(), {
        capacityBytes: 100, usedBytes: 60, peakUsedBytes: 60, availableBytes: 40,
        overcommittedBytes: 0, activeLeases: 1, closed: false,
        refusedReservations: 1, refusedGrowths: 0,
    });

    const newPrivate = lease(ledger, 40);
    assert.equal(ledger.tryGrow(newPrivate, 1), false);
    assert.equal(ledger.tryResize(newPrivate, 41), false);
    assert.equal(newPrivate.bytes, 40);
    assert.equal(ledger.snapshot().usedBytes, 100, "failed growth partially changed accounting");
    assert.equal(ledger.snapshot().refusedGrowths, 2);

    // These calls model actual native cleanup already having completed.
    assert.equal(ledger.tryResize(oldResident, 50), true);
    assert.equal(oldResident.bytes, 50);
    assert.equal(ledger.tryGrow(newPrivate, 10), true);
    assert.equal(ledger.snapshot().usedBytes, 100);
    assert.equal(ledger.snapshot().peakUsedBytes, 100);
    assert.equal(ledger.tryResize(newPrivate, newPrivate.bytes), true);
    assert.equal(ledger.snapshot().refusedGrowths, 2, "no-op resize was counted as refusal");
    oldResident.release();
    newPrivate.release();
    assert.equal(ledger.snapshot().usedBytes, 0);
}

function splitTransferAndExactOwnerBranding(): void {
    const ledger = new NativeResidentLedger({ capacityBytes: 100 });
    const privateBuild = lease(ledger, 100);
    assert.throws(() => ledger.split(privateBuild, 100), RangeError);
    assert.equal(privateBuild.bytes, 100);

    const residualRetirement = ledger.split(privateBuild, 30);
    assert.equal(privateBuild.bytes, 70);
    assert.equal(residualRetirement.bytes, 30);
    assert.equal(ledger.snapshot().usedBytes, 100);
    assert.equal(ledger.snapshot().activeLeases, 2);
    assert.equal(ledger.snapshot().peakUsedBytes, 100);

    const newResident = ledger.transfer(privateBuild);
    assert.equal(privateBuild.bytes, 0);
    assert.equal(newResident.bytes, 70);
    assert.equal(ledger.snapshot().usedBytes, 100);
    assert.equal(ledger.snapshot().activeLeases, 2);
    privateBuild.release();
    assert.equal(ledger.snapshot().usedBytes, 100, "transferred source double-released its owner");
    assert.throws(() => ledger.tryResize(privateBuild, 1), NativeResidentLeaseReleasedError);
    assert.throws(() => ledger.transfer(privateBuild), NativeResidentLeaseReleasedError);

    const other = new NativeResidentLedger({ capacityBytes: 100 });
    const foreign = lease(other, 10);
    // Even copying every visible descriptor (including the private type-brand
    // symbol) cannot reproduce the exact WeakMap-owned accounting capability.
    const forged = Object.freeze(Object.defineProperties(
        {}, Object.getOwnPropertyDescriptors(newResident),
    )) as NativeResidentLease;
    assert.throws(() => ledger.tryResize(foreign, 5), NativeResidentLeaseOwnershipError);
    assert.throws(() => ledger.split(foreign, 5), NativeResidentLeaseOwnershipError);
    assert.throws(() => ledger.transfer(forged), NativeResidentLeaseOwnershipError);
    assert.throws(() => ledger.release(forged), NativeResidentLeaseOwnershipError);
    assert.equal(other.snapshot().usedBytes, 10, "foreign rejection mutated its real owner");
    assert.equal(ledger.snapshot().usedBytes, 100, "foreign rejection mutated target ledger");

    residualRetirement.release();
    residualRetirement.release();
    assert.equal(ledger.snapshot().usedBytes, 70);
    assert.equal(ledger.snapshot().activeLeases, 1);
    newResident.release();
    assert.equal(ledger.snapshot().usedBytes, 0);
    assert.equal(ledger.snapshot().activeLeases, 0);
    foreign.release();
}

function capacityShrinkCloseAndExplicitLifecycle(): void {
    const ledger = new NativeResidentLedger({ capacityBytes: 100 });
    const resident = lease(ledger, 80);
    ledger.setCapacity(50);
    assert.deepEqual(ledger.snapshot(), {
        capacityBytes: 50, usedBytes: 80, peakUsedBytes: 80, availableBytes: 0,
        overcommittedBytes: 30, activeLeases: 1, closed: false,
        refusedReservations: 0, refusedGrowths: 0,
    });
    assert.equal(ledger.tryReserve(1), null);
    assert.equal(ledger.tryGrow(resident, 1), false);
    ledger.close();
    ledger.close();
    assert.equal(ledger.tryReserve(1), null, "closed ledger admitted a new owner");
    assert.equal(ledger.tryResize(resident, 81), false, "closed ledger admitted growth");
    // Ownership-only moves and actual-free shrink remain available after close.
    const retirement = ledger.split(resident, 20);
    const transferred = ledger.transfer(retirement);
    assert.equal(ledger.tryResize(resident, 40), true);
    assert.equal(ledger.snapshot().usedBytes, 60);
    assert.equal(ledger.snapshot().activeLeases, 2);
    assert.equal(ledger.snapshot().closed, true);
    assert.equal(ledger.snapshot().refusedReservations, 2);
    assert.equal(ledger.snapshot().refusedGrowths, 2);
    resident.release();
    transferred.release();
    assert.deepEqual(ledger.snapshot(), {
        capacityBytes: 50, usedBytes: 0, peakUsedBytes: 80, availableBytes: 50,
        overcommittedBytes: 0, activeLeases: 0, closed: true,
        refusedReservations: 2, refusedGrowths: 2,
    });
}

function absorbIsAtomicOwnerLocalAndAllocationFree(): void {
    const ledger = new NativeResidentLedger({ capacityBytes: 100 });
    const aggregate = lease(ledger, 60);
    const cohort = lease(ledger, 30);
    ledger.absorb(aggregate, cohort);
    assert.equal(aggregate.bytes, 90);
    assert.equal(cohort.bytes, 0);
    assert.deepEqual(ledger.snapshot(), {
        capacityBytes: 100, usedBytes: 90, peakUsedBytes: 90, availableBytes: 10,
        overcommittedBytes: 0, activeLeases: 1, closed: false,
        refusedReservations: 0, refusedGrowths: 0,
    });
    assert.throws(() => ledger.absorb(aggregate, cohort), NativeResidentLeaseReleasedError);
    assert.throws(() => ledger.absorb(aggregate, aggregate), NativeResidentLeaseOwnershipError);

    const other = new NativeResidentLedger({ capacityBytes: 100 });
    const foreign = lease(other, 10);
    assert.throws(() => ledger.absorb(aggregate, foreign), NativeResidentLeaseOwnershipError);
    assert.equal(aggregate.bytes, 90, "foreign absorb partially changed target");
    assert.equal(foreign.bytes, 10, "foreign absorb partially changed source");

    ledger.close();
    const tail = ledger.split(aggregate, 20);
    ledger.absorb(aggregate, tail);
    assert.equal(aggregate.bytes, 90, "closed ledger rejected ownership-only absorb");
    assert.equal(ledger.snapshot().activeLeases, 1);
    aggregate.release();
    foreign.release();
}

function safeIntegerBoundaryAndSnapshotIsolation(): void {
    const ledger = new NativeResidentLedger({ capacityBytes: Number.MAX_SAFE_INTEGER });
    const all = lease(ledger, Number.MAX_SAFE_INTEGER);
    assert.equal(ledger.tryReserve(1), null);
    assert.throws(() => ledger.tryGrow(all, 1), RangeError);
    assert.equal(all.bytes, Number.MAX_SAFE_INTEGER);
    const snapshot = ledger.snapshot();
    assert.equal(snapshot.usedBytes, Number.MAX_SAFE_INTEGER);
    assert.equal(snapshot.peakUsedBytes, Number.MAX_SAFE_INTEGER);
    snapshot.usedBytes = 0;
    assert.equal(ledger.snapshot().usedBytes, Number.MAX_SAFE_INTEGER,
        "caller mutation of snapshot changed the ledger");
    all.release();
}

validationIsExactAndCallbackFree();
failFastReservationAndAtomicResize();
splitTransferAndExactOwnerBranding();
capacityShrinkCloseAndExplicitLifecycle();
absorbIsAtomicOwnerLocalAndAllocationFree();
safeIntegerBoundaryAndSnapshotIsolation();
console.log("resident-memory.test: 6 fail-fast/accounting/ownership groups passed");
