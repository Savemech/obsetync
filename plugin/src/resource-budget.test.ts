import {
    ResourceBudget,
    ResourceBudgetClosedError,
    ResourceBudgetOversizedError,
    ResourceBudgetQueueFullError,
    type ResourceReservation,
} from "./resource-budget";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

function expectThrow(
    action: () => unknown,
    expected: new (...args: any[]) => Error,
    message: string,
): void {
    let caught: unknown;
    try { action(); } catch (error) { caught = error; }
    check(caught instanceof expected, message);
}

/** Attach rejection handlers before intentionally aborting/shrinking a queue. */
function rejected(promise: Promise<unknown>): Promise<unknown> {
    return promise.then(
        () => { throw new Error("expected resource reservation to reject"); },
        (error: unknown) => error,
    );
}

function assertEmpty(budget: ResourceBudget, message: string): void {
    const state = budget.snapshot();
    check(state.usedBytes === 0 && state.queuedBytes === 0 &&
        state.activeReservations === 0 && state.queuedRequests === 0, message);
}

async function validationAndOversizedAdmission(): Promise<void> {
    const invalid = [0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1];
    for (const capacityBytes of invalid) {
        expectThrow(() => new ResourceBudget({ capacityBytes }), RangeError, "invalid capacity accepted");
    }
    for (const maxQueuedRequests of [0, -1, 0.5, Infinity, 1025]) {
        expectThrow(() => new ResourceBudget({ capacityBytes: 10, maxQueuedRequests }),
            RangeError, "unbounded/invalid queue count accepted");
    }
    for (const maxQueuedBytes of invalid) {
        expectThrow(() => new ResourceBudget({ capacityBytes: 10, maxQueuedBytes }),
            RangeError, "invalid queue demand bound accepted");
    }
    const budget = new ResourceBudget({ capacityBytes: 10 });
    for (const bytes of invalid) {
        check(await rejected(budget.reserve(bytes)) instanceof RangeError, "invalid reservation accepted");
        expectThrow(() => budget.setCapacity(bytes), RangeError, "invalid capacity update accepted");
    }
    check(budget.snapshot().capacityBytes === 10, "invalid update changed capacity");
    const oversized = await rejected(budget.reserve(11));
    check(oversized instanceof ResourceBudgetOversizedError, "oversized request waited instead of rejecting");
    check((oversized as ResourceBudgetOversizedError).requestedBytes === 11 &&
        (oversized as ResourceBudgetOversizedError).capacityBytes === 10, "oversized error lost limits");
    assertEmpty(budget, "invalid requests changed accounting");

    // Numeric accounting must remain exact even at the largest supported cap.
    const huge = new ResourceBudget({ capacityBytes: Number.MAX_SAFE_INTEGER });
    check(huge.snapshot().maxQueuedBytes === Number.MAX_SAFE_INTEGER, "default queue byte bound overflowed");
    const all = await huge.reserve(Number.MAX_SAFE_INTEGER);
    const waiting = huge.reserve(1);
    check(huge.snapshot().usedBytes === Number.MAX_SAFE_INTEGER, "large admission rounded used bytes");
    all.release();
    (await waiting).release();
    assertEmpty(huge, "large reservation accounting underflowed");
    budget.close();
    huge.close();
}

async function synchronousAccountingAndFifo(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 10 });
    const first = budget.reserve(6);
    const order: string[] = [];
    const second = budget.reserve(6).then((lease) => { order.push("second"); return lease; });
    const third = budget.reserve(4).then((lease) => { order.push("third"); return lease; });
    const pending = budget.snapshot();
    check(pending.usedBytes === 6 && pending.activeReservations === 1,
        "simultaneous calls overcommitted before await resumed");
    check(pending.queuedRequests === 2 && pending.queuedBytes === 10,
        "small caller bypassed a blocked older waiter");
    const firstLease = await first;
    check(firstLease.bytes === 6, "reservation token has wrong size");
    firstLease.release();
    check(budget.snapshot().usedBytes === 10 && budget.snapshot().queuedRequests === 0,
        "release did not synchronously admit the FIFO window");
    const [secondLease, thirdLease] = await Promise.all([second, third]);
    check(order.join(",") === "second,third", "waiters were granted out of FIFO order");
    secondLease.release();
    secondLease.release();
    check(budget.snapshot().usedBytes === 4 && budget.snapshot().activeReservations === 1,
        "duplicate release undercounted another live lease");
    thirdLease.release();
    check(budget.snapshot().peakUsedBytes === 10, "peak accounting was reset on release");
    const copy = budget.snapshot();
    copy.usedBytes = 900;
    check(budget.snapshot().usedBytes === 0, "snapshot mutates internal accounting");
    assertEmpty(budget, "FIFO drain leaked accounting");
    budget.close();
}

function trackedController(): { controller: AbortController; listeners: () => number } {
    const controller = new AbortController();
    const signal = controller.signal;
    const add = signal.addEventListener.bind(signal);
    const remove = signal.removeEventListener.bind(signal);
    let listeners = 0;
    signal.addEventListener = ((...args: Parameters<AbortSignal["addEventListener"]>) => {
        listeners++;
        add(...args);
    }) as AbortSignal["addEventListener"];
    signal.removeEventListener = ((...args: Parameters<AbortSignal["removeEventListener"]>) => {
        listeners--;
        remove(...args);
    }) as AbortSignal["removeEventListener"];
    return { controller, listeners: () => listeners };
}

async function cancellationAndOwnership(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 10 });
    const reason = new Error("cancelled before allocation");
    const preAborted = trackedController();
    preAborted.controller.abort(reason);
    check(await rejected(budget.reserve(1, { signal: preAborted.controller.signal })) === reason,
        "pre-aborted reservation lost its reason");
    check(preAborted.listeners() === 0, "pre-aborted reservation added a listener");
    assertEmpty(budget, "pre-aborted caller received a reservation");

    const held = await budget.reserve(6);
    const blocked = trackedController();
    const head = rejected(budget.reserve(5, { signal: blocked.controller.signal }));
    const smaller = trackedController();
    const tail = budget.reserve(4, { signal: smaller.controller.signal });
    check(blocked.listeners() === 1 && smaller.listeners() === 1, "queued cancellation was not armed");
    blocked.controller.abort(reason);
    check(await head === reason, "queued cancellation reason was replaced");
    check(blocked.listeners() === 0, "cancelled waiter retained its abort listener");
    check(budget.snapshot().usedBytes === 10 && budget.snapshot().queuedBytes === 0,
        "cancelling the head did not admit its newly fitting successor");
    check(smaller.listeners() === 0, "granted waiter retained its abort listener");
    smaller.controller.abort(reason); // Granted, but await tail has not resumed.
    check(budget.snapshot().usedBytes === 10, "abort after grant prematurely freed live bytes");
    const tailLease = await tail;
    tailLease.release();
    held.release();

    const immediate = trackedController();
    const granted = budget.reserve(10, { signal: immediate.controller.signal });
    immediate.controller.abort(reason);
    check(budget.snapshot().usedBytes === 10, "same-turn abort revoked an immediate grant");
    check(immediate.listeners() === 0, "immediate grant retained a cancellation listener");
    (await granted).release();
    assertEmpty(budget, "cancel/release race leaked bytes");
    budget.close();
}

async function queueBounds(): Promise<void> {
    const countBound = new ResourceBudget({ capacityBytes: 10, maxQueuedRequests: 1 });
    const held = await countBound.reserve(10);
    const queued = countBound.reserve(6);
    check(await rejected(countBound.reserve(1)) instanceof ResourceBudgetQueueFullError,
        "queue count cap was bypassed");
    check(countBound.snapshot().queuedRequests === 1 && countBound.snapshot().queuedBytes === 6,
        "rejected count-bound admission changed queued demand");
    held.release();
    (await queued).release();
    assertEmpty(countBound, "count-bound queue did not drain");
    countBound.close();

    const byteBound = new ResourceBudget({ capacityBytes: 10, maxQueuedBytes: 5 });
    const active = await byteBound.reserve(10);
    const four = byteBound.reserve(4);
    check(await rejected(byteBound.reserve(2)) instanceof ResourceBudgetQueueFullError,
        "queued byte ceiling was bypassed");
    const one = byteBound.reserve(1);
    check(byteBound.snapshot().queuedBytes === 5 && byteBound.snapshot().queuedRequests === 2,
        "exact queue byte ceiling was rejected");
    active.release();
    for (const lease of await Promise.all([four, one])) lease.release();
    assertEmpty(byteBound, "byte-bound queue did not drain");
    byteBound.close();
}

async function resizing(): Promise<void> {
    const shrinking = new ResourceBudget({ capacityBytes: 10 });
    const held = await shrinking.reserve(8);
    const oversizedSignal = trackedController();
    const tooLarge = rejected(shrinking.reserve(7, { signal: oversizedSignal.controller.signal }));
    const small = shrinking.reserve(2);
    shrinking.setCapacity(3);
    const error = await tooLarge;
    check(error instanceof ResourceBudgetOversizedError && error.capacityBytes === 3,
        "shrink stranded an impossible FIFO head");
    check(oversizedSignal.listeners() === 0, "oversized-on-resize waiter retained listener");
    check(shrinking.snapshot().usedBytes === 8 && shrinking.snapshot().availableBytes === 0,
        "shrink revoked existing bytes or exposed negative available bytes");
    check(shrinking.snapshot().queuedBytes === 2, "shrink dropped a fitting queued reservation");
    const last = shrinking.reserve(1);
    check(shrinking.snapshot().usedBytes === 8 && shrinking.snapshot().queuedRequests === 2,
        "new reservation bypassed a shrunken capacity");
    held.release();
    check(shrinking.snapshot().usedBytes === 3, "release did not drain within the new capacity");
    for (const lease of await Promise.all([small, last])) lease.release();
    assertEmpty(shrinking, "shrink leaked accounting");
    shrinking.close();

    const growing = new ResourceBudget({ capacityBytes: 10 });
    const initial = await growing.reserve(10);
    const six = growing.reserve(6);
    const four = growing.reserve(4);
    growing.setCapacity(16);
    check(growing.snapshot().usedBytes === 16 && growing.snapshot().queuedBytes === 4,
        "capacity increase did not admit only the fitting FIFO prefix");
    const sixLease = await six;
    initial.release();
    const fourLease = await four;
    check(growing.snapshot().peakUsedBytes === 16, "resize lost peak usage");
    check(growing.snapshot().maxQueuedBytes === 40, "resize silently changed the demand ceiling");
    sixLease.release();
    fourLease.release();
    assertEmpty(growing, "grow leaked accounting");
    growing.close();
}

async function reservationShrinking(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 12 });
    const owner = await budget.reserve(10);
    const queued = budget.reserve(5);
    check(budget.snapshot().queuedRequests === 1, "shrink fixture did not block its waiter");
    owner.shrinkTo(7);
    check(owner.bytes === 7 && budget.snapshot().usedBytes === 12,
        "reservation shrink did not atomically lend bytes to the FIFO waiter");
    const admitted = await queued;
    expectThrow(() => owner.shrinkTo(8), RangeError, "reservation shrink allowed growth");
    owner.shrinkTo(7);
    admitted.release();
    owner.release();
    owner.release();
    assertEmpty(budget, "shrunk reservation leaked or underflowed accounting");
    budget.close();
}

async function closePreservesOwnedReservations(): Promise<void> {
    const budget = new ResourceBudget({ capacityBytes: 10 });
    const granted = budget.reserve(10);
    const signal = trackedController();
    const queued = rejected(budget.reserve(3, { signal: signal.controller.signal }));
    budget.close();
    budget.close();
    check(await queued instanceof ResourceBudgetClosedError, "close did not reject queued work");
    check(signal.listeners() === 0, "close retained queued abort listener");
    signal.controller.abort();
    const stopped = budget.snapshot();
    check(stopped.closed && stopped.queuedRequests === 0 && stopped.queuedBytes === 0,
        "close did not drain pending demand");
    check(stopped.usedBytes === 10 && stopped.activeReservations === 1,
        "close hid bytes still owned by an active reservation");
    check(await rejected(budget.reserve(1)) instanceof ResourceBudgetClosedError,
        "closed budget accepted new work");
    expectThrow(() => budget.setCapacity(20), ResourceBudgetClosedError, "closed budget accepted resize");
    const lease: ResourceReservation = await granted;
    lease.release();
    lease.release();
    assertEmpty(budget, "release after close leaked or underflowed accounting");
    check(budget.snapshot().peakUsedBytes === 10, "close lost historical peak");
}

void validationAndOversizedAdmission()
    .then(synchronousAccountingAndFifo)
    .then(cancellationAndOwnership)
    .then(queueBounds)
    .then(resizing)
    .then(reservationShrinking)
    .then(closePreservesOwnedReservations)
    .then(() => console.log(`resource-budget.test: ${assertions} assertions passed`))
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
