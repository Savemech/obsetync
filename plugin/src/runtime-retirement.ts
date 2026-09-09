/** Same-renderer reload coordination only. This cannot survive process death
 * or prove native termination; callers supply actual completion boundaries. */
const REGISTRY = Symbol.for("obsetync.runtime-retirement.v1");
type Registry = WeakMap<object, Promise<void>>;
function registry(): Registry {
    const host = globalThis as typeof globalThis & { [REGISTRY]?: Registry };
    return host[REGISTRY] ??= new WeakMap();
}

/** Join every owner even when another rejects. A failure cannot allow a new
 * runtime to open shared storage while a sibling is still completing writes. */
export async function settleRuntimeOwners(owners: readonly Promise<unknown>[]): Promise<void> {
    const results = await Promise.allSettled(owners);
    for (const result of results) if (result.status === "rejected") throw result.reason;
}

/** Start synchronously (closing admission now), while still allowing callers
 * to start and join independent cleanup if this owner throws immediately. */
export function startRuntimeOwner(work: () => Promise<unknown> | void): Promise<unknown> {
    try { return Promise.resolve(work()); }
    catch (error) { return Promise.reject(error); }
}

/** Capture the previous retirement cut synchronously. Do not follow future
 * registrations: unloading THIS initializing instance may depend on its load
 * finishing, so following its own retirement would create a cycle. */
export function awaitRuntimeRetirement(vault: object): Promise<void> {
    return registry().get(vault) ?? Promise.resolve();
}

/** Register before returning from synchronous plugin onunload. Failures remain
 * fail-closed for later instances; success clears only the exact current cut.
 * Weak keys retain no vault paths, credentials or cross-vault shared state. */
export function registerRuntimeRetirement(vault: object, work: Promise<void>): Promise<void> {
    const owners = registry();
    const previous = owners.get(vault);
    const retirement = settleRuntimeOwners(previous ? [previous, work] : [work]);
    owners.set(vault, retirement);
    void retirement.then(() => {
        if (owners.get(vault) === retirement) owners.delete(vault);
    }, () => {});
    return retirement;
}
