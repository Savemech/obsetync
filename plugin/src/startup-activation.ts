/** Narrow production seam for plugin ordering; no Obsidian/native lifecycle emulation. */
export interface PreparedSyncEngine {
    prepareLocal(): Promise<void>;
    start(): Promise<void>;
    stop(): void;
    quiesceAndDrain(): Promise<void>;
    isStopped(): boolean;
}

/** Native compilation need not be abortable, but publishing its module and
 * runtime profile must belong to the still-current plugin initialization.
 * The caller must check its generation again after awaiting this helper before
 * constructing further resources: promise resumption is another async boundary. */
export async function publishStartupResult<T>(
    pending: Promise<T>,
    isCurrent: () => boolean,
    publish: (value: T) => void,
): Promise<{ value: T } | null> {
    let value: T;
    try { value = await pending; }
    catch (error) {
        if (!isCurrent()) return null;
        throw error;
    }
    if (!isCurrent()) return null;
    publish(value);
    return { value };
}

/** Negotiation has already started in parallel with WASM loading. Capture and
 * replay must finish before awaiting it, and format selection precedes activation.
 * The generation fence prevents an old init/unload completion reviving an engine. */
export async function activatePreparedSync<T>(options: {
    engine: PreparedSyncEngine;
    negotiation: Promise<T>;
    selectTree: (result: T) => void;
    isCurrent: () => boolean;
}): Promise<boolean> {
    const { engine } = options;
    // Keep a rejection handled even while bounded local replay is still busy.
    const negotiation = options.negotiation.then(
        value => ({ value }), error => ({ error }),
    );
    const current = () => options.isCurrent() && !engine.isStopped();
    // A superseding initialization needs these listeners until its capture
    // handoff. Terminal stop belongs to plugin unload, not a stale activation
    // continuation that may finish during the replacement's drain.
    const quiesce = () => { void engine.quiesceAndDrain(); };
    if (!current()) { quiesce(); return false; }
    try {
        await engine.prepareLocal();
        if (!current()) { quiesce(); return false; }
        const negotiated = await negotiation;
        if (!current()) { quiesce(); return false; }
        if ("error" in negotiated) throw negotiated.error;
        options.selectTree(negotiated.value);
        if (!current()) { quiesce(); return false; }
        await engine.start();
        return current();
    } catch (error) {
        if (!current()) { quiesce(); return false; }
        throw error;
    }
}
