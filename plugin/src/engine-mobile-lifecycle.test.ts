import { strict as assert } from "node:assert";
import { ResourceVisibilityGate } from "./resource-governor";
import { JOURNAL_STORE_PATH } from "./journal";
import { fixture, hashFor, pathAt, rows, turns } from "./engine-root-test-fixture";

(globalThis as any).window ??= globalThis;
const hostTurn = () => new Promise<void>(resolve => setTimeout(resolve, 0));

async function run(): Promise<void> {
    const globals = globalThis as any;
    const oldNotice = globals.__obsetyncTestNotice;
    const oldDebounce = globals.__obsetyncTestDebounce;
    globals.__obsetyncTestNotice = () => ({ setMessage() {}, hide() {} });
    globals.__obsetyncTestDebounce = () => () => {};
    const visibility = new ResourceVisibilityGate("mobile", true);
    const f = await fixture(), active = await f.create("mobile-lifecycle", { visibilityGate: visibility });
    const path = pathAt(0);
    try {
        f.control.contentMissing = true;
        await f.event(active.engine, path, 2);
        let interrupted = false;
        f.control.beforePut = async signal => {
            if (interrupted) return;
            interrupted = true;
            visibility.setVisible(false, "pagehide");
            assert(signal?.aborted, "active upload did not receive mobile lifecycle cancellation");
            throw new DOMException("host wrapped request cancellation", "AbortError");
        };

        const drain = active.engine.pushPending();
        for (let count = 0; count < 40 && !interrupted; count++) { await turns(); await hostTurn(); }
        assert.equal(interrupted, true, "fixture never entered the upload boundary");
        for (let count = 0; count < 40 && visibility.snapshot().activeWork !== 0; count++) { await turns(); await hostTurn(); }
        assert.equal(visibility.snapshot().paused, true);
        assert.equal(visibility.snapshot().activeWork, 0,
            "mobile upload owner remained live after its rejected IO tail settled");
        assert.equal(active.engine.getState(), "idle", "quiesced engine remained falsely busy");
        assert.equal(f.base.getHash(path), hashFor(1), "hidden upload advanced the committed base");
        assert.equal(f.journal.unsyncedCount(), 1, "hidden upload acknowledged its dirty journal row");
        assert.equal(f.server.requests.length, 0, "hidden upload published a root");
        assert.equal(active.treeState.aborts, 1, "hidden upload did not abort its candidate exactly once");
        assert.equal(f.server.dataTransportInvalidations, 0,
            "hidden transition invalidated transport before foreground resume");
        assert.equal(visibility.setVisible(false, "freeze"), false);
        assert.equal(visibility.snapshot().epoch, 1, "pagehide/freeze created retry churn");

        // Host listener order is not an API contract. Model the notify WS
        // foreground callback arriving while the shared gate still reports
        // paused epoch N; the gate's following N+1 resume must deduplicate it.
        active.engine.invalidateDataTransportForForegroundResume();
        assert.equal(f.server.dataTransportInvalidations, 1,
            "early realtime foreground callback did not invalidate stale data transport");
        assert.equal(visibility.setVisible(true), true);
        assert.equal(f.server.dataTransportInvalidations, 1,
            "visibility listener duplicated the early foreground invalidation");
        assert.equal(visibility.setVisible(true), false, "pageshow/visible resumed the drain twice");
        assert.equal(f.server.dataTransportInvalidations, 1,
            "duplicate visible event invalidated data transport twice in one epoch");
        // The reverse listener order is idempotent as well.
        active.engine.invalidateDataTransportForForegroundResume();
        assert.equal(f.server.dataTransportInvalidations, 1,
            "realtime resume callback duplicated the mobile epoch invalidation");
        await drain;
        await active.engine.lifecycleResume;
        assert.equal(f.base.getHash(path), hashFor(2));
        assert.equal(f.journal.unsyncedCount(), 0);
        assert.equal(f.server.requests.length, 1, "foreground did not publish exactly once");
        assert.equal(visibility.snapshot().activeWork, 0);

        // The same lifecycle epoch contract owns the renderer hashing phase
        // of Full Rescan, not merely transport. The interrupted scan keeps its
        // disk truth uncommitted and is restarted once on foreground.
        f.source.set(path, 3);
        let scanInterrupted = false;
        f.control.beforeRead = async () => {
            if (scanInterrupted) return;
            scanInterrupted = true;
            visibility.setVisible(false, "freeze");
        };
        const scan = active.engine.fullScan();
        for (let count = 0; count < 40 && !scanInterrupted; count++) { await turns(); await hostTurn(); }
        assert.equal(scanInterrupted, true, "full scan never entered its hash read boundary");
        await scan;
        assert.equal(f.base.getHash(path), hashFor(2), "quiesced scan committed partial hash state");
        assert.equal(active.engine.getState(), "idle");
        assert.equal(visibility.snapshot().activeWork, 0);
        assert.equal(visibility.setVisible(true), true);
        assert.equal(f.server.dataTransportInvalidations, 2,
            "next foreground epoch did not synchronously invalidate data transport once");
        assert.equal(visibility.setVisible(true), false);
        assert.equal(f.server.dataTransportInvalidations, 2,
            "next foreground epoch accepted a duplicate transport invalidation");
        await active.engine.lifecycleResume;
        assert.equal(f.base.getHash(path), hashFor(3), "foreground did not resume the full scan exactly once");
        assert.equal(f.journal.unsyncedCount(), 0);

        f.source.set(path, 4);
        let immediateResume = false;
        f.control.beforeRead = async () => {
            if (immediateResume) return;
            immediateResume = true;
            visibility.setVisible(false, "pagehide");
            assert.equal(f.server.dataTransportInvalidations, 2,
                "pagehide invalidated data transport before its matching foreground event");
            visibility.setVisible(true);
            assert.equal(f.server.dataTransportInvalidations, 3,
                "racing foreground epoch did not synchronously invalidate data transport");
        };
        const racedScan = active.engine.fullScan();
        for (let count = 0; count < 40 && !immediateResume; count++) { await turns(); await hostTurn(); }
        assert.equal(immediateResume, true);
        await racedScan;
        await active.engine.lifecycleResume;
        assert.equal(f.base.getHash(path), hashFor(4),
            "visible event racing the aborted hash tail lost the one rescan resume");
        assert.equal(f.server.dataTransportInvalidations, 3,
            "racing resume invalidated data transport more than once in its epoch");

        await active.engine.stopAndDrain();
        active.engine.invalidateDataTransportForForegroundResume();
        assert.equal(f.server.dataTransportInvalidations, 3,
            "stopped engine invalidated data transport from a stale foreground callback");

        const deletionVisibility = new ResourceVisibilityGate("mobile", true);
        const deletions = await fixture(600);
        const deletionEngine = await deletions.create("mobile-deletion-scan", {
            visibilityGate: deletionVisibility,
        });
        try {
            deletions.source.clear();
            let durableBatchRows = 0;
            deletions.control.boundary = event => {
                const written = rows(event, JOURNAL_STORE_PATH).filter(row => row.op === "append");
                if (!written.length || durableBatchRows) return;
                durableBatchRows = written.length;
                deletionVisibility.setVisible(false, "pagehide");
            };
            await deletionEngine.engine.fullScan();
            assert(durableBatchRows > 0 && durableBatchRows <= 256,
                "scan journal crossed its bounded lifecycle cut");
            assert(deletions.journal.unsyncedCount() > 0 && deletions.journal.unsyncedCount() <= 256,
                "hidden deletion scan appended beyond its one in-flight durable batch");
            assert.equal(deletionVisibility.snapshot().activeWork, 0);
        } finally {
            deletionVisibility.dispose();
            await deletions.close();
        }
        console.log("engine mobile lifecycle: hidden upload quiesced and foreground resumed once");
    } finally {
        visibility.dispose();
        await f.close();
        if (oldNotice === undefined) delete globals.__obsetyncTestNotice;
        else globals.__obsetyncTestNotice = oldNotice;
        if (oldDebounce === undefined) delete globals.__obsetyncTestDebounce;
        else globals.__obsetyncTestDebounce = oldDebounce;
    }
}

void run().catch(error => { setTimeout(() => { throw error; }, 0); });
