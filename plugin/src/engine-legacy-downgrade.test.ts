import { strict as assert } from "node:assert";
import { JOURNAL_STORE_PATH } from "./journal";
import { fixture, gate, observe, pathAt, rejection, rows, turns } from "./engine-root-test-fixture";
import type { EngineLegacyDowngradeLease } from "./sync";

async function freezeWaitsForAcceptedRootAndCapturedCallback(): Promise<void> {
    const f = await fixture(2), root = gate(), callback = gate();
    try {
        const { engine } = await f.create("downgrade-drain");
        await f.queue(engine, [pathAt(0)], 2);
        f.control.beforeRootAnswer = root.wait;
        const pushing = observe(engine.pushPending());
        await root.entered;
        let callbackHeld = false;
        f.control.boundary = async event => {
            if (!callbackHeld && rows(event, JOURNAL_STORE_PATH).some(row =>
                row.op === "append" && row.entry?.path === pathAt(1))) {
                callbackHeld = true; await callback.wait();
            }
        };
        const localEvent = observe(f.event(engine, pathAt(1), 3));
        await callback.entered;
        let wsStops = 0;
        engine.syncTimer = 1;
        engine.presenceHeartbeat = 2;
        engine.wsChannel = { stop() { wsStops++; } };

        const first = engine.freezeForLegacyDowngrade() as Promise<EngineLegacyDowngradeLease>;
        const repeated = engine.freezeForLegacyDowngrade() as Promise<EngineLegacyDowngradeLease>;
        assert.equal(first, repeated, "repeated freeze created a second authority owner");
        const frozen = observe(first);
        assert.equal(engine.isStopped(), true, "freeze did not synchronously stop engine admission");
        assert.equal(f.refs.size, 0, "freeze left vault listeners attached");
        assert.equal(engine.syncTimer, null); assert.equal(engine.presenceHeartbeat, null);
        assert.equal(wsStops, 1); assert(f.events.includes("downgrade-drain:lane-close"));
        await turns();
        assert.equal(frozen.state.settled, false, "freeze bypassed accepted root/callback owners");

        root.resolve(); await turns();
        assert.equal(frozen.state.settled, false, "root response bypassed the captured durable callback owner");
        callback.resolve();
        await Promise.all([pushing.done, localEvent.done, frozen.done]);
        assert.equal(pushing.state.error, undefined); assert.equal(localEvent.state.error, undefined);
        assert.equal(frozen.state.error, undefined);
        const lease = frozen.state.value!;
        lease.assertHeld();
        assert.equal(lease.treeVersion, 1);
        assert.equal(lease.rootIntents.pending(), null);
        assert.equal(lease.rootIntents.lastSequence, 1);
        assert.deepEqual(engine.rootRuntime.snapshot(), { loaded: true, pending: false, closed: true },
            "freeze disposed root authority before converter retirement");
        const closed = lease.closeAndDrain();
        assert.equal(closed, lease.closeAndDrain(), "lease returned two retirement owners");
        await closed;
        assert.throws(() => lease.assertHeld(), /authority changed/);
        assert.deepEqual(engine.rootRuntime.snapshot(), { loaded: false, pending: false, closed: true });
    } finally {
        root.resolve(); callback.resolve(); await f.close();
    }
}

async function freezeRejectsUnsafeAuthorityWithoutTakingALease(): Promise<void> {
    {
        const f = await fixture();
        try {
            const { engine } = await f.create("unloaded", { active: false });
            assert.match(String(await rejection(engine.freezeForLegacyDowngrade())), /loaded root authority/);
            assert.equal(engine.isStopped(), false);
        } finally { await f.close(); }
    }
    {
        const f = await fixture();
        try {
            const created = await f.create("tree-v2");
            created.tree.tree_version = () => 2;
            assert.match(String(await rejection(created.engine.freezeForLegacyDowngrade())), /tree format v1/);
            assert.equal(created.engine.isStopped(), false);
        } finally { await f.close(); }
    }
    {
        const f = await fixture();
        try {
            const { engine } = await f.create("root-repair");
            engine.rootRepairRequired = true;
            assert.match(String(await rejection(engine.freezeForLegacyDowngrade())), /settled root authority/);
            assert.equal(engine.isStopped(), false);
        } finally { await f.close(); }
    }
    {
        const f = await fixture();
        try {
            const { engine } = await f.create("pending-root");
            await f.queue(engine, [pathAt(0)], 2);
            f.control.beforeRootAnswer = async () => { throw new Error("lost accepted root response"); };
            await engine.pushPending();
            assert.equal(engine.rootRuntime.snapshot().pending, true);
            assert.match(String(await rejection(engine.freezeForLegacyDowngrade())), /settled root authority/);
            assert.equal(engine.isStopped(), false);
        } finally { await f.close(); }
    }
}

async function revokeAndNormalStopFenceTheExactLease(): Promise<void> {
    {
        const f = await fixture();
        try {
            const { engine } = await f.create("revoked");
            const lease = await engine.freezeForLegacyDowngrade();
            lease.assertHeld(); lease.revoke();
            assert.throws(() => lease.assertHeld(), /authority changed/);
            assert.throws(() => lease.rootIntents.pending(), /authority changed/);
            await lease.closeAndDrain();
            assert.equal(engine.rootRuntime.snapshot().loaded, false);
        } finally { await f.close(); }
    }
    {
        const f = await fixture();
        try {
            const { engine } = await f.create("normal-stop");
            const lease = await engine.freezeForLegacyDowngrade();
            const first = engine.stopAndDrain(), repeated = engine.stopAndDrain();
            assert.equal(first, repeated, "normal stop created a second terminal owner");
            await first;
            assert.throws(() => lease.assertHeld(), /authority changed/);
            assert.equal(lease.closeAndDrain(), lease.closeAndDrain());
            await lease.closeAndDrain();
            assert.equal(engine.rootRuntime.snapshot().loaded, false);
        } finally { await f.close(); }
    }
}

async function run(): Promise<void> {
    const globals = globalThis as any;
    const oldNotice = globals.__obsetyncTestNotice, oldDebounce = globals.__obsetyncTestDebounce;
    const notice = () => ({ setMessage() {}, hide() {} }), debounce = () => () => {};
    globals.__obsetyncTestNotice = notice; globals.__obsetyncTestDebounce = debounce;
    try {
        await freezeWaitsForAcceptedRootAndCapturedCallback();
        await freezeRejectsUnsafeAuthorityWithoutTakingALease();
        await revokeAndNormalStopFenceTheExactLease();
        console.log("engine-legacy-downgrade.test: exclusive freeze lifecycle passed");
    } finally {
        if (globals.__obsetyncTestNotice === notice) {
            if (oldNotice === undefined) delete globals.__obsetyncTestNotice; else globals.__obsetyncTestNotice = oldNotice;
        }
        if (globals.__obsetyncTestDebounce === debounce) {
            if (oldDebounce === undefined) delete globals.__obsetyncTestDebounce; else globals.__obsetyncTestDebounce = oldDebounce;
        }
    }
}

let completed = false;
process.once("beforeExit", () => {
    if (!completed) { console.error("engine legacy downgrade suite did not finish"); process.exitCode = 1; }
});
void run().then(() => { completed = true; }).catch(error => {
    completed = true; console.error(error); process.exitCode = 1;
});
