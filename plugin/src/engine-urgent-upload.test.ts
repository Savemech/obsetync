import { strict as assert } from "node:assert";
import { BulkObjectKind } from "./bulk-codec";
import { fixture, hashFor, pathAt } from "./engine-root-test-fixture";

(globalThis as any).window ??= globalThis;

async function run(): Promise<void> {
    const globals = globalThis as any;
    const oldNotice = globals.__obsetyncTestNotice;
    globals.__obsetyncTestNotice = () => ({ setMessage() {}, hide() {} });
    const f = await fixture(), active = await f.create("urgent-object-route");
    const path = pathAt(0);
    try {
        f.control.contentMissing = true;
        f.control.activePath = path;
        await f.queue(active.engine, [path], 2);
        await active.engine.pushPending();
        assert.deepEqual(f.server.uploadPriorities, ["urgent-file"],
            "actual engine active-file lane did not reach the object API");
        assert.deepEqual(f.server.uploadedKinds, [BulkObjectKind.Content],
            "urgent active-file save was not one exact content record");
        assert.equal(f.base.getHash(path), hashFor(2));
        f.control.activePath = null;
        await f.queue(active.engine, [path], 3);
        await active.engine.pushPending();
        await f.event(active.engine, path, 4);
        await active.engine.pushPending();
        assert.deepEqual(f.server.uploadPriorities, ["urgent-file", undefined, "urgent-file"],
            "inactive backlog or fresh inactive edit used the wrong upload priority");
        assert.equal(f.base.getHash(path), hashFor(4));
        console.log("engine urgent upload: active note, ordinary backlog and fresh edit routed independently");
    } finally {
        await f.close();
        if (oldNotice === undefined) delete globals.__obsetyncTestNotice;
        else globals.__obsetyncTestNotice = oldNotice;
    }
}

void run().catch(error => { setTimeout(() => { throw error; }, 0); });
