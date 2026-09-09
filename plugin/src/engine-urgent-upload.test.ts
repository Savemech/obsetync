import { strict as assert } from "node:assert";
import { BulkObjectKind } from "./bulk-codec";
import { fixture, hashFor, pathAt } from "./engine-root-test-fixture";

(globalThis as any).window ??= globalThis;

async function run(): Promise<void> {
    const globals = globalThis as any;
    const oldNotice = globals.__obsetyncTestNotice;
    const oldDebounce = globals.__obsetyncTestDebounce;
    globals.__obsetyncTestNotice = () => ({ setMessage() {}, hide() {} });
    globals.__obsetyncTestDebounce = () => () => {};
    const f = await fixture(), active = await f.create("urgent-object-route");
    const path = pathAt(0);
    try {
        f.control.contentMissing = true;
        f.control.activePath = path;
        await f.event(active.engine, path, 2);
        await active.engine.pushPending();
        assert.deepEqual(f.server.uploadPriorities, ["urgent-file"],
            "actual engine active-file lane did not reach the object API");
        assert.deepEqual(f.server.uploadedKinds, [BulkObjectKind.Content],
            "urgent active-file save was not one exact content record");
        assert.equal(f.base.getHash(path), hashFor(2));
        console.log("engine urgent upload: active-file lane reached one bounded content PUT");
    } finally {
        await f.close();
        if (oldNotice === undefined) delete globals.__obsetyncTestNotice;
        else globals.__obsetyncTestNotice = oldNotice;
        if (oldDebounce === undefined) delete globals.__obsetyncTestDebounce;
        else globals.__obsetyncTestDebounce = oldDebounce;
    }
}

void run().catch(error => { setTimeout(() => { throw error; }, 0); });
