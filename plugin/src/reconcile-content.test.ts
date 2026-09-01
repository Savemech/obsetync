import { strict as assert } from "node:assert";
import {
    buildReconcileContentIndex,
    sumIndexedContentBytes,
} from "./reconcile-content";

function run(): void {
    const index = buildReconcileContentIndex([
        { path: "first.md", hash: "same", size: 10 },
        { path: "copy.md", hash: "same", size: 10 },
        { path: "other.md", hash: "other", size: 5 },
        { path: "large.bin", hash: "large", size: 20 },
    ], 16);

    assert.equal(index.filesTotal, 4, "physical tracked file count was deduplicated");
    assert.equal(index.bytesTotal, 45, "physical tracked bytes were deduplicated");
    assert.equal(index.smallByHash.size, 2, "small content index did not deduplicate hashes");
    assert.equal(index.largeByHash.size, 1, "large content index did not classify threshold");
    assert.deepEqual(index.smallByHash.get("same"), { path: "first.md", size: 10 });
    assert.equal(
        sumIndexedContentBytes(["same", "same", "unknown"], index.smallByHash),
        10,
        "planned content bytes counted a duplicate or unknown hash",
    );

    console.log("reconcile-content.test: 6 assertions passed");
}

run();
