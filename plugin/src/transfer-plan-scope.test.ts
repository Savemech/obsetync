import { strict as assert } from "node:assert";
import { preparedScopeForTree, type PreparedScopeIdentity } from "./transfer-plan-scope";

async function run(): Promise<void> {
    const identity: PreparedScopeIdentity = { vaultId: "synthetic", serverUrl: "https://sync.invalid",
        serverBoxPub: "public-key", deviceId: "local-device", syncObsidianConfig: false,
        ignorePatterns: ["ignored/"] };
    const original = { ...identity, ignorePatterns: [...identity.ignorePatterns] };
    const scoped = preparedScopeForTree(identity);
    identity.vaultId = "changed";
    (identity.ignorePatterns as string[]).push("new/");
    const first = await scoped(1);
    assert.match(first, /^[0-9a-f]{64}$/);
    assert.equal(first, await preparedScopeForTree(original)(1), "identity inputs were not captured");
    assert.equal(scoped(1), scoped(1), "same identity tree scope rehashed needlessly");
    assert.notEqual(first, await scoped(2));
    for (const key of ["vaultId", "serverUrl", "serverBoxPub", "deviceId"] as const) {
        assert.notEqual(first, await preparedScopeForTree({ ...original, [key]: original[key] + "-new" })(1));
    }
    assert.notEqual(first, await preparedScopeForTree({ ...original, syncObsidianConfig: true })(1));
    assert.notEqual(first, await preparedScopeForTree({ ...original, ignorePatterns: [] })(1));
    await assert.rejects(scoped(3));
    assert.throws(() => preparedScopeForTree({ ...original, ignorePatterns: ["x".repeat(8193)] }));
    assert.throws(() => preparedScopeForTree({ ...original, ignorePatterns: Array(20).fill("x".repeat(8192)) }));
    assert.throws(() => preparedScopeForTree({ ...original, ignorePatterns: Array(4097).fill("") }));
    console.log("transfer-plan-scope.test: captured enrollment/policy/format isolation and bounds passed");
}
void run().catch(error => { console.error(error); process.exitCode = 1; });
