import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolveBuildIdentity, validateBuildIdentityInputs } from "./build-identity-config.mjs";

const SHA = "1".repeat(40);
const OTHER_SHA = "2".repeat(40);
const manifest = version => ({ id: "obsetync", version });
const valid = overrides => ({
    rootManifest: manifest("1.11.4"),
    pluginManifest: manifest("1.11.4"),
    gitCommit: SHA,
    production: true,
    ...overrides,
});

assert.deepEqual(validateBuildIdentityInputs(valid()), {
    semver: "1.11.4",
    gitCommit: SHA,
    sourceState: "clean",
    buildMode: "production",
});
assert.throws(() => validateBuildIdentityInputs(valid({
    expectedCommit: OTHER_SHA,
})), /does not match the checkout/);
assert.throws(() => validateBuildIdentityInputs(valid({
    expectedCommit: undefined,
    requireExpectedCommit: true,
})), /required expected Git commit is missing/);
assert.throws(() => validateBuildIdentityInputs(valid({
    gitCommit: undefined,
})), /cannot determine the checked-out Git commit/);
assert.throws(() => validateBuildIdentityInputs(valid({
    pluginManifest: manifest("1.11.5"),
})), /manifest version mismatch/);
assert.throws(() => validateBuildIdentityInputs(valid({
    dirty: true,
    requireClean: true,
})), /tracked modifications/);
assert.equal(validateBuildIdentityInputs(valid({ dirty: true })).sourceState, "dirty");
assert.deepEqual(validateBuildIdentityInputs(valid({
    gitCommit: undefined,
    production: false,
})), {
    semver: "1.11.4",
    gitCommit: "unknown",
    sourceState: "local-unknown",
    buildMode: "development",
});

const isolated = await mkdtemp(join(tmpdir(), "obsetync-build-identity-"));
try {
    const pluginDirectory = join(isolated, "plugin");
    await mkdir(pluginDirectory);
    await writeFile(join(pluginDirectory, "manifest.json"), JSON.stringify(manifest("1.11.4")));
    assert.deepEqual(await resolveBuildIdentity({
        pluginDirectory,
        production: true,
        environment: {
            OBSETYNC_BUILD_GIT_COMMIT: SHA,
            OBSETYNC_BUILD_EXPECTED_COMMIT: SHA,
            OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT: "1",
            OBSETYNC_BUILD_REQUIRE_CLEAN: "1",
            OBSETYNC_BUILD_EXPECTED_VERSION: "1.11.4",
        },
    }), {
        semver: "1.11.4",
        gitCommit: SHA,
        sourceState: "clean",
        buildMode: "production",
    });
    await assert.rejects(() => resolveBuildIdentity({
        pluginDirectory,
        production: true,
        environment: {},
    }), /cannot determine the checked-out Git commit/);
    assert.equal((await resolveBuildIdentity({
        pluginDirectory,
        production: true,
        environment: {
            OBSETYNC_BUILD_GIT_COMMIT: "unknown",
            OBSETYNC_BUILD_SOURCE_STATE: "local-unknown",
            OBSETYNC_BUILD_ALLOW_LOCAL_UNKNOWN: "1",
        },
    })).sourceState, "local-unknown");
} finally {
    await rm(isolated, { recursive: true, force: true });
}
console.log("build identity config tests passed");
