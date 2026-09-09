import assert from "node:assert/strict";
import {
    BUILD_IDENTITY_SCHEMA,
    BUILD_PROTOCOL_CAPABILITIES,
    BUILD_PROTOCOL_SCHEMA,
    compareBuildIdentity,
    createBuildIdentity,
} from "./build-identity-contract";

const SHA = "a".repeat(40);
const identity = createBuildIdentity({
    semver: "1.11.4",
    gitCommit: SHA,
    sourceState: "clean",
    buildMode: "production",
});
assert.equal(identity.schema, BUILD_IDENTITY_SCHEMA);
assert.equal(identity.protocol.schema, BUILD_PROTOCOL_SCHEMA);
assert.equal(identity.protocol.httpWire, "0x02");
assert.deepEqual(identity.protocol.capabilities, [...BUILD_PROTOCOL_CAPABILITIES]);
assert.deepEqual(compareBuildIdentity("1.11.4", identity), {
    status: "match", compatible: true, diagnostic: "MATCH",
});
assert.deepEqual(compareBuildIdentity("1.11.5", identity), {
    status: "version-mismatch",
    compatible: false,
    diagnostic: "MISMATCH manifest 1.11.5 != bundle 1.11.4",
});
assert.throws(() => createBuildIdentity({
    semver: "1.11.4", gitCommit: "missing", sourceState: "clean", buildMode: "production",
}), /requires a full Git commit/);
assert.equal(createBuildIdentity({
    semver: "1.11.4", gitCommit: "unknown", sourceState: "local-unknown", buildMode: "production",
}).sourceState, "local-unknown");
assert.equal(compareBuildIdentity("1.11.4", createBuildIdentity({
    semver: "1.11.4", gitCommit: SHA, sourceState: "dirty", buildMode: "production",
})).status, "dirty");
assert.equal(compareBuildIdentity("1.11.4", createBuildIdentity({
    semver: "1.11.4", gitCommit: "unknown", sourceState: "local-unknown", buildMode: "development",
})).status, "local-unknown");

console.log("build-identity.test: embedded identity and mismatch diagnostics passed");
