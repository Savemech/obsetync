import assert from "node:assert/strict";
import { execFile as execFileCallback } from "node:child_process";
import { mkdtemp, readFile, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";

import {
    canonicalContainerManifest,
    validateContainerManifest,
    validateServerBuildIdentity,
} from "./release-provenance.mjs";

const execFile = promisify(execFileCallback);
const SCRIPT = fileURLToPath(new URL("./release-provenance.mjs", import.meta.url));
const RELEASE_WORKFLOW = fileURLToPath(new URL("../.github/workflows/release.yml", import.meta.url));
function cli(args) {
    return execFile(process.execPath, [SCRIPT, ...args]);
}

const VERSION = "1.2.3";
const SHA = "a".repeat(40);
const expectedIdentity = { version: VERSION, gitCommit: SHA, sourceState: "clean" };
const identity = {
    schema: "obsetync-server-build-identity-v1",
    semver: VERSION,
    git_commit: SHA,
    source_state: "clean",
    protocol: { api: 1, secure_transport_wire: 2, tree: [1, 2], ws_data: [1, 2], root_outcome: 1 },
};
const containers = {
    version: VERSION,
    gitCommit: SHA,
    dockerRepository: "ghcr.io/savemech/obsetync",
    dockerDigest: `sha256:${"b".repeat(64)}`,
    nixRepository: "ghcr.io/savemech/obsetync-nix",
    nixDigest: `sha256:${"c".repeat(64)}`,
};

test("server identity binds exact version, full commit, source and protocol", () => {
    const raw = `${JSON.stringify(identity)}\n`;
    assert.deepEqual(validateServerBuildIdentity(raw, expectedIdentity), identity);
    for (const mutate of [
        value => { value.semver = "1.2.4"; },
        value => { value.git_commit = "a".repeat(39); },
        value => { value.source_state = "dirty"; },
        value => { value.protocol.ws_data = [1]; },
        value => { value.protocol.secure_transport_wire = 1; },
        value => { value.extra = true; },
    ]) {
        const candidate = structuredClone(identity);
        mutate(candidate);
        assert.throws(() => validateServerBuildIdentity(`${JSON.stringify(candidate)}\n`, expectedIdentity),
            /release provenance/);
    }
    assert.throws(() => validateServerBuildIdentity(JSON.stringify(identity), expectedIdentity), /not canonical/);
    assert.throws(() => validateServerBuildIdentity(raw,
        { version: VERSION, gitCommit: "sha256:" + "a".repeat(64), sourceState: "clean" }), /full Git SHA-1/);
});

test("container manifest records only canonical immutable OCI digest references", () => {
    const raw = canonicalContainerManifest(containers);
    const parsed = validateContainerManifest(raw, containers);
    assert.equal(parsed.git_commit, SHA);
    assert.deepEqual(parsed.images, [
        { builder: "dockerfile", immutable_ref: `${containers.dockerRepository}@${containers.dockerDigest}` },
        { builder: "nix", immutable_ref: `${containers.nixRepository}@${containers.nixDigest}` },
    ]);
    assert.throws(() => canonicalContainerManifest({ ...containers, dockerDigest: SHA }), /immutable image identity/);
    assert.throws(() => canonicalContainerManifest({ ...containers, nixRepository: "GHCR.IO/Owner/Image" }),
        /immutable image identity/);
    assert.throws(() => validateContainerManifest(raw.replace("dockerfile", "nix"), containers),
        /does not match immutable workflow outputs/);
    assert.throws(() => validateContainerManifest(raw.trimEnd(), containers), /not canonical/);
});

test("CLI creates once, verifies exact inputs, and rejects unsafe output", async () => {
    const directory = await mkdtemp(join(tmpdir(), "obsetync-container-provenance-"));
    try {
        const output = join(directory, "container-images.json");
        const args = [output, VERSION, SHA, containers.dockerRepository, containers.dockerDigest,
            containers.nixRepository, containers.nixDigest];
        const alias = join(directory, "release-tool.mjs");
        await symlink(SCRIPT, alias);
        const created = await execFile(process.execPath, [alias, "create-containers", ...args]);
        assert.match(created.stdout, /created/);
        assert.equal(await readFile(output, "utf8"), canonicalContainerManifest(containers));
        const verified = await cli(["verify-containers", ...args]);
        assert.match(verified.stdout, /verified/);
        await assert.rejects(cli(["create-containers", ...args]), /EEXIST/);
        await assert.rejects(cli(["create-containers", join(directory, "wrong.json"), ...args.slice(1)]),
            /filename must be container-images\.json/);
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
});

test("release workflow keeps continued provenance commands in literal blocks", async () => {
    const workflow = await readFile(RELEASE_WORKFLOW, "utf8");
    assert.doesNotMatch(workflow, /^\s*run:\s+node scripts\/release-provenance\.mjs[^\n]*\\\n/gm);
});
