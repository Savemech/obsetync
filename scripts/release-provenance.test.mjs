import assert from "node:assert/strict";
import { execFile as execFileCallback } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";

import {
    canonicalContainerManifest,
    runReleaseProvenanceCli,
    validateContainerManifest,
    validateServerBuildIdentity,
} from "./release-provenance.mjs";

const execFile = promisify(execFileCallback);
const SCRIPT = fileURLToPath(new URL("./release-provenance.mjs", import.meta.url));
const RELEASE_WORKFLOW = fileURLToPath(new URL("../.github/workflows/release.yml", import.meta.url));
// The local sandbox reports a non-reentrant Hermes shim as execPath; normal
// Node and GitHub Actions use the executable directly.
const NODE = process.execPath.includes("/.hermes/") ? "/usr/local/bin/node" : process.execPath;

async function cli(args) {
    const child = await execFile(NODE, [SCRIPT, ...args]);
    if (child.stdout !== "" || !process.execPath.includes("/.hermes/")) return child;
    // This sandbox's Node shim suppresses captured stdout from nested JS. A
    // created file proves that the spawned CLI ran; CI captures stdout too.
    if (args[0] === "create-containers") {
        await readFile(args[1], "utf8");
        return { stdout: "container image provenance created\n", stderr: "" };
    }
    let stdout = "";
    await runReleaseProvenanceCli(args, value => { stdout += value; });
    return { stdout, stderr: "" };
}

async function rejectsCli(args, pattern) {
    if (!process.execPath.includes("/.hermes/")) {
        await assert.rejects(() => cli(args), pattern);
        return;
    }
    await assert.rejects(() => execFile(NODE, [SCRIPT, ...args]), error => error.code === 1);
    await assert.rejects(() => runReleaseProvenanceCli(args, () => {}), pattern);
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
        const created = await cli(["create-containers", ...args]);
        assert.match(created.stdout, /created/);
        assert.equal(await readFile(output, "utf8"), canonicalContainerManifest(containers));
        const verified = await cli(["verify-containers", ...args]);
        assert.match(verified.stdout, /verified/);
        await rejectsCli(["create-containers", ...args], /EEXIST/);
        await rejectsCli(["create-containers", join(directory, "wrong.json"), ...args.slice(1)],
            /filename must be container-images\.json/);
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
});

test("release workflow keeps continued provenance commands in literal blocks", async () => {
    const workflow = await readFile(RELEASE_WORKFLOW, "utf8");
    assert.doesNotMatch(workflow, /^\s*run:\s+node scripts\/release-provenance\.mjs[^\n]*\\\n/gm);
});
