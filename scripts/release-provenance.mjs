#!/usr/bin/env node

import { execFile as execFileCallback } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { basename, resolve } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";

const execFile = promisify(execFileCallback);
export const SERVER_IDENTITY_SCHEMA = "obsetync-server-build-identity-v1";
export const CONTAINER_MANIFEST_SCHEMA = "obsetync-container-images-v1";
const SEMVER = /^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$/;
const GIT_SHA1 = /^[0-9a-f]{40}$/;
const OCI_DIGEST = /^sha256:[0-9a-f]{64}$/;
const REPOSITORY = /^ghcr\.io\/[a-z0-9]+(?:[._-][a-z0-9]+)*\/[a-z0-9]+(?:[._-][a-z0-9]+)*$/;

function fail(message) {
    throw new Error(`release provenance: ${message}`);
}

function exactObject(value, keys, label) {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).sort().join(",") !== [...keys].sort().join(",")) {
        fail(`${label} has an invalid schema`);
    }
    return value;
}

function expectedIdentity(version, gitCommit, sourceState) {
    if (!SEMVER.test(version)) fail("expected server version must be exact X.Y.Z");
    if (sourceState === "local-unknown") {
        if (gitCommit !== "unknown") fail("local-unknown identity must use unknown commit");
    } else if (!GIT_SHA1.test(gitCommit) || !["clean", "dirty"].includes(sourceState)) {
        fail("expected server identity needs a full Git SHA-1 and clean/dirty source state");
    }
    return { version, gitCommit, sourceState };
}

export function validateServerBuildIdentity(raw, expected) {
    const expectation = expectedIdentity(expected.version, expected.gitCommit, expected.sourceState);
    let parsed;
    try { parsed = JSON.parse(raw); }
    catch { fail("server build identity is not JSON"); }
    exactObject(parsed, ["schema", "semver", "git_commit", "source_state", "protocol"], "server build identity");
    const protocol = exactObject(parsed.protocol,
        ["api", "secure_transport_wire", "tree", "ws_data", "root_outcome"], "server protocol identity");
    if (raw !== `${JSON.stringify(parsed)}\n`) fail("server build identity is not canonical single-line JSON");
    if (parsed.schema !== SERVER_IDENTITY_SCHEMA || parsed.semver !== expectation.version ||
        parsed.git_commit !== expectation.gitCommit || parsed.source_state !== expectation.sourceState) {
        fail("server build identity does not match the release commit/version/source state");
    }
    if (protocol.api !== 1 || protocol.secure_transport_wire !== 2 || protocol.root_outcome !== 1 ||
        JSON.stringify(protocol.tree) !== "[1,2]" || JSON.stringify(protocol.ws_data) !== "[1,2]") {
        fail("server protocol identity does not match the release contract");
    }
    return parsed;
}

export async function verifyServerBinary(path, expected) {
    const binary = resolve(path);
    let stdout, stderr;
    try {
        ({ stdout, stderr } = await execFile(binary, ["build-identity"], {
            encoding: "utf8", maxBuffer: 64 * 1024, timeout: 10_000,
        }));
    } catch (error) {
        fail(`cannot execute server build identity: ${error instanceof Error ? error.message : String(error)}`);
    }
    if (stderr !== "") fail("server build identity wrote unexpected stderr");
    return validateServerBuildIdentity(stdout, expected);
}

function immutableImage(builder, repository, digest) {
    if (!["dockerfile", "nix"].includes(builder) || !REPOSITORY.test(repository) || !OCI_DIGEST.test(digest)) {
        fail(`invalid ${builder} immutable image identity`);
    }
    return { builder, immutable_ref: `${repository}@${digest}` };
}

export function canonicalContainerManifest({ version, gitCommit, dockerRepository, dockerDigest,
    nixRepository, nixDigest }) {
    expectedIdentity(version, gitCommit, "clean");
    const value = {
        schema: CONTAINER_MANIFEST_SCHEMA,
        semver: version,
        git_commit: gitCommit,
        images: [
            immutableImage("dockerfile", dockerRepository, dockerDigest),
            immutableImage("nix", nixRepository, nixDigest),
        ],
    };
    return `${JSON.stringify(value, null, 2)}\n`;
}

export function validateContainerManifest(raw, expected) {
    let parsed;
    try { parsed = JSON.parse(raw); }
    catch { fail("container image manifest is not JSON"); }
    exactObject(parsed, ["schema", "semver", "git_commit", "images"], "container image manifest");
    if (raw !== `${JSON.stringify(parsed, null, 2)}\n`) fail("container image manifest is not canonical JSON");
    const canonical = canonicalContainerManifest(expected);
    if (raw !== canonical) fail("container image manifest does not match immutable workflow outputs");
    return parsed;
}

export async function runReleaseProvenanceCli(args, write = value => process.stdout.write(value)) {
    const [command, ...rest] = args;
    if (command === "verify-server" && rest.length === 4) {
        const [path, version, gitCommit, sourceState] = rest;
        await verifyServerBinary(path, { version, gitCommit, sourceState });
        write("server build provenance verified\n");
        return;
    }
    if (command === "verify-server-json" && rest.length === 4) {
        const [path, version, gitCommit, sourceState] = rest;
        validateServerBuildIdentity(await readFile(resolve(path), "utf8"), { version, gitCommit, sourceState });
        write("server build provenance JSON verified\n");
        return;
    }
    if ((command === "create-containers" || command === "verify-containers") && rest.length === 7) {
        const [path, version, gitCommit, dockerRepository, dockerDigest, nixRepository, nixDigest] = rest;
        const expected = { version, gitCommit, dockerRepository, dockerDigest, nixRepository, nixDigest };
        if (basename(path) !== "container-images.json") fail("container manifest filename must be container-images.json");
        if (command === "create-containers") {
            await writeFile(resolve(path), canonicalContainerManifest(expected), { encoding: "utf8", flag: "wx", mode: 0o644 });
        }
        validateContainerManifest(await readFile(resolve(path), "utf8"), expected);
        write(`container image provenance ${command === "create-containers" ? "created" : "verified"}\n`);
        return;
    }
    fail("usage: release-provenance.mjs <verify-server|verify-server-json> <binary-or-json> " +
        "<version> <git-sha> <source-state> | " +
        "<create-containers|verify-containers> <container-images.json> <version> <git-sha> " +
        "<docker-repository> <docker-digest> <nix-repository> <nix-digest>");
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    runReleaseProvenanceCli(process.argv.slice(2)).catch(error => {
        process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
        process.exitCode = 1;
    });
}
