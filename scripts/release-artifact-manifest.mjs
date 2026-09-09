#!/usr/bin/env node

import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { open, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { basename, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const RELEASE_ARTIFACT_MANIFEST = "artifact-manifest.json";
export const RELEASE_ARTIFACT_SCHEMA = "obsetync-release-artifact-manifest-v1";
export const RELEASE_STATIC_ASSETS = Object.freeze([
    "container-images.json",
    "main.js",
    "manifest.json",
    "sync-server",
    "styles.css",
    "sync_core.js",
    "sync_core_bg.wasm",
    "sync_core_simd.js",
    "sync_core_simd_bg.wasm",
    "versions.json",
]);

const MAX_MANIFEST_BYTES = 128 * 1024;
const ZIP_NAME = /^obsetync-[A-Za-z0-9][A-Za-z0-9._-]{0,100}\.zip$/;
const SHA256 = /^[0-9a-f]{64}$/;

function fail(message) {
    throw new Error(`release artifact manifest: ${message}`);
}

function validateZipName(zipName) {
    if (typeof zipName !== "string" || basename(zipName) !== zipName ||
        !ZIP_NAME.test(zipName) || zipName.includes("..")) {
        fail("invalid release ZIP name");
    }
    return zipName;
}

export function expectedReleaseAssets(zipName) {
    return [...RELEASE_STATIC_ASSETS, validateZipName(zipName)].sort();
}

function canonicalManifest(artifacts) {
    return `${JSON.stringify({
        schema: RELEASE_ARTIFACT_SCHEMA,
        algorithm: "sha256",
        artifacts,
    }, null, 2)}\n`;
}

async function inventory(directory, zipName, requireManifest) {
    const expectedAssets = expectedReleaseAssets(zipName);
    const expected = new Set(expectedAssets);
    if (requireManifest) expected.add(RELEASE_ARTIFACT_MANIFEST);
    const entries = await readdir(directory, { withFileTypes: true });
    const actual = new Set(entries.map(entry => entry.name));

    for (const name of expected) {
        if (!actual.has(name)) fail(`missing required flat asset: ${name}`);
    }
    for (const entry of entries) {
        const allowedGeneratedManifest = !requireManifest &&
            entry.name === RELEASE_ARTIFACT_MANIFEST;
        if (!expected.has(entry.name) && !allowedGeneratedManifest) {
            fail(`unexpected release entry: ${entry.name}`);
        }
        if (!entry.isFile()) fail(`release entry is not a flat regular file: ${entry.name}`);
    }
    return expectedAssets;
}

async function hashFile(directory, name) {
    const path = join(directory, name);
    let handle;
    try {
        handle = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
        const before = await handle.stat();
        if (!before.isFile() || !Number.isSafeInteger(before.size) || before.size < 0) {
            fail(`invalid regular file: ${name}`);
        }
        const digest = createHash("sha256");
        for await (const chunk of handle.createReadStream({ autoClose: false })) digest.update(chunk);
        const after = await handle.stat();
        if (after.size !== before.size || after.mtimeMs !== before.mtimeMs) {
            fail(`asset changed while hashing: ${name}`);
        }
        return { name, bytes: before.size, sha256: digest.digest("hex") };
    } finally {
        await handle?.close();
    }
}

async function measuredArtifacts(directory, names) {
    const artifacts = [];
    for (const name of names) artifacts.push(await hashFile(directory, name));
    return artifacts;
}

function validateParsedManifest(value, expectedNames) {
    if (!value || typeof value !== "object" || Array.isArray(value) ||
        Object.keys(value).sort().join(",") !== "algorithm,artifacts,schema" ||
        value.schema !== RELEASE_ARTIFACT_SCHEMA || value.algorithm !== "sha256" ||
        !Array.isArray(value.artifacts) || value.artifacts.length !== expectedNames.length) {
        fail("invalid manifest schema");
    }
    for (let index = 0; index < expectedNames.length; index++) {
        const row = value.artifacts[index];
        if (!row || typeof row !== "object" || Array.isArray(row) ||
            Object.keys(row).sort().join(",") !== "bytes,name,sha256" ||
            row.name !== expectedNames[index] || row.name === RELEASE_ARTIFACT_MANIFEST ||
            !Number.isSafeInteger(row.bytes) || row.bytes < 0 ||
            typeof row.sha256 !== "string" || !SHA256.test(row.sha256)) {
            fail(`invalid manifest artifact at index ${index}`);
        }
    }
    return value.artifacts;
}

export async function verifyReleaseArtifactManifest(directory, zipName) {
    const releaseDirectory = resolve(directory);
    const names = await inventory(releaseDirectory, zipName, true);
    const manifestPath = join(releaseDirectory, RELEASE_ARTIFACT_MANIFEST);
    const info = await open(manifestPath, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
    let raw;
    try {
        const stat = await info.stat();
        if (!stat.isFile() || stat.size <= 0 || stat.size > MAX_MANIFEST_BYTES) {
            fail("manifest size is invalid");
        }
        raw = await readFile(info, "utf8");
    } finally {
        await info.close();
    }
    let parsed;
    try { parsed = JSON.parse(raw); }
    catch { fail("manifest is not valid JSON"); }
    const declared = validateParsedManifest(parsed, names);
    if (raw !== canonicalManifest(declared)) fail("manifest encoding is not canonical");
    const actual = await measuredArtifacts(releaseDirectory, names);
    if (canonicalManifest(actual) !== raw) fail("artifact size or SHA-256 mismatch");
    return parsed;
}

export async function generateReleaseArtifactManifest(directory, zipName) {
    const releaseDirectory = resolve(directory);
    const names = await inventory(releaseDirectory, zipName, false);
    const artifacts = await measuredArtifacts(releaseDirectory, names);
    const output = canonicalManifest(artifacts);
    const manifestPath = join(releaseDirectory, RELEASE_ARTIFACT_MANIFEST);
    const temporaryPath = join(releaseDirectory, `.${RELEASE_ARTIFACT_MANIFEST}.${process.pid}.tmp`);
    try {
        await writeFile(temporaryPath, output, { encoding: "utf8", flag: "wx", mode: 0o644 });
        await rename(temporaryPath, manifestPath);
    } finally {
        await rm(temporaryPath, { force: true });
    }
    await verifyReleaseArtifactManifest(releaseDirectory, zipName);
    return output;
}

function usage() {
    return "usage: node scripts/release-artifact-manifest.mjs <generate|verify> <release-dir> <zip-name>";
}

async function main(argv) {
    if (argv.length !== 3) fail(usage());
    const [command, directory, zipName] = argv;
    if (command === "generate") {
        await generateReleaseArtifactManifest(directory, zipName);
    } else if (command === "verify") {
        await verifyReleaseArtifactManifest(directory, zipName);
    } else {
        fail(usage());
    }
    process.stdout.write(`release artifact manifest ${command}: ok\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    main(process.argv.slice(2)).catch(error => {
        process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
        process.exitCode = 1;
    });
}
