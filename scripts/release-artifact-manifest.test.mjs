import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, rm, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
    RELEASE_ARTIFACT_MANIFEST,
    RELEASE_ARTIFACT_SCHEMA,
    RELEASE_STATIC_ASSETS,
    expectedReleaseAssets,
    generateReleaseArtifactManifest,
    verifyReleaseArtifactManifest,
} from "./release-artifact-manifest.mjs";

const ZIP = "obsetync-v1.2.3.zip";

async function fixture() {
    const directory = await mkdtemp(join(tmpdir(), "obsetync-release-manifest-"));
    for (const [index, name] of expectedReleaseAssets(ZIP).entries()) {
        await writeFile(join(directory, name), Buffer.from(`asset-${index}-${name}\n`));
    }
    return directory;
}

async function rejects(operation, pattern) {
    await assert.rejects(operation, pattern);
}

async function deterministicAndSelfExcluding() {
    const directory = await fixture();
    try {
        const first = await generateReleaseArtifactManifest(directory, ZIP);
        const second = await generateReleaseArtifactManifest(directory, ZIP);
        assert.equal(second, first, "same final assets produced different manifests");
        const parsed = JSON.parse(first);
        assert.equal(parsed.schema, RELEASE_ARTIFACT_SCHEMA);
        assert.deepEqual(parsed.artifacts.map(row => row.name), expectedReleaseAssets(ZIP));
        assert.ok(!parsed.artifacts.some(row => row.name === RELEASE_ARTIFACT_MANIFEST),
            "manifest attempted to hash itself");
        for (const row of parsed.artifacts) {
            const bytes = await readFile(join(directory, row.name));
            assert.equal(row.bytes, bytes.byteLength);
            assert.equal(row.sha256, createHash("sha256").update(bytes).digest("hex"));
        }
        await verifyReleaseArtifactManifest(directory, ZIP);
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
}

async function exactFlatSetIsRequired() {
    const missing = await fixture();
    try {
        await unlink(join(missing, RELEASE_STATIC_ASSETS[0]));
        await rejects(() => generateReleaseArtifactManifest(missing, ZIP), /missing required flat asset/);
    } finally { await rm(missing, { recursive: true, force: true }); }

    const extra = await fixture();
    try {
        await writeFile(join(extra, "debug.map"), "not a release asset");
        await rejects(() => generateReleaseArtifactManifest(extra, ZIP), /unexpected release entry/);
    } finally { await rm(extra, { recursive: true, force: true }); }

    const nested = await fixture();
    try {
        await mkdir(join(nested, "nested"));
        await rejects(() => generateReleaseArtifactManifest(nested, ZIP), /unexpected release entry/);
    } finally { await rm(nested, { recursive: true, force: true }); }
}

async function tamperAndWrongZipFailClosed() {
    const directory = await fixture();
    try {
        await generateReleaseArtifactManifest(directory, ZIP);
        await writeFile(join(directory, "main.js"), "tampered\n");
        await rejects(() => verifyReleaseArtifactManifest(directory, ZIP), /artifact size or SHA-256 mismatch/);
        await rejects(() => verifyReleaseArtifactManifest(directory, "obsetync-v1.2.4.zip"),
            /missing required flat asset/);
    } finally { await rm(directory, { recursive: true, force: true }); }
}

async function nonCanonicalAndSelfEntriesAreRejected() {
    const directory = await fixture();
    try {
        const canonical = await generateReleaseArtifactManifest(directory, ZIP);
        await writeFile(join(directory, RELEASE_ARTIFACT_MANIFEST), canonical.trimEnd());
        await rejects(() => verifyReleaseArtifactManifest(directory, ZIP), /encoding is not canonical/);

        const parsed = JSON.parse(canonical);
        parsed.artifacts.push({ name: RELEASE_ARTIFACT_MANIFEST, bytes: 0, sha256: "0".repeat(64) });
        await writeFile(join(directory, RELEASE_ARTIFACT_MANIFEST), `${JSON.stringify(parsed, null, 2)}\n`);
        await rejects(() => verifyReleaseArtifactManifest(directory, ZIP), /invalid manifest schema/);
    } finally { await rm(directory, { recursive: true, force: true }); }
}

await deterministicAndSelfExcluding();
await exactFlatSetIsRequired();
await tamperAndWrongZipFailClosed();
await nonCanonicalAndSelfEntriesAreRejected();
console.log("release artifact manifest tests passed");
