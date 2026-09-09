import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

const FULL_GIT_COMMIT = /^[0-9a-f]{40}$/;
const SEMVER = /^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;

function fail(message) {
    throw new Error(`build identity: ${message}`);
}

function manifestVersion(manifest, label) {
    if (!manifest || typeof manifest !== "object" || Array.isArray(manifest) ||
        typeof manifest.version !== "string" || !SEMVER.test(manifest.version)) {
        fail(`${label} has no valid semantic version`);
    }
    return manifest.version;
}

export function validateBuildIdentityInputs({
    rootManifest,
    pluginManifest,
    gitCommit,
    dirty = false,
    production = false,
    expectedCommit,
    requireExpectedCommit = false,
    requireClean = false,
    allowUnknownProduction = false,
}) {
    const pluginVersion = manifestVersion(pluginManifest, "plugin manifest");
    const rootVersion = rootManifest === undefined
        ? pluginVersion : manifestVersion(rootManifest, "root manifest");
    if (rootManifest !== undefined && rootVersion !== pluginVersion) {
        fail(`manifest version mismatch (${rootVersion} != ${pluginVersion})`);
    }
    if (expectedCommit !== undefined && !FULL_GIT_COMMIT.test(expectedCommit)) {
        fail("expected Git commit must be a full lowercase SHA-1");
    }
    if (requireExpectedCommit && expectedCommit === undefined) {
        fail("required expected Git commit is missing");
    }
    if (gitCommit !== undefined && !FULL_GIT_COMMIT.test(gitCommit)) {
        fail("checked-out Git commit is not a full lowercase SHA-1");
    }
    if (production && gitCommit === undefined && !allowUnknownProduction) {
        fail("production build cannot determine the checked-out Git commit");
    }
    if (expectedCommit !== undefined && gitCommit !== expectedCommit) {
        fail("expected Git commit does not match the checkout");
    }
    if (requireClean && dirty) fail("release checkout has tracked modifications");
    return Object.freeze({
        semver: pluginVersion,
        gitCommit: gitCommit ?? "unknown",
        sourceState: gitCommit === undefined ? "local-unknown" : dirty ? "dirty" : "clean",
        buildMode: production ? "production" : "development",
    });
}

function git(repoDirectory, args) {
    try {
        return execFileSync("git", args, {
            cwd: repoDirectory,
            encoding: "utf8",
            stdio: ["ignore", "pipe", "ignore"],
        }).trim();
    } catch {
        return undefined;
    }
}

async function readJson(path, label) {
    try { return JSON.parse(await readFile(path, "utf8")); }
    catch { fail(`${label} is unreadable or invalid JSON`); }
}

async function readOptionalJson(path, label) {
    try { return JSON.parse(await readFile(path, "utf8")); }
    catch (error) {
        if (error?.code === "ENOENT") return undefined;
        fail(`${label} is unreadable or invalid JSON`);
    }
}

export async function resolveBuildIdentity({ pluginDirectory, production, environment = process.env }) {
    const pluginRoot = resolve(pluginDirectory);
    const repoDirectory = resolve(pluginRoot, "..");
    // Nix intentionally builds with src=./plugin, so the repo-root manifest
    // and .git are absent. A trusted source revision is injected there; when
    // the root manifest is present (normal checkout/release), parity remains
    // mandatory.
    const rootManifest = await readOptionalJson(join(repoDirectory, "manifest.json"), "root manifest");
    const pluginManifest = await readJson(join(pluginRoot, "manifest.json"), "plugin manifest");
    const checkoutCommit = git(repoDirectory, ["rev-parse", "--verify", "HEAD"]);
    const allowLocalUnknown = environment.OBSETYNC_BUILD_ALLOW_LOCAL_UNKNOWN === "1";
    const injectedValue = environment.OBSETYNC_BUILD_GIT_COMMIT || undefined;
    const injectedCommit = injectedValue === "unknown" && allowLocalUnknown &&
        environment.OBSETYNC_BUILD_SOURCE_STATE === "local-unknown"
        ? undefined : injectedValue;
    if (injectedCommit !== undefined && checkoutCommit !== undefined && injectedCommit !== checkoutCommit) {
        fail("injected Git commit does not match the checkout");
    }
    const commit = injectedCommit ?? checkoutCommit;
    const trackedChanges = git(repoDirectory, ["status", "--porcelain", "--untracked-files=no"]);
    const expectedCommit = environment.OBSETYNC_BUILD_EXPECTED_COMMIT || undefined;
    const expectedVersion = environment.OBSETYNC_BUILD_EXPECTED_VERSION || undefined;
    const dirty = checkoutCommit === undefined
        ? environment.OBSETYNC_BUILD_SOURCE_STATE === "dirty"
        : trackedChanges === undefined || trackedChanges.length > 0;
    if (expectedVersion !== undefined && expectedVersion !== pluginManifest.version) {
        fail(`expected semantic version mismatch (${expectedVersion} != ${pluginManifest.version})`);
    }
    return validateBuildIdentityInputs({
        rootManifest,
        pluginManifest,
        gitCommit: commit,
        dirty,
        production,
        expectedCommit,
        requireExpectedCommit: environment.OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT === "1",
        requireClean: environment.OBSETYNC_BUILD_REQUIRE_CLEAN === "1",
        allowUnknownProduction: allowLocalUnknown &&
            environment.OBSETYNC_BUILD_SOURCE_STATE === "local-unknown",
    });
}
