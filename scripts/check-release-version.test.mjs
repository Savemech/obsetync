import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";

import { checkReleaseVersion } from "./check-release-version.mjs";

const REPO = resolve(import.meta.dirname, "..");
const FILES = [
    "Cargo.toml",
    "manifest.json",
    "plugin/manifest.json",
    "plugin/package.json",
    "plugin/package-lock.json",
    "versions.json",
    "flake.nix",
];
const source = Object.fromEntries(await Promise.all(FILES.map(async path => [path, await readFile(join(REPO, path), "utf8")])));
const VERSION = JSON.parse(source["manifest.json"]).version;
const MINIMUM = JSON.parse(source["manifest.json"]).minAppVersion;
const escaped = value => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

function replaceOne(value, before, after, label = before) {
    const first = value.indexOf(before);
    assert.notEqual(first, -1, `fixture did not find ${label}`);
    assert.equal(value.indexOf(before, first + before.length), -1, `fixture found duplicate ${label}`);
    return value.slice(0, first) + after + value.slice(first + before.length);
}

async function fixture(mutate = () => {}) {
    const root = await mkdtemp(join(tmpdir(), "obsetync-release-version-"));
    const files = { ...source };
    mutate(files);
    for (const [path, contents] of Object.entries(files)) {
        await mkdir(dirname(join(root, path)), { recursive: true });
        await writeFile(join(root, path), contents);
    }
    return { root, close: () => rm(root, { recursive: true, force: true }) };
}

async function run(mutate, options = {}) {
    const copy = await fixture(mutate);
    try {
        return await checkReleaseVersion({
            rootDir: copy.root,
            eventName: options.eventName ?? "push",
            refName: options.refName ?? `v${VERSION}`,
        });
    } finally {
        await copy.close();
    }
}

async function rejects(mutate, pattern, options) {
    await assert.rejects(() => run(mutate, options), pattern);
}

test("accepts only exact matching bare and v-prefixed release tags", async () => {
    const prefixed = await run(() => {});
    const bare = await run(() => {}, { refName: VERSION });
    assert.deepEqual(prefixed, { version: VERSION, minimumAppVersion: MINIMUM,
        tag: `v${VERSION}`, declarations: 10 });
    assert.deepEqual(bare, { version: VERSION, minimumAppVersion: MINIMUM,
        tag: VERSION, declarations: 10 });
});

test("requires the release push event and an exact stable tag", async () => {
    const next = VERSION.replace(/([0-9]+)$/, value => String(Number(value) + 1));
    await rejects(() => {}, /release event must be push/, { eventName: "workflow_dispatch" });
    await rejects(() => {}, /release ref name is missing/, { refName: "" });
    await rejects(() => {}, /exact X\.Y\.Z/, { refName: `v${VERSION}-rc.1` });
    await rejects(() => {}, /Git SHA-1, not an X\.Y\.Z/, { refName: "a".repeat(40) });
    await rejects(() => {}, /exact X\.Y\.Z/, { refName: `vv${VERSION}` });
    await rejects(() => {}, /release tag is .* expected/, { refName: `v${next}` });
});

test("rejects every mismatched structured version source", async () => {
    const next = VERSION.replace(/([0-9]+)$/, value => String(Number(value) + 1));
    const cases = [
        ["Cargo.toml", `version = "${VERSION}"`, `version = "${next}"`, /manifest\.json version is .* expected/],
        ["manifest.json", `"version": "${VERSION}"`, `"version": "${next}"`, /manifest\.json version is/],
        ["plugin/manifest.json", `"version": "${VERSION}"`, `"version": "${next}"`, /plugin\/manifest\.json version is/],
        ["plugin/package.json", `"version": "${VERSION}"`, `"version": "${next}"`, /plugin\/package\.json version is/],
        ["plugin/package-lock.json", `{\n  "name": "obsetync",\n  "version": "${VERSION}",`,
            `{\n  "name": "obsetync",\n  "version": "${next}",`, /package-lock\.json version is/],
        ["flake.nix", `pname = "obsetync";\n          version = "${VERSION}";`,
            `pname = "obsetync";\n          version = "${next}";`, /commonArgs\.version is/],
        ["flake.nix", `pname   = "sync-core-wasm-bindings";\n          version = "${VERSION}";`,
            `pname   = "sync-core-wasm-bindings";\n          version = "${next}";`, /sync-core-wasm\.version is/],
        ["flake.nix", `pname   = "obsetync-plugin";\n          version = "${VERSION}";`,
            `pname   = "obsetync-plugin";\n          version = "${next}";`, /flake\.nix plugin\.version is/],
        ["flake.nix", `OBSETYNC_BUILD_EXPECTED_VERSION = "${VERSION}";`,
            `OBSETYNC_BUILD_EXPECTED_VERSION = "${next}";`, /OBSETYNC_BUILD_EXPECTED_VERSION is/],
    ];
    for (const [path, before, after, pattern] of cases) {
        await rejects(files => { files[path] = replaceOne(files[path], before, after, path); }, pattern);
    }

    await rejects(files => {
        const needle = `"": {\n      "name": "obsetync",\n      "version": "${VERSION}"`;
        files["plugin/package-lock.json"] = replaceOne(files["plugin/package-lock.json"], needle,
            `"": {\n      "name": "obsetync",\n      "version": "${next}"`, "package-lock root package version");
    }, /packages\[''\]\.version is/);
});

test("fails closed on missing, duplicate, and non-literal declarations", async () => {
    await rejects(files => {
        files["manifest.json"] = replaceOne(files["manifest.json"], `"version": "${VERSION}",`,
            `"version": "${VERSION}",\n    "version": "${VERSION}",`);
    }, /duplicate JSON key \$\.version/);
    await rejects(files => {
        files["Cargo.toml"] = replaceOne(files["Cargo.toml"], `version = "${VERSION}"`,
            `version = "${VERSION}"\nversion = "${VERSION}"`);
    }, /exactly one workspace\.package\.version \(found 2\)/);
    await rejects(files => {
        files["flake.nix"] += `\ncommonArgs = { version = "${VERSION}"; };\n`;
    }, /declare commonArgs as an attribute set exactly once \(found 2\)/);
    await rejects(files => {
        files["flake.nix"] = replaceOne(files["flake.nix"],
            `pname = "obsetync";\n          version = "${VERSION}";`,
            "pname = \"obsetync\";\n          version = currentVersion;");
    }, /commonArgs\.version must be one literal string/);
    await rejects(files => {
        files["plugin/package.json"] = replaceOne(files["plugin/package.json"], `  "version": "${VERSION}",\n`, "");
    }, /plugin\/package\.json version is missing/);
});

test("requires one matching versions.json compatibility entry and manifest minimum", async () => {
    const nextMinimum = MINIMUM.replace(/([0-9]+)$/, value => String(Number(value) + 1));
    await rejects(files => {
        files["plugin/manifest.json"] = replaceOne(files["plugin/manifest.json"],
            `"minAppVersion": "${MINIMUM}"`, `"minAppVersion": "${nextMinimum}"`);
    }, new RegExp(`plugin/manifest\\.json minAppVersion is ${escaped(nextMinimum)}, expected ${escaped(MINIMUM)}`));
    await rejects(files => {
        files["versions.json"] = replaceOne(files["versions.json"], `    "${VERSION}": "${MINIMUM}"`,
            `    "${VERSION}": "${nextMinimum}"`);
    }, new RegExp(`versions\\.json\\[${escaped(VERSION)}\\] is ${escaped(nextMinimum)}, expected minAppVersion ${escaped(MINIMUM)}`));
    await rejects(files => {
        files["versions.json"] = replaceOne(files["versions.json"], `    "${VERSION}": "${MINIMUM}"`,
            `    "${VERSION}-missing": "${MINIMUM}"`);
    }, /versions\.json release key .* is missing/);
    await rejects(files => {
        files["versions.json"] = replaceOne(files["versions.json"], `    "${VERSION}": "${MINIMUM}"`,
            `    "${VERSION}": "${MINIMUM}",\n    "${VERSION}": "${MINIMUM}"`);
    }, new RegExp(`duplicate JSON key \\$\\.${escaped(VERSION)}`));
});

test("keeps Nix sha256 and Git SHA-1 domains separate", async () => {
    const withShaComment = await run(files => {
        files["flake.nix"] = `# release commit ${"b".repeat(40)}\n${files["flake.nix"]}`;
    });
    assert.equal(withShaComment.version, VERSION);

    await rejects(files => {
        files["flake.nix"] = files["flake.nix"].replace(/npmDepsHash = "sha256-[A-Za-z0-9+/]+=\";/,
            `npmDepsHash = "${"c".repeat(40)}";`);
    }, /canonical Nix SRI sha256, not a Git SHA-1 or version/);
});
