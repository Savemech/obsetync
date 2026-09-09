#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const SEMVER = /^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$/;
const GIT_SHA1 = /^[0-9a-f]{40}$/;
const NIX_SHA256 = /^sha256-([A-Za-z0-9+/]{43}=)$/;

function fail(message) {
    throw new Error(`release version check: ${message}`);
}

function semanticVersion(value, label) {
    if (typeof value !== "string" || !SEMVER.test(value)) {
        if (typeof value === "string" && GIT_SHA1.test(value)) {
            fail(`${label} is a Git SHA-1, not an X.Y.Z semantic version`);
        }
        fail(`${label} must be an exact X.Y.Z semantic version`);
    }
    return value;
}

function releaseTag(value) {
    if (typeof value !== "string" || value.length === 0) fail("release ref name is missing");
    const version = value.startsWith("v") ? value.slice(1) : value;
    semanticVersion(version, "release tag");
    if (value !== version && value !== `v${version}`) fail("release tag must be X.Y.Z or vX.Y.Z");
    return version;
}

class JsonParser {
    constructor(source, label) {
        this.source = source;
        this.label = label;
        this.offset = 0;
    }

    error(message) {
        fail(`${this.label}: ${message} at byte ${this.offset}`);
    }

    whitespace() {
        while (/[\t\n\r ]/.test(this.source[this.offset] ?? "")) this.offset++;
    }

    parse() {
        this.whitespace();
        const value = this.value("$");
        this.whitespace();
        if (this.offset !== this.source.length) this.error("unexpected trailing input");
        return value;
    }

    value(path) {
        this.whitespace();
        const char = this.source[this.offset];
        if (char === "{") return this.object(path);
        if (char === "[") return this.array(path);
        if (char === '"') return this.string();
        if (char === "t") return this.literal("true", true);
        if (char === "f") return this.literal("false", false);
        if (char === "n") return this.literal("null", null);
        const match = this.source.slice(this.offset).match(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/);
        if (!match) this.error("expected JSON value");
        this.offset += match[0].length;
        return Number(match[0]);
    }

    string() {
        const start = this.offset++;
        let escaped = false;
        while (this.offset < this.source.length) {
            const char = this.source[this.offset++];
            if (!escaped && char === '"') {
                try {
                    return JSON.parse(this.source.slice(start, this.offset));
                } catch {
                    this.error("invalid JSON string");
                }
            }
            if (!escaped && char.charCodeAt(0) < 0x20) this.error("control character in JSON string");
            if (!escaped && char === "\\") escaped = true;
            else escaped = false;
        }
        this.error("unterminated JSON string");
    }

    literal(text, value) {
        if (!this.source.startsWith(text, this.offset)) this.error(`expected ${text}`);
        this.offset += text.length;
        return value;
    }

    object(path) {
        this.offset++;
        const value = Object.create(null);
        const keys = new Set();
        this.whitespace();
        if (this.source[this.offset] === "}") {
            this.offset++;
            return value;
        }
        while (true) {
            this.whitespace();
            if (this.source[this.offset] !== '"') this.error("expected object key");
            const key = this.string();
            if (keys.has(key)) fail(`${this.label}: duplicate JSON key ${path}.${key}`);
            keys.add(key);
            this.whitespace();
            if (this.source[this.offset++] !== ":") this.error("expected ':' after object key");
            value[key] = this.value(`${path}.${key}`);
            this.whitespace();
            const delimiter = this.source[this.offset++];
            if (delimiter === "}") return value;
            if (delimiter !== ",") this.error("expected ',' or '}'");
        }
    }

    array(path) {
        this.offset++;
        const value = [];
        this.whitespace();
        if (this.source[this.offset] === "]") {
            this.offset++;
            return value;
        }
        while (true) {
            value.push(this.value(`${path}[${value.length}]`));
            this.whitespace();
            const delimiter = this.source[this.offset++];
            if (delimiter === "]") return value;
            if (delimiter !== ",") this.error("expected ',' or ']'");
        }
    }
}

function parseJson(source, label) {
    return new JsonParser(source, label).parse();
}

function requiredOwn(object, key, label) {
    if (object === null || typeof object !== "object" || Array.isArray(object) ||
        !Object.prototype.hasOwnProperty.call(object, key)) {
        fail(`${label} is missing`);
    }
    return object[key];
}

function cargoWorkspaceVersion(source) {
    let section = "";
    let workspacePackageSections = 0;
    const versions = [];
    for (const [index, original] of source.split(/\r?\n/).entries()) {
        let line = "", quoted = false, escaped = false;
        for (const char of original) {
            if (!quoted && char === "#") break;
            line += char;
            if (quoted && !escaped && char === "\\") escaped = true;
            else {
                if (!escaped && char === '"') quoted = !quoted;
                escaped = false;
            }
        }
        line = line.trim();
        if (!line) continue;
        if (line.startsWith("[")) {
            if (!line.endsWith("]")) fail(`Cargo.toml: malformed section on line ${index + 1}`);
            section = line.slice(1, -1).trim();
            if (section === "workspace.package") workspacePackageSections++;
            continue;
        }
        if (section !== "workspace.package") continue;
        const equals = line.indexOf("=");
        if (equals < 0) continue;
        const key = line.slice(0, equals).trim();
        if (key !== "version") continue;
        const literal = line.slice(equals + 1).trim();
        if (!literal.startsWith('"') || !literal.endsWith('"')) {
            fail(`Cargo.toml workspace.package.version must be a literal string on line ${index + 1}`);
        }
        try {
            versions.push(JSON.parse(literal));
        } catch {
            fail(`Cargo.toml has an invalid version string on line ${index + 1}`);
        }
    }
    if (workspacePackageSections !== 1) {
        fail(`Cargo.toml must contain exactly one [workspace.package] section (found ${workspacePackageSections})`);
    }
    if (versions.length !== 1) {
        fail(`Cargo.toml must contain exactly one workspace.package.version (found ${versions.length})`);
    }
    return semanticVersion(versions[0], "Cargo.toml workspace.package.version");
}

function nixTokens(source) {
    const tokens = [];
    let offset = 0;
    while (offset < source.length) {
        const char = source[offset];
        if (/\s/.test(char)) { offset++; continue; }
        if (char === "#") {
            while (offset < source.length && source[offset] !== "\n") offset++;
            continue;
        }
        if (source.startsWith("/*", offset)) {
            const end = source.indexOf("*/", offset + 2);
            if (end < 0) fail("flake.nix has an unterminated block comment");
            offset = end + 2;
            continue;
        }
        if (source.startsWith("''", offset)) {
            const end = source.indexOf("''", offset + 2);
            if (end < 0) fail("flake.nix has an unterminated indented string");
            tokens.push({ type: "string", value: source.slice(offset + 2, end) });
            offset = end + 2;
            continue;
        }
        if (char === '"') {
            let value = "", closed = false;
            offset++;
            while (offset < source.length) {
                const current = source[offset++];
                if (current === '"') { closed = true; break; }
                if (current === "\\") {
                    if (offset >= source.length) break;
                    const escaped = source[offset++];
                    const escapes = { n: "\n", r: "\r", t: "\t", '"': '"', "\\": "\\" };
                    if (!Object.prototype.hasOwnProperty.call(escapes, escaped)) {
                        fail(`flake.nix target string contains unsupported escape \\${escaped}`);
                    }
                    value += escapes[escaped];
                } else value += current;
            }
            if (!closed) fail("flake.nix has an unterminated string");
            tokens.push({ type: "string", value });
            continue;
        }
        if (/[A-Za-z_]/.test(char)) {
            const start = offset++;
            while (/[A-Za-z0-9_'-]/.test(source[offset] ?? "")) offset++;
            tokens.push({ type: "ident", value: source.slice(start, offset) });
            continue;
        }
        tokens.push({ type: "symbol", value: char });
        offset++;
    }
    return tokens;
}

function nixBlock(tokens, name) {
    const starts = [];
    for (let index = 0; index + 1 < tokens.length; index++) {
        if (tokens[index].type === "ident" && tokens[index].value === name && tokens[index + 1].value === "=") {
            let open = index + 2;
            while (open < tokens.length && tokens[open].value !== "{" && tokens[open].value !== ";") open++;
            if (tokens[open]?.value === "{") starts.push(open);
        }
    }
    if (starts.length !== 1) fail(`flake.nix must declare ${name} as an attribute set exactly once (found ${starts.length})`);
    const open = starts[0];
    let depth = 1, close = open + 1;
    for (; close < tokens.length && depth > 0; close++) {
        if (tokens[close].value === "{") depth++;
        else if (tokens[close].value === "}") depth--;
    }
    if (depth !== 0) fail(`flake.nix ${name} has an unterminated attribute set`);
    return tokens.slice(open + 1, close - 1);
}

function directNixStrings(tokens, blockName, wanted) {
    const found = new Map(wanted.map(key => [key, []]));
    let braces = 0, brackets = 0, parentheses = 0;
    for (let index = 0; index + 1 < tokens.length; index++) {
        const token = tokens[index];
        if (token.value === "{") braces++;
        else if (token.value === "}") braces--;
        else if (token.value === "[") brackets++;
        else if (token.value === "]") brackets--;
        else if (token.value === "(") parentheses++;
        else if (token.value === ")") parentheses--;
        if (braces !== 0 || brackets !== 0 || parentheses !== 0 || token.type !== "ident" ||
            !found.has(token.value) || tokens[index + 1].value !== "=") continue;
        const value = tokens[index + 2];
        if (value?.type !== "string" || tokens[index + 3]?.value !== ";") {
            fail(`flake.nix ${blockName}.${token.value} must be one literal string`);
        }
        found.get(token.value).push(value.value);
    }
    const result = Object.create(null);
    for (const [key, values] of found) {
        if (values.length !== 1) {
            fail(`flake.nix ${blockName}.${key} must be declared exactly once (found ${values.length})`);
        }
        result[key] = values[0];
    }
    return result;
}

function flakeDeclarations(source) {
    const tokens = nixTokens(source);
    const common = directNixStrings(nixBlock(tokens, "commonArgs"), "commonArgs", ["version"]);
    const wasm = directNixStrings(nixBlock(tokens, "sync-core-wasm"), "sync-core-wasm", ["version"]);
    const plugin = directNixStrings(nixBlock(tokens, "plugin"), "plugin",
        ["version", "npmDepsHash", "OBSETYNC_BUILD_EXPECTED_VERSION"]);
    const hash = NIX_SHA256.exec(plugin.npmDepsHash);
    if (!hash || Buffer.from(hash[1], "base64").length !== 32 ||
        Buffer.from(hash[1], "base64").toString("base64") !== hash[1]) {
        fail("flake.nix plugin.npmDepsHash must be a canonical Nix SRI sha256, not a Git SHA-1 or version");
    }
    return {
        "flake.nix commonArgs.version": semanticVersion(common.version, "flake.nix commonArgs.version"),
        "flake.nix sync-core-wasm.version": semanticVersion(wasm.version, "flake.nix sync-core-wasm.version"),
        "flake.nix plugin.version": semanticVersion(plugin.version, "flake.nix plugin.version"),
        "flake.nix OBSETYNC_BUILD_EXPECTED_VERSION": semanticVersion(plugin.OBSETYNC_BUILD_EXPECTED_VERSION,
            "flake.nix plugin.OBSETYNC_BUILD_EXPECTED_VERSION"),
    };
}

async function jsonFile(root, relative) {
    return parseJson(await readFile(join(root, relative), "utf8"), relative);
}

export async function checkReleaseVersion({ rootDir, eventName, refName }) {
    const root = resolve(rootDir);
    if (eventName !== "push") fail(`release event must be push (received ${eventName || "missing"})`);
    const tagVersion = releaseTag(refName);
    const cargoVersion = cargoWorkspaceVersion(await readFile(join(root, "Cargo.toml"), "utf8"));
    const rootManifest = await jsonFile(root, "manifest.json");
    const pluginManifest = await jsonFile(root, "plugin/manifest.json");
    const packageJson = await jsonFile(root, "plugin/package.json");
    const packageLock = await jsonFile(root, "plugin/package-lock.json");
    const compatibility = await jsonFile(root, "versions.json");
    const lockRoot = requiredOwn(requiredOwn(packageLock, "packages", "plugin/package-lock.json packages"), "",
        "plugin/package-lock.json packages['']");
    const versions = {
        "Cargo.toml workspace.package.version": cargoVersion,
        "manifest.json version": semanticVersion(requiredOwn(rootManifest, "version", "manifest.json version"),
            "manifest.json version"),
        "plugin/manifest.json version": semanticVersion(requiredOwn(pluginManifest, "version", "plugin/manifest.json version"),
            "plugin/manifest.json version"),
        "plugin/package.json version": semanticVersion(requiredOwn(packageJson, "version", "plugin/package.json version"),
            "plugin/package.json version"),
        "plugin/package-lock.json version": semanticVersion(requiredOwn(packageLock, "version", "plugin/package-lock.json version"),
            "plugin/package-lock.json version"),
        "plugin/package-lock.json packages[''].version": semanticVersion(requiredOwn(lockRoot, "version",
            "plugin/package-lock.json packages[''].version"), "plugin/package-lock.json packages[''].version"),
        ...flakeDeclarations(await readFile(join(root, "flake.nix"), "utf8")),
    };
    for (const [label, version] of Object.entries(versions)) {
        if (version !== cargoVersion) fail(`${label} is ${version}, expected ${cargoVersion}`);
    }
    if (tagVersion !== cargoVersion) fail(`release tag is ${tagVersion}, expected ${cargoVersion}`);
    const rootMinimum = semanticVersion(requiredOwn(rootManifest, "minAppVersion", "manifest.json minAppVersion"),
        "manifest.json minAppVersion");
    const pluginMinimum = semanticVersion(requiredOwn(pluginManifest, "minAppVersion", "plugin/manifest.json minAppVersion"),
        "plugin/manifest.json minAppVersion");
    if (pluginMinimum !== rootMinimum) {
        fail(`plugin/manifest.json minAppVersion is ${pluginMinimum}, expected ${rootMinimum}`);
    }
    const releaseMinimum = semanticVersion(requiredOwn(compatibility, cargoVersion,
        `versions.json release key ${cargoVersion}`), `versions.json[${cargoVersion}]`);
    if (releaseMinimum !== rootMinimum) {
        fail(`versions.json[${cargoVersion}] is ${releaseMinimum}, expected minAppVersion ${rootMinimum}`);
    }
    return { version: cargoVersion, minimumAppVersion: rootMinimum, tag: refName,
        declarations: Object.keys(versions).length };
}

async function main() {
    const rootDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
    const result = await checkReleaseVersion({
        rootDir,
        eventName: process.env.OBSETYNC_RELEASE_EVENT_NAME ?? process.env.GITHUB_EVENT_NAME,
        refName: process.env.OBSETYNC_RELEASE_REF_NAME ?? process.env.GITHUB_REF_NAME,
    });
    process.stdout.write(`release version check passed: ${result.version} (${result.declarations} declarations, tag ${result.tag})\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    main().catch(error => {
        process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
        process.exitCode = 1;
    });
}
