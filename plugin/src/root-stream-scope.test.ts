import { strict as assert } from "node:assert";
import { fingerprintLegacyCopies } from "./legacy-source";
import { captureRootStreamScope, RootStreamScopeError, type RootStreamSettings } from "./root-stream-scope";

const serverPin = (byte = 1) => btoa(String.fromCharCode(...new Uint8Array(32).fill(byte)));
const fixture = (): RootStreamSettings => ({
    serverUrl: "http://sync.invalid:27182", serverBoxPub: serverPin(),
    vaultId: "synthetic", deviceId: "0123456789abcdef0123456789abcdef",
    enrolled: true, bearerToken: "b".repeat(64),
});
function effective(value: string): string {
    // Explicit fixture model of the existing constructor, not a new production
    // URL policy. Tests below check the edge cases independently as well.
    let url = value.replace(/\/$/, "");
    if (url.startsWith("https://")) url = "http://" + url.slice(8);
    return url;
}
function capture(settings = fixture(), owner: object = {}) {
    return captureRootStreamScope(settings, owner, effective(settings.serverUrl));
}
function assertCode(action: () => unknown, code: "INVALID" | "STALE") {
    assert.throws(action, error => error instanceof RootStreamScopeError && error.code === code);
}

async function stableIdentityAndSecretFreeDigest() {
    const settings = fixture(), owner = { toJSON() { throw new Error("API owner must never be serialized"); } };
    const scope = capture(settings, owner), digest = await scope.scopeHash;
    assert.match(digest, /^[0-9a-f]{64}$/);
    // Independent Node SHA-256 reconstruction of the documented existing
    // fingerprint framing. Changing this durable scope requires migration,
    // not an unnoticed helper refactor that starts another sequence stream.
    assert.equal(digest, "4adaa995d57747e5e8c24e42dc2397fc27016c626644be5625c071bd62255e5c");
    assert.equal(scope.scopeHash, scope.scopeHash, "scope allocated a fresh digest for each caller");
    assert(Object.isFrozen(scope));
    assert.equal(scope.vaultId, settings.vaultId); assert.equal(scope.deviceId, settings.deviceId);
    const encoded = JSON.stringify({ schema: 1, endpoint: "http://sync.invalid:27182",
        serverBoxPub: settings.serverBoxPub, vaultId: settings.vaultId, deviceId: settings.deviceId });
    assert.equal(digest, await fingerprintLegacyCopies([{ role: "root-stream-identity-v1", raw: encoded }]));
    assert(!encoded.includes(settings.bearerToken));
    assert(!JSON.stringify(scope).includes(settings.bearerToken));
    assert.deepEqual(Object.keys(scope).sort(), ["assertCurrent", "deviceId", "scopeHash", "vaultId"]);
    for (const field of ["vaultId", "deviceId", "serverBoxPub", "serverUrl"] as const) {
        const changed = { ...settings };
        changed[field] = field === "serverBoxPub" ? serverPin(2) : field === "serverUrl"
            ? "http://changed.invalid:27182" : settings[field] + "-new";
        assert.notEqual(await capture(changed).scopeHash, digest, `${field} did not partition the root stream`);
        assertCode(() => scope.assertCurrent(changed, owner, effective(changed.serverUrl)), "STALE");
    }
    const rotated = { ...settings, bearerToken: "c".repeat(64) };
    assert.equal(await capture(rotated).scopeHash, digest, "bearer replacement changed durable stream identity");
    assertCode(() => scope.assertCurrent(rotated, owner, effective(rotated.serverUrl)), "STALE");
    assertCode(() => scope.assertCurrent(settings, {}, effective(settings.serverUrl)), "STALE");
    scope.assertCurrent(settings, owner, effective(settings.serverUrl));
}

async function unrelatedMutableStateDoesNotPartitionOrFence() {
    const settings = fixture(), owner = {}, scope = capture(settings, owner), digest = await scope.scopeHash;
    const mutable = { ...settings,
        ignorePatterns: ["new/**"], syncObsidianConfig: true, treeVersion: 2, treeProtocol: "v2",
        serverIncarnation: "a".repeat(64), deviceName: "renamed", esPub: "rotated ephemeral key",
        esPubValidUntil: 1, lastOutgoingSeq: 9000, wireVersion: "0x02", autoSync: false,
    };
    assert.equal(await capture(mutable).scopeHash, digest);
    scope.assertCurrent(mutable, owner, effective(mutable.serverUrl));
    // The owner assertion must not even inspect unrelated transport/policy
    // fields or stringify the entire settings object.
    for (const field of ["ignorePatterns", "syncObsidianConfig", "treeVersion", "serverIncarnation",
        "deviceName", "esPub", "esPubValidUntil", "lastOutgoingSeq", "wireVersion", "toJSON"]) {
        Object.defineProperty(mutable, field, { get() { throw new Error(`unexpected read: ${field}`); } });
    }
    assert.equal(await capture(mutable).scopeHash, digest);
    scope.assertCurrent(mutable, owner, effective(mutable.serverUrl));
    assertCode(() => scope.assertCurrent({ ...settings, enrolled: false }, owner, effective(settings.serverUrl)), "STALE");
}

async function capturedBeforeHashAwait() {
    const settings = fixture(), original = { ...settings }, owner = {};
    const scope = capture(settings, owner);
    settings.serverUrl = "http://new.invalid"; settings.serverBoxPub = serverPin(2);
    settings.vaultId = "future"; settings.deviceId = "future-device"; settings.bearerToken = "c".repeat(64);
    assert.equal(await scope.scopeHash, await capture(original).scopeHash, "pending hash followed mutable settings");
    assert.equal(scope.vaultId, original.vaultId); assert.equal(scope.deviceId, original.deviceId);
    scope.assertCurrent(original, owner, effective(original.serverUrl));
    assertCode(() => scope.assertCurrent(settings, owner, effective(settings.serverUrl)), "STALE");
}

async function canonicalEndpointPreservesDispatchSemantics() {
    for (const equivalent of [
        ["http://SYNC.invalid:80", "http://sync.invalid/"],
        ["http://SYNC.invalid:80/base/", "http://sync.invalid/base"],
        ["https://sync.invalid:27182/", "http://sync.invalid:27182"],
        ["HTTP://SYNC.invalid/base", "http://sync.invalid/base/"],
        ["http://sync.invalid/a/../base", "http://sync.invalid/base"],
        ["http://[0:0:0:0:0:0:0:1]:80", "http://[::1]/"],
    ]) {
        const [left, right] = equivalent.map(serverUrl => ({ ...fixture(), serverUrl }));
        const owner = {}, scoped = capture(left, owner);
        assert.equal(await scoped.scopeHash, await capture(right).scopeHash, `${equivalent} were not canonical aliases`);
        scoped.assertCurrent(right, owner, effective(right.serverUrl));
    }
    for (const different of [
        ["http://sync.invalid/", "http://sync.invalid//"],
        ["http://sync.invalid/base/", "http://sync.invalid/base//"],
        ["https://sync.invalid/", "HTTPS://sync.invalid/"],
        ["https://sync.invalid:443", "http://sync.invalid:80"],
        ["http://sync.invalid/base", "http://sync.invalid/Base"],
        ["http://sync.invalid/a%2Fb", "http://sync.invalid/a/b"],
        ["http://sync.invalid:27182", "http://sync.invalid:27183"],
    ]) {
        const [left, right] = different.map(serverUrl => ({ ...fixture(), serverUrl }));
        const owner = {}, scoped = capture(left, owner);
        assert.notEqual(await scoped.scopeHash, await capture(right).scopeHash, `${different} collapsed distinct dispatch endpoints`);
        assertCode(() => scoped.assertCurrent(right, owner, effective(right.serverUrl)), "STALE");
    }
    const settings = { ...fixture(), serverUrl: "http://sync.invalid//" }, owner = {};
    assert.doesNotThrow(() => captureRootStreamScope(settings, owner, "http://sync.invalid/"));
    assertCode(() => captureRootStreamScope(settings, owner, "http://sync.invalid"), "INVALID");
    assertCode(() => captureRootStreamScope(fixture(), owner, "http://another.invalid:27182"), "INVALID");
    const scoped = capture(fixture(), owner);
    assertCode(() => scoped.assertCurrent(fixture(), owner, "http://another.invalid:27182"), "STALE");
}

async function malformedAndBoundedInputs() {
    for (const serverUrl of ["", " http://sync.invalid", "http://sync.invalid ", "http://sync.invalid/\n",
        "http://sync.invalid/?token=secret", "http://sync.invalid/?", "http://sync.invalid/#", "http://secret@sync.invalid",
        "http://@sync.invalid", "http://user:secret@sync.invalid", "http://sync.invalid\\other", "ws://sync.invalid",
        "file:///private", "//sync.invalid", "http://", "http://sync.invalid:999999", "http://sync.invalid/" + "x".repeat(8192)]) {
        assertCode(() => capture({ ...fixture(), serverUrl }), "INVALID");
    }
    for (const key of ["vaultId", "deviceId"] as const) {
        for (const value of ["", ".", "..", "x/y", "x\\y", "a\u0000b", "x".repeat(129), "é".repeat(65), "\ud800"]) {
            assertCode(() => capture({ ...fixture(), [key]: value }), "INVALID");
        }
    }
    for (const serverBoxPub of ["", "key", "A".repeat(43), "A".repeat(44), "A".repeat(42) + "B=",
        "A".repeat(43) + "=\n", "-".repeat(43) + "=", btoa("short"), "x".repeat(8193)]) {
        assertCode(() => capture({ ...fixture(), serverBoxPub }), "INVALID");
    }
    for (const bearerToken of ["", "b".repeat(63), "b".repeat(65), "z".repeat(64), "token-secret", "x".repeat(8193)]) {
        assertCode(() => capture({ ...fixture(), bearerToken }), "INVALID");
    }
    assertCode(() => capture({ ...fixture(), enrolled: false }), "INVALID");
    assertCode(() => captureRootStreamScope(fixture(), null as unknown as object, effective(fixture().serverUrl)), "INVALID");
    const boundary = { ...fixture(), vaultId: "x".repeat(128), deviceId: "y".repeat(128),
        serverUrl: "http://sync.invalid/" + "x".repeat(8192 - "http://sync.invalid/".length) };
    assert.match(await capture(boundary).scopeHash, /^[0-9a-f]{64}$/);
}

async function errorsDoNotRevealSecrets() {
    const secret = "private-token-value", settings = fixture(), owner = {}, scoped = capture(settings, owner);
    await scoped.scopeHash;
    for (const action of [
        () => capture({ ...settings, serverUrl: `http://user:${secret}@sync.invalid` }),
        () => scoped.assertCurrent({ ...settings, bearerToken: secret }, owner, effective(settings.serverUrl)),
        () => scoped.assertCurrent(settings, owner, `http://sync.invalid/?${secret}`),
        () => capture(Object.defineProperty({ ...settings }, "bearerToken", { get() { throw new Error(secret); } })),
    ]) {
        let caught: unknown;
        try { action(); } catch (error) { caught = error; }
        assert(caught instanceof RootStreamScopeError);
        assert(!String(caught).includes(secret)); assert(!JSON.stringify(caught).includes(secret));
        assert(!(caught as Error).stack?.includes(secret));
    }
}

async function run() {
    await stableIdentityAndSecretFreeDigest();
    await unrelatedMutableStateDoesNotPartitionOrFence();
    await capturedBeforeHashAwait();
    await canonicalEndpointPreservesDispatchSemantics();
    await malformedAndBoundedInputs();
    await errorsDoNotRevealSecrets();
    console.log("root-stream-scope.test: 6 stable ownership, endpoint, bounds and secret-free suites passed");
}
void run().catch(error => { setTimeout(() => { throw error; }, 0); });
