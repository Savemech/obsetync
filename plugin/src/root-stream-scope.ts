import { fingerprintLegacyCopies } from "./legacy-source";
import { validateRootScopeId } from "./root-outcome";

const MAX_ENDPOINT_UNITS = 8192;
const DISPATCH_SUFFIX = "/__obsetync_root_stream_identity__";

export interface RootStreamSettings {
    serverUrl: string;
    serverBoxPub: string;
    vaultId: string;
    deviceId: string;
    enrolled: boolean;
    bearerToken: string;
}

export class RootStreamScopeError extends Error {
    constructor(readonly code: "INVALID" | "STALE") {
        // Fixed, path/credential-free diagnostics. Never include a URL or the
        // original parsing error: invalid userinfo/query can contain secrets.
        super(code === "INVALID" ? "Invalid root stream ownership" : "Root stream ownership changed");
        this.name = "RootStreamScopeError";
    }
}

export interface CapturedRootStreamScope {
    readonly vaultId: string;
    readonly deviceId: string;
    /** Stable local storage scope, not an authenticated storage-incarnation
     * proof, live capability lease or root request digest. Persist only this
     * digest; neither a bearer token nor the API owner is exposed here. */
    readonly scopeHash: Promise<string>;
    /** Synchronous dispatch fence. Main must additionally check its lifecycle
     * generation. Supply the ACTUAL current API object and its effective
     * baseUrl; call immediately before handing work to that owner. */
    assertCurrent(settings: RootStreamSettings, credentialOwner: object, effectiveBaseUrl: string): void;
}

function invalid(): never { throw new RootStreamScopeError("INVALID"); }

/** Canonicalize actual URL dispatch semantics, without changing the existing
 * transport policy. Appending a probe suffix BEFORE URL parsing preserves
 * the difference between a base ending '/' and one not ending '/' (their
 * actual `/api/...` requests have different path separators).
 *
 * No trailing slash removal or HTTPS rewrite here: an API's effective base
 * has already undergone those transformations exactly once. */
function canonicalEffectiveEndpoint(value: string): string {
    if (typeof value !== "string" || !value.length || value.length > MAX_ENDPOINT_UNITS ||
        /[\s\x00-\x1f\x7f\\?#]/.test(value)) invalid();
    const authority = /^[A-Za-z][A-Za-z0-9+.-]*:\/\/([^/]*)/.exec(value);
    if (!authority || authority[1].includes("@")) invalid();
    const parsed = new URL(value + DISPATCH_SUFFIX);
    if ((parsed.protocol !== "http:" && parsed.protocol !== "https:") || !parsed.hostname ||
        parsed.username || parsed.password || parsed.search || parsed.hash) invalid();
    if (!parsed.href.endsWith(DISPATCH_SUFFIX)) invalid();
    const endpoint = parsed.href.slice(0, -DISPATCH_SUFFIX.length);
    if (endpoint.length > MAX_ENDPOINT_UNITS) invalid();
    return endpoint;
}

function settingsEndpoint(value: string): string {
    if (typeof value !== "string" || value.length > MAX_ENDPOINT_UNITS) invalid();
    // Match ObsetyncApi's constructor exactly, including case sensitivity and
    // removing ONE slash only. Do not invent a broader HTTPS/double-slash alias.
    let effective = value.replace(/\/$/, "");
    if (effective.startsWith("https://")) effective = "http://" + effective.slice("https://".length);
    return canonicalEffectiveEndpoint(effective);
}

interface CapturedIdentity {
    endpoint: string;
    serverBoxPub: string;
    vaultId: string;
    deviceId: string;
    bearerToken: string;
}

function readIdentity(settings: RootStreamSettings, effectiveBaseUrl: string): CapturedIdentity {
    try {
        if (!settings || settings.enrolled !== true) invalid();
        const endpoint = settingsEndpoint(settings.serverUrl);
        if (endpoint !== canonicalEffectiveEndpoint(effectiveBaseUrl)) invalid();
        const { serverBoxPub, vaultId, deviceId, bearerToken } = settings;
        // Pinned long-term X25519 key, not the rotating esPub transport cache.
        // Canonical encoding prevents aliases of the same enrolled public key.
        if (typeof serverBoxPub !== "string" || !/^[A-Za-z0-9+/]{43}=$/.test(serverBoxPub) ||
            atob(serverBoxPub).length !== 32 || btoa(atob(serverBoxPub)) !== serverBoxPub) invalid();
        validateRootScopeId(vaultId);
        validateRootScopeId(deviceId);
        if (typeof bearerToken !== "string" || !/^[0-9a-fA-F]{64}$/.test(bearerToken)) invalid();
        return { endpoint, serverBoxPub, vaultId, deviceId, bearerToken };
    } catch { return invalid(); }
}

/** Call synchronously alongside construction of the immutable API credential
 * owner, BEFORE awaiting storage or capability work. The stable identity is
 * endpoint + long-term server pin + vault + authenticated device only.
 *
 * Ignores/config policy, tree protocol, server incarnation, display name,
 * ephemeral keys/expiry and outgoing transport sequence do not partition a
 * server-side root stream. Same-device bearer replacement likewise preserves
 * its durable scope, but the previous immutable API must stop dispatching.
 * The captured bearer is compared only in this private in-memory closure;
 * it is never hashed, serialized, returned or logged by this module.
 *
 * This is a local ownership fence, not proof that a token authenticates the
 * stated device. Enrollment/server authentication establish that binding. */
export function captureRootStreamScope(
    settings: RootStreamSettings, credentialOwner: object, effectiveBaseUrl: string,
): CapturedRootStreamScope {
    if (typeof credentialOwner !== "object" || credentialOwner === null) invalid();
    const captured = readIdentity(settings, effectiveBaseUrl);
    // Enumerate stable public fields explicitly: never spread a credential
    // snapshot/settings object into the hash preimage.
    const encoded = JSON.stringify({ schema: 1, endpoint: captured.endpoint,
        serverBoxPub: captured.serverBoxPub, vaultId: captured.vaultId, deviceId: captured.deviceId });
    const scopeHash = fingerprintLegacyCopies([{ role: "root-stream-identity-v1", raw: encoded }]);
    // Initialization can be superseded before the engine awaits this result.
    // Keep failure handled without converting the original promise to success.
    void scopeHash.catch(() => {});
    return Object.freeze({
        vaultId: captured.vaultId,
        deviceId: captured.deviceId,
        scopeHash,
        assertCurrent(current: RootStreamSettings, currentOwner: object, currentBaseUrl: string): void {
            if (currentOwner !== credentialOwner) throw new RootStreamScopeError("STALE");
            let identity: CapturedIdentity;
            try { identity = readIdentity(current, currentBaseUrl); }
            catch { throw new RootStreamScopeError("STALE"); }
            if (identity.endpoint !== captured.endpoint || identity.serverBoxPub !== captured.serverBoxPub ||
                identity.vaultId !== captured.vaultId || identity.deviceId !== captured.deviceId ||
                identity.bearerToken !== captured.bearerToken) throw new RootStreamScopeError("STALE");
        },
    });
}
