/**
 * Secure transport — client half.
 *
 * Wraps every sync-API request in an encrypted envelope: X25519 ECDH +
 * HKDF-SHA256 + AES-256-GCM over plain HTTP. No TLS, no certs, no CA trust
 * store involvement.
 *
 * X25519 is handled by `@noble/curves/ed25519` because Web Crypto's X25519
 * algorithm only landed in Chromium 133 (Feb 2025), Safari 17, iOS 17. Older
 * Electron builds (which Obsidian desktop still ships) and pre-17 iOS lack it.
 * Noble is a pure-JS, audited, zero-dep implementation — same bytes in, same
 * bytes out as SubtleCrypto. HKDF-SHA256 + AES-256-GCM stay on SubtleCrypto
 * (ubiquitous since 2014).
 *
 * Wire format (matches `crates/sync-server/src/secure.rs`):
 *
 *   request:   [1B ver=0x01] [12B nonce] [32B eph_pub] [AES-GCM ct || 16B tag]
 *   response:  [1B ver=0x01] [12B nonce]               [AES-GCM ct || 16B tag]
 *
 * Inner request plaintext:
 *
 *   [64B bearer_token_hex_ASCII] [actual body bytes]
 *
 * AAD (authenticated, never on the wire):
 *
 *   request:   "obsetync/v1 <METHOD> <PATH>"
 *   response:  "obsetync/v1 <METHOD> <PATH>" || nonce_req
 *
 * The response AAD binds the 12-byte nonce of the request it answers, so an
 * in-session MITM can't substitute the response of one request for another
 * with the same method + path (e.g. feeding us a stale GET /root answer).
 *
 * A single `ObsetyncSecureChannel` instance caches the ECDH shared secret for its
 * lifetime, so per-request work is just HKDF + AES-GCM (microseconds).
 * Forward secrecy is per-session (per plugin load), same as TLS with session
 * tickets.
 */

import { x25519 } from "@noble/curves/ed25519";

const WIRE_VERSION = 0x01;
const NONCE_LEN = 12;
const PUBKEY_LEN = 32;
const TAG_LEN = 16;
const BEARER_LEN = 64;
const REQUEST_HEADER_LEN = 1 + NONCE_LEN + PUBKEY_LEN; // 45
const RESPONSE_HEADER_LEN = 1 + NONCE_LEN;             // 13

const AAD_PREFIX = "obsetync/v1";
const INFO_C2S = "obsetync/v1/c2s";
const INFO_S2C = "obsetync/v1/s2c";

export class ObsetyncSecureTransportError extends Error {
    constructor(msg: string) {
        super(msg);
        this.name = "ObsetyncSecureTransportError";
    }
}

function decodeBase64(b64: string): Uint8Array {
    // atob gives a binary string; translate one char → one byte.
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
}

function concat(...chunks: Uint8Array[]): Uint8Array {
    const len = chunks.reduce((n, c) => n + c.length, 0);
    const out = new Uint8Array(len);
    let off = 0;
    for (const c of chunks) {
        out.set(c, off);
        off += c.length;
    }
    return out;
}

function buildAad(method: string, path: string): Uint8Array {
    const text = `${AAD_PREFIX} ${method} ${path}`;
    return new TextEncoder().encode(text);
}

/** Response AAD = request AAD || nonce_req (replay binding, see header). */
function buildResponseAad(
    method: string,
    path: string,
    nonceReq: Uint8Array,
): Uint8Array {
    return concat(buildAad(method, path), nonceReq);
}

/**
 * Extract the 12-byte nonce from a sealed request envelope (bytes 1..13).
 * The caller keeps it to verify the response — wire-format knowledge stays
 * in this module.
 */
export function extractRequestNonce(wireRequest: Uint8Array): Uint8Array {
    if (wireRequest.length < REQUEST_HEADER_LEN) {
        throw new ObsetyncSecureTransportError(
            `request envelope too short to contain a nonce: ${wireRequest.length} bytes`,
        );
    }
    return wireRequest.slice(1, 1 + NONCE_LEN);
}

/**
 * Coerce a Uint8Array into BufferSource for Web Crypto. Modern TypeScript
 * narrows `Uint8Array<ArrayBufferLike>` into something incompatible with the
 * BufferSource signature because of SharedArrayBuffer pedantry. In practice
 * every Uint8Array we construct here is ArrayBuffer-backed — the cast is safe.
 */
const bs = (u: Uint8Array): BufferSource => u as unknown as BufferSource;

function randomNonce(): Uint8Array {
    const n = new Uint8Array(NONCE_LEN);
    crypto.getRandomValues(n);
    return n;
}

/**
 * Per-session encrypted HTTP transport to the obsetync server.
 *
 * Instantiate once via `ObsetyncSecureChannel.create(...)`. Reuse for every API
 * request during the plugin's lifetime.
 */
export class ObsetyncSecureChannel {
    private readonly ephPubRaw: Uint8Array;
    private readonly hkdfKey: CryptoKey;
    private readonly bearerBytes: Uint8Array;

    private constructor(
        ephPubRaw: Uint8Array,
        hkdfKey: CryptoKey,
        bearerBytes: Uint8Array,
    ) {
        this.ephPubRaw = ephPubRaw;
        this.hkdfKey = hkdfKey;
        this.bearerBytes = bearerBytes;
    }

    /**
     * Establish a new session. Generates a fresh ephemeral X25519 keypair via
     * @noble/curves (works on every platform Obsidian runs on — no Chromium
     * 133 dependency), performs ECDH with the server's long-term public key,
     * and imports the shared secret as HKDF key material. All subsequent
     * requests through this instance reuse the same shared secret (different
     * AES key per request via HKDF with random nonce as salt).
     */
    static async create(
        serverBoxPubBase64: string,
        bearerTokenHex: string,
    ): Promise<ObsetyncSecureChannel> {
        if (bearerTokenHex.length !== BEARER_LEN || !/^[0-9a-fA-F]+$/.test(bearerTokenHex)) {
            throw new ObsetyncSecureTransportError("bearer token is not 64 hex chars");
        }

        const serverPubBytes = decodeBase64(serverBoxPubBase64);
        if (serverPubBytes.length !== PUBKEY_LEN) {
            throw new ObsetyncSecureTransportError(
                `server box pubkey must be ${PUBKEY_LEN} bytes, got ${serverPubBytes.length}`,
            );
        }

        // Ephemeral X25519 keypair via noble. The private key is a 32-byte
        // Uint8Array held in memory for the channel's lifetime; there's no
        // non-extractable CryptoKey handle on platforms lacking Web Crypto
        // X25519, and pre-existing attacks on our process can read it either
        // way — TLS session keys have the same property.
        const ephPrivBytes = new Uint8Array(PUBKEY_LEN);
        crypto.getRandomValues(ephPrivBytes);
        const ephPubRaw = x25519.getPublicKey(ephPrivBytes);
        const shared = x25519.getSharedSecret(ephPrivBytes, serverPubBytes);
        // Zero the private key — we've already derived the shared secret,
        // everything from here on uses HKDF-derived per-request AES keys.
        ephPrivBytes.fill(0);

        // Import shared as HKDF key material so we can deriveBits per request.
        const hkdfKey = await crypto.subtle.importKey(
            "raw",
            bs(shared),
            "HKDF",
            false,
            ["deriveBits"],
        );

        const bearerBytes = new TextEncoder().encode(bearerTokenHex);

        return new ObsetyncSecureChannel(ephPubRaw, hkdfKey, bearerBytes);
    }

    /** Derive an AES-256-GCM key for the given direction + nonce. */
    private async deriveAesKey(
        nonce: Uint8Array,
        info: string,
        usage: KeyUsage,
    ): Promise<CryptoKey> {
        const keyBytes = await crypto.subtle.deriveBits(
            {
                name: "HKDF",
                hash: "SHA-256",
                salt: bs(nonce),
                info: bs(new TextEncoder().encode(info)),
            },
            this.hkdfKey,
            256,
        );
        return crypto.subtle.importKey(
            "raw",
            keyBytes,
            { name: "AES-GCM", length: 256 },
            false,
            [usage],
        );
    }

    /**
     * Seal a request body for POST/PUT to `path` using `method`. Returns the
     * full wire-format bytes to place in the HTTP body.
     */
    async encryptRequest(
        method: string,
        path: string,
        body: Uint8Array,
    ): Promise<Uint8Array> {
        const nonce = randomNonce();
        const aad = buildAad(method, path);
        const key = await this.deriveAesKey(nonce, INFO_C2S, "encrypt");

        const plaintext = concat(this.bearerBytes, body);
        const ct = new Uint8Array(
            await crypto.subtle.encrypt(
                { name: "AES-GCM", iv: bs(nonce), additionalData: bs(aad) },
                key,
                bs(plaintext),
            ),
        );

        const out = new Uint8Array(REQUEST_HEADER_LEN + ct.length);
        out[0] = WIRE_VERSION;
        out.set(nonce, 1);
        out.set(this.ephPubRaw, 1 + NONCE_LEN);
        out.set(ct, REQUEST_HEADER_LEN);
        return out;
    }

    /**
     * Open a response body received for the given request line. `nonceReq`
     * is the nonce of the request this response answers (see
     * `extractRequestNonce`) — the AAD binds it, so a response minted for a
     * different request fails authentication. Throws
     * `ObsetyncSecureTransportError` if the body is malformed, tampered,
     * replayed from another request, or encrypted against a different
     * session.
     */
    async decryptResponse(
        method: string,
        path: string,
        nonceReq: Uint8Array,
        wireBody: Uint8Array,
    ): Promise<Uint8Array> {
        if (wireBody.length < RESPONSE_HEADER_LEN + TAG_LEN) {
            throw new ObsetyncSecureTransportError(
                `response too short: ${wireBody.length} bytes, need at least ${RESPONSE_HEADER_LEN + TAG_LEN}`,
            );
        }
        if (wireBody[0] !== WIRE_VERSION) {
            throw new ObsetyncSecureTransportError(`unsupported response wire version ${wireBody[0]}`);
        }
        // slice() returns a fresh ArrayBuffer-backed view — subarray() would
        // return a view sharing wireBody's backing store, which modern TS
        // types as potentially SharedArrayBuffer and rejects for Web Crypto.
        if (nonceReq.length !== NONCE_LEN) {
            throw new ObsetyncSecureTransportError(
                `request nonce must be ${NONCE_LEN} bytes, got ${nonceReq.length}`,
            );
        }
        const nonce = wireBody.slice(1, 1 + NONCE_LEN);
        const ct = wireBody.slice(RESPONSE_HEADER_LEN);
        const aad = buildResponseAad(method, path, nonceReq);
        const key = await this.deriveAesKey(nonce, INFO_S2C, "decrypt");

        try {
            const plaintext = new Uint8Array(
                await crypto.subtle.decrypt(
                    { name: "AES-GCM", iv: bs(nonce), additionalData: bs(aad) },
                    key,
                    bs(ct),
                ),
            );
            return plaintext;
        } catch {
            throw new ObsetyncSecureTransportError(
                "response decryption failed (tampered, wrong server key, or mismatched AAD)",
            );
        }
    }
}
