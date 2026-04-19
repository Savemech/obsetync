/**
 * Option-B secure transport — client half.
 *
 * Wraps every sync-API request in an encrypted envelope built from standard
 * Web Crypto primitives: X25519 ECDH + HKDF-SHA256 + AES-256-GCM. No TLS, no
 * certs, no CA trust store involvement. Plain HTTP transport carries an
 * opaque ciphertext blob.
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
 * AAD (authenticated but visible on wire):
 *
 *   "obsetync/v1 <METHOD> <PATH>"
 *
 * A single `SecureChannel` instance caches the ECDH shared secret for its
 * lifetime, so per-request work is just HKDF + AES-GCM (microseconds).
 * Forward secrecy is per-session (per plugin load), same as TLS with session
 * tickets.
 */

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

export class UnsupportedPlatformError extends Error {
    constructor(msg: string) {
        super(msg);
        this.name = "UnsupportedPlatformError";
    }
}

export class SecureTransportError extends Error {
    constructor(msg: string) {
        super(msg);
        this.name = "SecureTransportError";
    }
}

/** Feature-detect native X25519 support in Web Crypto. iOS 17+, Safari 17+, recent Electron. */
async function hasX25519(): Promise<boolean> {
    try {
        await crypto.subtle.generateKey({ name: "X25519" }, false, ["deriveBits"]);
        return true;
    } catch {
        return false;
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
 * Instantiate once via `SecureChannel.create(...)`. Reuse for every API
 * request during the plugin's lifetime.
 */
export class SecureChannel {
    private readonly ephPrivKey: CryptoKey;
    private readonly ephPubRaw: Uint8Array;
    private readonly hkdfKey: CryptoKey;
    private readonly bearerBytes: Uint8Array;

    private constructor(
        ephPrivKey: CryptoKey,
        ephPubRaw: Uint8Array,
        hkdfKey: CryptoKey,
        bearerBytes: Uint8Array,
    ) {
        this.ephPrivKey = ephPrivKey;
        this.ephPubRaw = ephPubRaw;
        this.hkdfKey = hkdfKey;
        this.bearerBytes = bearerBytes;
    }

    /**
     * Establish a new session. Generates a fresh ephemeral X25519 keypair,
     * performs ECDH with the server's long-term public key, and imports the
     * shared secret as HKDF key material. All subsequent requests through
     * this instance reuse the same shared secret (different AES key per
     * request via HKDF with random nonce as salt).
     */
    static async create(
        serverBoxPubBase64: string,
        bearerTokenHex: string,
    ): Promise<SecureChannel> {
        if (!(await hasX25519())) {
            throw new UnsupportedPlatformError(
                "WebCrypto X25519 not available — iOS 17+ or Safari 17+ required",
            );
        }

        if (bearerTokenHex.length !== BEARER_LEN || !/^[0-9a-fA-F]+$/.test(bearerTokenHex)) {
            throw new SecureTransportError("bearer token is not 64 hex chars");
        }

        const serverPubBytes = decodeBase64(serverBoxPubBase64);
        if (serverPubBytes.length !== PUBKEY_LEN) {
            throw new SecureTransportError(
                `server box pubkey must be ${PUBKEY_LEN} bytes, got ${serverPubBytes.length}`,
            );
        }

        // Ephemeral keypair (private key non-extractable — the browser keeps
        // it, we only ever hold a handle).
        const ephKeys = await crypto.subtle.generateKey(
            { name: "X25519" },
            false,
            ["deriveBits"],
        ) as CryptoKeyPair;

        const ephPubRaw = new Uint8Array(
            await crypto.subtle.exportKey("raw", ephKeys.publicKey),
        );

        const serverPubKey = await crypto.subtle.importKey(
            "raw",
            bs(serverPubBytes),
            { name: "X25519" },
            false,
            [],
        );

        const shared = await crypto.subtle.deriveBits(
            { name: "X25519", public: serverPubKey },
            ephKeys.privateKey,
            256,
        );

        // Import shared as HKDF key material so we can deriveBits per request.
        const hkdfKey = await crypto.subtle.importKey(
            "raw",
            shared,
            "HKDF",
            false,
            ["deriveBits"],
        );

        const bearerBytes = new TextEncoder().encode(bearerTokenHex);

        return new SecureChannel(ephKeys.privateKey, ephPubRaw, hkdfKey, bearerBytes);
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
     * Open a response body received for the given request line. Throws
     * `SecureTransportError` if the body is malformed, tampered, or was
     * encrypted against a different session.
     */
    async decryptResponse(
        method: string,
        path: string,
        wireBody: Uint8Array,
    ): Promise<Uint8Array> {
        if (wireBody.length < RESPONSE_HEADER_LEN + TAG_LEN) {
            throw new SecureTransportError(
                `response too short: ${wireBody.length} bytes, need at least ${RESPONSE_HEADER_LEN + TAG_LEN}`,
            );
        }
        if (wireBody[0] !== WIRE_VERSION) {
            throw new SecureTransportError(`unsupported response wire version ${wireBody[0]}`);
        }
        // slice() returns a fresh ArrayBuffer-backed view — subarray() would
        // return a view sharing wireBody's backing store, which modern TS
        // types as potentially SharedArrayBuffer and rejects for Web Crypto.
        const nonce = wireBody.slice(1, 1 + NONCE_LEN);
        const ct = wireBody.slice(RESPONSE_HEADER_LEN);
        const aad = buildAad(method, path);
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
            throw new SecureTransportError(
                "response decryption failed (tampered, wrong server key, or mismatched AAD)",
            );
        }
    }
}
