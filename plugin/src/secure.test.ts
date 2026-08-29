import { strict as assert } from "node:assert";
import { x25519 } from "@noble/curves/ed25519";
import {
    HTTP_WIRE_VERSION,
    ObsetyncSecureChannel,
    ObsetyncSessionStaleError,
    extractRequestNonce,
} from "./secure";

const bs = (value: Uint8Array): BufferSource => value as unknown as BufferSource;

function concat(...parts: Uint8Array[]): Uint8Array {
    const result = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
    let offset = 0;
    for (const part of parts) {
        result.set(part, offset);
        offset += part.length;
    }
    return result;
}

function b64(value: Uint8Array): string {
    return Buffer.from(value).toString("base64");
}

async function aesKey(material: Uint8Array, nonce: Uint8Array, info: string): Promise<CryptoKey> {
    const hkdf = await crypto.subtle.importKey("raw", bs(material), "HKDF", false, ["deriveBits"]);
    const bits = await crypto.subtle.deriveBits(
        {
            name: "HKDF",
            hash: "SHA-256",
            salt: bs(nonce),
            info: bs(new TextEncoder().encode(info)),
        },
        hkdf,
        256,
    );
    return crypto.subtle.importKey("raw", bits, "AES-GCM", false, ["encrypt", "decrypt"]);
}

async function run(): Promise<void> {
    const staticPrivate = crypto.getRandomValues(new Uint8Array(32));
    const ephemeralPrivate = crypto.getRandomValues(new Uint8Array(32));
    const staticPublic = x25519.getPublicKey(staticPrivate);
    const ephemeralPublic = x25519.getPublicKey(ephemeralPrivate);
    const bearer = "ab".repeat(32);
    const channel = await ObsetyncSecureChannel.create(
        b64(staticPublic),
        bearer,
        b64(ephemeralPublic),
        Math.floor(Date.now() / 1000) + 7200,
    );

    const method = "PUT";
    const path = "/api/v1/content/example";
    const body = new TextEncoder().encode("payload");
    const wire = await channel.encryptRequest(method, path, body, 42);
    assert.equal(wire[0], HTTP_WIRE_VERSION);
    assert.equal(wire.length, 53 + 64 + 8 + body.length + 16);

    const clientPublic = wire.slice(13, 45);
    const expectedFingerprint = new Uint8Array(
        await crypto.subtle.digest("SHA-256", bs(ephemeralPublic)),
    ).slice(0, 8);
    assert.deepEqual(wire.slice(45, 53), expectedFingerprint);

    const keyMaterial = concat(
        x25519.getSharedSecret(staticPrivate, clientPublic),
        x25519.getSharedSecret(ephemeralPrivate, clientPublic),
    );
    const requestNonce = extractRequestNonce(wire);
    const requestKey = await aesKey(keyMaterial, requestNonce, "obsetync/v2/c2s");
    const requestPlaintext = new Uint8Array(
        await crypto.subtle.decrypt(
            {
                name: "AES-GCM",
                iv: bs(requestNonce),
                additionalData: bs(new TextEncoder().encode(`obsetync/v2 ${method} ${path}`)),
            },
            requestKey,
            bs(wire.slice(53)),
        ),
    );
    assert.equal(new TextDecoder().decode(requestPlaintext.slice(0, 64)), bearer);
    assert.equal(new DataView(requestPlaintext.buffer).getBigUint64(64, false), 42n);
    assert.equal(new TextDecoder().decode(requestPlaintext.slice(72)), "payload");

    const responseNonce = crypto.getRandomValues(new Uint8Array(12));
    const responseKey = await aesKey(keyMaterial, responseNonce, "obsetync/v2/s2c");
    const responsePlaintext = concat(new Uint8Array([0x01, 0x99]), new TextEncoder().encode("conflict"));
    const responseAad = concat(
        new TextEncoder().encode(`obsetync/v2 ${method} ${path}`),
        requestNonce,
    );
    const responseCiphertext = new Uint8Array(
        await crypto.subtle.encrypt(
            { name: "AES-GCM", iv: bs(responseNonce), additionalData: bs(responseAad) },
            responseKey,
            bs(responsePlaintext),
        ),
    );
    const opened = await channel.decryptResponse(
        method,
        path,
        requestNonce,
        concat(new Uint8Array([HTTP_WIRE_VERSION]), responseNonce, responseCiphertext),
    );
    assert.equal(opened.status, 409);
    assert.equal(new TextDecoder().decode(opened.body), "conflict");
    await assert.rejects(
        channel.decryptResponse(method, path, requestNonce, new Uint8Array(256)),
        ObsetyncSessionStaleError,
    );

    const bootstrap = await ObsetyncSecureChannel.createBootstrap(b64(staticPublic));
    const bootstrapWire = await bootstrap.encryptRequest(
        "POST",
        "/api/v1/server-eph",
        new Uint8Array(),
        0,
    );
    assert.equal(bootstrapWire.length, 53 + 16);
    assert.deepEqual(bootstrapWire.slice(45, 53), new Uint8Array(8));
    const bootstrapNonce = extractRequestNonce(bootstrapWire);
    const bootstrapShared = x25519.getSharedSecret(
        staticPrivate,
        bootstrapWire.slice(13, 45),
    );
    const bootstrapKey = await aesKey(
        bootstrapShared,
        bootstrapNonce,
        "obsetync/v2/c2s-boot",
    );
    const bootstrapPlaintext = new Uint8Array(
        await crypto.subtle.decrypt(
            {
                name: "AES-GCM",
                iv: bs(bootstrapNonce),
                additionalData: bs(
                    new TextEncoder().encode("obsetync/v2 POST /api/v1/server-eph"),
                ),
            },
            bootstrapKey,
            bs(bootstrapWire.slice(53)),
        ),
    );
    assert.equal(bootstrapPlaintext.length, 0);

    staticPrivate.fill(0);
    ephemeralPrivate.fill(0);
    keyMaterial.fill(0);
    console.log("secure.test: 12 assertions passed");
}

void run();
