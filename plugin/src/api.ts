import { requestUrl, RequestUrlParam } from "obsidian";
import {
    ObsetyncSecureChannel,
    ObsetyncSecureTransportError,
    extractRequestNonce,
} from "./secure";

export interface FileDelta {
    action: "added" | "modified" | "deleted" | "renamed";
    path: string;
    old_path?: string;
    hash?: string;
    size?: number;
}

export interface PushResult {
    accepted?: boolean;
    merged?: boolean;
    root_hash: string;
    conflicts?: Array<{
        path: string;
        resolution: string;
        preserved_as?: string;
    }>;
    auto_resolved?: number;
}

export interface FileManifest {
    file_hash: string;
    total_size: number;
    chunks: Array<{ hash: string; offset: number; size: number }>;
}

/** Enrollment bundle returned by the admin UI's /admin/enrollment/{code} endpoint. */
export interface EnrollmentBundle {
    device_name: string;
    device_id: string;
    bearer_token: string;
    server_box_pub: string;
}

interface FetchLike {
    status: number;
    ok: boolean;
    arrayBuffer(): Promise<ArrayBuffer>;
    json(): Promise<any>;
}

/**
 * HTTP client for the ObsetyNC sync server.
 *
 * Transport uses the AEAD envelope defined in `secure.ts`:
 *   X25519 ECDH + HKDF-SHA256 + AES-256-GCM over plain HTTP. No TLS, no certs,
 *   no CA trust store involvement — the server's X25519 public key, learned
 *   at enrollment, is the only pinning.
 *
 * Identical code path on desktop (Electron) and mobile (iOS WKWebView) via
 * Obsidian's `requestUrl`. One allowlist, one transport, one bug surface.
 */
export class ObsetyncApi {
    private channel: ObsetyncSecureChannel | null = null;

    constructor(
        private readonly serverUrl: string,
        private readonly serverBoxPubBase64: string,
        private readonly bearerTokenHex: string,
    ) {
        // The sync port speaks plain HTTP — the AEAD envelope is the trust
        // boundary. Fold legacy https:// URLs down to http:// transparently
        // so users migrating from 1.0.x don't trip ERR_SSL_PROTOCOL_ERROR
        // after the server drops its cert stack.
        let u = serverUrl.replace(/\/$/, "");
        if (u.startsWith("https://")) {
            u = "http://" + u.slice("https://".length);
            console.warn(
                "[obsetync] rewrote legacy https:// server URL to http:// " +
                "(transport is plaintext HTTP + AEAD envelope)"
            );
        }
        this.serverUrl = u;
    }

    /** Lazily establish the ObsetyncSecureChannel. Called before the first encrypted
     *  request; subsequent requests reuse the same shared secret. */
    private async getChannel(): Promise<ObsetyncSecureChannel> {
        if (this.channel) return this.channel;
        if (!this.serverBoxPubBase64) {
            throw new Error("ObsetyncApi: server box pubkey missing — re-enroll the device");
        }
        if (!this.bearerTokenHex) {
            throw new Error("ObsetyncApi: bearer token missing — re-enroll the device");
        }
        this.channel = await ObsetyncSecureChannel.create(this.serverBoxPubBase64, this.bearerTokenHex);
        return this.channel;
    }

    // --- Root ---

    async getRoot(vaultId: string): Promise<Uint8Array | null> {
        const path = `/api/v1/root/${vaultId}`;
        const res = await this.sealed("GET", path, new Uint8Array());
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`getRoot failed: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putRoot(
        vaultId: string,
        rootBytes: Uint8Array,
        parentHash: string,
    ): Promise<PushResult> {
        const path = `/api/v1/root/${vaultId}`;
        // Parent-root used to go as a header. With encryption the header would
        // be outside the AEAD envelope; prepend it to the body as a 64-char
        // ASCII hex prefix instead so the server authenticates it too.
        const header = new TextEncoder().encode(parentHash.padEnd(64, " "));
        const body = new Uint8Array(header.length + rootBytes.length);
        body.set(header, 0);
        body.set(rootBytes, header.length);
        const res = await this.sealed("PUT", path, body);
        if (!res.ok) throw new Error(`putRoot failed: ${res.status}`);
        return res.json();
    }

    // --- Diff ---

    async getDiff(vaultId: string, deviceRootHash: string): Promise<FileDelta[] | null> {
        const path = `/api/v1/diff/${vaultId}`;
        // Same trick — device-root prepended to body instead of a header.
        const body = new TextEncoder().encode(deviceRootHash.padEnd(64, " "));
        const res = await this.sealed("POST", path, body);
        if (res.status === 304) return null;
        // 404 = vault has no root on the server yet (fresh server, first
        // push hasn't landed). Treat as "nothing to pull" and let the push
        // path seed the vault.
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`getDiff failed: ${res.status}`);
        return res.json();
    }

    // --- Index chunks ---

    async getChunk(hash: string): Promise<Uint8Array> {
        const res = await this.sealed("GET", `/api/v1/chunk/${hash}`, new Uint8Array());
        if (!res.ok) throw new Error(`getChunk ${hash}: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putChunk(hash: string, data: Uint8Array): Promise<void> {
        const res = await this.sealed("PUT", `/api/v1/chunk/${hash}`, data);
        if (!res.ok) throw new Error(`putChunk ${hash}: ${res.status}`);
    }

    async checkChunks(hashes: string[]): Promise<string[]> {
        const body = new TextEncoder().encode(JSON.stringify(hashes));
        const res = await this.sealed("POST", "/api/v1/chunks/check", body);
        if (!res.ok) throw new Error(`checkChunks: ${res.status}`);
        return (await res.json()).needed;
    }

    // --- Content (small files) ---

    async getContent(hash: string): Promise<Uint8Array> {
        const res = await this.sealed("GET", `/api/v1/content/${hash}`, new Uint8Array());
        if (!res.ok) throw new Error(`getContent ${hash}: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putContent(hash: string, data: Uint8Array): Promise<void> {
        const res = await this.sealed("PUT", `/api/v1/content/${hash}`, data);
        if (!res.ok) throw new Error(`putContent ${hash}: ${res.status}`);
    }

    async checkContent(hashes: string[]): Promise<string[]> {
        const body = new TextEncoder().encode(JSON.stringify(hashes));
        const res = await this.sealed("POST", "/api/v1/content/check", body);
        if (!res.ok) throw new Error(`checkContent: ${res.status}`);
        return (await res.json()).needed;
    }

    // --- Content manifests (large files) ---

    async getManifest(hash: string): Promise<FileManifest> {
        const res = await this.sealed("GET", `/api/v1/content/manifest/${hash}`, new Uint8Array());
        if (!res.ok) throw new Error(`getManifest ${hash}: ${res.status}`);
        return res.json();
    }

    async putManifest(hash: string, manifest: FileManifest): Promise<void> {
        const body = new TextEncoder().encode(JSON.stringify(manifest));
        const res = await this.sealed("PUT", `/api/v1/content/manifest/${hash}`, body);
        if (!res.ok) throw new Error(`putManifest ${hash}: ${res.status}`);
    }

    async checkManifests(hashes: string[]): Promise<string[]> {
        const body = new TextEncoder().encode(JSON.stringify(hashes));
        const res = await this.sealed("POST", "/api/v1/content/manifests/check", body);
        if (!res.ok) throw new Error(`checkManifests: ${res.status}`);
        return (await res.json()).needed;
    }

    // --- Content sub-file chunks ---

    async getContentChunk(hash: string): Promise<Uint8Array> {
        const res = await this.sealed("GET", `/api/v1/content/chunk/${hash}`, new Uint8Array());
        if (!res.ok) throw new Error(`getContentChunk ${hash}: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putContentChunk(hash: string, data: Uint8Array): Promise<void> {
        const res = await this.sealed("PUT", `/api/v1/content/chunk/${hash}`, data);
        if (!res.ok) throw new Error(`putContentChunk ${hash}: ${res.status}`);
    }

    async checkContentChunks(hashes: string[]): Promise<string[]> {
        const body = new TextEncoder().encode(JSON.stringify(hashes));
        const res = await this.sealed("POST", "/api/v1/content/chunks/check", body);
        if (!res.ok) throw new Error(`checkContentChunks: ${res.status}`);
        return (await res.json()).needed;
    }

    // --- Health / connectivity ---

    async ping(): Promise<{ serverUrl: string; ok: boolean; transport: string }> {
        // /health is the only plaintext route. Client calls it pre-enrollment
        // to verify the URL is reachable without needing serverBoxPub yet.
        try {
            const res = await requestUrl({
                url: `${this.serverUrl}/health`,
                method: "GET",
                throw: false,
            });
            return {
                serverUrl: this.serverUrl,
                ok: res.status >= 200 && res.status < 300,
                transport: "http + AEAD envelope",
            };
        } catch (e: any) {
            return { serverUrl: this.serverUrl, ok: false, transport: `error: ${e?.message ?? e}` };
        }
    }

    // --- Enrollment ---

    async claimEnrollment(code: string): Promise<EnrollmentBundle> {
        // Admin port is plain HTTP (enrollment UX). User runs it behind
        // whatever trust boundary they want (localhost, VPN, SSH tunnel).
        const adminUrl = this.serverUrl
            .replace(/^https:/, "http:")
            .replace(/:\d+$/, ":27183");
        const res = await fetch(`${adminUrl}/admin/enrollment/${code}`);
        const body = await res.json();
        if (!res.ok) throw new Error(`enrollment failed: ${body.error ?? res.status}`);
        return body;
    }

    // --- Internal: encrypted request/response ---

    /**
     * Seal `body` with the ObsetyncSecureChannel, POST it to `path`, unseal the
     * response. This is the single code path for every sync API call.
     *
     * Note: every route maps to POST on the wire even if the semantic method
     * is GET/PUT/DELETE. The semantic method is preserved in the AAD so the
     * server still routes correctly, but HTTP-level always POST avoids
     * issues with iOS's requestUrl not sending a body on GET.
     */
    private async sealed(method: string, path: string, body: Uint8Array): Promise<FetchLike> {
        const channel = await this.getChannel();
        const wireBody = await channel.encryptRequest(method, path, body);
        // The response AAD binds this request's nonce — keep it so the
        // answer can't be swapped with one minted for another request.
        const nonceReq = extractRequestNonce(wireBody);

        const params: RequestUrlParam = {
            url: `${this.serverUrl}${path}`,
            method: "POST",
            headers: {
                "Content-Type": "application/octet-stream",
                "X-Obsetync-Method": method,
            },
            body: (wireBody.buffer as ArrayBuffer).slice(
                wireBody.byteOffset,
                wireBody.byteOffset + wireBody.byteLength,
            ),
            throw: false,
        };

        const res = await requestUrl(params);

        // Non-2xx responses are never decrypted. Middleware-generated errors
        // (401 / 403 / 400) are plaintext strings; handler-generated errors
        // (e.g. 404 from get_root) arrive as encrypted envelopes — but no
        // caller reads a non-2xx body, only the status, so neither needs
        // opening here.
        const isOk = res.status >= 200 && res.status < 300;
        if (!isOk) {
            return {
                status: res.status,
                ok: false,
                arrayBuffer: async () => res.arrayBuffer,
                json: async () => res.json,
            };
        }

        const wireResp = new Uint8Array(res.arrayBuffer);
        let plaintext: Uint8Array;
        try {
            plaintext = await channel.decryptResponse(method, path, nonceReq, wireResp);
        } catch (e) {
            if (e instanceof ObsetyncSecureTransportError) {
                throw new Error(`decrypt ${method} ${path}: ${e.message}`);
            }
            throw e;
        }

        const ptBuffer = (plaintext.buffer as ArrayBuffer).slice(
            plaintext.byteOffset,
            plaintext.byteOffset + plaintext.byteLength,
        );
        return {
            status: res.status,
            ok: true,
            arrayBuffer: async () => ptBuffer,
            json: async () => {
                const text = new TextDecoder().decode(plaintext);
                return text.length ? JSON.parse(text) : null;
            },
        };
    }
}
