import { requestUrl, RequestUrlParam } from "obsidian";
import {
    ObsetyncSecureChannel,
    ObsetyncSecureTransportError,
    ObsetyncSessionStaleError,
    extractRequestNonce,
} from "./secure";
import { DurableSequenceAllocator } from "./transport-sequence";
import { validateFileDeltas } from "./delta-validation";
import { exactArrayBuffer } from "./binary";

export interface FileDelta {
    action: "added" | "modified" | "deleted" | "renamed";
    path: string;
    old_path?: string;
    hash?: string;
    size?: number;
    /** Server-side mtime of the entry. Present on added/modified deltas from
     *  servers ≥ 1.4.0. Required to rebase the local Merkle tree to the exact
     *  server root after a pull (leaf hashes cover mtime); when absent the
     *  rebase still runs but root parity can't be byte-verified. */
    mtime_ms?: number;
}

/** One unmergeable same-file divergence from a server-side merge. The server
 *  keeps side A (its current) in the tree; side B's blob stays retrievable by
 *  `side_b_hash` so the losing device can preserve its version as a conflict
 *  copy. (This mirrors the server's actual JSON — an earlier version of this
 *  type described a resolution/preserved_as flow that never existed.) */
export interface PushConflict {
    path: string;
    base_hash: string;
    side_a_hash: string;
    side_b_hash: string;
}

/** One point of the vault's server-side root history. */
export interface HistoryEntry {
    root: string;
    parent: string | null;
    created_ms: number;
    device_id: string;
    total_files: number;
    current: boolean;
}

export interface PushResult {
    accepted?: boolean;
    merged?: boolean;
    root_hash: string;
    conflicts?: PushConflict[];
    auto_resolved?: number;
    /** Same-file two-sided text edits line-merged server-side (server ≥ 1.5.0). */
    text_merged?: number;
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
    wire_version: "0x02";
    eph_endpoint: string;
    Es_pub_initial: string;
    Es_pub_valid_until: number;
}

export interface TransportPersistentState {
    wireVersion: string;
    esPub: string;
    esPubValidUntil: number;
    /** Highest sequence durably reserved, not merely the last one sent. */
    lastOutgoingSeq: number;
}

export interface TransportPersistence {
    get(): TransportPersistentState;
    update(patch: Partial<TransportPersistentState>): Promise<void>;
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
    private channelPromise: Promise<ObsetyncSecureChannel> | null = null;
    private refreshPromise: Promise<void> | null = null;
    private readonly sequences: DurableSequenceAllocator;

    constructor(
        private readonly serverUrl: string,
        private readonly serverBoxPubBase64: string,
        private readonly bearerTokenHex: string,
        private readonly transportPersistence?: TransportPersistence,
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
        this.sequences = new DurableSequenceAllocator({
            readReservedThrough: () =>
                this.requireTransportPersistence().get().lastOutgoingSeq,
            persistReservedThrough: async (value) => {
                await this.requireTransportPersistence().update({ lastOutgoingSeq: value });
            },
        });
    }

    /** Normalized server base URL — the WS channel derives ws:// from it. */
    get baseUrl(): string {
        return this.serverUrl;
    }

    /** Mint a single-use, short-TTL WebSocket ticket over the sealed channel,
     *  exchanging ephemeral X25519 pubkeys so both sides derive the sealed-
     *  frame session keys (wire v2). The ticket travels once, in the first
     *  (plaintext) auth frame — never in a URL. */
    async mintWsTicket(clientEphPubB64: string): Promise<{
        ticket: string;
        expires_at: number;
        server_eph_pub?: string;
    }> {
        const body = new TextEncoder().encode(
            JSON.stringify({ client_eph_pub: clientEphPubB64 }),
        );
        const res = await this.sealed("POST", "/api/v1/ws-ticket", body);
        if (!res.ok) throw new Error(`ws-ticket failed: ${res.status}`);
        return await res.json();
    }

    /** Lazily establish the ObsetyncSecureChannel. Called before the first encrypted
     *  request; subsequent requests reuse the same shared secret. */
    private async getChannel(): Promise<ObsetyncSecureChannel> {
        if (this.channel && !this.channel.isStale()) return this.channel;
        if (this.channelPromise) return this.channelPromise;
        if (this.refreshPromise) await this.refreshPromise;
        if (this.channel && !this.channel.isStale()) return this.channel;
        if (!this.serverBoxPubBase64) {
            throw new Error("ObsetyncApi: server box pubkey missing — re-enroll the device");
        }
        if (!this.bearerTokenHex) {
            throw new Error("ObsetyncApi: bearer token missing — re-enroll the device");
        }
        const persistence = this.requireTransportPersistence();
        this.channelPromise = (async () => {
            let state = persistence.get();
            if (state.wireVersion !== "0x02") {
                throw new Error("server transport upgraded to wire 0x02 — re-enroll this device");
            }
            if (!state.esPub || Date.now() / 1000 >= state.esPubValidUntil - 3600) {
                await this.refreshServerEphemeral();
                state = persistence.get();
            }
            this.channel = await ObsetyncSecureChannel.create(
                this.serverBoxPubBase64,
                this.bearerTokenHex,
                state.esPub,
                state.esPubValidUntil,
            );
            return this.channel;
        })();
        try {
            return await this.channelPromise;
        } finally {
            this.channelPromise = null;
        }
    }

    // --- Root ---

    async getRoot(vaultId: string): Promise<Uint8Array | null> {
        const path = `/api/v1/root/${vaultId}`;
        const res = await this.sealed("GET", path, new Uint8Array());
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`getRoot failed: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    /** Recent root history for the rollback UI, newest first. */
    async getHistory(vaultId: string): Promise<HistoryEntry[]> {
        const path = `/api/v1/history/${vaultId}`;
        const res = await this.sealed("GET", path, new Uint8Array());
        if (!res.ok) throw new Error(`getHistory failed: ${res.status}`);
        const body = await res.json();
        return (body?.roots ?? []) as HistoryEntry[];
    }

    /** Roll the vault's current root back to an earlier point in history.
     *  The server validates the hash exists; devices converge on their next
     *  pull. Deliberately bypasses the stale-tree guard server-side — this
     *  is the explicit, human-initiated revert. */
    async rollbackVault(vaultId: string, rootHash: string): Promise<void> {
        const path = `/api/v1/rollback/${vaultId}`;
        const res = await this.sealed("POST", path, new TextEncoder().encode(rootHash));
        if (!res.ok) throw new Error(`rollback failed: ${res.status}`);
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
        return validateFileDeltas(await res.json());
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
        const res = await requestUrl({
            url: `${adminUrl}/admin/enrollment/${code}`,
            throw: false,
        });
        const body = res.json;
        if (res.status < 200 || res.status >= 300) {
            throw new Error(`enrollment failed: ${body?.error ?? res.status}`);
        }
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
        let refreshed = false;
        let replayRecovered = false;
        for (;;) {
            const channel = await this.getChannel();
            const sequence = await this.nextSequence();
            let response: FetchLike;
            try {
                response = await this.sendEncrypted(channel, method, path, body, sequence);
            } catch (error) {
                if (error instanceof ObsetyncSessionStaleError && !refreshed) {
                    refreshed = true;
                    if (this.channel === channel || this.channel === null) {
                        this.channel = null;
                        await this.refreshServerEphemeral();
                    }
                    continue;
                }
                if (error instanceof ObsetyncSecureTransportError) {
                    throw new Error(`decrypt ${method} ${path}: ${error.message}`);
                }
                throw error;
            }

            if (response.status === 401 && !replayRecovered) {
                const error = await response.json().catch(() => null);
                if (error?.error === "replay" && Number.isSafeInteger(error.last_seen_seq)) {
                    replayRecovered = true;
                    await this.recoverSequence(error.last_seen_seq);
                    continue;
                }
            }
            return response;
        }
    }

    private async sendEncrypted(
        channel: ObsetyncSecureChannel,
        method: string,
        path: string,
        body: Uint8Array,
        sequence: number,
    ): Promise<FetchLike> {
        const wireBody = await channel.encryptRequest(method, path, body, sequence);
        const nonceReq = extractRequestNonce(wireBody);
        const params: RequestUrlParam = {
            url: `${this.serverUrl}${path}`,
            method: "POST",
            headers: {
                "Content-Type": "application/octet-stream",
                "X-Obsetync-Method": method,
            },
            body: exactArrayBuffer(wireBody),
            throw: false,
        };
        const wireResponse = await requestUrl(params);
        if (wireResponse.status !== 200) {
            // Reverse proxies can still emit their own plaintext failures;
            // the application server itself always responds with wire 200.
            return {
                status: wireResponse.status,
                ok: false,
                arrayBuffer: async () => wireResponse.arrayBuffer,
                json: async () => wireResponse.json,
            };
        }

        const opened = await channel.decryptResponse(
            method,
            path,
            nonceReq,
            new Uint8Array(wireResponse.arrayBuffer),
        );
        const plaintext = opened.body;
        return {
            status: opened.status,
            ok: opened.status >= 200 && opened.status < 300,
            arrayBuffer: async () => exactArrayBuffer(plaintext),
            json: async () => {
                const text = new TextDecoder().decode(plaintext);
                return text.length ? JSON.parse(text) : null;
            },
        };
    }

    private async refreshServerEphemeral(): Promise<void> {
        if (this.refreshPromise) return this.refreshPromise;
        this.refreshPromise = this.refreshServerEphemeralOnce();
        try {
            await this.refreshPromise;
        } finally {
            this.refreshPromise = null;
        }
    }

    private async refreshServerEphemeralOnce(): Promise<void> {
        const persistence = this.requireTransportPersistence();
        const bootstrap = await ObsetyncSecureChannel.createBootstrap(
            this.serverBoxPubBase64,
        );
        const response = await this.sendEncrypted(
            bootstrap,
            "POST",
            "/api/v1/server-eph",
            new Uint8Array(),
            0,
        );
        if (!response.ok) {
            throw new Error(
                `server ephemeral refresh failed: ${response.status}; ` +
                "re-enroll device",
            );
        }
        const bundle = await response.json();
        if (
            typeof bundle?.Es_pub !== "string" ||
            !Number.isSafeInteger(bundle?.valid_until) ||
            bundle.valid_until <= Date.now() / 1000
        ) {
            throw new Error("server returned an invalid transport-v2 ephemeral bundle");
        }
        await persistence.update({
            esPub: bundle.Es_pub,
            esPubValidUntil: bundle.valid_until,
            wireVersion: "0x02",
        });
        this.channel = null;
    }

    private async nextSequence(): Promise<number> {
        return this.sequences.next();
    }

    private async recoverSequence(greatestSeen: number): Promise<void> {
        await this.sequences.recover(greatestSeen);
    }

    private requireTransportPersistence(): TransportPersistence {
        if (!this.transportPersistence) {
            throw new Error("transport-v2 state unavailable — re-enroll device");
        }
        return this.transportPersistence;
    }
}
