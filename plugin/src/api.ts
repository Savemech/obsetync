import { requestUrl, RequestUrlParam } from "obsidian";

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

interface FetchLike {
    status: number;
    ok: boolean;
    arrayBuffer(): Promise<ArrayBuffer>;
    json(): Promise<any>;
}

/**
 * HTTP client for the ObsetyNC sync server API.
 *
 * Transport is selected automatically at construction:
 *   Desktop (Electron): Node.js `https` module — supports mTLS client cert.
 *   Mobile (iOS/Android): Obsidian `requestUrl` — standard HTTPS, no client cert.
 *
 * Auth is bearer token on BOTH platforms. Desktop also presents the mTLS client
 * cert as an extra layer, but the bearer token is what the server actually checks.
 * This allows the same server to serve all platforms without separate endpoints.
 */
export class SyncApi {
    private readonly isNode: boolean;
    private https: any; // Node.js https module — only loaded on desktop
    private readonly hostname: string;
    private readonly port: string;

    constructor(
        private readonly serverUrl: string,
        private readonly certPem = "",
        private readonly keyPem = "",
        private readonly bearerToken = ""
    ) {
        this.serverUrl = serverUrl.replace(/\/$/, "");
        // Detect Electron: window.require exists in Electron renderer, not in iOS WKWebView.
        this.isNode = typeof (window as any).require === "function";
        if (this.isNode) {
            this.https = (window as any).require("https");
        }
        const u = new URL(this.serverUrl);
        this.hostname = u.hostname;
        this.port = u.port || (u.protocol === "https:" ? "443" : "80");
    }

    // --- Root ---

    async getRoot(vaultId: string): Promise<Uint8Array | null> {
        const res = await this.fetch(`/api/v1/root/${vaultId}`);
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`getRoot failed: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putRoot(
        vaultId: string,
        rootBytes: Uint8Array,
        parentHash: string
    ): Promise<PushResult> {
        const res = await this.fetch(`/api/v1/root/${vaultId}`, {
            method: "PUT",
            headers: { "X-Parent-Root": parentHash },
            body: rootBytes,
        });
        if (!res.ok) throw new Error(`putRoot failed: ${res.status}`);
        return res.json();
    }

    // --- Diff ---

    async getDiff(
        vaultId: string,
        deviceRootHash: string
    ): Promise<FileDelta[] | null> {
        const res = await this.fetch(`/api/v1/diff/${vaultId}`, {
            method: "POST",
            headers: { "X-Device-Root": deviceRootHash },
        });
        if (res.status === 304) return null;
        if (!res.ok) throw new Error(`getDiff failed: ${res.status}`);
        return res.json();
    }

    // --- Index chunks ---

    async getChunk(hash: string): Promise<Uint8Array> {
        const res = await this.fetch(`/api/v1/chunk/${hash}`);
        if (!res.ok) throw new Error(`getChunk ${hash}: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putChunk(hash: string, data: Uint8Array): Promise<void> {
        const res = await this.fetch(`/api/v1/chunk/${hash}`, {
            method: "PUT",
            body: data,
        });
        if (!res.ok) throw new Error(`putChunk ${hash}: ${res.status}`);
    }

    async checkChunks(hashes: string[]): Promise<string[]> {
        const res = await this.fetch("/api/v1/chunks/check", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(hashes),
        });
        if (!res.ok) throw new Error(`checkChunks: ${res.status}`);
        return (await res.json()).needed;
    }

    // --- Content (small files) ---

    async getContent(hash: string): Promise<Uint8Array> {
        const res = await this.fetch(`/api/v1/content/${hash}`);
        if (!res.ok) throw new Error(`getContent ${hash}: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putContent(hash: string, data: Uint8Array): Promise<void> {
        const res = await this.fetch(`/api/v1/content/${hash}`, {
            method: "PUT",
            body: data,
        });
        if (!res.ok) throw new Error(`putContent ${hash}: ${res.status}`);
    }

    async checkContent(hashes: string[]): Promise<string[]> {
        const res = await this.fetch("/api/v1/content/check", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(hashes),
        });
        if (!res.ok) throw new Error(`checkContent: ${res.status}`);
        return (await res.json()).needed;
    }

    // --- Content manifests (large files) ---

    async getManifest(hash: string): Promise<FileManifest> {
        const res = await this.fetch(`/api/v1/content/manifest/${hash}`);
        if (!res.ok) throw new Error(`getManifest ${hash}: ${res.status}`);
        return res.json();
    }

    async putManifest(hash: string, manifest: FileManifest): Promise<void> {
        const res = await this.fetch(`/api/v1/content/manifest/${hash}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(manifest),
        });
        if (!res.ok) throw new Error(`putManifest ${hash}: ${res.status}`);
    }

    // --- Content sub-file chunks ---

    async getContentChunk(hash: string): Promise<Uint8Array> {
        const res = await this.fetch(`/api/v1/content/chunk/${hash}`);
        if (!res.ok) throw new Error(`getContentChunk ${hash}: ${res.status}`);
        return new Uint8Array(await res.arrayBuffer());
    }

    async putContentChunk(hash: string, data: Uint8Array): Promise<void> {
        const res = await this.fetch(`/api/v1/content/chunk/${hash}`, {
            method: "PUT",
            body: data,
        });
        if (!res.ok) throw new Error(`putContentChunk ${hash}: ${res.status}`);
    }

    async checkContentChunks(hashes: string[]): Promise<string[]> {
        const res = await this.fetch("/api/v1/content/chunks/check", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(hashes),
        });
        if (!res.ok) throw new Error(`checkContentChunks: ${res.status}`);
        return (await res.json()).needed;
    }

    // --- Health / connectivity ---

    async ping(): Promise<{
        serverUrl: string;
        tlsVersion: string;
        cipher: string;
        serverFingerprint: string;
        deviceCert: boolean;
    }> {
        if (!this.isNode) {
            // Mobile: requestUrl doesn't expose TLS socket details.
            const res = await this.fetch("/health");
            if (!res.ok) throw new Error("server unreachable");
            return {
                serverUrl: this.serverUrl,
                tlsVersion: "N/A",
                cipher:     "N/A",
                serverFingerprint: "N/A",
                deviceCert: false,
            };
        }

        // Desktop: use raw Node.js https to capture TLS socket info.
        return new Promise((resolve, reject) => {
            const options: any = {
                hostname: this.hostname,
                port:     this.port,
                path:     "/health",
                method:   "GET",
                rejectUnauthorized: false,
            };
            if (this.certPem && this.keyPem) {
                options.cert = this.certPem;
                options.key  = this.keyPem;
            }
            const req = this.https.request(options, (res: any) => {
                const sock = res.socket;
                const cert = sock?.getPeerCertificate?.() ?? {};
                const info = {
                    serverUrl:         this.serverUrl,
                    tlsVersion:        sock?.getProtocol?.() ?? "unknown",
                    cipher:            sock?.getCipher?.()?.name ?? "unknown",
                    serverFingerprint: cert.fingerprint256 ?? cert.fingerprint ?? "unknown",
                    deviceCert:        !!(this.certPem && this.keyPem),
                };
                res.resume();
                res.on("end",   () => resolve(info));
                res.on("error", reject);
            });
            req.on("error", reject);
            req.end();
        });
    }

    // --- Enrollment ---

    async claimEnrollment(
        code: string
    ): Promise<{ cert_pem: string; key_pem: string; fingerprint: string; bearer_token: string }> {
        // Enrollment goes through the admin port (plain HTTP).
        // global fetch works on all platforms — no mTLS needed here.
        const adminUrl = this.serverUrl
            .replace(/^https/, "http")
            .replace(/:\d+$/, ":27183");
        const res = await fetch(`${adminUrl}/admin/enrollment/${code}`);
        const body = await res.json();
        if (!res.ok) throw new Error(`enrollment failed: ${body.error ?? res.status}`);
        return body;
    }

    // --- Internal fetch dispatcher ---

    private fetch(
        path: string,
        init: {
            method?:  string;
            headers?: Record<string, string>;
            body?:    Uint8Array | string;
        } = {}
    ): Promise<FetchLike> {
        return this.isNode
            ? this.fetchNode(path, init)
            : this.fetchWeb(path, init);
    }

    /** Desktop path: Node.js https with mTLS client cert + bearer token header. */
    private fetchNode(
        path: string,
        init: { method?: string; headers?: Record<string, string>; body?: Uint8Array | string }
    ): Promise<FetchLike> {
        return new Promise((resolve, reject) => {
            const headers: Record<string, string> = { ...(init.headers ?? {}) };
            if (this.bearerToken) {
                headers["Authorization"] = `Bearer ${this.bearerToken}`;
            }
            if (init.body instanceof Uint8Array) {
                headers["Content-Length"] = String(init.body.byteLength);
            } else if (typeof init.body === "string") {
                headers["Content-Length"] = String(Buffer.byteLength(init.body));
            }

            const options: any = {
                hostname: this.hostname,
                port:     this.port,
                path,
                method:   init.method ?? "GET",
                headers,
                rejectUnauthorized: false, // self-signed CA
            };
            if (this.certPem && this.keyPem) {
                options.cert = this.certPem;
                options.key  = this.keyPem;
            }

            const req = this.https.request(options, (res: any) => {
                const chunks: Buffer[] = [];
                res.on("data",  (chunk: Buffer) => chunks.push(chunk));
                res.on("end",   () => {
                    const buf    = Buffer.concat(chunks);
                    const status = res.statusCode ?? 0;
                    resolve({
                        status,
                        ok: status >= 200 && status < 300,
                        arrayBuffer: async () =>
                            buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength) as ArrayBuffer,
                        json: async () => JSON.parse(buf.toString("utf8")),
                    });
                });
                res.on("error", reject);
            });
            req.on("error", reject);

            if (init.body instanceof Uint8Array) {
                req.write(Buffer.from(init.body));
            } else if (typeof init.body === "string") {
                req.write(init.body);
            }
            req.end();
        });
    }

    /**
     * Mobile path: Obsidian requestUrl + bearer token header.
     * requestUrl uses NSURLSession (iOS) / Electron net (desktop fallback).
     * Obsidian ships with NSAllowsArbitraryLoads so self-signed server certs work.
     * No client cert — bearer token is the sole auth mechanism on mobile.
     */
    private async fetchWeb(
        path: string,
        init: { method?: string; headers?: Record<string, string>; body?: Uint8Array | string }
    ): Promise<FetchLike> {
        const headers: Record<string, string> = { ...(init.headers ?? {}) };
        if (this.bearerToken) {
            headers["Authorization"] = `Bearer ${this.bearerToken}`;
        }

        let body: ArrayBuffer | string | undefined;
        if (init.body instanceof Uint8Array) {
            // Slice correctly — Uint8Array may be a view into a larger buffer.
            const u = init.body;
            body = (u.buffer as ArrayBuffer).slice(u.byteOffset, u.byteOffset + u.byteLength);
            headers["Content-Length"] = String(u.byteLength);
        } else if (typeof init.body === "string") {
            body = init.body;
        }

        const params: RequestUrlParam = {
            url:    `${this.serverUrl}${path}`,
            method: init.method ?? "GET",
            headers,
            throw:  false, // never throw — we inspect status ourselves
        };
        if (body !== undefined) params.body = body;

        const res = await requestUrl(params);
        return {
            status: res.status,
            ok:     res.status >= 200 && res.status < 300,
            arrayBuffer: async () => res.arrayBuffer,
            json:        async () => res.json,
        };
    }
}
