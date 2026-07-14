/**
 * Ph2 notify channel (client side): a WebSocket that tells us "this vault's
 * root changed — pull now" seconds after another device pushes, instead of
 * waiting out the 30s poll.
 *
 * Design invariants (tasks/realtime-roadmap.md):
 * - Data NEVER depends on this channel. Frames carry only a root hash; the
 *   actual delta still travels over the sealed HTTP pull. If the socket is
 *   down, the regular poll covers everything.
 * - Auth is a single-use short-TTL ticket minted over the sealed API — the
 *   long-lived bearer never appears in a ws:// URL.
 * - Native `WebSocket` exists in both Electron and iOS WKWebView; no
 *   requestUrl involvement.
 */
import { ObsetyncApi } from "./api";

export type WsState = "off" | "connecting" | "connected" | "backoff";

const PING_INTERVAL_MS = 30_000;
const BACKOFF_START_MS = 1_000;
const BACKOFF_CAP_MS = 60_000;

export class ObsetyncWsChannel {
    private ws: WebSocket | null = null;
    private state: WsState = "off";
    private backoffMs = BACKOFF_START_MS;
    private reconnectTimer: number | null = null;
    private pingTimer: number | null = null;
    private lastFrameMs = 0;
    private stopped = true;
    private visibilityListener: (() => void) | null = null;

    constructor(
        private api: ObsetyncApi,
        private vaultId: string,
        private onRootChanged: (root: string) => void,
    ) {}

    start(): void {
        this.stopped = false;
        // Mobile suspends sockets in the background; on resume, reconnect
        // immediately instead of waiting out a backoff window.
        this.visibilityListener = () => {
            if (document.visibilityState === "visible") this.ensureConnected();
        };
        document.addEventListener("visibilitychange", this.visibilityListener);
        void this.connect();
    }

    stop(): void {
        this.stopped = true;
        if (this.reconnectTimer !== null) window.clearTimeout(this.reconnectTimer);
        if (this.pingTimer !== null) window.clearInterval(this.pingTimer);
        this.reconnectTimer = null;
        this.pingTimer = null;
        if (this.visibilityListener) {
            document.removeEventListener("visibilitychange", this.visibilityListener);
            this.visibilityListener = null;
        }
        try {
            this.ws?.close();
        } catch {
            /* already dead */
        }
        this.ws = null;
        this.setState("off");
    }

    isConnected(): boolean {
        return this.state === "connected";
    }

    getState(): WsState {
        return this.state;
    }

    /** ms since the last frame (root or pong), -1 if never. */
    lastFrameAgeMs(): number {
        return this.lastFrameMs ? Date.now() - this.lastFrameMs : -1;
    }

    /** Reconnect now if the socket is not live (app resume, manual sync). */
    ensureConnected(): void {
        if (this.stopped || this.state === "connected" || this.state === "connecting") return;
        if (this.reconnectTimer !== null) {
            window.clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        this.backoffMs = BACKOFF_START_MS;
        void this.connect();
    }

    private setState(s: WsState): void {
        this.state = s;
    }

    private wsUrl(ticket: string): string {
        const base = this.api.baseUrl
            .replace(/^https:/, "wss:")
            .replace(/^http:/, "ws:")
            .replace(/\/+$/, "");
        return `${base}/api/v1/ws?ticket=${ticket}`;
    }

    private async connect(): Promise<void> {
        if (this.stopped) return;
        this.setState("connecting");
        let ticket: string;
        try {
            ticket = (await this.api.mintWsTicket()).ticket;
        } catch (e) {
            console.warn("[obsetync] ws: ticket mint failed, backing off:", e);
            this.scheduleReconnect();
            return;
        }
        if (this.stopped) return;

        let ws: WebSocket;
        try {
            ws = new WebSocket(this.wsUrl(ticket));
        } catch (e) {
            console.warn("[obsetync] ws: connect failed:", e);
            this.scheduleReconnect();
            return;
        }
        this.ws = ws;

        ws.onopen = () => {
            if (this.stopped) return;
            ws.send(JSON.stringify({ v: 1, t: "sub", vaults: [this.vaultId] }));
            this.setState("connected");
            this.backoffMs = BACKOFF_START_MS;
            this.lastFrameMs = Date.now();
            console.log("[obsetync] ws: connected");
            if (this.pingTimer !== null) window.clearInterval(this.pingTimer);
            this.pingTimer = window.setInterval(() => {
                try {
                    ws.send(JSON.stringify({ v: 1, t: "ping" }));
                } catch {
                    /* onclose will fire */
                }
            }, PING_INTERVAL_MS);
        };

        ws.onmessage = (ev: MessageEvent) => {
            this.lastFrameMs = Date.now();
            let frame: { t?: string; vault?: string; root?: string };
            try {
                frame = JSON.parse(String(ev.data));
            } catch {
                return;
            }
            if (frame.t === "root" && frame.vault === this.vaultId && frame.root) {
                console.log(`[obsetync] ws: root changed → ${frame.root.slice(0, 12)}, pulling`);
                this.onRootChanged(frame.root);
            }
        };

        const onGone = () => {
            if (this.pingTimer !== null) {
                window.clearInterval(this.pingTimer);
                this.pingTimer = null;
            }
            if (this.ws === ws) this.ws = null;
            this.scheduleReconnect();
        };
        ws.onclose = onGone;
        ws.onerror = onGone;
    }

    private scheduleReconnect(): void {
        if (this.stopped) return;
        if (this.reconnectTimer !== null) return; // one pending attempt at a time
        this.setState("backoff");
        const delay = this.backoffMs;
        this.backoffMs = Math.min(this.backoffMs * 2, BACKOFF_CAP_MS);
        this.reconnectTimer = window.setTimeout(() => {
            this.reconnectTimer = null;
            void this.connect();
        }, delay);
    }
}
