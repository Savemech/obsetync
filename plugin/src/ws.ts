/**
 * Realtime channel (client): notify (Ph2) + presence (Ph3) over WebSocket,
 * sealed frames (wire v2).
 *
 * Handshake:
 *   1. generate ephemeral X25519 pair → sealed HTTP mint of a single-use
 *      ticket carrying the server's ephemeral pub → both sides derive
 *      directional AES-256-GCM session keys (secure.ObsetyncWsSession).
 *   2. connect ws:// WITHOUT the ticket in the URL; first frame is a
 *      plaintext `{"v":2,"t":"auth","ticket":...}` (single-use, burns on
 *      arrival — it never appears in access logs or proxy history).
 *   3. server answers with a SEALED `{"t":"ready"}` — proof both sides hold
 *      the same keys — then everything both ways is sealed Binary frames
 *      with per-direction sequence counters.
 *
 * Design invariants (tasks/realtime-roadmap.md): data NEVER depends on this
 * channel — root frames carry only hashes (pull stays sealed HTTP), presence
 * frames are ephemeral awareness. Socket down = plain polling, nothing lost.
 */
import { ObsetyncApi } from "./api";
import { generateWsEphKeypair, ObsetyncWsSession } from "./secure";

export type WsState = "off" | "connecting" | "connected" | "backoff";

export interface PresenceUpdate {
    device: string;
    name: string;
    file: string | null;
    state: "active" | "idle" | "offline";
}

const PING_INTERVAL_MS = 30_000;
const BACKOFF_START_MS = 1_000;
const BACKOFF_CAP_MS = 60_000;

export class ObsetyncWsChannel {
    private ws: WebSocket | null = null;
    private session: ObsetyncWsSession | null = null;
    private state: WsState = "off";
    private backoffMs = BACKOFF_START_MS;
    private reconnectTimer: number | null = null;
    private pingTimer: number | null = null;
    private lastFrameMs = 0;
    private stopped = true;
    private visibilityListener: (() => void) | null = null;
    /** Serialize decrypts/encrypts so sequence counters match wire order. */
    private rxChain: Promise<void> = Promise.resolve();
    private txChain: Promise<void> = Promise.resolve();

    constructor(
        private api: ObsetyncApi,
        private vaultId: string,
        private onRootChanged: (root: string) => void,
        private onPresence: (p: PresenceUpdate) => void = () => {},
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
        this.session = null;
        this.setState("off");
    }

    isConnected(): boolean {
        return this.state === "connected";
    }

    getState(): WsState {
        return this.state;
    }

    /** ms since the last frame (any sealed frame), -1 if never. */
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

    /** Share what this device is looking at (Ph3). No-op unless connected. */
    sendPresence(file: string | null, state: "active" | "idle"): void {
        this.sendSealed({
            v: 2,
            t: "presence",
            vault: this.vaultId,
            file,
            state,
        });
    }

    private setState(s: WsState): void {
        this.state = s;
    }

    private wsUrl(): string {
        const base = this.api.baseUrl
            .replace(/^https:/, "wss:")
            .replace(/^http:/, "ws:")
            .replace(/\/+$/, "");
        return `${base}/api/v1/ws`;
    }

    /** Queue a sealed send; the chain keeps seq order == wire order. */
    private sendSealed(frame: unknown): void {
        const ws = this.ws;
        const session = this.session;
        if (!ws || !session || this.state !== "connected") return;
        const json = JSON.stringify(frame);
        this.txChain = this.txChain
            .then(async () => {
                const sealed = await session.seal(json);
                if (this.ws === ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(sealed.buffer as ArrayBuffer);
                }
            })
            .catch((e) => console.warn("[obsetync] ws: seal/send failed:", e));
    }

    private async connect(): Promise<void> {
        if (this.stopped) return;
        this.setState("connecting");

        // 1. Key exchange + ticket over the sealed HTTP channel.
        let ticket: string;
        let session: ObsetyncWsSession;
        try {
            const keys = generateWsEphKeypair();
            const minted = await this.api.mintWsTicket(keys.pubB64);
            if (!minted.server_eph_pub) {
                throw new Error("server did not return an ephemeral pubkey (server < 1.8.0?)");
            }
            ticket = minted.ticket;
            session = await ObsetyncWsSession.create(
                keys.priv,
                minted.server_eph_pub,
                ticket,
            );
        } catch (e) {
            console.warn("[obsetync] ws: ticket/keys failed, backing off:", e);
            this.scheduleReconnect();
            return;
        }
        if (this.stopped) return;

        let ws: WebSocket;
        try {
            ws = new WebSocket(this.wsUrl());
        } catch (e) {
            console.warn("[obsetync] ws: connect failed:", e);
            this.scheduleReconnect();
            return;
        }
        ws.binaryType = "arraybuffer";
        this.ws = ws;
        this.session = session;
        this.rxChain = Promise.resolve();
        this.txChain = Promise.resolve();
        let ready = false;

        ws.onopen = () => {
            if (this.stopped) return;
            // Plaintext auth frame — the single-use ticket's only trip over
            // the wire; everything after is sealed.
            ws.send(JSON.stringify({ v: 2, t: "auth", ticket }));
        };

        ws.onmessage = (ev: MessageEvent) => {
            if (!(ev.data instanceof ArrayBuffer)) {
                // Plaintext frames only carry pre-auth "bye" errors.
                console.warn("[obsetync] ws: server said:", String(ev.data).slice(0, 200));
                return;
            }
            const data = new Uint8Array(ev.data);
            this.rxChain = this.rxChain
                .then(async () => {
                    const inner = await session.open(data);
                    this.lastFrameMs = Date.now();
                    const frame = JSON.parse(inner) as {
                        t?: string;
                        vault?: string;
                        root?: string;
                        device?: string;
                        name?: string;
                        file?: string | null;
                        state?: string;
                    };
                    switch (frame.t) {
                        case "ready": {
                            ready = true;
                            this.setState("connected");
                            this.backoffMs = BACKOFF_START_MS;
                            console.log("[obsetync] ws: sealed session ready");
                            this.sendSealed({ v: 2, t: "sub", vaults: [this.vaultId] });
                            if (this.pingTimer !== null) window.clearInterval(this.pingTimer);
                            this.pingTimer = window.setInterval(
                                () => this.sendSealed({ v: 2, t: "ping" }),
                                PING_INTERVAL_MS,
                            );
                            break;
                        }
                        case "root": {
                            if (frame.vault === this.vaultId && frame.root) {
                                console.log(
                                    `[obsetync] ws: root changed → ${frame.root.slice(0, 12)}, pulling`,
                                );
                                this.onRootChanged(frame.root);
                            }
                            break;
                        }
                        case "presence": {
                            if (frame.vault === this.vaultId && frame.device && frame.name) {
                                this.onPresence({
                                    device: frame.device,
                                    name: frame.name,
                                    file: frame.file ?? null,
                                    state:
                                        frame.state === "idle"
                                            ? "idle"
                                            : frame.state === "offline"
                                              ? "offline"
                                              : "active",
                                });
                            }
                            break;
                        }
                        default:
                            break; // pong etc. — lastFrameMs already updated
                    }
                })
                .catch((e) => {
                    // Failed open = tamper or counter desync — reconnect fresh.
                    console.warn("[obsetync] ws: frame failed to open, reconnecting:", e);
                    try {
                        ws.close();
                    } catch {
                        /* noop */
                    }
                });
        };

        const onGone = () => {
            if (this.pingTimer !== null) {
                window.clearInterval(this.pingTimer);
                this.pingTimer = null;
            }
            if (this.ws === ws) {
                this.ws = null;
                this.session = null;
            }
            if (!ready) {
                console.warn("[obsetync] ws: closed before ready (auth rejected?)");
            }
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
