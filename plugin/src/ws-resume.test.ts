import assert from "node:assert/strict";
import { webcrypto } from "node:crypto";
import test from "node:test";

import { ObsetyncApi } from "./api";
import { generateWsEphKeypair } from "./secure";
import { ObsetyncWsChannel } from "./ws";

class FakeEvents {
    visibilityState: DocumentVisibilityState = "visible";
    private readonly listeners = new Map<string, Set<() => void>>();

    addEventListener(name: string, listener: () => void): void {
        let rows = this.listeners.get(name);
        if (!rows) { rows = new Set(); this.listeners.set(name, rows); }
        rows.add(listener);
    }
    removeEventListener(name: string, listener: () => void): void {
        this.listeners.get(name)?.delete(listener);
    }
    dispatch(name: string): void {
        for (const listener of [...(this.listeners.get(name) ?? [])]) listener();
    }
    count(name: string): number { return this.listeners.get(name)?.size ?? 0; }
}

class FakeSocket {
    static readonly OPEN = 1;
    static readonly sockets: FakeSocket[] = [];
    readonly url: string;
    readyState = 0;
    binaryType = "";
    onopen: (() => void) | null = null;
    onmessage: ((event: MessageEvent) => void) | null = null;
    onclose: (() => void) | null = null;
    onerror: (() => void) | null = null;
    closes = 0;

    constructor(url: string) { this.url = url; FakeSocket.sockets.push(this); }
    send(): void {}
    close(): void {
        this.closes++;
        this.readyState = 3;
        const callback = this.onclose;
        if (callback) queueMicrotask(callback);
    }
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
    for (let attempt = 0; attempt < 200; attempt++) {
        if (predicate()) return;
        await new Promise(resolve => setTimeout(resolve, 0));
    }
    throw new Error(message);
}

test("foreground resume invalidates an apparently-connected stale session exactly once", async () => {
    const previous = {
        crypto: globalThis.crypto,
        document: globalThis.document,
        window: globalThis.window,
        WebSocket: globalThis.WebSocket,
    };
    const documentEvents = new FakeEvents();
    const windowEvents = new FakeEvents();
    FakeSocket.sockets.length = 0;
    Object.defineProperty(globalThis, "crypto", { configurable: true, value: webcrypto });
    Object.defineProperty(globalThis, "document", { configurable: true, value: documentEvents });
    Object.defineProperty(globalThis, "window", { configurable: true, value: Object.assign(windowEvents, {
        setTimeout, clearTimeout, setInterval, clearInterval,
    }) });
    Object.defineProperty(globalThis, "WebSocket", { configurable: true, value: FakeSocket });

    const server = generateWsEphKeypair();
    let mints = 0;
    const api = {
        baseUrl: "http://sync.example",
        async mintWsTicket() {
            mints++;
            return { ticket: "a".repeat(64), server_eph_pub: server.pubB64 };
        },
    };
    const observedConnected: boolean[] = [];
    const channel = new ObsetyncWsChannel(api as any, "vault", () => {}, () => {},
        () => observedConnected.push(channel.isConnected()));
    try {
        channel.start();
        await eventually(() => FakeSocket.sockets.length === 1, "initial WS did not start");
        const stale = FakeSocket.sockets[0];
        stale.readyState = FakeSocket.OPEN;
        (channel as any).state = "connected";

        documentEvents.visibilityState = "hidden";
        documentEvents.dispatch("visibilitychange");
        documentEvents.visibilityState = "visible";
        documentEvents.dispatch("visibilitychange");
        assert.deepEqual(observedConnected, [false], "resume callback ran before stale state was invalidated");
        await eventually(() => FakeSocket.sockets.length === 2, "resume did not mint a fresh WS session");
        assert.equal(stale.closes, 1);
        assert.equal(channel.getState(), "connecting");
        assert.equal(channel.lastFrameAgeMs(), -1);
        assert.equal(mints, 2);

        // A duplicate visible/pageshow event from the same host transition is
        // coalesced; it must not cause a reconnect/reload loop.
        documentEvents.dispatch("visibilitychange");
        windowEvents.dispatch("pageshow");
        await new Promise(resolve => setTimeout(resolve, 0));
        assert.equal(FakeSocket.sockets.length, 2);
        assert.deepEqual(observedConnected, [false]);

        windowEvents.dispatch("pagehide");
        windowEvents.dispatch("pageshow");
        await eventually(() => FakeSocket.sockets.length === 3, "pagehide epoch did not refresh WS");
        assert.equal(FakeSocket.sockets[1].closes, 1);
        assert.deepEqual(observedConnected, [false, false]);
        assert.equal(channel.getState(), "connecting",
            "superseded socket close changed the fresh connection state");
    } finally {
        channel.stop();
        assert.equal(documentEvents.count("visibilitychange"), 0);
        assert.equal(windowEvents.count("pagehide"), 0);
        assert.equal(windowEvents.count("pageshow"), 0);
        Object.defineProperty(globalThis, "crypto", { configurable: true, value: previous.crypto });
        Object.defineProperty(globalThis, "document", { configurable: true, value: previous.document });
        Object.defineProperty(globalThis, "window", { configurable: true, value: previous.window });
        Object.defineProperty(globalThis, "WebSocket", { configurable: true, value: previous.WebSocket });
    }
});

test("foreground transport invalidation closes data WS and forgets stale measurements", () => {
    const api = new ObsetyncApi("http://sync.example", "", "");
    const internal = api as any;
    let closes = 0;
    let resets = 0;
    internal.wsDataLane = { close: () => { closes++; } };
    internal.transportRouter.resetMeasurements = () => { resets++; };
    api.invalidateDataTransportAfterResume();
    assert.equal(closes, 1);
    assert.equal(resets, 1);
    assert.equal(internal.wsDataLane, null);
});
