import assert from "node:assert/strict";
import type { App } from "obsidian";
import { crashLog, debugLog, sanitizeDebugLogLine } from "./debug-log";

const samples: string[] = [
    "[obsetync] pushPending: 784 final path states — first 3 paths: " +
        "deleted:Projects/Example/private.md, modified:Notes/secret.md",
    "[obsetync] pull: deferring Folder/private note.md: EIO: open " +
        "'C:\\Vault\\Folder\\private note.md'",
    "[obsetync] pull: 2 file(s) could not be fetched — deferred to next pull: " +
        "Clients/acme.md, Personal/tax.pdf",
    "[obsetync] conflict on Notes/private.md — our version preserved as " +
        "Notes/private (conflict test-device).md",
    '[obsetync] candidate error {"path":"Private/a.md","old_path":"Old/a.md"}',
    "[obsetync] read failed at /private/var/mobile/Containers/Data/private.md:8:2",
];

for (const line of samples) assert.equal(sanitizeDebugLogLine(line), line);

assert.equal(
    sanitizeDebugLogLine(
        "[obsetync] transport-v2 request method=POST path=/api/v1/diff/test-vault profile=x86-desktop/recovery",
    ),
    "[obsetync] transport-v2 request method=POST path=/api/v1/diff/test-vault profile=x86-desktop/recovery",
    "protocol routes or resource profiles were mistaken for vault paths",
);

const long = `[obsetync] ${"x".repeat(20_000)} Notes/private.md`;
const capped = sanitizeDebugLogLine(long);
assert.ok(capped.length <= 12_020, "support-log line was not bounded");
assert.match(capped, /\[truncated\]$/);

// Prove the console ring preserves diagnostic paths rather than testing an
// unattached helper. Each suite runs in its own process, so replacing
// console.log here cannot race another test module.
const originalLog = console.log;
try {
    console.log = (() => {}) as typeof console.log;
    debugLog.clear();
    debugLog.install();
    console.log("[obsetync] pull-echo verification failed for Secret/note.md:", new Error(
        "ENOENT: open '/home/me/Vault/Secret/note.md'",
    ));
    const inner = console.log;
    const otherPlugin = (...args: unknown[]) => inner(...args);
    console.log = otherPlugin;
    debugLog.uninstall();
    assert.equal(console.log, otherPlugin, "unload removed another plugin's console hook");
    console.log("[obsetync] inactive");
    debugLog.install();
    const hostile = Object.defineProperty(new Error(), "stack", { get() { throw new Error("unavailable"); } });
    assert.doesNotThrow(() => console.log("[obsetync] hostile argument", hostile));
    console.log("[obsetync] reinstalled");
    assert.equal(debugLog.recent().filter(line => line.includes("reinstalled")).length, 1);
    assert.ok(debugLog.recent().every(line => !line.includes("inactive")));
    debugLog.uninstall();
    const captured = debugLog.recent().join("\n");
    assert.equal(captured.includes("Secret/note.md"), true);
    assert.equal(captured.includes("/home/me/Vault/Secret/note.md"), true);
} finally {
    debugLog.uninstall();
    console.log = originalLog;
    debugLog.clear();
}

async function crashLogPreservesExistingReports(): Promise<void> {
    let content: string | null = "previous report";
    let readable = false;
    const adapter = {
        async read() { if (!readable || content === null) throw new Error("unavailable"); return content; },
        async stat() { return content === null ? null : { type: "file", size: content.length, mtime: 1 }; },
        async write(_path: string, value: string) { content = value; },
        async append(_path: string, value: string) { content = (content ?? "") + value; },
    };
    async function record(message: string): Promise<void> {
        const host = Object.assign(new EventTarget(), { setTimeout: () => 1, clearTimeout() {} });
        const original = Object.getOwnPropertyDescriptor(globalThis, "window");
        Object.defineProperty(globalThis, "window", { configurable: true, value: host });
        try {
            crashLog.install({ vault: { adapter } } as unknown as App, "test");
            host.dispatchEvent(Object.assign(new Event("error"), { message }));
            crashLog.uninstall();
        } finally {
            if (original) Object.defineProperty(globalThis, "window", original);
            else Reflect.deleteProperty(globalThis, "window");
        }
        await new Promise<void>(resolve => setImmediate(resolve));
    }
    await record("unreadable");
    assert.equal(content, "previous report", "read failure overwrote the existing crash log");
    content = null;
    await record("first error");
    assert.match(content ?? "", /session start.*\n.*first error/s);
    readable = true;
    const previous = content;
    await record("second error");
    assert.ok(String(content).startsWith(String(previous)));
    assert.match(content ?? "", /second error/);
    content = "old line\n".repeat(65_000) + "surviving line\n";
    await record("after rotation");
    assert.ok(content.startsWith("[rotated]\n") && content.length < 101_000);
    assert.match(content, /surviving line\n.*after rotation/s);
}

async function stalledFlushCoalescesDuplicates(): Promise<void> {
    let fire: (() => void) | undefined;
    const host = Object.assign(new EventTarget(), {
        setTimeout(callback: () => void) { fire = callback; return 1; },
        clearTimeout() { fire = undefined; },
    });
    let release!: (text: string) => void;
    const stalled = new Promise<string>(resolve => { release = resolve; });
    const blocks: string[] = [];
    const adapter = {
        read: () => stalled,
        async append(_path: string, text: string) { blocks.push(text); },
    };
    const original = Object.getOwnPropertyDescriptor(globalThis, "window");
    Object.defineProperty(globalThis, "window", { configurable: true, value: host });
    try {
        crashLog.install({ vault: { adapter } } as unknown as App, "test");
        const emit = () => host.dispatchEvent(Object.assign(new Event("error"), { message: "repeated" }));
        emit();
        const first = fire!;
        fire = undefined;
        first();
        for (let n = 0; n < 2000; n++) emit();
        assert.equal(fire, undefined, "stalled IO queued more timed flushes");
        release("");
        await new Promise<void>(resolve => setImmediate(resolve));
        assert.equal(typeof fire, "function", "settlement did not schedule pending repeats");
        crashLog.uninstall();
        await new Promise<void>(resolve => setImmediate(resolve));
        assert.equal(blocks.length, 2);
        assert.match(blocks[1], /repeated 2000 more times/);
    } finally {
        release("");
        crashLog.uninstall();
        if (original) Object.defineProperty(globalThis, "window", original);
        else Reflect.deleteProperty(globalThis, "window");
    }
}

void crashLogPreservesExistingReports().then(stalledFlushCoalescesDuplicates).then(() => console.log("debug-log.test: passed"))
    .catch(error => { console.error(error); process.exitCode = 1; });
