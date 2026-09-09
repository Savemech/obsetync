import assert from "node:assert/strict";
import { debugLog, sanitizeDebugLogLine } from "./debug-log";

const samples: Array<[string, string[]]> = [
    [
        "[obsetync] pushPending: 784 final path states — first 3 paths: " +
            "deleted:Projects/Example/private.md, modified:Notes/secret.md",
        ["Projects/Example/private.md", "Notes/secret.md"],
    ],
    [
        "[obsetync] pull: deferring Folder/private note.md: EIO: open " +
            "'C:\\Vault\\Folder\\private note.md'",
        ["Folder/private note.md", "C:\\Vault\\Folder\\private note.md"],
    ],
    [
        "[obsetync] pull: 2 file(s) could not be fetched — deferred to next pull: " +
            "Clients/acme.md, Personal/tax.pdf",
        ["Clients/acme.md", "Personal/tax.pdf"],
    ],
    [
        "[obsetync] conflict on Notes/private.md — our version preserved as " +
            "Notes/private (conflict test-device).md",
        ["Notes/private.md", "Notes/private (conflict test-device).md"],
    ],
    [
        '[obsetync] candidate error {"path":"Private/a.md","old_path":"Old/a.md"}',
        ["Private/a.md", "Old/a.md"],
    ],
    [
        "[obsetync] read failed at /private/var/mobile/Containers/Data/private.md:8:2",
        ["/private/var/mobile/Containers/Data/private.md"],
    ],
];

for (const [line, secrets] of samples) {
    const sanitized = sanitizeDebugLogLine(line);
    assert.match(sanitized, /\[path redacted\]/);
    for (const secret of secrets) assert.equal(sanitized.includes(secret), false, secret);
}

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

// Prove the console ring uses the sanitizer rather than testing an unattached
// helper. Each suite runs in its own process, so replacing console.log here
// cannot race another test module.
const originalLog = console.log;
try {
    console.log = (() => {}) as typeof console.log;
    debugLog.clear();
    debugLog.install();
    console.log("[obsetync] pull-echo verification failed for Secret/note.md:", new Error(
        "ENOENT: open '/home/me/Vault/Secret/note.md'",
    ));
    debugLog.uninstall();
    const captured = debugLog.recent().join("\n");
    assert.match(captured, /pull-echo verification failed for \[path redacted\]:/);
    assert.equal(captured.includes("Secret/note.md"), false);
    assert.equal(captured.includes("/home/me/Vault"), false);
} finally {
    debugLog.uninstall();
    console.log = originalLog;
    debugLog.clear();
}

console.log("debug-log.test: 22 assertions passed");
