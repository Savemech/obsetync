import { validateFileDeltas } from "./delta-validation";

const check = (condition: unknown, message: string) => {
    if (!condition) throw new Error(message);
};

const hash = "ab".repeat(32);
const valid = validateFileDeltas([{
    action: "renamed",
    old_path: "notes/old.md",
    path: "notes/new.md",
    hash,
    size: 4,
    mtime_ms: 10,
}]);
check(valid.length === 1 && valid[0].hash === hash, "valid rename was rejected");

for (const bad of [
    [{ action: "added", path: "../outside.md", hash, size: 1, mtime_ms: 1 }],
    [{ action: "added", path: "/absolute.md", hash, size: 1, mtime_ms: 1 }],
    [{ action: "added", path: "dir\\windows.md", hash, size: 1, mtime_ms: 1 }],
    [{ action: "modified", path: "a.md", hash: "not-a-hash", size: 1, mtime_ms: 1 }],
    [{ action: "renamed", old_path: "../old.md", path: "new.md", hash, size: 1, mtime_ms: 1 }],
    [{ action: "added", path: "a.md", hash, size: Number.MAX_SAFE_INTEGER + 1, mtime_ms: 1 }],
]) {
    let rejected = false;
    try { validateFileDeltas(bad); } catch { rejected = true; }
    check(rejected, `unsafe delta was accepted: ${JSON.stringify(bad)}`);
}

console.log("delta-validation.test: 7 assertions passed");
