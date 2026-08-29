import { PullEchoTracker } from "./pull-echo";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

let now = 1_000;
const tracker = new PullEchoTracker(100, () => now);

tracker.register([{ path: "note.md", action: "upsert", hash: "server" }]);
check(tracker.expectsUpsert("note.md"), "upsert expectation was not discoverable");
check(tracker.consumeUpsert("note.md", "server"), "matching pull write is an echo");
check(tracker.size === 0, "matching echo is consumed");

tracker.register([{ path: "note.md", action: "upsert", hash: "server" }]);
check(!tracker.consumeUpsert("note.md", "local"), "different bytes are a local edit");
check(tracker.size === 0, "mismatched expectation cannot swallow a later event");

tracker.register([{ path: "gone.md", action: "delete" }]);
check(tracker.consumeDelete("gone.md"), "matching delete is an echo");

tracker.register([
    { path: "old.md", action: "delete" },
    { path: "new.md", action: "upsert", hash: "renamed" },
]);
check(tracker.expectsRename("old.md", "new.md"), "rename expectation was not discoverable");
check(
    tracker.consumeRename("old.md", "new.md", "renamed"),
    "both halves of a pull rename must match",
);

tracker.register([{ path: "late.md", action: "upsert", hash: "x" }]);
now += 101;
check(!tracker.consumeUpsert("late.md", "x"), "expired writes are not echoes");
check(tracker.size === 0, "expired entries are swept");

console.log(`pull-echo.test: ${assertions} assertions passed`);
