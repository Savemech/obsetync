import { metadataScanNeedsReview, planMetadataScan } from "./scan-planner";

const check = (condition: unknown, message: string) => {
    if (!condition) throw new Error(message);
};

const stats = new Map([
    ["same.md", { mtime: 10, size: 4 }],
    ["local-old.md", { mtime: 1, size: 3 }],
    ["regressed.md", { mtime: 5, size: 7 }],
    ["ignored.tmp", { mtime: 20, size: 8 }],
]);
const bases = new Map([
    ["same.md", { mtime: 10, size: 4 }],
    ["regressed.md", { mtime: 12, size: 7 }],
    ["gone.md", { mtime: 9, size: 2 }],
    ["ignored-gone.tmp", { mtime: 9, size: 2 }],
]);

const result = planMetadataScan(
    stats,
    [...bases.keys()],
    (path) => bases.get(path) ?? null,
    (path) => path.endsWith(".tmp"),
);
const candidates = new Set(result.toHash.map((item) => item.path));

check(!candidates.has("same.md"), "unchanged metadata was scheduled for hashing");
check(candidates.has("local-old.md"), "old-mtime local-only file was missed");
check(candidates.has("regressed.md"), "backwards mtime change was missed");
check(!candidates.has("ignored.tmp"), "ignored local file entered the scan");
check(result.deleted.length === 1 && result.deleted[0] === "gone.md", "offline deletion was missed");

check(
    !metadataScanNeedsReview(result, bases.size),
    "small metadata audit was incorrectly blocked",
);
check(
    metadataScanNeedsReview(
        { toHash: Array.from({ length: 10_000 }, (_, i) => ({ path: `${i}.md`, stat: { mtime: i, size: 1 } })), deleted: [] },
        20_000,
    ),
    "vault-sized automatic audit was not blocked",
);
check(
    metadataScanNeedsReview(
        { toHash: [], deleted: Array.from({ length: 100 }, (_, i) => `${i}.md`) },
        400,
    ),
    "mass deletion was not blocked",
);

console.log("scan-planner.test: 8 assertions passed");
