import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { fingerprintLegacyCopies } from "./legacy-source";
import { parseJournalCooperatively, parseJournal } from "./journal-format";

type Copy = { role: string; raw: string | null };
function reference(copies: Copy[]): string {
    const hash = (data: string | Uint8Array) => createHash("sha256").update(data).digest();
    const manifest = copies.map(({ role, raw }) => {
        if (raw === null) return { role, units: null, sha256: null };
        let digest = hash(JSON.stringify(["obsetync-legacy-source-chunks-v1", role, raw.length]));
        for (let offset = 0; offset < raw.length; offset += 16 * 1024) {
            digest = hash(Buffer.concat([digest, Buffer.from(JSON.stringify(raw.slice(offset, offset + 16 * 1024)))]));
        }
        return { role, units: raw.length, sha256: digest.toString("hex") };
    });
    return hash(JSON.stringify({ domain: "obsetync-legacy-source-manifest-v1", copies: manifest })).toString("hex");
}

async function run(): Promise<void> {
    const boundary = "x".repeat(16 * 1024 - 1) + "😀" + "\ud800" + "\u0000".repeat(16 * 1024);
    const copies: Copy[] = [{ role: "main", raw: boundary }, { role: "next", raw: null }, { role: "backup", raw: "" }];
    assert.equal(await fingerprintLegacyCopies(copies), reference(copies));
    const initial = await fingerprintLegacyCopies(copies);
    const mutableCopies = copies.map(copy => ({ ...copy }));
    const capturing = fingerprintLegacyCopies(mutableCopies);
    mutableCopies[1].raw = "changed during native digest";
    mutableCopies.reverse();
    assert.equal(await capturing, initial, "native hashing followed a newer caller-owned source manifest");
    for (const offset of [0, 16 * 1024 - 1, 16 * 1024, boundary.length - 1]) {
        const changed = boundary.slice(0, offset) + "y" + boundary.slice(offset + 1);
        assert.notEqual(await fingerprintLegacyCopies([{ ...copies[0], raw: changed }, ...copies.slice(1)]), initial);
    }
    assert.notEqual(await fingerprintLegacyCopies([{ role: "main", raw: null }]),
        await fingerprintLegacyCopies([{ role: "main", raw: "" }]));
    assert.notEqual(await fingerprintLegacyCopies([{ role: "main", raw: "x" }]),
        await fingerprintLegacyCopies([{ role: "backup", raw: "x" }]));
    await assert.rejects(fingerprintLegacyCopies([{ role: "invalid/path", raw: "x" }]));
    await assert.rejects(fingerprintLegacyCopies([{ role: "main", raw: "x" }, { role: "main", raw: "x" }]));
    await assert.rejects(fingerprintLegacyCopies(Array.from({ length: 17 }, (_, index) => ({ role: "r" + index, raw: null }))));

    let timerRan = false;
    const timer = setTimeout(() => { timerRan = true; }, 0);
    try {
        await fingerprintLegacyCopies([{ role: "main", raw: boundary.repeat(4) }]);
        assert(timerRan, "fingerprinting held all bounded crypto chunks in one UI task");
    } finally { clearTimeout(timer); }

    // Cooperative parser also yields through whitespace, not only mutations.
    timerRan = false;
    const parserTimer = setTimeout(() => { timerRan = true; }, 0);
    try {
        const raw = "\n".repeat(3000) + Array.from({ length: 600 }, (_, index) => JSON.stringify({
            id: index + 1, action: "modified", path: "note-" + index + ".md", ts: 1,
        })).join("\n") + "\n";
        assert.deepEqual(await parseJournalCooperatively(raw), parseJournal(raw));
        assert(timerRan, "cooperative legacy parser did not yield through blank rows");
    } finally { clearTimeout(parserTimer); }
    const rename = JSON.stringify({ id: 1, action: "renamed", oldPath: "old.md", path: "new.md", ts: 1 }) + "\n";
    for (const [path, expectedPath, expectedAction] of [["old.md", "new.md", "modified"], ["new.md", "old.md", "deleted"]]) {
        const parsed = await parseJournalCooperatively(rename + JSON.stringify({ op: "ack", path, throughId: 1 }) + "\n");
        assert.equal(parsed.entries.length, 1);
        assert.equal(parsed.entries[0].path, expectedPath);
        assert.equal(parsed.entries[0].action, expectedAction);
        assert.equal(parsed.mutations[0].action, "renamed");
        assert.deepEqual(parsed.acknowledgements, [{ path, throughId: 1 }]);
    }
    console.log("legacy-source.test: lossless bounded fingerprints, role/absence identity and cooperative parser regressions passed");
}

void run().catch(error => { console.error(error); process.exitCode = 1; });
