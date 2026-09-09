import assert from "node:assert/strict";
import { test } from "node:test";
import { lstat, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { runNativeRootChild, NativeRootChildError } from "./lib/native-root-child.mjs";

async function fixture(source, work) {
    const directory = await mkdtemp("/tmp/obsetync-native-root-child-"), original = await lstat(directory);
    const bundle = join(directory, "child.cjs");
    await writeFile(bundle, source, { mode: 0o600 });
    try { await work(bundle, directory); }
    finally {
        const current = await lstat(directory);
        assert.equal(await realpath(directory), directory); assert.equal(current.ino, original.ino); assert.equal(current.dev, original.dev);
        await rm(directory, { recursive: true, force: false });
    }
}
async function failure(work, expected, check = () => {}) {
    await assert.rejects(work, error => {
        assert(error instanceof NativeRootChildError); assert.equal(error.code, expected);
        assert.equal(error.outcome.safeToCleanup, true); assert.equal(error.outcome.groupExecutableMembers, 0);
        assert(Object.isFrozen(error.outcome)); check(error); return true;
    });
}
const delay = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));

test("owned detached Node result follows actual asynchronous native tail completion", async () => {
    await fixture(`const fs=require('node:fs/promises');const params=JSON.parse(process.argv[2]);
        (async()=>{await new Promise(r=>setTimeout(r,30));await fs.writeFile(params.marker,'native tail');
        const raw=await fs.readFile('/proc/self/stat','utf8');const group=Number(raw.slice(raw.lastIndexOf(')')+2).split(' ')[2]);
        console.log(JSON.stringify({ok:true,detached:group===process.pid,value:params.value}));})();`, async (bundle, directory) => {
        const result = await runNativeRootChild(bundle, { marker: join(directory, "tail"), value: 7 }, { cwd: directory, timeoutMs: 2000 });
        assert.deepEqual(result, { ok: true, detached: true, value: 7 });
        assert.equal(await readFile(join(directory, "tail"), "utf8"), "native tail");
    });
});

test("nonzero exit and invalid JSON are failures without leaking child output or params", async () => {
    await fixture(`process.stderr.write('secret bearer must not leak');process.stdout.write('secret stdout');process.exitCode=7;`, async (bundle, directory) => {
        await failure(runNativeRootChild(bundle, { sensitive: "secret param" }, { cwd: directory }), "CHILD_FAILED", error => {
            assert.equal(error.outcome.exitCode, 7); assert.equal(error.outcome.forcedGroupKill, false);
            assert(error.outcome.stdoutBytes > 0 && error.outcome.stderrBytes > 0);
            assert.doesNotMatch(JSON.stringify(error) + error.message, /secret|bearer|param|\/tmp/);
        });
    });
    await fixture(`process.stdout.write('not JSON');`, async (bundle, directory) => {
        await failure(runNativeRootChild(bundle, {}, { cwd: directory }), "INVALID_JSON");
    });
});

test("a child SIGKILL is reported only after the owned process group is gone", async () => {
    await fixture(`require('node:fs').writeSync(2,'BOUNDARY\\n');process.kill(process.pid,'SIGKILL');`, async (bundle, directory) => {
        let boundary = false;
        await failure(runNativeRootChild(bundle, {}, { cwd: directory, timeoutMs: 2000,
            onStderr: chunk => { if (chunk.includes("BOUNDARY")) boundary = true; } }), "CHILD_FAILED", error => {
            assert.equal(boundary, true); assert.equal(error.outcome.exitCode, null);
            assert.equal(error.outcome.signal, "SIGKILL"); assert.equal(error.outcome.forcedGroupKill, false);
            assert.equal(error.outcome.timedOut, false); assert.equal(error.outcome.cancelled, false);
        });
    });
});

test("stdout and stderr overflow bound retention, kill only the owned group, and fail", async () => {
    for (const stream of ["stdout", "stderr"]) {
        await fixture(`process.on('SIGTERM',()=>{});process.${stream}.write('x'.repeat(1024));setInterval(()=>{},1000);`, async (bundle, directory) => {
            await failure(runNativeRootChild(bundle, {}, { cwd: directory, timeoutMs: 2000, graceMs: 30,
                stdoutLimitBytes: 64, stderrLimitBytes: 64 }), stream === "stdout" ? "STDOUT_LIMIT" : "STDERR_LIMIT", error => {
                assert.equal(error.outcome.outputLimitExceeded, true); assert.equal(error.outcome.forcedGroupKill, true);
                assert.equal(error.outcome.signal, "SIGKILL"); assert.equal(error.outcome[stream + "Bytes"], 1024);
            });
        });
    }
});

const graceful = `const fs=require('node:fs/promises');const params=JSON.parse(process.argv[2]);let closing=false;
    const alive=setInterval(()=>{},1000);process.on('SIGTERM',()=>{if(closing)return;closing=true;
    (async()=>{await new Promise(r=>setTimeout(r,30));await fs.writeFile(params.marker,'joined');
    process.stdout.write(JSON.stringify({graceful:true}));clearInterval(alive);})();});process.stderr.write('READY');`;
test("timeout joins a graceful child's native tail but cannot turn cancellation into success", async () => {
    await fixture(graceful, async (bundle, directory) => {
        await failure(runNativeRootChild(bundle, { marker: join(directory, "tail") },
            { cwd: directory, timeoutMs: 400, graceMs: 1000 }), "TIMEOUT", error => {
            assert.equal(error.outcome.timedOut, true); assert.equal(error.outcome.forcedGroupKill, false);
            assert.equal(error.outcome.exitCode, 0);
        });
        assert.equal(await readFile(join(directory, "tail"), "utf8"), "joined");
    });
});

test("parent SIGINT/SIGTERM forward graceful cancellation and restore all installed handlers", async () => {
    for (const signal of ["SIGINT", "SIGTERM"]) {
        await fixture(graceful, async (bundle, directory) => {
            const original = { int: process.listenerCount("SIGINT"), term: process.listenerCount("SIGTERM"), exit: process.listenerCount("exit") };
            let requested = false;
            await failure(runNativeRootChild(bundle, { marker: join(directory, "tail") }, { cwd: directory, timeoutMs: 2000,
                graceMs: 1000, onStderr: chunk => {
                    if (chunk.includes("READY") && !requested) { requested = true; process.kill(process.pid, signal); }
                } }), "CANCELLED", error => {
                assert.equal(error.outcome.cancelled, true); assert.equal(error.outcome.forcedGroupKill, false);
                assert.equal(error.outcome.exitCode, 0);
            });
            assert.equal(await readFile(join(directory, "tail"), "utf8"), "joined");
            assert.equal(process.listenerCount("SIGINT"), original.int); assert.equal(process.listenerCount("SIGTERM"), original.term);
            assert.equal(process.listenerCount("exit"), original.exit);
        });
    }
});

test("forced termination joins an ignoring grandchild, including inherited stdout pipes", async () => {
    const grandchild = `const fs=require('node:fs');const path=process.argv[1];process.on('SIGTERM',()=>{});
        fs.writeFileSync(path,'ready');setInterval(()=>fs.appendFileSync(path,'x'),10);`;
    await fixture(`const {spawn}=require('node:child_process');const params=JSON.parse(process.argv[2]);
        process.on('SIGTERM',()=>{});spawn(process.execPath,['-e',${JSON.stringify(grandchild)},params.marker],{stdio:'inherit'});
        setInterval(()=>{},1000);`, async (bundle, directory) => {
        const marker = join(directory, "grandchild");
        await failure(runNativeRootChild(bundle, { marker }, { cwd: directory, timeoutMs: 500, graceMs: 30 }), "TIMEOUT", error => {
            assert.equal(error.outcome.forcedGroupKill, true); assert.equal(error.outcome.signal, "SIGKILL");
        });
        const content = await readFile(marker, "utf8"); await delay(40);
        assert.equal(await readFile(marker, "utf8"), content, "a supposedly joined native grandchild continued writing");
    });
});

test("a zero-exit leader abandoning a pipe-owning grandchild fails promptly, not at the long watchdog", async () => {
    const grandchild = `const fs=require('node:fs');fs.writeFileSync(process.argv[1],'ready');setInterval(()=>{},1000);`;
    await fixture(`const fs=require('node:fs');const {spawn}=require('node:child_process');const params=JSON.parse(process.argv[2]);
        spawn(process.execPath,['-e',${JSON.stringify(grandchild)},params.marker],{stdio:'inherit'});
        const timer=setInterval(()=>{if(fs.existsSync(params.marker)){clearInterval(timer);process.exit(0);}},5);`, async (bundle, directory) => {
        const began = performance.now();
        await failure(runNativeRootChild(bundle, { marker: join(directory, "ready") }, { cwd: directory, timeoutMs: 5000 }),
            "ORPHANED_GROUP", error => {
                assert.equal(error.outcome.exitCode, 0); assert.equal(error.outcome.forcedGroupKill, true);
                assert.equal(error.outcome.timedOut, false);
            });
        assert(performance.now() - began < 4000, "orphan detection waited for the unrelated long timeout");
    });
});

test("observer failure propagates as a safe fixed error only after child teardown", async () => {
    await fixture(`process.stderr.write('READY');setInterval(()=>{},1000);`, async (bundle, directory) => {
        await failure(runNativeRootChild(bundle, {}, { cwd: directory, timeoutMs: 2000,
            onStderr: () => { throw new Error("secret observer data"); } }), "STDERR_OBSERVER_FAILED", error => {
            assert.doesNotMatch(error.message + JSON.stringify(error), /secret|observer data/);
        });
    });
});

test("invalid paths and limits fail before spawning and never acquire arbitrary PID authority", async () => {
    for (const path of ["/usr/bin/node", "/tmp/unowned.cjs", "relative.cjs", "/tmp/obsetync-native-root-bogus/../escape.cjs"]) {
        await failure(runNativeRootChild(path, {}, { cwd: "/tmp" }), "INVALID_INVOCATION");
    }
    await fixture(`console.log('{}');`, async (bundle, directory) => {
        for (const options of [{ timeoutMs: 0 }, { graceMs: -1 }, { joinMs: Infinity }, { stdoutLimitBytes: 8 * 1024 * 1024 },
            { stderrLimitBytes: 1024 * 1024 }, { onStderr: 1 }]) {
            await failure(runNativeRootChild(bundle, {}, { cwd: directory, ...options }), "INVALID_INVOCATION");
        }
    });
});
