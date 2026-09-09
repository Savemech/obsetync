/** Linux-only test/benchmark child owner. No API accepts a PID to signal.
 * Only the detached group created by THIS invocation can be terminated.
 * Teardown failure is never successful native completion or a durability test.
 * safeToCleanup requires the leader's close AND no executable group members:
 * kernel-blocked members still count; dead zombies cannot write/run and are
 * left to their real parent/reaper. /proc inspection failure fails closed.
 */
import { spawn } from "node:child_process";
import { lstat, readFile, readdir, realpath } from "node:fs/promises";
import { basename, dirname, isAbsolute } from "node:path";

export class NativeRootChildError extends Error {
    constructor(code, outcome) {
        super(`native root child failed: ${code}`); this.name = "NativeRootChildError";
        this.code = code; this.outcome = Object.freeze({ ...outcome });
    }
}
const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
function bounded(value, fallback, maximum) {
    const result = value ?? fallback;
    if (!Number.isSafeInteger(result) || result <= 0 || result > maximum) throw new TypeError("invalid child limit");
    return result;
}
function groupExists(group) {
    try { process.kill(-group, 0); return true; }
    catch (error) { if (error?.code === "ESRCH") return false; throw error; }
}
async function executableMembers(group) {
    if (!groupExists(group)) return 0;
    let members = 0;
    for (const name of await readdir("/proc")) {
        if (!/^[1-9][0-9]*$/u.test(name)) continue;
        let raw;
        try { raw = await readFile(`/proc/${name}/stat`, "utf8"); }
        catch (error) { if (error?.code === "ENOENT" || error?.code === "ESRCH") continue; throw error; }
        // comm can contain whitespace and ')'; fields follow its LAST ')'.
        const end = raw.lastIndexOf(")"), fields = raw.slice(end + 2).trim().split(/\s+/u);
        if (end < 0 || fields.length < 4 || !/^\d+$/u.test(fields[2])) throw new Error("invalid process-group observation");
        if (Number(fields[2]) === group && fields[0] !== "Z" && fields[0] !== "X") members++;
    }
    return members;
}

/** Spawn a strict, private-fixture Node bundle and return its single JSON
 * result only after orderly whole-group completion. onStderr is a synchronous
 * bounded-output observer; no stdout/stderr/params/paths enter thrown outcomes.
 * Params must be JSON and at most64KiB; files/fixtures remain caller-owned.
 *
 * @param {string} bundle Canonical regular file directly within an existing
 * private /tmp/obsetync-native-root-* directory (not a symlink).
 * @param {unknown} params
 * @param {{cwd:string, timeoutMs?:number, graceMs?:number, joinMs?:number,
 * stdoutLimitBytes?:number, stderrLimitBytes?:number,
 * onStderr?:(chunk:string)=>void}} options
 * @returns {Promise<unknown>}
 */
export async function runNativeRootChild(bundle, params, options = {}) {
    const outcome = { exitCode: null, signal: null, timedOut: false, cancelled: false,
        outputLimitExceeded: false, forcedGroupKill: false, safeToCleanup: true,
        groupExecutableMembers: 0, stdoutBytes: 0, stderrBytes: 0 };
    let timeoutMs, graceMs, joinMs, stdoutLimit, stderrLimit, serialized, cwd, onStderr;
    try {
        if (process.platform !== "linux") throw new Error("Linux process groups and /proc are required");
        timeoutMs = bounded(options.timeoutMs, 30 * 60 * 1000, 30 * 60 * 1000);
        graceMs = bounded(options.graceMs, 5000, 30000); joinMs = bounded(options.joinMs, 5000, 30000);
        stdoutLimit = bounded(options.stdoutLimitBytes, 4 * 1024 * 1024, 4 * 1024 * 1024);
        stderrLimit = bounded(options.stderrLimitBytes, 256 * 1024, 256 * 1024);
        onStderr = options.onStderr;
        if (onStderr !== undefined && typeof onStderr !== "function") throw new TypeError("invalid stderr observer");
        if (typeof bundle !== "string" || !isAbsolute(bundle) ||
            !/^\/tmp\/obsetync-native-root-[A-Za-z0-9_-]+$/u.test(dirname(bundle)) ||
            !/^[A-Za-z0-9_-]+\.(?:cjs|mjs|js)$/u.test(basename(bundle))) throw new TypeError("invalid private child bundle");
        const directory = await lstat(dirname(bundle)), file = await lstat(bundle);
        if (!directory.isDirectory() || directory.isSymbolicLink() || directory.uid !== process.getuid() ||
            (directory.mode & 0o777) !== 0o700 || !file.isFile() || file.isSymbolicLink() || file.nlink !== 1 ||
            file.uid !== process.getuid() || await realpath(bundle) !== bundle) throw new TypeError("invalid private child bundle ownership");
        if (typeof options.cwd !== "string" || !isAbsolute(options.cwd)) throw new TypeError("invalid child cwd");
        cwd = await realpath(options.cwd); if (!(await lstat(cwd)).isDirectory()) throw new TypeError("invalid child cwd");
        serialized = JSON.stringify(params);
        if (typeof serialized !== "string" || Buffer.byteLength(serialized) > 64 * 1024) throw new TypeError("invalid bounded child params");
    } catch { throw new NativeRootChildError(process.platform === "linux" ? "INVALID_INVOCATION" : "UNSUPPORTED_HOST", outcome); }

    let child, group, primaryError, timeout, grace, hardDeadline, releaseCompletion,
        closed = false, groupInspected = false, exitInspection;
    const output = [];
    const fail = code => { primaryError ??= code; };
    const killGroup = () => {
        if (!Number.isSafeInteger(group) || group <= 0) return;
        try { process.kill(-group, "SIGKILL"); outcome.forcedGroupKill = true; }
        catch (error) { if (error?.code !== "ESRCH") fail("GROUP_SIGNAL_FAILED"); }
        if (!closed && !hardDeadline) hardDeadline = setTimeout(() => {
            if (closed) return;
            // A kernel-blocked process need not close even after SIGKILL.
            // This is an UNSAFE FAILURE, never a native-completion shortcut:
            // caller must retain its fixtures and report the unjoined owner.
            fail("GROUP_NOT_DRAINED"); outcome.safeToCleanup = false;
            outcome.groupExecutableMembers = null;
            child?.stdout.destroy(); child?.stderr.destroy(); child?.unref();
            releaseCompletion?.();
        }, joinMs);
    };
    const stop = code => {
        fail(code);
        try { if (!closed) child?.kill("SIGTERM"); }
        catch { fail("CHILD_SIGNAL_FAILED"); }
        if (!grace) grace = setTimeout(() => { if (!closed || !groupInspected) killGroup(); }, graceMs);
    };
    const onSignal = () => { outcome.cancelled = true; stop("CANCELLED"); };
    // Emergency only; this cannot asynchronously join native IO. The orderly
    // signal path below always waits for close and group observations.
    const onExit = () => { if (!outcome.safeToCleanup) killGroup(); };
    process.on("SIGINT", onSignal); process.on("SIGTERM", onSignal); process.once("exit", onExit);
    try {
        try {
            child = spawn(process.execPath, ["--unhandled-rejections=strict", bundle, serialized],
                { cwd, detached: true, stdio: ["ignore", "pipe", "pipe"] });
        } catch { throw new NativeRootChildError("SPAWN_FAILED", outcome); }
        group = child.pid;
        outcome.safeToCleanup = !Number.isSafeInteger(group);
        const completion = new Promise(resolve => {
            releaseCompletion = resolve;
            child.once("error", () => fail("SPAWN_FAILED"));
            child.once("exit", () => {
                // A grandchild can inherit these pipes, so leader exit need
                // not emit close. Detect an orphan before waiting on its pipes.
                exitInspection = (async () => {
                    if (Number.isSafeInteger(group) && await executableMembers(group) > 0) {
                        fail("ORPHANED_GROUP"); killGroup();
                    }
                })().catch(() => { fail("GROUP_INSPECTION_FAILED"); killGroup(); });
            });
            child.once("close", (code, signal) => {
                closed = true; outcome.exitCode = code; outcome.signal = signal; resolve();
            });
        });
        child.stdout.on("data", chunk => {
            outcome.stdoutBytes += chunk.length;
            if (outcome.stdoutBytes > stdoutLimit) { outcome.outputLimitExceeded = true; stop("STDOUT_LIMIT"); return; }
            output.push(chunk);
        });
        child.stderr.on("data", chunk => {
            outcome.stderrBytes += chunk.length;
            if (outcome.stderrBytes > stderrLimit) { outcome.outputLimitExceeded = true; stop("STDERR_LIMIT"); return; }
            if (onStderr) {
                try { onStderr(chunk.toString("utf8")); }
                catch { stop("STDERR_OBSERVER_FAILED"); }
            }
        });
        child.stdout.once("error", () => stop("STDOUT_FAILED"));
        child.stderr.once("error", () => stop("STDERR_FAILED"));
        timeout = setTimeout(() => { outcome.timedOut = true; stop("TIMEOUT"); }, timeoutMs);
        // Timeout cannot count as native completion. Only an explicitly
        // unsafe hard-deadline failure can leave this wait before real close.
        await completion;
        if (!closed) throw new NativeRootChildError("GROUP_NOT_DRAINED", outcome);
        await exitInspection;
        clearTimeout(timeout);
        if (Number.isSafeInteger(group)) {
            const deadline = performance.now() + joinMs;
            try {
                for (;;) {
                    outcome.groupExecutableMembers = await executableMembers(group);
                    if (outcome.groupExecutableMembers === 0) { outcome.safeToCleanup = true; groupInspected = true; break; }
                    // A leader's zero exit is not success if it abandoned a
                    // live server in its detached session/process group.
                    fail("ORPHANED_GROUP"); killGroup();
                    if (performance.now() >= deadline) { fail("GROUP_JOIN_TIMEOUT"); break; }
                    await sleep(20);
                }
            } catch { outcome.groupExecutableMembers = null; fail("GROUP_INSPECTION_FAILED"); }
        }
        if (!outcome.safeToCleanup) throw new NativeRootChildError("GROUP_NOT_DRAINED", outcome);
        if (outcome.forcedGroupKill) fail("FORCED_GROUP_KILL");
        if (outcome.exitCode !== 0 || outcome.signal !== null) fail("CHILD_FAILED");
        if (primaryError) throw new NativeRootChildError(primaryError, outcome);
        try { return JSON.parse(Buffer.concat(output).toString("utf8")); }
        catch { throw new NativeRootChildError("INVALID_JSON", outcome); }
    } finally {
        clearTimeout(timeout); clearTimeout(grace); clearTimeout(hardDeadline);
        process.off("SIGINT", onSignal); process.off("SIGTERM", onSignal); process.off("exit", onExit);
    }
}
