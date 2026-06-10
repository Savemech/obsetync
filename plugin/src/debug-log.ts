import type { App } from "obsidian";

/**
 * Ring-buffer capture of every `[obsetync] …` console line the plugin emits.
 * Installed once at plugin load. Hooks console.log/warn/error, filters for
 * messages containing `[obsetync]`, and records them. Every other console call
 * passes through untouched so other plugins aren't affected.
 */
type LogLevel = "info" | "warn" | "error";

interface LogEntry {
    ts: number;
    level: LogLevel;
    msg: string;
}

class ObsetyncDebugLog {
    private buf: LogEntry[] = [];
    private readonly MAX = 200;
    private installed = false;

    private origLog:   typeof console.log   | null = null;
    private origWarn:  typeof console.warn  | null = null;
    private origError: typeof console.error | null = null;

    install(): void {
        if (this.installed) return;
        this.installed = true;

        this.origLog   = console.log;
        this.origWarn  = console.warn;
        this.origError = console.error;

        const make = (level: LogLevel, orig: (...a: any[]) => void) =>
            (...args: any[]) => {
                orig.apply(console, args);
                if (
                    args.length > 0 &&
                    typeof args[0] === "string" &&
                    args[0].includes("[obsetync]")
                ) {
                    this.append(level, args.map(stringify).join(" "));
                }
            };

        console.log   = make("info",  this.origLog)   as any;
        console.warn  = make("warn",  this.origWarn)  as any;
        console.error = make("error", this.origError) as any;
    }

    uninstall(): void {
        if (!this.installed) return;
        if (this.origLog)   console.log   = this.origLog;
        if (this.origWarn)  console.warn  = this.origWarn;
        if (this.origError) console.error = this.origError;
        this.installed = false;
    }

    private append(level: LogLevel, msg: string): void {
        this.buf.push({ ts: Date.now(), level, msg });
        if (this.buf.length > this.MAX) this.buf.shift();
    }

    /** Returns lines formatted as `HH:MM:SS.mmm [level] msg`. */
    recent(): string[] {
        return this.buf.map(e => {
            const t = new Date(e.ts).toISOString().slice(11, 23);
            return `${t} [${e.level}] ${e.msg}`;
        });
    }

    clear(): void {
        this.buf = [];
    }
}

function stringify(v: any): string {
    if (typeof v === "string") return v;
    if (v instanceof Error) return v.stack || v.message;
    try { return JSON.stringify(v); } catch { return String(v); }
}

export const debugLog = new ObsetyncDebugLog();

// ---------------------------------------------------------------------------
// User Timing spans — paint obsetync phases onto the DevTools Performance
// timeline ("Timings" track) so sync work is attributable at a glance:
//
//   const end = perfSpan("sync.push");
//   try { ... } finally { end(); }
//
// Marks and measures are cleared right after each measure() — DevTools
// recordings capture User Timing events as they happen, so clearing keeps
// the performance entry buffer from accumulating over a long session.
// A span abandoned on an exception path leaks one mark entry; harmless.
// ---------------------------------------------------------------------------

let perfSeq = 0;

/** Start a named span. Returns the closer — call it when the phase ends. */
export function perfSpan(name: string): () => void {
    const label = `obsetync.${name}`;
    // Unique mark per call so overlapping spans of the same name don't
    // measure against each other's start marks.
    const mark = `${label}.start.${perfSeq++}`;
    try {
        performance.mark(mark);
    } catch {
        return () => {};
    }
    return () => {
        try {
            performance.measure(label, mark);
            performance.clearMarks(mark);
            performance.clearMeasures(label);
        } catch {
            // Mark cleared externally — losing one span is fine.
        }
    };
}

// ---------------------------------------------------------------------------
// Crash log — window-level error capture that survives a renderer kill.
//
// DevTools die with the renderer, so console evidence is lost exactly when
// the process OOMs. This logger appends window "error" / "unhandledrejection"
// events — from the WHOLE renderer, not only obsetync; the point is to
// identify what killed it — to a dotfile in the vault root. Dotfiles are
// invisible to Obsidian's indexer and to obsetync's own sync.
//
// Storm-safe: entries are buffered and flushed at most once per second in a
// single read+write, consecutive duplicates are collapsed into a repeat
// marker, and the logger goes silent after MAX_SESSION_ENTRIES per session.
// The file self-rotates (keeps the tail) when it outgrows half a megabyte.
// ---------------------------------------------------------------------------

const CRASH_LOG_PATH = ".obsetync-crash.log";
const CRASH_FLUSH_MS = 1000;
const MAX_SESSION_ENTRIES = 500;
const ROTATE_ABOVE_BYTES = 512_000;
const ROTATE_KEEP_BYTES = 100_000;

class ObsetyncCrashLogger {
    private app: App | null = null;
    private written = 0;
    private capped = false;
    private pending: string[] = [];
    private flushTimer: ReturnType<typeof setTimeout> | null = null;
    /** Serialises read+write cycles so appends never interleave. */
    private writeChain: Promise<void> = Promise.resolve();
    private lastKey = "";
    private repeats = 0;

    private readonly onError = (e: ErrorEvent) => {
        this.record(
            "error",
            `${e.message} (${e.filename || "?"}:${e.lineno ?? "?"})`,
            e.error?.stack,
        );
    };

    private readonly onRejection = (e: PromiseRejectionEvent) => {
        const r: any = e.reason;
        this.record("rejection", String(r?.message ?? r), r?.stack);
    };

    install(app: App, version: string): void {
        if (this.app) return;
        this.app = app;
        window.addEventListener("error", this.onError);
        window.addEventListener("unhandledrejection", this.onRejection);
        this.pending.push(
            `--- obsetync ${version} session start ${new Date().toISOString()} ---`,
        );
        this.scheduleFlush();
    }

    uninstall(): void {
        if (!this.app) return;
        window.removeEventListener("error", this.onError);
        window.removeEventListener("unhandledrejection", this.onRejection);
        if (this.flushTimer !== null) {
            clearTimeout(this.flushTimer);
            this.flushTimer = null;
        }
        this.flushNow(); // best effort — plugin is unloading
        this.app = null;
    }

    private record(kind: string, msg: string, stack?: string): void {
        try {
            if (this.capped) return;

            // Collapse consecutive duplicates (error storms repeat one line).
            const key = `[${kind}] ${msg}`;
            if (key === this.lastKey) {
                this.repeats++;
                this.scheduleFlush();
                return;
            }
            this.flushRepeats();
            this.lastKey = key;

            if (this.written >= MAX_SESSION_ENTRIES) {
                this.capped = true;
                this.pending.push(
                    `${new Date().toISOString()} [crash-log] entry cap reached — suppressing until next session`,
                );
                this.scheduleFlush();
                return;
            }
            this.written++;

            let line = `${new Date().toISOString()} ${key}`;
            if (stack) {
                line += `\n  ${String(stack).split("\n").slice(0, 6).join("\n  ")}`;
            }
            this.pending.push(line);
            this.scheduleFlush();
        } catch {
            // The crash logger must never crash anything.
        }
    }

    private flushRepeats(): void {
        if (this.repeats > 0) {
            this.pending.push(
                `${new Date().toISOString()} [crash-log] last entry repeated ${this.repeats} more times`,
            );
            this.repeats = 0;
        }
    }

    private scheduleFlush(): void {
        if (this.flushTimer !== null) return;
        this.flushTimer = setTimeout(() => {
            this.flushTimer = null;
            this.flushNow();
        }, CRASH_FLUSH_MS);
    }

    private flushNow(): void {
        const app = this.app;
        if (!app) return;
        this.flushRepeats();
        if (this.pending.length === 0) return;
        const lines = this.pending.splice(0);
        this.writeChain = this.writeChain
            .then(async () => {
                let current = "";
                try {
                    current = await app.vault.adapter.read(CRASH_LOG_PATH);
                } catch {
                    // No file yet — start fresh.
                }
                if (current.length > ROTATE_ABOVE_BYTES) {
                    current = "[rotated]\n" + current.slice(-ROTATE_KEEP_BYTES);
                }
                const sep = current && !current.endsWith("\n") ? "\n" : "";
                await app.vault.adapter.write(
                    CRASH_LOG_PATH,
                    current + sep + lines.join("\n") + "\n",
                );
            })
            .catch(() => {
                // Disk full / adapter gone — drop the lines, never throw.
            });
    }
}

export const crashLog = new ObsetyncCrashLogger();
