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
