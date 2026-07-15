/**
 * Ignore-pattern matching (Slice 2). A gitignore-inspired *subset* — enough to
 * keep build-output trees (target/, node_modules/, .git/) and OS junk
 * (.DS_Store, *.tmp) out of sync, without pulling in a full gitignore engine.
 *
 * SAFETY: a false positive here silently stops a real note from syncing (looks
 * like data loss), so the semantics are deliberately narrow and unit-tested:
 *
 *   - `#…`                 comment, ignored.
 *   - `target/`            directory anywhere — excludes the whole subtree
 *                          (matches a path with `target` as a non-final segment).
 *   - `node_modules`       no-slash, no trailing / — matches ANY path segment
 *                          (file or dir) at any depth.
 *   - `*.tmp` / `.DS_Store`no-slash glob/name — matched against every segment
 *                          (so it catches basenames at any depth).
 *   - `docs/target/`        contains `/` → anchored at the vault root.
 *   - `*`  matches within a segment; `**` matches across segments; `?` one char.
 *
 * Matching is case-sensitive (vault paths are). Leading `./` or `/` on the input
 * path is normalized away.
 */

export interface CompiledIgnore {
    /** True when `path` (vault-relative, `/`-separated) is ignored. */
    test(path: string): boolean;
    /** Number of active rules (0 ⇒ nothing is ignored; fast-path). */
    readonly size: number;
}

/** Translate one glob token (a segment, or a full slashed pattern) to a regex
 *  source string. `**` → `.*` (crosses `/`), `*` → `[^/]*`, `?` → `[^/]`. */
function globToSource(glob: string): string {
    let out = "";
    for (let i = 0; i < glob.length; i++) {
        const c = glob[i];
        if (c === "*") {
            if (glob[i + 1] === "*") {
                out += ".*";
                i++;
            } else {
                out += "[^/]*";
            }
        } else if (c === "?") {
            out += "[^/]";
        } else if ("\\^$.|+()[]{}".includes(c)) {
            out += "\\" + c;
        } else {
            out += c;
        }
    }
    return out;
}

type Rule = (segments: string[], path: string) => boolean;

function compileRule(pattern: string): Rule | null {
    let p = pattern.trim();
    if (!p || p.startsWith("#")) return null;

    const dirOnly = p.endsWith("/");
    if (dirOnly) p = p.slice(0, -1);
    const anchored = p.startsWith("/");
    if (anchored) p = p.slice(1);
    if (!p) return null;

    const hasSlash = p.includes("/");

    if (!hasSlash) {
        // Floating pattern — matches at any depth.
        const rx = new RegExp("^" + globToSource(p) + "$");
        if (dirOnly) {
            // `target/` — only a DIRECTORY match excludes the subtree, so test
            // every segment EXCEPT the basename (a file named `target` is not a
            // `target/` directory).
            return (segments) => {
                for (let i = 0; i < segments.length - 1; i++) {
                    if (rx.test(segments[i])) return true;
                }
                return false;
            };
        }
        // `node_modules`, `*.tmp`, `.DS_Store` — match any segment incl. basename.
        return (segments) => segments.some((s) => rx.test(s));
    }

    // Anchored path pattern (`a/b`, `docs/target/`, `docs/**/tmp`). Matches the
    // path exactly or as a directory prefix (`a/b` also excludes `a/b/**`).
    const rx = new RegExp("^" + globToSource(p) + "(?:/.*)?$");
    return (_segments, path) => rx.test(path);
}

/** Compile a list of patterns into a fast matcher. Empty/comment lines drop out. */
export function compileIgnore(patterns: string[]): CompiledIgnore {
    const rules: Rule[] = [];
    for (const raw of patterns) {
        const r = compileRule(raw);
        if (r) rules.push(r);
    }
    return {
        size: rules.length,
        test(path: string): boolean {
            if (rules.length === 0) return false;
            const norm = path.replace(/^\.?\//, "");
            if (!norm) return false;
            const segments = norm.split("/");
            for (const r of rules) {
                if (r(segments, norm)) return true;
            }
            return false;
        },
    };
}

/** Sensible defaults: regenerated build output + VCS metadata + OS junk. These
 *  are never meaningful to sync and are the usual cause of a vault ballooning
 *  to tens of thousands of files. */
export const DEFAULT_IGNORE_PATTERNS: string[] = [
    "target/",
    "node_modules/",
    ".git/",
    ".DS_Store",
    "Thumbs.db",
    "*.tmp",
];
