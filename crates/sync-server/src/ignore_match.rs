//! Minimal path-pattern matcher for the admin "purge" action (Slice 2b).
//!
//! The plugin's `ignore.ts` is the source of truth for ONGOING sync; this is a
//! pragmatic subset used only to identify the paths an operator asks to lift out
//! of the shared tree in a one-shot purge. A purge deletes leaves from the
//! current root, so a false positive would drop a real note — the tests below
//! guard exactly that (the purge is COW-reversible via rollback, but we still
//! never want to remove something the operator didn't name).
//!
//! Supported forms (mirroring the ignore.ts defaults):
//!   - `target/`      directory anywhere — matches a non-final path segment.
//!   - `node_modules` no slash — matches any segment (dir or basename).
//!   - `*.tmp`        `*` wildcard within a segment (matched against segments).
//!   - `a/b/`         contains `/` → anchored path prefix.
//!   - `# comment`, blank → contribute no rule.

/// True when `path` matches ANY of `patterns`.
pub fn matches_any(path: &str, patterns: &[String]) -> bool {
    let norm = path.trim_start_matches("./").trim_start_matches('/');
    if norm.is_empty() {
        return false;
    }
    let segments: Vec<&str> = norm.split('/').collect();
    patterns.iter().any(|raw| matches_one(norm, &segments, raw))
}

fn matches_one(path: &str, segments: &[&str], raw: &str) -> bool {
    let mut p = raw.trim();
    if p.is_empty() || p.starts_with('#') {
        return false;
    }
    let dir_only = p.ends_with('/');
    if dir_only {
        p = &p[..p.len() - 1];
    }
    if let Some(rest) = p.strip_prefix('/') {
        p = rest; // anchored
    }
    if p.is_empty() {
        return false;
    }

    if p.contains('/') {
        // Anchored path prefix: the path IS `p` or lives under `p/`.
        return path == p || path.starts_with(&format!("{}/", p));
    }
    if dir_only {
        // Directory anywhere: only a non-final segment (an ancestor dir) counts,
        // so a *file* named `target` is not matched by `target/`.
        let ancestors = &segments[..segments.len().saturating_sub(1)];
        return ancestors.iter().any(|s| wildcard(s, p));
    }
    // Bare name / glob — match any segment, including the basename.
    segments.iter().any(|s| wildcard(s, p))
}

/// `*`-glob match within a single segment (no `?`/`**` — those are rare for
/// operator purge patterns; the client's ignore.ts is the full engine). Byte
/// slicing is char-boundary-safe: every `pos` advance lands on the end of a
/// substring match returned by `starts_with`/`find`.
fn wildcard(text: &str, pat: &str) -> bool {
    if !pat.contains('*') {
        return text == pat;
    }
    let parts: Vec<&str> = pat.split('*').collect();
    let mut pos = 0usize;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            if !text[pos..].starts_with(part) {
                return false;
            }
            pos += part.len();
        } else if i == parts.len() - 1 {
            // Trailing literal must reach the end of the segment.
            if !text[pos..].ends_with(part) {
                return false;
            }
        } else {
            match text[pos..].find(part) {
                Some(idx) => pos += idx + part.len(),
                None => return false,
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::matches_any;

    fn pats(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn dir_anywhere_matches_nested_subtree() {
        let p = pats(&["target/"]);
        assert!(matches_any("code/myapp/target/debug/deps/lib.rmeta", &p));
        assert!(matches_any("target/x.rlib", &p));
    }

    #[test]
    fn dir_pattern_does_not_match_a_file_of_that_name() {
        let p = pats(&["target/"]);
        assert!(!matches_any("notes/target", &p)); // file, not a dir
        assert!(!matches_any("projects/target.md", &p));
        assert!(!matches_any("data/targets/list.md", &p)); // 'targets' != 'target'
    }

    #[test]
    fn bare_name_matches_any_segment() {
        let p = pats(&["node_modules", ".git"]);
        assert!(matches_any("app/node_modules/react/index.js", &p));
        assert!(matches_any("repo/.git/objects/ab/cd", &p));
        assert!(!matches_any("repo/.gitignore", &p)); // .gitignore != .git
    }

    #[test]
    fn glob_basename() {
        let p = pats(&["*.tmp", ".DS_Store"]);
        assert!(matches_any("drafts/foo.tmp", &p));
        assert!(matches_any("notes/.DS_Store", &p));
        assert!(!matches_any("templates/note.tmpl", &p)); // .tmpl != .tmp
        assert!(!matches_any("real/note.md", &p));
    }

    #[test]
    fn anchored_prefix() {
        let p = pats(&["_attachments/big/"]);
        assert!(matches_any("_attachments/big/video.mp4", &p));
        assert!(!matches_any("other/_attachments/big/x", &p));
    }

    #[test]
    fn comments_and_blanks_match_nothing() {
        let p = pats(&["# a comment", "", "   "]);
        assert!(!matches_any("anything/at/all.md", &p));
    }

    #[test]
    fn empty_pattern_set_matches_nothing() {
        assert!(!matches_any("a/b/c.md", &[]));
    }

    #[test]
    fn real_notes_never_matched_by_defaults() {
        let p = pats(&["target/", "node_modules/", ".git/", ".DS_Store", "*.tmp"]);
        for real in [
            "notes/projects/design.md",
            "daily/2026-07-15.md",
            "attachments/diagram.png",
            "targeting-notes.md",
            "build-log.md",
        ] {
            assert!(!matches_any(real, &p), "false positive on {real}");
        }
    }
}
