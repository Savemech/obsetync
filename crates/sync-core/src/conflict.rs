use crate::hash::FileHash;
use similar::TextDiff;

/// Result of classifying a file's sync state using three hashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncAction {
    /// Local and remote hashes match — nothing to do.
    NoChange,
    /// Local matches base, remote differs — accept remote (fast-forward).
    FastForward,
    /// Remote matches base, local differs — push local.
    LocalOnly,
    /// All three differ — real conflict, needs resolution.
    Conflict {
        base_hash: FileHash,
        local_hash: FileHash,
        remote_hash: FileHash,
    },
}

/// Classify a file's sync state using three hashes.
pub fn classify_sync(base: &FileHash, local: &FileHash, remote: &FileHash) -> SyncAction {
    if local == remote {
        SyncAction::NoChange
    } else if local == base {
        SyncAction::FastForward
    } else if remote == base {
        SyncAction::LocalOnly
    } else {
        SyncAction::Conflict {
            base_hash: *base,
            local_hash: *local,
            remote_hash: *remote,
        }
    }
}

/// Result of a three-way text merge.
#[derive(Debug)]
pub enum TextMergeResult {
    /// Non-overlapping changes — auto-merged successfully.
    Merged { content: Vec<u8> },
    /// Overlapping changes — cannot auto-merge.
    Overlap,
}

/// Three-way merge for text files.
/// Takes base, local, and remote as UTF-8 byte slices.
/// Returns merged content if changes don't overlap, or Overlap if they do.
pub fn three_way_text_merge(base: &[u8], local: &[u8], remote: &[u8]) -> TextMergeResult {
    let (Ok(base), Ok(local), Ok(remote)) = (
        std::str::from_utf8(base),
        std::str::from_utf8(local),
        std::str::from_utf8(remote),
    ) else {
        return TextMergeResult::Overlap;
    };
    let local_diff = TextDiff::from_lines(base, local);
    let remote_diff = TextDiff::from_lines(base, remote);
    let mut edits = Vec::new();
    for diff in [&local_diff, &remote_diff] {
        for op in diff.ops() {
            if !matches!(op, similar::DiffOp::Equal { .. }) {
                edits.push((op.old_range(), &diff.new_slices()[op.new_range()]));
            }
        }
    }
    edits.sort_unstable_by_key(|(range, _)| range.start);

    let lines = local_diff.old_slices();
    let mut merged = Vec::new();
    let mut cursor = 0;
    let mut previous_start = None;
    for (range, inserted) in edits {
        if range.start < cursor || previous_start == Some(range.start) {
            return TextMergeResult::Overlap;
        }
        for line in lines[cursor..range.start].iter().chain(inserted) {
            merged.extend_from_slice(line.as_bytes());
        }
        // An insertion consumes no base lines. Diff slices retain their terminators.
        cursor = range.end;
        previous_start = Some(range.start);
    }
    for line in &lines[cursor..] {
        merged.extend_from_slice(line.as_bytes());
    }
    TextMergeResult::Merged { content: merged }
}

/// How a file-level conflict was resolved.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "resolution", rename_all = "snake_case")]
pub enum FileResolution {
    /// Text file was auto-merged (non-overlapping edits).
    TextMerge { merged_hash: FileHash },
    /// Conflict copy: both versions preserved.
    ConflictCopy {
        winner_hash: FileHash,
        preserved_hash: FileHash,
        preserved_path: String,
    },
    /// Immutable file: auto-renamed.
    AutoRename {
        keep_hash: FileHash,
        rename_hash: FileHash,
        rename_path: String,
    },
    /// Skipped (local-only file).
    Skipped,
}

/// A file conflict that needs resolution.
#[derive(Debug, Clone)]
pub struct FileConflict {
    pub path: String,
    pub base_hash: FileHash,
    pub side_a_hash: FileHash, // current server version
    pub side_b_hash: FileHash, // incoming push version
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;

    #[test]
    fn classify_no_change() {
        let h = hash_bytes(b"same");
        assert_eq!(classify_sync(&h, &h, &h), SyncAction::NoChange);
    }

    #[test]
    fn classify_fast_forward() {
        let base = hash_bytes(b"base");
        let remote = hash_bytes(b"remote");
        assert_eq!(
            classify_sync(&base, &base, &remote),
            SyncAction::FastForward
        );
    }

    #[test]
    fn classify_local_only() {
        let base = hash_bytes(b"base");
        let local = hash_bytes(b"local");
        assert_eq!(classify_sync(&base, &local, &base), SyncAction::LocalOnly);
    }

    #[test]
    fn classify_conflict() {
        let base = hash_bytes(b"base");
        let local = hash_bytes(b"local");
        let remote = hash_bytes(b"remote");
        assert!(matches!(
            classify_sync(&base, &local, &remote),
            SyncAction::Conflict { .. }
        ));
    }

    #[test]
    fn merge_non_overlapping() {
        let base = b"line1\nline2\nline3\nline4\n";
        let local = b"line1\nline2-modified\nline3\nline4\n";
        let remote = b"line1\nline2\nline3\nline4\nline5\n";

        match three_way_text_merge(base, local, remote) {
            TextMergeResult::Merged { content } => {
                let result = String::from_utf8(content).unwrap();
                assert!(result.contains("line2-modified"));
                assert!(result.contains("line5"));
            }
            TextMergeResult::Overlap => panic!("expected merge, got overlap"),
        }
    }

    #[test]
    fn merge_overlapping() {
        let base = b"line1\nline2\nline3\n";
        let local = b"line1\nlocal-change\nline3\n";
        let remote = b"line1\nremote-change\nline3\n";

        match three_way_text_merge(base, local, remote) {
            TextMergeResult::Overlap => {} // expected
            TextMergeResult::Merged { .. } => panic!("expected overlap, got merge"),
        }
    }

    #[test]
    fn merge_preserves_insertions_and_line_terminators() {
        let base = b"title\r\nkeep\r\nend\r\n";
        let insert = b"title\r\ninsert\r\nkeep\r\nend\r\n";
        let edit = b"title\r\nkeep\r\nchanged";
        for (local, remote) in [
            (insert.as_slice(), edit.as_slice()),
            (edit.as_slice(), insert.as_slice()),
        ] {
            let TextMergeResult::Merged { content } = three_way_text_merge(base, local, remote)
            else {
                panic!("disjoint edits must merge");
            };
            assert_eq!(content, b"title\r\ninsert\r\nkeep\r\nchanged");
        }
        assert!(matches!(
            three_way_text_merge(base, insert, b"title\r\nother\r\nkeep\r\nend\r\n"),
            TextMergeResult::Overlap
        ));
    }

    #[test]
    fn classify_with_zero_hashes() {
        let zero = [0u8; 32];
        assert_eq!(classify_sync(&zero, &zero, &zero), SyncAction::NoChange);
    }

    #[test]
    fn classify_extracts_hashes_in_conflict() {
        let base = hash_bytes(b"base");
        let local = hash_bytes(b"local");
        let remote = hash_bytes(b"remote");
        match classify_sync(&base, &local, &remote) {
            SyncAction::Conflict {
                base_hash,
                local_hash,
                remote_hash,
            } => {
                assert_eq!(base_hash, base);
                assert_eq!(local_hash, local);
                assert_eq!(remote_hash, remote);
            }
            _ => panic!("expected Conflict"),
        }
    }

    #[test]
    fn merge_empty_inputs() {
        let TextMergeResult::Merged { content } = three_way_text_merge(b"", b"", b"") else {
            panic!("identical empty inputs must merge");
        };
        assert!(content.is_empty());
    }

    #[test]
    fn merge_local_only_returns_local_lines() {
        // Local edits a line, remote unchanged. Result must contain the local change.
        let base = b"a\nb\nc\n";
        let local = b"a\nB\nc\n";
        let remote = b"a\nb\nc\n";
        match three_way_text_merge(base, local, remote) {
            TextMergeResult::Merged { content } => {
                let s = String::from_utf8(content).unwrap();
                assert!(s.contains("B"));
            }
            TextMergeResult::Overlap => panic!("expected merge, got overlap"),
        }
    }

    #[test]
    fn file_resolution_serde_roundtrip() {
        let h1 = hash_bytes(b"win");
        let h2 = hash_bytes(b"keep");
        let r = FileResolution::ConflictCopy {
            winner_hash: h1,
            preserved_hash: h2,
            preserved_path: "notes/conflict.md".into(),
        };
        let json = serde_json::to_string(&r).unwrap();
        // Tag-based serde: must include the discriminant.
        assert!(json.contains("conflict_copy"));
        let _back: FileResolution = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn file_resolution_skipped_serde() {
        let r = FileResolution::Skipped;
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("skipped"));
        let _back: FileResolution = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn file_conflict_clone_keeps_fields() {
        let c = FileConflict {
            path: "x.md".into(),
            base_hash: hash_bytes(b"b"),
            side_a_hash: hash_bytes(b"a"),
            side_b_hash: hash_bytes(b"bb"),
        };
        let c2 = c.clone();
        assert_eq!(c2.path, "x.md");
        assert_eq!(c2.base_hash, c.base_hash);
    }
}
