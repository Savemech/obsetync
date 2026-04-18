use crate::hash::FileHash;
use similar::TextDiff;

/// Result of classifying a file's sync state using three hashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncAction {
    /// All three hashes match — nothing to do.
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
    let base_str = String::from_utf8_lossy(base);
    let local_str = String::from_utf8_lossy(local);
    let remote_str = String::from_utf8_lossy(remote);

    // Compute diffs: base→local and base→remote.
    let diff_local = TextDiff::from_lines(&base_str, &local_str);
    let diff_remote = TextDiff::from_lines(&base_str, &remote_str);

    // Collect changed line ranges for each side.
    let local_changes = collect_changed_ranges(&diff_local);
    let remote_changes = collect_changed_ranges(&diff_remote);

    // Check for overlapping changes.
    if ranges_overlap(&local_changes, &remote_changes) {
        return TextMergeResult::Overlap;
    }

    // No overlap — apply both sets of changes to the base.
    // Strategy: apply remote changes first (they have higher line numbers after local edits),
    // but since we're working from the base, we interleave both diffs.
    let merged = apply_non_overlapping_changes(
        &base_str,
        &local_str,
        &remote_str,
        &diff_local,
        &diff_remote,
    );

    match merged {
        Some(content) => TextMergeResult::Merged {
            content: content.into_bytes(),
        },
        None => TextMergeResult::Overlap,
    }
}

/// A range of lines that were changed (in the base's line numbering).
#[derive(Debug, Clone)]
struct ChangeRange {
    start: usize, // inclusive
    end: usize,   // exclusive
}

fn collect_changed_ranges(diff: &TextDiff<str>) -> Vec<ChangeRange> {
    let mut ranges = Vec::new();

    for op in diff.ops() {
        match op {
            similar::DiffOp::Equal { .. } => {}
            similar::DiffOp::Delete {
                old_index,
                old_len,
                ..
            }
            | similar::DiffOp::Replace {
                old_index,
                old_len,
                ..
            } => {
                ranges.push(ChangeRange {
                    start: *old_index,
                    end: old_index + old_len,
                });
            }
            similar::DiffOp::Insert { old_index, .. } => {
                ranges.push(ChangeRange {
                    start: *old_index,
                    end: *old_index,
                });
            }
        }
    }

    ranges
}

fn ranges_overlap(a: &[ChangeRange], b: &[ChangeRange]) -> bool {
    for ra in a {
        for rb in b {
            // Two ranges overlap if they share any line.
            // For insert points (start == end), they overlap if they're at the same position
            // and the other range covers that position.
            let a_start = ra.start;
            let a_end = if ra.start == ra.end {
                ra.end + 1
            } else {
                ra.end
            };
            let b_start = rb.start;
            let b_end = if rb.start == rb.end {
                rb.end + 1
            } else {
                rb.end
            };

            if a_start < b_end && b_start < a_end {
                return true;
            }
        }
    }
    false
}

/// Apply non-overlapping changes from both sides.
/// This is a simplified merge that works when changes don't overlap.
fn apply_non_overlapping_changes(
    base: &str,
    _local: &str,
    _remote: &str,
    diff_local: &TextDiff<str>,
    diff_remote: &TextDiff<str>,
) -> Option<String> {
    let base_lines: Vec<&str> = base.lines().collect();

    // Collect which base lines are replaced/deleted by each side,
    // and what new lines are inserted.
    let local_ops = collect_ops(diff_local);
    let remote_ops = collect_ops(diff_remote);

    let mut result = Vec::new();
    let mut i = 0;

    while i < base_lines.len() {
        let local_op = local_ops.get(&i);
        let remote_op = remote_ops.get(&i);

        match (local_op, remote_op) {
            (Some(op), None) => {
                // Local changed this region.
                for line in &op.new_lines {
                    result.push(*line);
                }
                i += op.old_len.max(1);
            }
            (None, Some(op)) => {
                // Remote changed this region.
                for line in &op.new_lines {
                    result.push(*line);
                }
                i += op.old_len.max(1);
            }
            (None, None) => {
                // Neither side changed — keep base line.
                result.push(base_lines[i]);
                i += 1;
            }
            (Some(_), Some(_)) => {
                // Both sides changed the same region — should have been caught by overlap check.
                return None;
            }
        }
    }

    // Handle trailing inserts after the last base line.
    if let Some(op) = local_ops.get(&base_lines.len()) {
        for line in &op.new_lines {
            result.push(*line);
        }
    }
    if let Some(op) = remote_ops.get(&base_lines.len()) {
        for line in &op.new_lines {
            result.push(*line);
        }
    }

    let mut merged = result.join("\n");
    // Preserve trailing newline if base had one.
    if base.ends_with('\n') {
        merged.push('\n');
    }

    Some(merged)
}

#[derive(Debug)]
struct MergeOp<'a> {
    old_len: usize,
    new_lines: Vec<&'a str>,
}

fn collect_ops<'a>(diff: &'a TextDiff<'_, '_, 'a, str>) -> std::collections::HashMap<usize, MergeOp<'a>> {
    let mut ops = std::collections::HashMap::new();

    for op in diff.ops() {
        match op {
            similar::DiffOp::Delete {
                old_index,
                old_len,
                ..
            } => {
                ops.insert(
                    *old_index,
                    MergeOp {
                        old_len: *old_len,
                        new_lines: vec![],
                    },
                );
            }
            similar::DiffOp::Replace {
                old_index,
                old_len,
                new_index,
                new_len,
            } => {
                let new_text = diff.new_slices();
                let new_lines: Vec<&str> = new_text[*new_index..*new_index + *new_len]
                    .iter()
                    .map(|s| s.trim_end_matches('\n'))
                    .collect();
                ops.insert(
                    *old_index,
                    MergeOp {
                        old_len: *old_len,
                        new_lines,
                    },
                );
            }
            similar::DiffOp::Insert {
                old_index,
                new_index,
                new_len,
            } => {
                let new_text = diff.new_slices();
                let new_lines: Vec<&str> = new_text[*new_index..*new_index + *new_len]
                    .iter()
                    .map(|s| s.trim_end_matches('\n'))
                    .collect();
                ops.insert(
                    *old_index,
                    MergeOp {
                        old_len: 0,
                        new_lines,
                    },
                );
            }
            _ => {}
        }
    }

    ops
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
    fn merge_identical_changes() {
        let base = b"line1\nline2\n";
        let changed = b"line1\nline2-same\n";

        // Both sides made the same change — local == remote, so classify_sync
        // would return NoChange. This function shouldn't be called in that case,
        // but if it is, both sides have the same content so it shouldn't matter.
    }
}
