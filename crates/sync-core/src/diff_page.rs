//! Bounded binary pages for file-level tree deltas.
//!
//! The server may still materialize a Tree v1 diff internally, but neither the
//! wire envelope nor a client needs to hold the complete JSON delta.  A cursor
//! binds every continuation to immutable `from`/`to` roots and to the exact
//! last emitted record.  Recomputing a page therefore cannot silently skip a
//! record when the vault's current pointer changes between requests.

use crate::diff::FileDelta;
use crate::hash::FileHash;
use std::cmp::Ordering;

pub const REQUEST_MAGIC: &[u8; 4] = b"OBQ1";
pub const PAGE_MAGIC: &[u8; 4] = b"OBD1";
const CURSOR_MAGIC: &[u8; 4] = b"OBC1";
const CURSOR_VERSION: u8 = 1;

pub const REQUEST_HEADER_BYTES: usize = 4 + 32 + 32 + 4 + 4 + 2;
pub const PAGE_HEADER_BYTES: usize = 4 + 32 + 32 + 4 + 2;
pub const MIN_PAGE_BYTES: usize = 64 * 1024;
pub const MAX_PAGE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_PAGE_RECORDS: usize = 8_192;
pub const MAX_PATH_BYTES: usize = 4_096;
pub const MAX_CURSOR_BYTES: usize = 4 + 1 + 32 + 32 + 1 + 2 + 2 + 2 * MAX_PATH_BYTES;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError(String);

impl CodecError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CodecError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffCursor {
    pub from_root: FileHash,
    pub to_root: FileHash,
    key: CursorKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CursorKey {
    path: String,
    action: u8,
    old_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffPageRequest {
    pub from_root: FileHash,
    /// `None` on the first page means "snapshot the current root". Every
    /// continuation carries the exact root returned by the first page.
    pub to_root: Option<FileHash>,
    pub max_records: usize,
    pub max_plain_bytes: usize,
    pub cursor: Option<DiffCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedPage {
    pub bytes: Vec<u8>,
    pub record_count: usize,
    pub has_more: bool,
}

/// Decode and fully validate a client page request before any tree work.
pub fn decode_request(bytes: &[u8]) -> Result<DiffPageRequest, CodecError> {
    if bytes.len() < REQUEST_HEADER_BYTES {
        return Err(CodecError::new("diff request is truncated"));
    }
    if bytes.get(..4) != Some(REQUEST_MAGIC.as_slice()) {
        return Err(CodecError::new("invalid diff request magic"));
    }

    let from_root = read_hash(bytes, 4)?;
    let raw_to_root = read_hash(bytes, 36)?;
    let max_records = read_u32(bytes, 68)? as usize;
    let max_plain_bytes = read_u32(bytes, 72)? as usize;
    let cursor_len = read_u16(bytes, 76)? as usize;
    if max_records == 0 || max_records > MAX_PAGE_RECORDS {
        return Err(CodecError::new("diff request record cap is invalid"));
    }
    if !(MIN_PAGE_BYTES..=MAX_PAGE_BYTES).contains(&max_plain_bytes) {
        return Err(CodecError::new("diff request byte cap is invalid"));
    }
    if cursor_len > MAX_CURSOR_BYTES {
        return Err(CodecError::new("diff cursor exceeds hard limit"));
    }
    let expected = REQUEST_HEADER_BYTES
        .checked_add(cursor_len)
        .ok_or_else(|| CodecError::new("diff request length overflow"))?;
    if bytes.len() != expected {
        return Err(CodecError::new(
            "diff request has trailing or missing bytes",
        ));
    }

    let to_root = (raw_to_root != [0; 32]).then_some(raw_to_root);
    let cursor = if cursor_len == 0 {
        None
    } else {
        let cursor = decode_cursor(&bytes[REQUEST_HEADER_BYTES..])?;
        let requested_to =
            to_root.ok_or_else(|| CodecError::new("continuation request omitted target root"))?;
        if cursor.from_root != from_root || cursor.to_root != requested_to {
            return Err(CodecError::new("diff cursor roots do not match request"));
        }
        Some(cursor)
    };

    Ok(DiffPageRequest {
        from_root,
        to_root,
        max_records,
        max_plain_bytes,
        cursor,
    })
}

/// Deterministically order a materialized Tree v1 delta for cursor paging.
/// Duplicate target paths are rejected rather than relying on action order to
/// make an invalid semantic delta appear well-formed.
pub fn sort_and_validate_deltas(deltas: &mut [FileDelta]) -> Result<(), CodecError> {
    for delta in deltas.iter() {
        validate_delta(delta)?;
    }
    deltas.sort_unstable_by(compare_deltas);
    for pair in deltas.windows(2) {
        if delta_path(&pair[0]) == delta_path(&pair[1]) {
            return Err(CodecError::new(
                "diff touches one target path more than once",
            ));
        }
        if compare_deltas(&pair[0], &pair[1]) != Ordering::Less {
            return Err(CodecError::new("diff records are not strictly ordered"));
        }
    }
    Ok(())
}

/// Encode the page immediately after `request.cursor`. The returned byte
/// vector is guaranteed not to exceed the requested plaintext cap.
pub fn encode_page(
    from_root: FileHash,
    to_root: FileHash,
    deltas: &[FileDelta],
    request: &DiffPageRequest,
) -> Result<EncodedPage, CodecError> {
    if request.from_root != from_root {
        return Err(CodecError::new("diff request source root changed"));
    }
    if request.to_root.is_some_and(|root| root != to_root) {
        return Err(CodecError::new("diff request target root changed"));
    }
    if let Some(cursor) = &request.cursor {
        if cursor.from_root != from_root || cursor.to_root != to_root {
            return Err(CodecError::new("diff cursor snapshot changed"));
        }
    }

    let start = match &request.cursor {
        None => 0,
        Some(cursor) => {
            deltas
                .binary_search_by(|delta| compare_delta_cursor(delta, &cursor.key))
                .map_err(|_| CodecError::new("diff cursor record is absent from snapshot"))?
                + 1
        }
    };

    let mut record_bytes = 0usize;
    let mut end = start;
    let mut next_cursor = Vec::new();
    while end < deltas.len() && end - start < request.max_records {
        let encoded_len = encoded_record_len(&deltas[end])?;
        let candidate_record_bytes = record_bytes
            .checked_add(encoded_len)
            .ok_or_else(|| CodecError::new("diff page length overflow"))?;
        let candidate_end = end + 1;
        let candidate_cursor = if candidate_end < deltas.len() {
            encode_cursor(from_root, to_root, &deltas[end])?
        } else {
            Vec::new()
        };
        let candidate_total = PAGE_HEADER_BYTES
            .checked_add(candidate_cursor.len())
            .and_then(|size| size.checked_add(candidate_record_bytes))
            .ok_or_else(|| CodecError::new("diff page length overflow"))?;
        if candidate_total > request.max_plain_bytes {
            if end == start {
                return Err(CodecError::new(
                    "one diff record exceeds negotiated page cap",
                ));
            }
            break;
        }
        record_bytes = candidate_record_bytes;
        next_cursor = candidate_cursor;
        end = candidate_end;
    }

    let record_count = end - start;
    let has_more = end < deltas.len();
    if has_more && next_cursor.is_empty() {
        let previous = end
            .checked_sub(1)
            .ok_or_else(|| CodecError::new("diff page made no cursor progress"))?;
        next_cursor = encode_cursor(from_root, to_root, &deltas[previous])?;
    }
    if !has_more {
        next_cursor.clear();
    }
    let cursor_len = u16::try_from(next_cursor.len())
        .map_err(|_| CodecError::new("diff cursor length overflow"))?;
    let count =
        u32::try_from(record_count).map_err(|_| CodecError::new("diff record count overflow"))?;
    let capacity = PAGE_HEADER_BYTES + next_cursor.len() + record_bytes;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(PAGE_MAGIC);
    output.extend_from_slice(&from_root);
    output.extend_from_slice(&to_root);
    output.extend_from_slice(&count.to_le_bytes());
    output.extend_from_slice(&cursor_len.to_le_bytes());
    output.extend_from_slice(&next_cursor);
    for delta in &deltas[start..end] {
        encode_record(delta, &mut output)?;
    }
    if output.len() > request.max_plain_bytes {
        return Err(CodecError::new("encoded diff page exceeded negotiated cap"));
    }
    Ok(EncodedPage {
        bytes: output,
        record_count,
        has_more,
    })
}

fn validate_delta(delta: &FileDelta) -> Result<(), CodecError> {
    validate_path(delta_path(delta))?;
    if let FileDelta::Renamed { path, old_path, .. } = delta {
        validate_path(old_path)?;
        if path == old_path {
            return Err(CodecError::new("rename source equals target"));
        }
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<(), CodecError> {
    if path.is_empty() || path.len() > MAX_PATH_BYTES || path.starts_with('/') {
        return Err(CodecError::new("invalid vault-relative path length"));
    }
    if path.contains('\\')
        || path
            .chars()
            .any(|character| character.is_control() || character == '\u{7f}')
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(CodecError::new("unsafe vault-relative path"));
    }
    Ok(())
}

fn action(delta: &FileDelta) -> u8 {
    match delta {
        FileDelta::Added { .. } => 1,
        FileDelta::Modified { .. } => 2,
        FileDelta::Deleted { .. } => 3,
        FileDelta::Renamed { .. } => 4,
    }
}

fn delta_path(delta: &FileDelta) -> &str {
    match delta {
        FileDelta::Added { path, .. }
        | FileDelta::Modified { path, .. }
        | FileDelta::Deleted { path, .. }
        | FileDelta::Renamed { path, .. } => path,
    }
}

fn old_path(delta: &FileDelta) -> &str {
    match delta {
        FileDelta::Renamed { old_path, .. } => old_path,
        _ => "",
    }
}

fn compare_deltas(left: &FileDelta, right: &FileDelta) -> Ordering {
    left_key(left).cmp(&left_key(right))
}

fn left_key(delta: &FileDelta) -> (&[u8], u8, &[u8]) {
    (
        delta_path(delta).as_bytes(),
        action(delta),
        old_path(delta).as_bytes(),
    )
}

fn compare_delta_cursor(delta: &FileDelta, cursor: &CursorKey) -> Ordering {
    left_key(delta).cmp(&(
        cursor.path.as_bytes(),
        cursor.action,
        cursor.old_path.as_bytes(),
    ))
}

fn encode_cursor(
    from_root: FileHash,
    to_root: FileHash,
    delta: &FileDelta,
) -> Result<Vec<u8>, CodecError> {
    let path = delta_path(delta).as_bytes();
    let old_path = old_path(delta).as_bytes();
    let path_len =
        u16::try_from(path.len()).map_err(|_| CodecError::new("cursor path length overflow"))?;
    let old_path_len = u16::try_from(old_path.len())
        .map_err(|_| CodecError::new("cursor old path length overflow"))?;
    let mut output = Vec::with_capacity(4 + 1 + 32 + 32 + 1 + 2 + 2 + path.len() + old_path.len());
    output.extend_from_slice(CURSOR_MAGIC);
    output.push(CURSOR_VERSION);
    output.extend_from_slice(&from_root);
    output.extend_from_slice(&to_root);
    output.push(action(delta));
    output.extend_from_slice(&path_len.to_le_bytes());
    output.extend_from_slice(&old_path_len.to_le_bytes());
    output.extend_from_slice(path);
    output.extend_from_slice(old_path);
    Ok(output)
}

fn decode_cursor(bytes: &[u8]) -> Result<DiffCursor, CodecError> {
    const FIXED: usize = 4 + 1 + 32 + 32 + 1 + 2 + 2;
    if bytes.len() < FIXED || bytes.len() > MAX_CURSOR_BYTES {
        return Err(CodecError::new("diff cursor length is invalid"));
    }
    if bytes.get(..4) != Some(CURSOR_MAGIC.as_slice()) || bytes[4] != CURSOR_VERSION {
        return Err(CodecError::new("unsupported diff cursor"));
    }
    let from_root = read_hash(bytes, 5)?;
    let to_root = read_hash(bytes, 37)?;
    let action = bytes[69];
    if !(1..=4).contains(&action) {
        return Err(CodecError::new("diff cursor action is invalid"));
    }
    let path_len = read_u16(bytes, 70)? as usize;
    let old_path_len = read_u16(bytes, 72)? as usize;
    if path_len == 0 || path_len > MAX_PATH_BYTES || old_path_len > MAX_PATH_BYTES {
        return Err(CodecError::new("diff cursor path length is invalid"));
    }
    if action == 4 && old_path_len == 0 || action != 4 && old_path_len != 0 {
        return Err(CodecError::new("diff cursor fields do not match action"));
    }
    let expected = FIXED
        .checked_add(path_len)
        .and_then(|size| size.checked_add(old_path_len))
        .ok_or_else(|| CodecError::new("diff cursor length overflow"))?;
    if bytes.len() != expected {
        return Err(CodecError::new("diff cursor has trailing or missing bytes"));
    }
    let path_end = FIXED + path_len;
    let path = std::str::from_utf8(&bytes[FIXED..path_end])
        .map_err(|_| CodecError::new("diff cursor path is not UTF-8"))?
        .to_owned();
    let old_path = std::str::from_utf8(&bytes[path_end..])
        .map_err(|_| CodecError::new("diff cursor old path is not UTF-8"))?
        .to_owned();
    validate_path(&path)?;
    if !old_path.is_empty() {
        validate_path(&old_path)?;
        if old_path == path {
            return Err(CodecError::new("diff cursor rename source equals target"));
        }
    }
    Ok(DiffCursor {
        from_root,
        to_root,
        key: CursorKey {
            path,
            action,
            old_path,
        },
    })
}

fn encoded_record_len(delta: &FileDelta) -> Result<usize, CodecError> {
    validate_delta(delta)?;
    let mut length = 1 + varint_len(delta_path(delta).len()) + delta_path(delta).len();
    if let FileDelta::Renamed { old_path, .. } = delta {
        length = length
            .checked_add(varint_len(old_path.len()) + old_path.len())
            .ok_or_else(|| CodecError::new("diff record length overflow"))?;
    }
    if !matches!(delta, FileDelta::Deleted { .. }) {
        length = length
            .checked_add(32 + 8 + 8)
            .ok_or_else(|| CodecError::new("diff record length overflow"))?;
    }
    Ok(length)
}

fn encode_record(delta: &FileDelta, output: &mut Vec<u8>) -> Result<(), CodecError> {
    output.push(action(delta));
    encode_varint(delta_path(delta).len(), output)?;
    output.extend_from_slice(delta_path(delta).as_bytes());
    if let FileDelta::Renamed { old_path, .. } = delta {
        encode_varint(old_path.len(), output)?;
        output.extend_from_slice(old_path.as_bytes());
    }
    match delta {
        FileDelta::Added {
            hash,
            size,
            mtime_ms,
            ..
        }
        | FileDelta::Modified {
            hash,
            size,
            mtime_ms,
            ..
        }
        | FileDelta::Renamed {
            hash,
            size,
            mtime_ms,
            ..
        } => {
            output.extend_from_slice(hash);
            output.extend_from_slice(&size.to_le_bytes());
            output.extend_from_slice(&mtime_ms.to_le_bytes());
        }
        FileDelta::Deleted { .. } => {}
    }
    Ok(())
}

fn varint_len(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

fn encode_varint(mut value: usize, output: &mut Vec<u8>) -> Result<(), CodecError> {
    if value > u32::MAX as usize {
        return Err(CodecError::new("diff varint overflow"));
    }
    loop {
        let low = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            output.push(low);
            return Ok(());
        }
        output.push(low | 0x80);
    }
}

fn read_hash(bytes: &[u8], offset: usize) -> Result<FileHash, CodecError> {
    bytes
        .get(offset..offset + 32)
        .and_then(|raw| raw.try_into().ok())
        .ok_or_else(|| CodecError::new("diff hash is truncated"))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, CodecError> {
    bytes
        .get(offset..offset + 2)
        .and_then(|raw| raw.try_into().ok())
        .map(u16::from_le_bytes)
        .ok_or_else(|| CodecError::new("diff u16 is truncated"))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, CodecError> {
    bytes
        .get(offset..offset + 4)
        .and_then(|raw| raw.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| CodecError::new("diff u32 is truncated"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn root(byte: u8) -> FileHash {
        [byte; 32]
    }

    fn added(path: &str, byte: u8) -> FileDelta {
        FileDelta::Added {
            path: path.to_owned(),
            hash: root(byte),
            size: u64::from(byte),
            mtime_ms: 1_700_000_000_000 + u64::from(byte),
        }
    }

    fn request(from_root: FileHash, max_records: usize, max_plain_bytes: usize) -> DiffPageRequest {
        DiffPageRequest {
            from_root,
            to_root: None,
            max_records,
            max_plain_bytes,
            cursor: None,
        }
    }

    fn request_bytes(
        from_root: FileHash,
        to_root: FileHash,
        max_records: u32,
        max_plain_bytes: u32,
        cursor: &[u8],
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(REQUEST_MAGIC);
        bytes.extend_from_slice(&from_root);
        bytes.extend_from_slice(&to_root);
        bytes.extend_from_slice(&max_records.to_le_bytes());
        bytes.extend_from_slice(&max_plain_bytes.to_le_bytes());
        bytes.extend_from_slice(&(cursor.len() as u16).to_le_bytes());
        bytes.extend_from_slice(cursor);
        bytes
    }

    #[test]
    fn request_rejects_truncation_caps_and_cursor_root_substitution() {
        let valid = request_bytes(root(1), [0; 32], 100, MIN_PAGE_BYTES as u32, &[]);
        assert!(decode_request(&valid).is_ok());
        for end in 0..valid.len() {
            assert!(
                decode_request(&valid[..end]).is_err(),
                "accepted prefix {end}"
            );
        }

        let zero_records = request_bytes(root(1), [0; 32], 0, MIN_PAGE_BYTES as u32, &[]);
        assert!(decode_request(&zero_records).is_err());
        let huge_page = request_bytes(root(1), [0; 32], 1, (MAX_PAGE_BYTES + 1) as u32, &[]);
        assert!(decode_request(&huge_page).is_err());

        let cursor = encode_cursor(root(1), root(2), &added("a.md", 7)).unwrap();
        let substituted = request_bytes(root(1), root(3), 1, MIN_PAGE_BYTES as u32, &cursor);
        assert!(decode_request(&substituted).is_err());

        let self_rename = FileDelta::Renamed {
            old_path: "same.md".into(),
            path: "same.md".into(),
            hash: root(9),
            size: 9,
            mtime_ms: 9,
        };
        let cursor = encode_cursor(root(1), root(2), &self_rename).unwrap();
        let invalid = request_bytes(root(1), root(2), 1, MIN_PAGE_BYTES as u32, &cursor);
        assert!(decode_request(&invalid).is_err());
    }

    #[test]
    fn pages_are_bounded_ordered_and_resume_without_overlap() {
        let from = root(1);
        let to = root(2);
        let mut deltas = vec![added("z.md", 3), added("a.md", 1), added("m.md", 2)];
        sort_and_validate_deltas(&mut deltas).unwrap();

        let first_request = request(from, 2, MIN_PAGE_BYTES);
        let first = encode_page(from, to, &deltas, &first_request).unwrap();
        assert_eq!(first.record_count, 2);
        assert!(first.has_more);
        assert!(first.bytes.len() <= MIN_PAGE_BYTES);
        assert_eq!(&first.bytes[..4], PAGE_MAGIC);

        let cursor_len = u16::from_le_bytes(first.bytes[72..74].try_into().unwrap()) as usize;
        let cursor =
            decode_cursor(&first.bytes[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + cursor_len]).unwrap();
        let second_request = DiffPageRequest {
            from_root: from,
            to_root: Some(to),
            max_records: 2,
            max_plain_bytes: MIN_PAGE_BYTES,
            cursor: Some(cursor),
        };
        let second = encode_page(from, to, &deltas, &second_request).unwrap();
        assert_eq!(second.record_count, 1);
        assert!(!second.has_more);
        assert_eq!(
            u16::from_le_bytes(second.bytes[72..74].try_into().unwrap()),
            0
        );
    }

    #[test]
    fn cursor_must_name_an_exact_snapshot_record() {
        let from = root(1);
        let to = root(2);
        let mut deltas = vec![added("a.md", 1), added("c.md", 2)];
        sort_and_validate_deltas(&mut deltas).unwrap();
        let forged = DiffCursor {
            from_root: from,
            to_root: to,
            key: CursorKey {
                path: "b.md".into(),
                action: 1,
                old_path: String::new(),
            },
        };
        let request = DiffPageRequest {
            from_root: from,
            to_root: Some(to),
            max_records: 1,
            max_plain_bytes: MIN_PAGE_BYTES,
            cursor: Some(forged),
        };
        assert!(encode_page(from, to, &deltas, &request).is_err());
    }

    #[test]
    fn unsafe_paths_and_duplicate_targets_fail_closed() {
        let mut traversal = vec![added("../secret", 1)];
        assert!(sort_and_validate_deltas(&mut traversal).is_err());
        let mut duplicate = vec![added("same.md", 1), added("same.md", 2)];
        assert!(sort_and_validate_deltas(&mut duplicate).is_err());
    }

    #[test]
    fn response_never_crosses_requested_byte_cap() {
        let from = root(1);
        let to = root(2);
        let mut deltas: Vec<_> = (0..20_000)
            .map(|index| added(&format!("folder/{index:05}.md"), (index % 251) as u8))
            .collect();
        sort_and_validate_deltas(&mut deltas).unwrap();
        let page = encode_page(
            from,
            to,
            &deltas,
            &request(from, MAX_PAGE_RECORDS, MIN_PAGE_BYTES),
        )
        .unwrap();
        assert!(page.bytes.len() <= MIN_PAGE_BYTES);
        assert!(page.record_count < deltas.len());
        assert!(page.has_more);
    }

    #[test]
    fn record_hash_fixture_is_stable() {
        // A compact cross-language fixture guard: changing action/length/u64
        // encoding changes this digest and must be coordinated with TS.
        let from = root(1);
        let to = root(2);
        let mut deltas = vec![
            added("notes/a.md", 9),
            FileDelta::Deleted {
                path: "old.md".into(),
                hash: root(8),
            },
        ];
        sort_and_validate_deltas(&mut deltas).unwrap();
        let page = encode_page(from, to, &deltas, &request(from, 10, MIN_PAGE_BYTES)).unwrap();
        let fixture = page.bytes.iter().fold(2_166_136_261u32, |hash, byte| {
            (hash ^ u32::from(*byte)).wrapping_mul(16_777_619)
        });
        assert_eq!(fixture, 0x44b5_235b);
    }
}
