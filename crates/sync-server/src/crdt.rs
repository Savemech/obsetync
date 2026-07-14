//! Durable per-note CRDT update log (Ph4 live co-editing).
//!
//! The server is a DUMB relay: it never runs a CRDT library, never
//! interprets an update. It only (a) fans out opaque Yjs update blobs to a
//! note's subscribers over the sealed WS channel and (b) persists them to an
//! append-only log so durability never depends on "the last client to close"
//! (iOS kills backgrounded apps without warning — that was the data-loss
//! hole every CRDT-vs-file architecture shares). Any device can bootstrap a
//! note by fetching the log and applying every frame; Yjs updates are
//! commutative + idempotent, so order and duplicates don't matter.
//!
//! Log format: a sequence of `[u32 LE length][update bytes]` records. Each
//! record is written in a single `O_APPEND` write() so concurrent appends
//! from multiple devices never interleave (atomic per record on local FS).
//!
//! At-rest posture matches the rest of obsetync: the self-hosted server is
//! trusted and already stores all vault content in the clear; CRDT logs
//! follow the same model. (A future at-rest-encryption pass would cover
//! files and CRDT logs uniformly.)

use crate::storage::StorageLayout;
use std::fs;
use std::io::{Read, Write};

/// Hard cap on a single update frame — a DoS guard (the relay must not fan a
/// giant/crafted blob out to the whole fleet) and a sanity bound. A keystroke
/// update is ~30 bytes; a big paste is far under this.
pub const MAX_UPDATE_BYTES: usize = 4 * 1024 * 1024; // 4 MiB

/// Stable filesystem key for a note path (blake3 hex) — keeps arbitrary,
/// possibly hostile note paths from escaping the crdt dir.
fn note_key(note_path: &str) -> String {
    sync_core::hash::hash_to_hex(&sync_core::hash::hash_bytes(note_path.as_bytes()))
}

fn log_path(layout: &StorageLayout, vault_id: &str, note_path: &str) -> std::path::PathBuf {
    layout.crdt_log_path(vault_id, &note_key(note_path))
}

/// Append one opaque update blob to a note's log. Rejects oversized updates.
pub fn append(
    layout: &StorageLayout,
    vault_id: &str,
    note_path: &str,
    update: &[u8],
) -> Result<(), std::io::Error> {
    if update.is_empty() {
        return Ok(());
    }
    if update.len() > MAX_UPDATE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "crdt update exceeds MAX_UPDATE_BYTES",
        ));
    }
    let path = log_path(layout, vault_id, note_path);
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    // One framed record, one write() → atomic append under O_APPEND.
    let mut record = Vec::with_capacity(4 + update.len());
    record.extend_from_slice(&(update.len() as u32).to_le_bytes());
    record.extend_from_slice(update);

    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    f.write_all(&record)?;
    Ok(())
}

/// Read a note's full log as raw framed bytes (`[u32 len][bytes]`…). Empty
/// vec when the note has no log yet. The client splits and applies each frame.
pub fn read_log(
    layout: &StorageLayout,
    vault_id: &str,
    note_path: &str,
) -> Result<Vec<u8>, std::io::Error> {
    let path = log_path(layout, vault_id, note_path);
    match fs::File::open(&path) {
        Ok(mut f) => {
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            Ok(buf)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

/// Replace a note's log with a single compacted snapshot update (client
/// sends `Y.encodeStateAsUpdateV2` when the log grows or the note goes cold).
/// Atomic via tmp + rename so a concurrent reader never sees a half-written log.
pub fn compact(
    layout: &StorageLayout,
    vault_id: &str,
    note_path: &str,
    snapshot: &[u8],
) -> Result<(), std::io::Error> {
    if snapshot.len() > MAX_UPDATE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "crdt snapshot exceeds MAX_UPDATE_BYTES",
        ));
    }
    let path = log_path(layout, vault_id, note_path);
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("log.tmp");
    {
        let mut record = Vec::with_capacity(4 + snapshot.len());
        record.extend_from_slice(&(snapshot.len() as u32).to_le_bytes());
        record.extend_from_slice(snapshot);
        let mut f = fs::File::create(&tmp)?;
        f.write_all(&record)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    Ok(())
}

/// Split a raw log buffer into individual update frames — used by tests and
/// available for any server-side consumer. Malformed tails are ignored.
pub fn split_frames(log: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 4 <= log.len() {
        let len = u32::from_le_bytes([log[i], log[i + 1], log[i + 2], log[i + 3]]) as usize;
        i += 4;
        if i + len > log.len() {
            break; // truncated tail
        }
        out.push(&log[i..i + len]);
        i += len;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn layout() -> (tempfile::TempDir, StorageLayout) {
        let d = tempdir().unwrap();
        let l = StorageLayout::new(d.path());
        l.init_directories().unwrap();
        (d, l)
    }

    #[test]
    fn append_then_read_roundtrips_frames() {
        let (_d, l) = layout();
        append(&l, "v", "notes/a.md", b"update-1").unwrap();
        append(&l, "v", "notes/a.md", b"update-two").unwrap();
        let log = read_log(&l, "v", "notes/a.md").unwrap();
        let frames = split_frames(&log);
        assert_eq!(frames, vec![&b"update-1"[..], &b"update-two"[..]]);
    }

    #[test]
    fn distinct_notes_have_distinct_logs() {
        let (_d, l) = layout();
        append(&l, "v", "a.md", b"aaa").unwrap();
        append(&l, "v", "b.md", b"bbb").unwrap();
        let a = read_log(&l, "v", "a.md").unwrap();
        let b = read_log(&l, "v", "b.md").unwrap();
        assert_eq!(split_frames(&a), vec![&b"aaa"[..]]);
        assert_eq!(split_frames(&b), vec![&b"bbb"[..]]);
    }

    #[test]
    fn compact_replaces_the_whole_log() {
        let (_d, l) = layout();
        append(&l, "v", "a.md", b"one").unwrap();
        append(&l, "v", "a.md", b"two").unwrap();
        compact(&l, "v", "a.md", b"SNAPSHOT").unwrap();
        let log = read_log(&l, "v", "a.md").unwrap();
        assert_eq!(split_frames(&log), vec![&b"SNAPSHOT"[..]]);
    }

    #[test]
    fn oversized_update_is_rejected() {
        let (_d, l) = layout();
        let huge = vec![0u8; MAX_UPDATE_BYTES + 1];
        assert!(append(&l, "v", "a.md", &huge).is_err());
    }

    #[test]
    fn missing_note_reads_empty() {
        let (_d, l) = layout();
        assert!(read_log(&l, "v", "never.md").unwrap().is_empty());
    }

    #[test]
    fn hostile_note_path_stays_inside_crdt_dir() {
        let (_d, l) = layout();
        append(&l, "v", "../../etc/passwd", b"x").unwrap();
        // Written under a hashed filename, not the traversal path.
        let escaped = l.crdt_log_path("v", "../../etc/passwd");
        assert!(!escaped.exists());
        assert!(!read_log(&l, "v", "../../etc/passwd").unwrap().is_empty());
    }
}
