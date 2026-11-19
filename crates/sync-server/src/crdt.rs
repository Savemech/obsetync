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
//! Log format: a sequence of `[u32 LE length][update bytes]` records.
//! Non-empty appends sync the file before relay, and on Unix also sync its
//! directory ancestry so a newly created log can be found after restart.
//!
//! At-rest posture matches the rest of obsetync: the self-hosted server is
//! trusted and already stores all vault content in the clear; CRDT logs
//! follow the same model. (A future at-rest-encryption pass would cover
//! files and CRDT logs uniformly.)

use crate::storage::StorageLayout;
use std::fs;
use std::io::{self, BufReader, Read, Seek, Write};
use std::path::Path;

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

// The log inode changes on compaction. Lock a stable sibling before opening it.
fn lock_writer(path: &Path) -> io::Result<fs::File> {
    fs::create_dir_all(path.parent().expect("note log has parent"))?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.with_extension("lock"))?;
    lock.lock()?;
    Ok(lock)
}

fn frame_size(header: [u8; 4]) -> io::Result<usize> {
    let size = u32::from_le_bytes(header) as usize;
    if size > MAX_UPDATE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid CRDT frame length",
        ));
    }
    Ok(size)
}

// Pin at most 32 Unix inodes so compaction or inode reuse cannot make an old
// frame boundary look valid in a different file. A miss only costs a scan.
#[cfg(unix)]
mod boundaries {
    use super::*;
    use std::os::unix::fs::MetadataExt;
    use std::sync::Mutex;

    struct Boundary {
        dev: u64,
        ino: u64,
        _file: fs::File,
        end: u64,
    }

    static CACHE: Mutex<[Option<Boundary>; 32]> = Mutex::new([const { None }; 32]);

    pub(super) fn known(meta: &fs::Metadata) -> u64 {
        let cache = CACHE.lock().unwrap();
        cache[meta.ino() as usize % cache.len()]
            .as_ref()
            .filter(|b| b.dev == meta.dev() && b.ino == meta.ino() && b.end <= meta.len())
            .map_or(0, |b| b.end)
    }

    pub(super) fn forget(meta: &fs::Metadata) {
        let mut cache = CACHE.lock().unwrap();
        let index = meta.ino() as usize % cache.len();
        let slot = &mut cache[index];
        if slot
            .as_ref()
            .is_some_and(|b| b.dev == meta.dev() && b.ino == meta.ino())
        {
            *slot = None;
        }
    }

    pub(super) fn remember(file: &fs::File, meta: &fs::Metadata, end: u64) {
        let mut cache = CACHE.lock().unwrap();
        let index = meta.ino() as usize % cache.len();
        let slot = &mut cache[index];
        if let Some(b) = slot
            .as_mut()
            .filter(|b| b.dev == meta.dev() && b.ino == meta.ino())
        {
            b.end = end;
        } else if let Ok(pin) = file.try_clone() {
            *slot = Some(Boundary {
                dev: meta.dev(),
                ino: meta.ino(),
                _file: pin,
                end,
            });
        }
    }
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
    let _writer = lock_writer(&path)?;
    let mut f = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&path)?;
    // Pair with read_log across truncate, append and sync on this inode.
    f.lock()?;
    let meta = f.metadata()?;
    #[cfg(unix)]
    let mut end = boundaries::known(&meta);
    #[cfg(not(unix))]
    let mut end = 0u64;
    if end < meta.len() {
        f.seek(io::SeekFrom::Start(end))?;
        let mut reader = BufReader::new(&f);
        while meta.len() - end >= 4 {
            let mut header = [0; 4];
            reader.read_exact(&mut header)?;
            let size = frame_size(header)? as u64;
            if meta.len() - end < 4 + size {
                break;
            }
            end += 4 + size;
            reader.seek_relative(size as i64)?;
        }
        if end != meta.len() {
            // Persist the shortened log before reusing the torn frame's offsets.
            f.set_len(end)?;
            f.sync_all()?;
        }
    }
    let mut record = Vec::with_capacity(4 + update.len());
    record.extend_from_slice(&(update.len() as u32).to_le_bytes());
    record.extend_from_slice(update);
    f.write_all(&record)?;
    f.sync_all()?;
    #[cfg(unix)]
    crate::storage::sync_directory_ancestors(path.parent().expect("note log has parent"))?;
    f.unlock()?;
    #[cfg(unix)]
    boundaries::remember(&f, &meta, end + record.len() as u64);
    Ok(())
}

/// Read a note's full log as raw framed bytes (`[u32 len][bytes]`…). Empty
/// vec when the note has no log yet. The client splits and applies each frame.
pub fn read_log(
    layout: &StorageLayout,
    vault_id: &str,
    note_path: &str,
    max_bytes: usize,
) -> Result<Vec<u8>, std::io::Error> {
    let path = log_path(layout, vault_id, note_path);
    match fs::File::open(&path) {
        Ok(mut f) => {
            f.lock_shared()?;
            let len = f.metadata()?.len();
            let too_large = || {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "crdt log exceeds response limit",
                )
            };
            if len > max_bytes as u64 {
                return Err(too_large());
            }
            let mut buf = Vec::with_capacity(len as usize);
            (&mut f).take(max_bytes as u64).read_to_end(&mut buf)?;
            if buf.len() == max_bytes && f.read(&mut [0])? != 0 {
                return Err(too_large());
            }
            // A failed or interrupted append may have left only part of its record.
            let mut end = 0;
            while let Some(header) = buf[end..].first_chunk::<4>() {
                let size = frame_size(*header)?;
                if buf.len() - end < 4 + size {
                    break;
                }
                end += 4 + size;
            }
            buf.truncate(end);
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
    let parent = path.parent().expect("note log has parent");
    let _writer = lock_writer(&path)?;
    let mut staged = tempfile::NamedTempFile::new_in(parent)?;
    staged.write_all(&(snapshot.len() as u32).to_le_bytes())?;
    staged.write_all(snapshot)?;
    staged.as_file().sync_all()?;
    #[cfg(unix)]
    if let Ok(old) = fs::metadata(&path) {
        boundaries::forget(&old);
    }
    staged.persist(&path).map_err(|err| err.error)?;
    #[cfg(unix)]
    crate::storage::sync_directory_ancestors(parent)?;
    Ok(())
}

/// Inspect update boundaries in storage fixtures; malformed tails are ignored.
#[cfg(test)]
fn split_frames(log: &[u8]) -> Vec<&[u8]> {
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
        let log = read_log(&l, "v", "notes/a.md", 1024).unwrap();
        let frames = split_frames(&log);
        assert_eq!(frames, vec![&b"update-1"[..], &b"update-two"[..]]);
    }

    #[test]
    fn read_returns_complete_frames_at_every_interrupted_write_boundary() {
        let (_dir, layout) = layout();
        append(&layout, "v", "a.md", b"saved").unwrap();
        let path = log_path(&layout, "v", "a.md");
        let first = fs::read(&path).unwrap();
        let second = [3, 0, 0, 0, b'n', b'e', b'w'];
        for cut in 0..=second.len() {
            let disk = [first.as_slice(), &second[..cut]].concat();
            fs::write(&path, &disk).unwrap();
            let expected = if cut == second.len() { &disk } else { &first };
            assert_eq!(
                &read_log(&layout, "v", "a.md", 1024).unwrap(),
                expected,
                "cut {cut}"
            );
            assert_eq!(fs::read(&path).unwrap(), disk);
        }
        fs::write(&path, [first.as_slice(), &u32::MAX.to_le_bytes()].concat()).unwrap();
        assert_eq!(
            read_log(&layout, "v", "a.md", 1024).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn next_append_repairs_only_an_incomplete_tail() {
        let (_dir, layout) = layout();
        for cut in 1..7 {
            let note = format!("cut-{cut}");
            append(&layout, "v", &note, b"saved").unwrap();
            let path = log_path(&layout, "v", &note);
            let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[3, 0, 0, 0, b'o', b'l', b'd'][..cut]).unwrap();
            append(&layout, "v", &note, b"next").unwrap();
            let expected = b"\x05\0\0\0saved\x04\0\0\0next";
            assert_eq!(fs::read(path).unwrap(), expected, "cut {cut}");
        }
    }

    #[test]
    fn legacy_torn_tail_with_racing_writers_preserves_all_updates() {
        let (_dir, layout) = layout();
        let path = log_path(&layout, "v", "legacy");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"\x03\0\0\0old\x20\0\0\0cut").unwrap();
        let start = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for id in 0..8 {
                let (l, start) = (&layout, &start);
                scope.spawn(move || {
                    start.wait();
                    append(l, "v", "legacy", &[id]).unwrap();
                });
            }
        });
        let raw = fs::read(&path).unwrap();
        assert_eq!(raw.len(), 7 + 8 * 5);
        let frames = split_frames(&raw);
        assert_eq!(frames[0], b"old");
        let mut ids: Vec<_> = frames[1..].iter().map(|frame| frame[0]).collect();
        ids.sort();
        assert_eq!(ids, (0..8).collect::<Vec<_>>());

        let mut corrupt = raw;
        corrupt.extend(u32::MAX.to_le_bytes());
        fs::write(&path, &corrupt).unwrap();
        assert_eq!(
            append(&layout, "v", "legacy", b"late").unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(fs::read(path).unwrap(), corrupt);
    }

    #[test]
    fn distinct_notes_have_distinct_logs() {
        let (_d, l) = layout();
        append(&l, "v", "a.md", b"aaa").unwrap();
        append(&l, "v", "b.md", b"bbb").unwrap();
        let a = read_log(&l, "v", "a.md", 1024).unwrap();
        let b = read_log(&l, "v", "b.md", 1024).unwrap();
        assert_eq!(split_frames(&a), vec![&b"aaa"[..]]);
        assert_eq!(split_frames(&b), vec![&b"bbb"[..]]);
    }

    #[test]
    fn compact_replaces_the_whole_log() {
        let (_d, l) = layout();
        append(&l, "v", "a.md", b"one").unwrap();
        append(&l, "v", "a.md", b"two").unwrap();
        compact(&l, "v", "a.md", b"SNAPSHOT").unwrap();
        let log = read_log(&l, "v", "a.md", 1024).unwrap();
        assert_eq!(split_frames(&log), vec![&b"SNAPSHOT"[..]]);
    }

    #[test]
    fn competing_compactions_publish_only_complete_snapshots() {
        let (_dir, l) = layout();
        let start = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for id in 0..8 {
                let (layout, start) = (&l, &start);
                scope.spawn(move || {
                    start.wait();
                    compact(layout, "v", "a.md", &vec![id; 65536]).unwrap();
                });
            }
        });
        let log = read_log(&l, "v", "a.md", 65540).unwrap();
        let frames = split_frames(&log);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].len(), 65536);
        assert!(frames[0].iter().all(|&byte| byte == frames[0][0]));
    }

    #[test]
    fn log_read_limit_includes_record_headers() {
        let (_dir, l) = layout();
        append(&l, "v", "a.md", b"x").unwrap();
        assert_eq!(read_log(&l, "v", "a.md", 5).unwrap().len(), 5);
        assert!(read_log(&l, "v", "a.md", 4).is_err());
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
        assert!(read_log(&l, "v", "never.md", 1024).unwrap().is_empty());
    }

    #[test]
    fn hostile_note_path_stays_inside_crdt_dir() {
        let (_d, l) = layout();
        append(&l, "v", "../../etc/passwd", b"x").unwrap();
        // Written under a hashed filename, not the traversal path.
        let escaped = l.crdt_log_path("v", "../../etc/passwd");
        assert!(!escaped.exists());
        assert!(!read_log(&l, "v", "../../etc/passwd", 1024)
            .unwrap()
            .is_empty());
    }
}
