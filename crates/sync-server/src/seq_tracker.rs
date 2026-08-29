//! Durable per-device anti-replay window for HTTP transport v2.
//!
//! The plugin intentionally pipelines small uploads. A strict `seq > last`
//! check would reject valid requests whenever the network reorders them, so we
//! keep a 1024-bit IPsec-style window in memory. Durability is amortized in
//! blocks: before accepting any sequence in a new 4096-number block, the
//! server persists that block's ceiling. After a restart the whole reserved
//! block is conservatively treated as consumed, so a crash cannot make a
//! captured request replayable and the hot path does not fsync every upload.

use crate::storage::StorageLayout;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::sync::Mutex;

const WINDOW_BITS: usize = 1024;
const WINDOW_BYTES: usize = WINDOW_BITS / 8;
const RESERVATION_BLOCK: u64 = 4096;
const FILE_BYTES: usize = 8;

#[derive(Debug, PartialEq, Eq)]
pub enum ReplayDecision {
    Accepted,
    Replay { greatest_seen: u64 },
}

#[derive(Clone)]
struct ReplayWindow {
    greatest_seen: u64,
    /// Durable ceiling reserved before any sequence at or below it is
    /// accepted. On restart all values through this ceiling are rejected.
    reserved_through: u64,
    /// Bit index is age: bit 0 = greatest_seen, bit 1 = greatest_seen - 1.
    seen: [u8; WINDOW_BYTES],
}

impl Default for ReplayWindow {
    fn default() -> Self {
        Self {
            greatest_seen: 0,
            reserved_through: 0,
            seen: [0; WINDOW_BYTES],
        }
    }
}

pub struct SequenceTracker {
    layout: StorageLayout,
    states: Mutex<HashMap<String, ReplayWindow>>,
}

impl SequenceTracker {
    pub fn new(layout: StorageLayout) -> Self {
        Self {
            layout,
            states: Mutex::new(HashMap::new()),
        }
    }

    /// Atomically reject duplicates/stale values and reserve durability before
    /// the application handler runs. Consuming a sequence on a handler 5xx is
    /// deliberate: clients allocate a fresh number for every retry.
    pub fn check_and_record(
        &self,
        device_id: &str,
        sequence: u64,
    ) -> Result<ReplayDecision, std::io::Error> {
        let mut states = self.states.lock().expect("sequence tracker poisoned");
        if !states.contains_key(device_id) {
            states.insert(device_id.to_string(), self.load(device_id)?);
        }
        let state = states.get_mut(device_id).expect("just inserted");
        if sequence == 0 || is_seen_or_stale(state, sequence) {
            return Ok(ReplayDecision::Replay {
                greatest_seen: state.reserved_through.max(state.greatest_seen),
            });
        }

        if sequence > state.reserved_through {
            let reserved_through = reservation_ceiling(sequence);
            self.persist(device_id, reserved_through)?;
            state.reserved_through = reserved_through;
        }
        record(state, sequence);
        Ok(ReplayDecision::Accepted)
    }

    fn load(&self, device_id: &str) -> Result<ReplayWindow, std::io::Error> {
        let path = self.layout.device_sequence_path(device_id);
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ReplayWindow::default())
            }
            Err(error) => return Err(error),
        };
        if bytes.len() != FILE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid sequence-reservation file length",
            ));
        }
        let reserved_through = u64::from_be_bytes(bytes.try_into().unwrap());
        // We intentionally forget which values inside the final reservation
        // were actually observed. Marking the complete tail as seen means a
        // restart can only create harmless gaps, never replay acceptance.
        let seen = [u8::MAX; WINDOW_BYTES];
        Ok(ReplayWindow {
            greatest_seen: reserved_through,
            reserved_through,
            seen,
        })
    }

    fn persist(&self, device_id: &str, reserved_through: u64) -> Result<(), std::io::Error> {
        let path = self.layout.device_sequence_path(device_id);
        let parent = path.parent().expect("device sequence path has parent");
        fs::create_dir_all(parent)?;
        let tmp = path.with_extension("tmp");
        let mut file = fs::File::create(&tmp)?;
        file.write_all(&reserved_through.to_be_bytes())?;
        file.sync_all()?;
        fs::rename(&tmp, &path)?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
}

fn reservation_ceiling(sequence: u64) -> u64 {
    let remainder = sequence % RESERVATION_BLOCK;
    if remainder == 0 {
        sequence
    } else {
        sequence.saturating_add(RESERVATION_BLOCK - remainder)
    }
}

fn is_seen_or_stale(state: &ReplayWindow, sequence: u64) -> bool {
    if sequence > state.greatest_seen {
        return false;
    }
    let age = state.greatest_seen - sequence;
    age >= WINDOW_BITS as u64 || bit_is_set(&state.seen, age as usize)
}

fn record(state: &mut ReplayWindow, sequence: u64) {
    if sequence > state.greatest_seen {
        let delta = sequence - state.greatest_seen;
        if delta >= WINDOW_BITS as u64 {
            state.seen.fill(0);
        } else {
            let shift = delta as usize;
            let previous = state.seen;
            state.seen.fill(0);
            for age in 0..WINDOW_BITS - shift {
                if bit_is_set(&previous, age) {
                    set_bit(&mut state.seen, age + shift);
                }
            }
        }
        state.greatest_seen = sequence;
        set_bit(&mut state.seen, 0);
        return;
    }
    set_bit(&mut state.seen, (state.greatest_seen - sequence) as usize);
}

fn bit_is_set(bits: &[u8; WINDOW_BYTES], index: usize) -> bool {
    bits[index / 8] & (1 << (index % 8)) != 0
}

fn set_bit(bits: &mut [u8; WINDOW_BYTES], index: usize) {
    bits[index / 8] |= 1 << (index % 8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn accepts_reordered_parallel_requests_once() {
        let dir = tempdir().unwrap();
        let tracker = SequenceTracker::new(StorageLayout::new(dir.path()));
        assert_eq!(
            tracker.check_and_record("device", 5).unwrap(),
            ReplayDecision::Accepted
        );
        assert_eq!(
            tracker.check_and_record("device", 3).unwrap(),
            ReplayDecision::Accepted
        );
        assert_eq!(
            tracker.check_and_record("device", 4).unwrap(),
            ReplayDecision::Accepted
        );
        assert_eq!(
            tracker.check_and_record("device", 3).unwrap(),
            ReplayDecision::Replay {
                greatest_seen: 4096
            }
        );
    }

    #[test]
    fn window_survives_restart_and_rejects_stale_values() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let tracker = SequenceTracker::new(layout.clone());
        tracker.check_and_record("device", 10_000).unwrap();
        tracker.check_and_record("device", 9_500).unwrap();

        let restarted = SequenceTracker::new(layout);
        assert!(matches!(
            restarted.check_and_record("device", 9_500).unwrap(),
            ReplayDecision::Replay {
                greatest_seen: 12_288
            }
        ));
        assert!(matches!(
            restarted.check_and_record("device", 8_000).unwrap(),
            ReplayDecision::Replay {
                greatest_seen: 12_288
            }
        ));
        assert!(matches!(
            restarted.check_and_record("device", 10_001).unwrap(),
            ReplayDecision::Replay {
                greatest_seen: 12_288
            }
        ));
        assert_eq!(
            restarted.check_and_record("device", 12_289).unwrap(),
            ReplayDecision::Accepted
        );
    }

    #[test]
    fn a_large_reserved_block_gap_is_valid() {
        let dir = tempdir().unwrap();
        let tracker = SequenceTracker::new(StorageLayout::new(dir.path()));
        assert_eq!(
            tracker.check_and_record("device", 1).unwrap(),
            ReplayDecision::Accepted
        );
        assert_eq!(
            tracker.check_and_record("device", 4097).unwrap(),
            ReplayDecision::Accepted
        );
    }

    #[test]
    fn durable_file_reserves_a_whole_block_before_acceptance() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let tracker = SequenceTracker::new(layout.clone());
        assert_eq!(
            tracker.check_and_record("device", 7).unwrap(),
            ReplayDecision::Accepted
        );

        let durable = fs::read(layout.device_sequence_path("device")).unwrap();
        assert_eq!(durable.len(), FILE_BYTES);
        assert_eq!(u64::from_be_bytes(durable.try_into().unwrap()), 4096);

        let restarted = SequenceTracker::new(layout);
        assert_eq!(
            restarted.check_and_record("device", 4096).unwrap(),
            ReplayDecision::Replay {
                greatest_seen: 4096
            }
        );
    }
}
