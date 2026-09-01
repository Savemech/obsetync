use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const RELAXED: Ordering = Ordering::Relaxed;

/// Process-lifetime, aggregate-only server telemetry. No method accepts an
/// identifier, route, path, token, hash, or payload, which makes the admin
/// export private-data-safe by construction.
#[derive(Default)]
pub struct ServerPerfCounters {
    requests_total: AtomicU64,
    request_errors: AtomicU64,
    request_objects: AtomicU64,
    wire_bytes_in: AtomicU64,
    plaintext_bytes_in: AtomicU64,
    wire_bytes_out: AtomicU64,
    plaintext_bytes_out: AtomicU64,
    queue_wait_ns: AtomicU64,
    envelope_open_ns: AtomicU64,
    token_replay_ns: AtomicU64,
    handler_ns: AtomicU64,
    response_seal_ns: AtomicU64,
    response_seal_failures: AtomicU64,

    loose_reads: AtomicU64,
    loose_writes: AtomicU64,
    pack_reads: AtomicU64,
    pack_appends: AtomicU64,
    fdatasyncs: AtomicU64,
    index_hits: AtomicU64,
    index_misses: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    bytes_rehashed: AtomicU64,
    corrupted_records: AtomicU64,
    storage_read_ns: AtomicU64,
    storage_write_ns: AtomicU64,
    durability_wait_ns: AtomicU64,
    hash_verify_ns: AtomicU64,
    open_count: AtomicU64,
    rename_count: AtomicU64,
    stat_count: AtomicU64,
    writer_queue_depth: AtomicU64,
    writer_queue_peak: AtomicU64,
    compaction_live_bytes: AtomicU64,
    compaction_dead_bytes: AtomicU64,

    diff_runs: AtomicU64,
    diff_nodes_visited: AtomicU64,
    diff_nodes_skipped: AtomicU64,
    diff_entries_materialized: AtomicU64,
    diff_serialized_bytes: AtomicU64,
    diff_pages: AtomicU64,
    diff_elapsed_ns: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub enum RequestPhase {
    QueueWait,
    EnvelopeOpen,
    TokenReplay,
    Handler,
    ResponseSeal,
}

#[derive(Debug, Clone, Copy)]
pub struct DiffSample {
    pub nodes_visited: u64,
    pub nodes_skipped: u64,
    pub entries_materialized: u64,
    pub serialized_bytes: u64,
    pub pages: u64,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ServerPerfSnapshot {
    pub schema_version: u8,
    pub requests: RequestPerfSnapshot,
    pub storage: StoragePerfSnapshot,
    pub diff: DiffPerfSnapshot,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RequestPerfSnapshot {
    pub total: u64,
    pub errors: u64,
    pub objects: u64,
    pub wire_bytes_in: u64,
    pub plaintext_bytes_in: u64,
    pub wire_bytes_out: u64,
    pub plaintext_bytes_out: u64,
    pub queue_wait_ns: u64,
    pub envelope_open_ns: u64,
    pub token_replay_ns: u64,
    pub handler_ns: u64,
    pub response_seal_ns: u64,
    pub response_seal_failures: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StoragePerfSnapshot {
    pub loose_reads: u64,
    pub loose_writes: u64,
    pub pack_reads: u64,
    pub pack_appends: u64,
    pub fdatasyncs: u64,
    pub index_hits: u64,
    pub index_misses: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub bytes_rehashed: u64,
    pub corrupted_records: u64,
    pub read_ns: u64,
    pub write_ns: u64,
    pub durability_wait_ns: u64,
    pub hash_verify_ns: u64,
    pub open_count: u64,
    pub rename_count: u64,
    pub stat_count: u64,
    pub writer_queue_depth: u64,
    pub writer_queue_peak: u64,
    pub compaction_live_bytes: u64,
    pub compaction_dead_bytes: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DiffPerfSnapshot {
    pub runs: u64,
    pub nodes_visited: u64,
    pub nodes_skipped: u64,
    pub entries_materialized: u64,
    pub serialized_bytes: u64,
    pub pages: u64,
    pub elapsed_ns: u64,
}

impl ServerPerfCounters {
    pub fn request_started(&self) {
        self.requests_total.fetch_add(1, RELAXED);
    }

    pub fn record_request_error(&self) {
        self.request_errors.fetch_add(1, RELAXED);
    }

    pub fn record_response_seal_failure(&self) {
        self.response_seal_failures.fetch_add(1, RELAXED);
    }

    pub fn record_request_objects(&self, objects: u64) {
        self.request_objects.fetch_add(objects, RELAXED);
    }

    #[cfg(test)]
    pub fn record_request_bytes(
        &self,
        wire_in: u64,
        plaintext_in: u64,
        wire_out: u64,
        plaintext_out: u64,
    ) {
        self.record_wire_request_bytes(wire_in);
        self.record_plaintext_request_bytes(plaintext_in);
        self.record_response_bytes(wire_out, plaintext_out);
    }

    pub fn record_wire_request_bytes(&self, bytes: u64) {
        self.wire_bytes_in.fetch_add(bytes, RELAXED);
    }

    pub fn record_plaintext_request_bytes(&self, bytes: u64) {
        self.plaintext_bytes_in.fetch_add(bytes, RELAXED);
    }

    pub fn record_response_bytes(&self, wire_bytes: u64, plaintext_bytes: u64) {
        self.wire_bytes_out.fetch_add(wire_bytes, RELAXED);
        self.plaintext_bytes_out.fetch_add(plaintext_bytes, RELAXED);
    }

    pub fn record_request_phase(&self, phase: RequestPhase, elapsed: Duration) {
        let counter = match phase {
            RequestPhase::QueueWait => &self.queue_wait_ns,
            RequestPhase::EnvelopeOpen => &self.envelope_open_ns,
            RequestPhase::TokenReplay => &self.token_replay_ns,
            RequestPhase::Handler => &self.handler_ns,
            RequestPhase::ResponseSeal => &self.response_seal_ns,
        };
        counter.fetch_add(duration_ns(elapsed), RELAXED);
    }

    pub fn record_loose_read(&self, bytes: u64, elapsed: Duration) {
        self.loose_reads.fetch_add(1, RELAXED);
        self.bytes_read.fetch_add(bytes, RELAXED);
        self.storage_read_ns
            .fetch_add(duration_ns(elapsed), RELAXED);
        self.open_count.fetch_add(1, RELAXED);
    }

    pub fn record_loose_write(
        &self,
        bytes: u64,
        write_elapsed: Duration,
        durability_elapsed: Duration,
        syncs: u64,
    ) {
        self.loose_writes.fetch_add(1, RELAXED);
        self.bytes_written.fetch_add(bytes, RELAXED);
        self.storage_write_ns
            .fetch_add(duration_ns(write_elapsed), RELAXED);
        self.durability_wait_ns
            .fetch_add(duration_ns(durability_elapsed), RELAXED);
        self.fdatasyncs.fetch_add(syncs, RELAXED);
        self.open_count.fetch_add(2, RELAXED);
        self.rename_count.fetch_add(1, RELAXED);
    }

    /// Record one durable journal group. `objects` counts immutable records,
    /// while `fdatasyncs` deliberately advances once per group rather than
    /// once per loose mirror file.
    pub fn record_pack_commit(
        &self,
        objects: u64,
        payload_bytes: u64,
        write_elapsed: Duration,
        durability_elapsed: Duration,
    ) {
        self.pack_appends.fetch_add(objects, RELAXED);
        self.bytes_written.fetch_add(payload_bytes, RELAXED);
        self.storage_write_ns
            .fetch_add(duration_ns(write_elapsed), RELAXED);
        self.durability_wait_ns
            .fetch_add(duration_ns(durability_elapsed), RELAXED);
        self.fdatasyncs.fetch_add(1, RELAXED);
    }

    pub fn record_pack_read(
        &self,
        bytes: u64,
        read_elapsed: Duration,
        verify_elapsed: Duration,
        valid: bool,
    ) {
        self.pack_reads.fetch_add(1, RELAXED);
        self.bytes_read.fetch_add(bytes, RELAXED);
        self.bytes_rehashed.fetch_add(bytes, RELAXED);
        self.storage_read_ns
            .fetch_add(duration_ns(read_elapsed), RELAXED);
        self.hash_verify_ns
            .fetch_add(duration_ns(verify_elapsed), RELAXED);
        self.open_count.fetch_add(1, RELAXED);
        if !valid {
            self.corrupted_records.fetch_add(1, RELAXED);
        }
    }

    pub fn record_writer_queue_add(&self, objects: u64) {
        let depth = self.writer_queue_depth.fetch_add(objects, RELAXED) + objects;
        let mut peak = self.writer_queue_peak.load(RELAXED);
        while depth > peak {
            match self
                .writer_queue_peak
                .compare_exchange_weak(peak, depth, RELAXED, RELAXED)
            {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }

    pub fn record_writer_queue_remove(&self, objects: u64) {
        let _ = self
            .writer_queue_depth
            .fetch_update(RELAXED, RELAXED, |depth| {
                Some(depth.saturating_sub(objects))
            });
    }

    pub fn record_hash_check(&self, bytes: u64, elapsed: Duration, matched: bool) {
        self.record_hash_verify(bytes, elapsed);
        if !matched {
            self.corrupted_records.fetch_add(1, RELAXED);
        }
    }

    pub fn record_hash_verify(&self, bytes: u64, elapsed: Duration) {
        self.bytes_rehashed.fetch_add(bytes, RELAXED);
        self.hash_verify_ns.fetch_add(duration_ns(elapsed), RELAXED);
    }

    pub fn record_index_lookup(&self, hit: bool) {
        self.stat_count.fetch_add(1, RELAXED);
        if hit {
            self.index_hits.fetch_add(1, RELAXED);
        } else {
            self.index_misses.fetch_add(1, RELAXED);
        }
    }

    pub fn record_diff(&self, sample: DiffSample) {
        self.diff_runs.fetch_add(1, RELAXED);
        self.diff_nodes_visited
            .fetch_add(sample.nodes_visited, RELAXED);
        self.diff_nodes_skipped
            .fetch_add(sample.nodes_skipped, RELAXED);
        self.diff_entries_materialized
            .fetch_add(sample.entries_materialized, RELAXED);
        self.diff_serialized_bytes
            .fetch_add(sample.serialized_bytes, RELAXED);
        self.diff_pages.fetch_add(sample.pages, RELAXED);
        self.diff_elapsed_ns
            .fetch_add(duration_ns(sample.elapsed), RELAXED);
    }

    pub fn snapshot(&self) -> ServerPerfSnapshot {
        ServerPerfSnapshot {
            schema_version: 1,
            requests: RequestPerfSnapshot {
                total: load(&self.requests_total),
                errors: load(&self.request_errors),
                objects: load(&self.request_objects),
                wire_bytes_in: load(&self.wire_bytes_in),
                plaintext_bytes_in: load(&self.plaintext_bytes_in),
                wire_bytes_out: load(&self.wire_bytes_out),
                plaintext_bytes_out: load(&self.plaintext_bytes_out),
                queue_wait_ns: load(&self.queue_wait_ns),
                envelope_open_ns: load(&self.envelope_open_ns),
                token_replay_ns: load(&self.token_replay_ns),
                handler_ns: load(&self.handler_ns),
                response_seal_ns: load(&self.response_seal_ns),
                response_seal_failures: load(&self.response_seal_failures),
            },
            storage: StoragePerfSnapshot {
                loose_reads: load(&self.loose_reads),
                loose_writes: load(&self.loose_writes),
                pack_reads: load(&self.pack_reads),
                pack_appends: load(&self.pack_appends),
                fdatasyncs: load(&self.fdatasyncs),
                index_hits: load(&self.index_hits),
                index_misses: load(&self.index_misses),
                bytes_read: load(&self.bytes_read),
                bytes_written: load(&self.bytes_written),
                bytes_rehashed: load(&self.bytes_rehashed),
                corrupted_records: load(&self.corrupted_records),
                read_ns: load(&self.storage_read_ns),
                write_ns: load(&self.storage_write_ns),
                durability_wait_ns: load(&self.durability_wait_ns),
                hash_verify_ns: load(&self.hash_verify_ns),
                open_count: load(&self.open_count),
                rename_count: load(&self.rename_count),
                stat_count: load(&self.stat_count),
                writer_queue_depth: load(&self.writer_queue_depth),
                writer_queue_peak: load(&self.writer_queue_peak),
                compaction_live_bytes: load(&self.compaction_live_bytes),
                compaction_dead_bytes: load(&self.compaction_dead_bytes),
            },
            diff: DiffPerfSnapshot {
                runs: load(&self.diff_runs),
                nodes_visited: load(&self.diff_nodes_visited),
                nodes_skipped: load(&self.diff_nodes_skipped),
                entries_materialized: load(&self.diff_entries_materialized),
                serialized_bytes: load(&self.diff_serialized_bytes),
                pages: load(&self.diff_pages),
                elapsed_ns: load(&self.diff_elapsed_ns),
            },
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn load(value: &AtomicU64) -> u64 {
    value.load(RELAXED)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn snapshot_aggregates_request_storage_and_diff_metrics() {
        let counters = ServerPerfCounters::default();
        counters.request_started();
        counters.record_request_bytes(120, 80, 200, 160);
        counters.record_request_phase(RequestPhase::EnvelopeOpen, Duration::from_micros(11));
        counters.record_request_phase(RequestPhase::TokenReplay, Duration::from_micros(7));
        counters.record_request_phase(RequestPhase::Handler, Duration::from_micros(13));
        counters.record_request_phase(RequestPhase::ResponseSeal, Duration::from_micros(5));
        counters.record_request_error();
        counters.record_loose_read(40, Duration::from_micros(3));
        counters.record_loose_write(50, Duration::from_micros(4), Duration::from_micros(6), 2);
        counters.record_pack_commit(8, 400, Duration::from_micros(9), Duration::from_micros(10));
        counters.record_writer_queue_add(5);
        counters.record_writer_queue_add(3);
        counters.record_writer_queue_remove(6);
        counters.record_hash_check(50, Duration::from_micros(2), false);
        counters.record_diff(DiffSample {
            nodes_visited: 9,
            nodes_skipped: 4,
            entries_materialized: 3,
            serialized_bytes: 111,
            pages: 1,
            elapsed: Duration::from_micros(8),
        });

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.requests.total, 1);
        assert_eq!(snapshot.requests.errors, 1);
        assert_eq!(snapshot.requests.wire_bytes_in, 120);
        assert_eq!(snapshot.requests.plaintext_bytes_in, 80);
        assert_eq!(snapshot.requests.envelope_open_ns, 11_000);
        assert_eq!(snapshot.storage.loose_reads, 1);
        assert_eq!(snapshot.storage.loose_writes, 1);
        assert_eq!(snapshot.storage.pack_appends, 8);
        assert_eq!(snapshot.storage.fdatasyncs, 3);
        assert_eq!(snapshot.storage.writer_queue_depth, 2);
        assert_eq!(snapshot.storage.writer_queue_peak, 8);
        assert_eq!(snapshot.storage.bytes_rehashed, 50);
        assert_eq!(snapshot.storage.corrupted_records, 1);
        assert_eq!(snapshot.diff.nodes_visited, 9);
        assert_eq!(snapshot.diff.nodes_skipped, 4);
        assert_eq!(snapshot.diff.entries_materialized, 3);
        assert_eq!(snapshot.diff.serialized_bytes, 111);
        assert_eq!(snapshot.diff.pages, 1);
    }

    #[test]
    fn exported_snapshot_has_no_private_identifier_fields() {
        let json = serde_json::to_value(ServerPerfCounters::default().snapshot()).unwrap();
        let object = json.as_object().unwrap();
        let encoded = serde_json::to_string(object).unwrap();
        for forbidden in ["\"path\"", "\"filename\"", "\"vault_id\"", "\"device_id\""] {
            assert!(
                !encoded.contains(forbidden),
                "private field leaked: {forbidden}"
            );
        }
    }
}
