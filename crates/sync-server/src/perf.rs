use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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

    http_receive_admitted: AtomicU64,
    http_receive_completed: AtomicU64,
    http_receive_busy: AtomicU64,
    http_receive_oversized: AtomicU64,
    http_receive_unknown_length: AtomicU64,
    http_receive_bounded_failures: AtomicU64,
    http_receive_inflight: AtomicU64,
    http_receive_inflight_bytes: AtomicU64,
    http_receive_peak_bytes: AtomicU64,

    ws_data_accepted: AtomicU64,
    ws_data_completed: AtomicU64,
    ws_data_errors: AtomicU64,
    ws_data_inflight_rpcs: AtomicU64,
    ws_data_responses_sending: AtomicU64,

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
    pub http_receive: HttpReceivePerfSnapshot,
    pub storage: StoragePerfSnapshot,
    pub diff: DiffPerfSnapshot,
    pub ws_data: WsDataPerfSnapshot,
}

/// Aggregate sealed-HTTP admission without paths or identities. The owner
/// covers request receive/decrypt and bounded response aggregation/sealing.
/// `completed` means the reservation owner was released; request success is
/// reported separately by `requests.errors`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HttpReceivePerfSnapshot {
    pub admitted: u64,
    pub completed: u64,
    pub busy: u64,
    pub oversized: u64,
    pub unknown_length: u64,
    pub bounded_receive_failures: u64,
    pub inflight: u64,
    pub inflight_bytes: u64,
    pub peak_bytes: u64,
}

pub struct HttpReceiveGuard {
    counters: Arc<ServerPerfCounters>,
    bytes: u64,
}

impl Drop for HttpReceiveGuard {
    fn drop(&mut self) {
        self.counters.http_receive_completed.fetch_add(1, RELAXED);
        self.counters.http_receive_inflight.fetch_sub(1, RELAXED);
        self.counters
            .http_receive_inflight_bytes
            .fetch_sub(self.bytes, RELAXED);
    }
}

/// Only admitted object RPCs are counted: auth, malformed frames and rejected
/// credit requests are excluded. Atomic fields are sampled independently.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct WsDataPerfSnapshot {
    pub accepted: u64,
    /// Non-error RPC responses flushed to the local socket, not peer receipt.
    pub completed: u64,
    /// Admitted RPCs ending in an error reply, send failure, cancellation or
    /// disconnect. Each admitted RPC has exactly one terminal outcome.
    pub errors: u64,
    /// Includes execution, queued responses and response transmission.
    pub inflight_rpcs: u64,
    /// Admitted RPC responses currently being sealed/flushed locally.
    pub responses_sending: u64,
}

pub struct WsDataRpcGuard {
    counters: Arc<ServerPerfCounters>,
    succeeded: bool,
}

impl WsDataRpcGuard {
    pub fn finish(mut self, succeeded: bool) {
        self.succeeded = succeeded;
    }
}

impl Drop for WsDataRpcGuard {
    fn drop(&mut self) {
        let terminal = if self.succeeded {
            &self.counters.ws_data_completed
        } else {
            &self.counters.ws_data_errors
        };
        terminal.fetch_add(1, RELAXED);
        self.counters.ws_data_inflight_rpcs.fetch_sub(1, RELAXED);
    }
}

pub struct WsDataResponseGuard<'a> {
    counters: &'a ServerPerfCounters,
}

impl Drop for WsDataResponseGuard<'_> {
    fn drop(&mut self) {
        self.counters
            .ws_data_responses_sending
            .fetch_sub(1, RELAXED);
    }
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
    pub fn begin_http_receive(self: &Arc<Self>, bytes: usize) -> HttpReceiveGuard {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.http_receive_admitted.fetch_add(1, RELAXED);
        self.http_receive_inflight.fetch_add(1, RELAXED);
        let current = self
            .http_receive_inflight_bytes
            .fetch_add(bytes, RELAXED)
            .saturating_add(bytes);
        update_peak(&self.http_receive_peak_bytes, current);
        HttpReceiveGuard {
            counters: self.clone(),
            bytes,
        }
    }

    pub fn record_http_receive_busy(&self) {
        self.http_receive_busy.fetch_add(1, RELAXED);
    }

    pub fn record_http_receive_oversized(&self) {
        self.http_receive_oversized.fetch_add(1, RELAXED);
    }

    pub fn record_http_receive_unknown_length(&self) {
        self.http_receive_unknown_length.fetch_add(1, RELAXED);
    }

    pub fn record_http_receive_bounded_failure(&self) {
        self.http_receive_bounded_failures.fetch_add(1, RELAXED);
    }

    pub fn begin_ws_data_rpc(self: &Arc<Self>) -> WsDataRpcGuard {
        self.ws_data_accepted.fetch_add(1, RELAXED);
        self.ws_data_inflight_rpcs.fetch_add(1, RELAXED);
        WsDataRpcGuard {
            counters: self.clone(),
            succeeded: false,
        }
    }

    pub fn begin_ws_data_response(&self) -> WsDataResponseGuard<'_> {
        self.ws_data_responses_sending.fetch_add(1, RELAXED);
        WsDataResponseGuard { counters: self }
    }

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
            http_receive: HttpReceivePerfSnapshot {
                admitted: load(&self.http_receive_admitted),
                completed: load(&self.http_receive_completed),
                busy: load(&self.http_receive_busy),
                oversized: load(&self.http_receive_oversized),
                unknown_length: load(&self.http_receive_unknown_length),
                bounded_receive_failures: load(&self.http_receive_bounded_failures),
                inflight: load(&self.http_receive_inflight),
                inflight_bytes: load(&self.http_receive_inflight_bytes),
                peak_bytes: load(&self.http_receive_peak_bytes),
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
            ws_data: WsDataPerfSnapshot {
                accepted: load(&self.ws_data_accepted),
                completed: load(&self.ws_data_completed),
                errors: load(&self.ws_data_errors),
                inflight_rpcs: load(&self.ws_data_inflight_rpcs),
                responses_sending: load(&self.ws_data_responses_sending),
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

fn update_peak(peak: &AtomicU64, value: u64) {
    let mut observed = peak.load(RELAXED);
    while value > observed {
        match peak.compare_exchange_weak(observed, value, RELAXED, RELAXED) {
            Ok(_) => break,
            Err(actual) => observed = actual,
        }
    }
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

    #[test]
    fn ws_rpc_remains_inflight_until_response_is_flushed() {
        let counters = Arc::new(ServerPerfCounters::default());
        let rpc = counters.begin_ws_data_rpc();
        assert_eq!(counters.snapshot().ws_data.accepted, 1);
        assert_eq!(counters.snapshot().ws_data.inflight_rpcs, 1);
        let sending = counters.begin_ws_data_response();
        assert_eq!(counters.snapshot().ws_data.responses_sending, 1);
        assert_eq!(counters.snapshot().ws_data.completed, 0);
        assert_eq!(counters.snapshot().ws_data.inflight_rpcs, 1);
        drop(sending);
        rpc.finish(true);
        let snapshot = counters.snapshot().ws_data;
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.errors, 0);
        assert_eq!(snapshot.inflight_rpcs, 0);
        assert_eq!(snapshot.responses_sending, 0);
    }

    #[test]
    fn http_receive_owner_reports_pressure_and_exact_release() {
        let counters = Arc::new(ServerPerfCounters::default());
        counters.record_http_receive_unknown_length();
        counters.record_http_receive_busy();
        counters.record_http_receive_oversized();
        counters.record_http_receive_bounded_failure();
        let owner = counters.begin_http_receive(4096);

        let active = counters.snapshot().http_receive;
        assert_eq!(active.admitted, 1);
        assert_eq!(active.completed, 0);
        assert_eq!(active.busy, 1);
        assert_eq!(active.oversized, 1);
        assert_eq!(active.unknown_length, 1);
        assert_eq!(active.bounded_receive_failures, 1);
        assert_eq!(active.inflight, 1);
        assert_eq!(active.inflight_bytes, 4096);
        assert_eq!(active.peak_bytes, 4096);

        drop(owner);
        let released = counters.snapshot().http_receive;
        assert_eq!(released.completed, 1);
        assert_eq!(released.inflight, 0);
        assert_eq!(released.inflight_bytes, 0);
        assert_eq!(released.peak_bytes, 4096);
    }

    #[test]
    fn ws_error_replies_and_dropped_rpcs_have_one_error_outcome() {
        let counters = Arc::new(ServerPerfCounters::default());
        counters.begin_ws_data_rpc().finish(false);
        let rpc = counters.begin_ws_data_rpc();
        let sending = counters.begin_ws_data_response();
        drop(sending);
        drop(rpc);
        let snapshot = counters.snapshot().ws_data;
        assert_eq!(snapshot.accepted, 2);
        assert_eq!(snapshot.completed, 0);
        assert_eq!(snapshot.errors, 2);
        assert_eq!(snapshot.inflight_rpcs, 0);
        assert_eq!(snapshot.responses_sending, 0);
    }

    #[tokio::test]
    async fn abort_during_ws_response_send_releases_both_gauges() {
        let counters = Arc::new(ServerPerfCounters::default());
        let task_counters = counters.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let rpc = task_counters.begin_ws_data_rpc();
            let sending = task_counters.begin_ws_data_response();
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(sending);
            rpc.finish(true);
        });
        started_rx.await.unwrap();
        let blocked = counters.snapshot().ws_data;
        assert_eq!(blocked.inflight_rpcs, 1);
        assert_eq!(blocked.responses_sending, 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let stopped = counters.snapshot().ws_data;
        assert_eq!(stopped.accepted, 1);
        assert_eq!(stopped.completed, 0);
        assert_eq!(stopped.errors, 1);
        assert_eq!(stopped.inflight_rpcs, 0);
        assert_eq!(stopped.responses_sending, 0);
    }
}
