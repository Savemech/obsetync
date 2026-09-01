# Obsetync 1.11.1 performance evidence

This directory contains deterministic, synthetic-only evidence produced while
implementing the 1.11.1 performance slices. Reports are additive: each slice
keeps the workload identity, platform, exact operation count, semantic oracle
result, and resource invariants needed to judge that slice.

`W6-slice1.json` is the full-scale transactional-tree soak, not a scaled smoke
run. It applies all 10,000 edits to the 10,000-file large-prefix workload. Each
edit uses begin/apply/commit plus reachability GC, and the final root is checked
against an independent flat rebuild. The mandatory Slice 1 invariant is
`unreachable_tree_chunks == 0`.

`W6-smoke.json` repeats the exact 1% dataset used by the 1.10.2 baseline. Its
semantic final root is identical while the final in-memory store drops from
101 chunks (one live + 100 historical) to exactly one reachable chunk.

`Tree-v1-prefix-slice2.json` compares the retained legacy repeated-scan
algorithm with production `update_tree` on the same deterministic batch:
10,000 existing entries, 5,000 upserts, and 5,000 deletes. Every root matches
the independent flat-state oracle. Across seven alternating-order iterations,
median update time falls from 362.41 ms to 6.97 ms (52.03x by median time); the
slowest observed speedup is 40.72x.

`WASM-SIMD-feed-slice3.json` measures the two production WASM artifacts over a
deterministic 32 MiB payload, with seven alternating-order iterations at 64,
256, 512, and 1024 KiB feeds. Every digest matches. On this Ryzen 5950X host,
the best SIMD median is 1,320 MiB/s versus 634 MiB/s scalar (2.08x). The parity
gate separately checks one-shot/batched BLAKE3 plus streaming BLAKE3 and
FastCDC manifests at five feed sizes over boundary-focused and 10 MiB inputs.

`desktop-workers-slice4.json` runs the production worker bundle through four
real Node `worker_threads` over 64 × 16 MiB deterministic files (1 GiB total).
The minified production worker bundle sustains 1,983 MiB/s from warm filesystem
cache while renderer-side event-loop lag remains 0.69 ms p95 / 0.86 ms p99
across 100 samples, against the 16 ms p95 gate.
The same run proves hash/manifest parity, cancellation, pre/post-stat drift
detection, and that neither request nor response carries file bytes.

`W1-bulk-http-slice5.json` runs the production bulk planner over all 100,000
W1 files (870.4 MB, deterministic 1–16 KiB distribution). The legacy data
path needs 100,391 check/upload requests. Bulk-v1 needs 782 on desktop and 810
under the 2 MiB mobile budget: 128.4x and 123.9x fewer requests respectively.
Every planned pack stays within the authenticated byte/count caps; the planner
does not materialize aggregate bodies while calculating those boundaries.

`W1-group-commit-slice6.json` sends those 100,000 W1 objects through the
production dedicated storage writer in 256-object batches. The fresh `/tmp`
run includes deterministic payload construction, server-side BLAKE3
verification, an 870.4 MB journal append, 100,000 atomic loose-mirror
publications, and every durability wait. It completes in 11.63 seconds at
8,598 files/s and 71.37 MiB/s with exactly 391 `fdatasync` calls—one per group,
instead of the legacy loose writer's 200,000 file-plus-directory barriers.
Crash tests separately cover torn tails, post-sync/pre-publication recovery,
partial publication, corrupt committed groups, and response loss after publish.

`W1-pack-storage-slice7.json` replaces the transitional journal plus 100,000
loose mirrors with four append-only segments and three sorted sidecar indexes.
The same 870.4 MB W1 workload completes in 4.69 seconds at 21,329 files/s and
177.05 MiB/s: 2.48x the Slice 6 writer while retaining the same 391 group
`fdatasync` barriers. A restart loads sealed sidecars and scans only the bounded
active tail: 199 ms and 65.08 MiB rehashed in this run, with zero sealed payload
bytes rehashed. Recovery tests cover torn tails, durable unpublished records,
response loss, corrupt sidecars, framed payload corruption, bounded loose
import, and the Slice 6 journal migration.

`W1-ws-data-slice8.json` seals the production W1 check/upload plan through
both production client envelopes, alternating order for three full runs per
profile. It covers 877.8 MB of identical bulk plaintext in 782 desktop or 810
mobile RPCs. On this host, keeping one ticket-derived WS AES-GCM session cuts
median client envelope time from 1,484 ms to 582 ms on the desktop profile
(2.55x) and from 1,042 ms to 810 ms with the 2 MiB mobile profile (1.29x).
This deliberately excludes socket/HTTP I/O and server work, so it does not
claim the additional request-routing/network win. Protocol/E2E tests separately
prove request multiplexing, negotiated request/byte credits, buffered-amount
hysteresis, protocol-specific AAD, strict decoding, bulk-HTTP fallback, and
recovery after loss at auth, HELLO, CHECK, GET, and unknown PUT boundaries.

Slice 9 changes execution topology rather than the W1 byte/request oracle, so
its gate is covered by deterministic concurrency and recovery tests instead of
a synthetic throughput claim. Bearer auth continues successfully after the
test removes both on-disk lookup files, proving the served hot path is memory
only. Restart tests reconstruct live/revoked/name/last-seen parity from
canonical files; injected revocation publication failure leaves auth and every
session token live, while a successful durable revoke immediately rejects HTTP
and cancels 128 concurrent session handles. Blocking-pool tests hold the
storage pool fully saturated and prove independent ticket/control work still
completes. Admission is fixed at four coarse storage operations and two
control-I/O operations, with permits acquired before `spawn_blocking`; bulk
CHECK/PUT/GET submits one operation per pack, never one task per object.

`W5-paged-diff-slice10.json` feeds 85,000 deterministic additions through the
production OBD1 encoder using the mobile 512 KiB plaintext budget. All records
are emitted exactly once in 11 independently releasable pages. The largest is
524,239 bytes (49 bytes below the hard cap) and 7,822 records, also below the
8,192-record cap. The client decoder rejects over-cap input before allocation
and the pull loop retains only that decoded page plus its separately bounded
apply/download batch. Cross-language fixtures, every truncated prefix, unsafe
paths, non-canonical/overflow lengths, root substitution, moving-current
snapshot tests, and cold-renderer recovery before and after the final cursor
cover the semantic and crash gates. Tree v1 still recomputes a materialized
delta on each page; the Tree v2 slices replace that server-side producer.

`W5-tree-v2-slice11.json` compares Tree v1 with the isolated path-CDC range-tree
prototype over 100,000 paths and three alternating-order runs per scenario.
Every incremental v2 root equals a fresh canonical rebuild, every v1/v2 delta
matches exactly, and every churn bound passes. Median update speedups are 93.58x
for a content edit, 95.34x for an insertion near the beginning, and 40.62x when
deleting a natural boundary. Median recursive-diff speedups are 143.34x, 154.18x,
and 43.87x. V2 loads at most 1,729 entries for an update and materializes at most
3,457 diff records, versus roughly 200,000 records in Tree v1. The disruptive
insert/delete cases create only 3/4 new reachable v2 nodes instead of 102/100 v1
nodes. This passes the Slice 11 gain/churn gate and selects the range-tree design
over the radix fallback; production root migration remains isolated to Slice 12.

This Linux/x86_64 run is implementation evidence, not the final cross-hardware
release report. A17, M1, Snapdragon, cold/warm cache, power, and network fields
are added by the final hardware gate.
