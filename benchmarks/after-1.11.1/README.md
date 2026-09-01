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

This Linux/x86_64 run is implementation evidence, not the final cross-hardware
release report. A17, M1, Snapdragon, cold/warm cache, power, and network fields
are added by the final hardware gate.
