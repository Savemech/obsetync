# Native root drain: reproducible harness

Related to PR01/05/07/08/09/10 in the [roadmap](responsive-sync-roadmap.md) and
the [reviewed queue](responsive-root-planning.md). The harness is part of the
implementation, not proof of complete release readiness.

## What actually runs

`plugin/scripts/bench-native-root.mjs` bundles the production TypeScript into
a separate instrumented Node child. Only Obsidian is replaced with a host
shell: `TFile`, cache, vault callbacks, Notice, and the requestUrl bridge. The
real `ObsetyncSyncEngine`, DesktopIO, journal, sync-base, root-intent, root
recovery/settlement, AEAD client, and packaged SIMD WASM execute. Each run
starts a separate release Rust sync-server with its own storage/device
identity on random IPv4 loopback ports.

The server gained an explicit `run --bind-address <IP>` option; its default
remains `0.0.0.0`, and no running-server configuration changes. Addresses
returned after bind contain the ports actually assigned, so the harness does
not reserve and release a supposedly free port in advance. Enrollment uses
an admin listener owned by the harness; sync requests use the real AEAD
client. The bridge refuses foreign origins and does not follow redirects.

The corpus contains 25,000 distinct 4-KiB notes, grouped into directories of
256. File number and generation are embedded in the content; unique objects
are not replaced with one deduplicated blob. A complete generation 1 is first
created on the client and server, including every WASM index chunk. After the
real `engine.start()`, generation 2 is prepared with a computed hash and a
confirmed adapter journal ID. This models a backlog after successful
preparation, not an initial full-scan measurement. Every generation-2 object
is initially absent from the server.

The measured boundary is the real `engine.pushPending()` through complete
local settlement. Seed work, v2 source writes/hash/journal, and subsequent
verification are excluded. One legacy root creates the initial fixture
baseline; legacy root calls are forbidden during the measured drain. Normal
short commits use durable outcomes with exact journal cuts.

Cases:

- `quiet`: an immutable queue of 25k prepared hints, one full review, and then
  only selected re-stat operations and 98 commits of at most 256 entries.
- `edits`: after API admission of the first real commit, without holding the
  response or adding artificial RTT, native writes and 70 `create` callbacks
  run; the last note receives three more `modify` generations. Callback
  latency begins on entry to the registered handler, not before the file is
  saved. This is **not** a test of editing during the first full audit; that
  lifecycle is covered by the separate `audit-edits` diagnostic. The report
  aggregates exact-generation latency separately from path-free root placement:
  the ordinal of the last API admission at callback time, the ordinal of the
  exact accepted admission, and their difference. The next available
  admission has distance `1`; per-probe records and paths are not retained in
  the JSON.
- `auto-edits`: a separate diagnostic gate for a real save → production
  `MaxLatencyCoalescer` → automatic push path. After the single explicit
  initial-backlog drain, the first `create` callback must start auto-push;
  the remaining callbacks arrive while it is running and must leave a
  retrigger. There is no explicit second `pushPending()`. Success requires at
  least two automatic invocations, completion of the whole automatic chain
  within 60 seconds, exact acceptance of every non-superseded generation,
  empty dirty/WAL/root owners, and zero rejection/observer overlap/miss
  counters. This is a Node timer diagnostic, not proof of Obsidian/iOS
  lifecycle behavior or responsiveness.
- `audit-edits`: a separate initial-review diagnostic. It injects the same
  callback wave after the first 256 metadata stats and before any root API
  admission. The first whole-review attempt must return without publishing
  its partially reviewed bulk prefix. One bounded review of the 70 durable
  callback paths then publishes two one-shot prefix roots: live-cut counts
  `[64, 6]` inside entry counts `[64, 6]`. Only after both roots settle may a
  new complete whole review begin. Every later root must contain only static
  bulk cuts, and their entry total must equal the original corpus size. This
  proves early exact callback publication without weakening the mandatory
  complete deletion review. The report keeps separate timing distributions
  for the interrupted attempt, first prefix admission, complete prefix
  admission, and the subsequent whole review. This remains synthetic
  Node/native-filesystem evidence, not a device responsiveness result.

If a new version interrupts a candidate that has not yet been sent, that
source drift is counted separately. The production engine may retain an
independent reviewed selection as urgent: a fully observed selection is not
repeated, and the total budget remains at most four continuations per drain,
even through a safe owner rebuild. Linked/cooling/structural paths and an
already-started root use the normal fail-closed recovery path. After the
writer finishes, the harness still permits one explicit additional
`pushPending()`; its duration and invocation count are included in the
result. An internally recovered drift does not remain as a false `Last
error`; an exhausted budget and other terminal errors remain visible and end
the current drain. The harness permits one explicit second drain after a
terminal drift; unexpected failures or a backlog left after it are a FAIL.

The explicit second-drain allowance above applies only to `edits`.
`auto-edits` fails unless callbacks and the production coalescer finish the
automatic chain after the one explicit initial-backlog drain.

`diagnostics.sourceDriftCountersVersion` defines the counter semantics. In
v2, `sourceOrGenerationDrifts` equals the sum of `retentionsAccepted` and
`terminalFailures`: the former is observed exactly when the production
reviewed queue successfully decides to retain, and the latter at
`recordSyncFailure`. Old reports without the marker counted terminal failures
only; absent v2 fields in those reports do not mean zero retained drift.
Accepted retention means that the final decision to retry was accepted, not
that the next attempt itself completed. Completion is proved separately by
one joined drain, empty dirty/WAL/root-intent owners, an exact server snapshot,
and cold-process readback.

## Measurements and verification

The harness records the number of full reviews and selected re-stat
operations, graphs actually created and selection visits, bounded claims,
completed host yields, root boundaries, native filesystem calls/bytes, and
network body bytes. Traversals and claims observe the real methods; they are
not a second queue algorithm. Instrumentation has explicit limit/drop/error
counters: success requires a complete observation set with no active,
unfinished methods.

The audit case expects exactly one `RootReviewPreempted` rejection from the
initial `materializeDirtyChanges` call and records it separately. That
scheduling signal must be followed by exactly one bounded prefix review and
one complete whole-review rebuild; every other instrumented method error must
remain zero.

Synchronous WASM `begin_candidate`, update, commit/abort, root/index exports,
and hashing are measured separately from actual-completion latency for
metadata/read, journal, base publication, API, and root runtime. This matters:
a root of 256 files does not itself bound an internal WASM graph walk.
Durations of nested methods cannot be added as independent wall time.

For the bounded live-probe set, path, journal epoch, own ID, and hash are
matched to the real accepted cut. Callback → journal completion, callback →
accepted server reply, and callback → durable-adapter ACK are separate. A
generation replaced by an accepted newer generation is not reported as an
exact published version. Passing requires the hash of the precise generation
named by the accepted watermark.

After the drain, the server returns a complete paged snapshot; every hash,
the semantic root, and stream sequence are checked. The measured child then
closes the engine, API, settings writer, and server. A **new process** reads
the same client files: it loads sync-base/journal/intents, requires an empty
WAL backlog and no pending intent, verifies every real source hash/stat,
rebuilds the complete packaged-WASM graph, and compares the final root. The
cold rebuild has its own timings; they are not part of the drain.

## Running it

From the project root and then from `plugin/`:

```sh
rtk proxy cargo build --release -p sync-server --bin sync-server --locked
rtk proxy node scripts/bench-native-root.mjs --check
rtk proxy node scripts/bench-native-root.mjs --smoke --case all
rtk proxy node scripts/bench-native-root.mjs --smoke --case auto-edits
rtk proxy node scripts/bench-native-root.mjs --smoke --case audit-edits
rtk proxy node scripts/bench-native-root.mjs --case all --runs 5 --output /tmp/obsetync-native-root-results-qualification.json
```

`--case auto-edits` and `--case audit-edits` are explicit-only, report
`diagnosticOnly=true`, and are not included in `--case all` or the local
quiet+edits qualification. Audit smoke uses 512 files so its injection occurs
inside a review larger than one 256-path materialization group.

`--check` does not start a listener. `--smoke` uses 32 files and one run; it
does not replace the 25k gate. `--files`, `--runs`, and `--binary` allow
explicit diagnostic variants. For `--case all`, order alternates in pairs:
quiet→edits, then edits→quiet. A local report receives the existing
`localSamplingEligible=true` field and its explicit
`localQualificationEligible=true` alias only for an exact 25,000-file corpus,
at least five runs of each case, and the complete result gate: p95 across the
per-run exact-generation callback p95 values must be at most 2,000 ms, and
p95 root-admission opportunities must be at most 3. The latter limit is a
local scheduler-placement guard from the deterministic regression; the
roadmap specifies a two-second wall-clock target but no separate numerical
root-placement gate. Placement does not replace latency. Any missing,
incomplete, or non-finite result fails closed; this does not change the
separate `releaseEligible=false`. With an odd five repeats, first-order
exposure is 3/2 and is recorded explicitly in `orderControl`, not described
as exactly balanced. Output is created exclusively; an existing report is
not overwritten. Credentials and concrete file paths are not included in
JSON. Each measured/cold child is isolated from the previous Node/WASM heap.
The runner uses its own detached Linux process group and `/proc`: a signal or
timeout does not count as a successful drain. Graceful or forced termination
requires that no executable descendants remain; an unfinished owner produces
an explicit error and preserves the temporary fixtures. A forcibly killed
server is not reported as having passed orderly cleanup; its own temporary
storage may remain for separate inspection. Stop other project benchmarks and
builds first. Summary retains the previous median aliases and adds
distributions: p50 is the interpolated median across isolated runs, and p95 is
exact nearest-rank. With the supported maximum of nine runs, that p95 equals
the maximum and is not presented as a reliable population tail.
`perRunCallbackToExactAcceptedP95Ms` is the distribution of each run's
internal p95 for exact accepted generations only, not pooled callback samples.
The previous `perRunCallbackToAcceptedP95Ms`, with callback-to-accepted-cut
semantics, remains alongside it for report compatibility.
`perRunRootAdmissionOpportunitiesToExactAcceptedP95` likewise aggregates the
internal p95 counts of subsequent root API admissions until exact acceptance;
this is placement evidence, not wall-clock, network, or device latency. A
single run is not presented as statistics or an old/new performance
improvement.

## Current qualification and initial-review diagnostic

The current-tree local qualification ran five isolated `quiet` and five
isolated `edits` processes against the exact 25,000-file corpus, alternating
case order in pairs. Raw report:
`/tmp/obsetync-native-root-results-qualification-preemption-r16.json`,
1,270,479 bytes, SHA-256
`0f78198e6a6186e03f30d65f9895daabca1ff935affe21183f83e1d9ddb12d99`;
instrumentation bundle SHA-256
`949c49c5c8a66c62b305a79ed58ef8b525bea01db0378021ebd3efcee05f1708`.

The report passed the local deterministic qualification: exact-generation
callback p95 was 1,279.184 ms against the 2,000-ms limit, and exact root
admission opportunity p95 was 3 against the limit of 3. Quiet median drain was
115,252.600 ms at 216.915 files/s; edits median drain was 119,894.049 ms at
208.517 files/s. Every measured and cold child verified the exact rebuilt
root, an empty journal and root-intent store, and zero source/generation drift,
terminal failure, unexpected failure, observer miss, or observer overlap.
This sets `localQualificationEligible=true`, but correctly leaves
`releaseEligible=false`: it is not a real-device UI/RSS result or a
same-durability reference-pipeline comparison.

The separate exact 25,000-file `audit-edits` diagnostic report is
`/tmp/obsetync-native-root-results-audit-preemption-r15.json`, SHA-256
`501d0c00eac9d3d8414db9f4e3505b11451b251a71548a44270f8d907b3eb39e`.
It interrupted the first whole-review attempt, published the bounded live
prefix as roots `[64, 6]`, then completed exactly one mandatory whole review
and all 25,000 static entries. Cold verification passed with no unfinished
owner. Exact callback p95 was 2,432.294 ms, compared with 7,325.154 ms before
initial-review preemption. This remains 432.294 ms above the local two-second
target. Callback-to-initial-review-return p95 was 2,077.230 ms because the
synthetic injector deliberately awaited all 73 sequential journal writes
inside one host yield. The result is retained as an honest stress boundary;
it does not replace the passing normal-save qualification or real Obsidian
input-to-paint evidence.

## Earlier diagnostic run (not qualification)

After content-pack coalescing (`responsive-stage-228`) and bounded cleanup (`responsive-stage-230`), one
quiet run of the complete 25,000-file corpus was performed. The raw JSON has
SHA-256
`4a13a42a7c078b4c2fccddcfd7f9b05af176e90392c7b913ad83cdd1e45365c9`
and size 124,509 bytes; the instrumentation bundle SHA-256 is
`82268c8afdb30d0c389ba2cf16e70cc386b4af3219ece0f2e7aa5df3049d1bf9`.

The drain took 136,674.463 ms (182.916 files/s); the first root was accepted
after 6,148.768 ms and the last after 136,566.537 ms. The 98 content-check and
98 content-put calls each transferred no more than 256 objects; the entire
wire used 784 requests. Heartbeat p95 did not exceed 32 ms, with a maximum of
143.866 ms. The exact cold root, 25,000 source hashes, absence of a pending
journal and root intent, and zero drift, terminal failures, and unfinished
operations were verified.

This is a diagnostic observation: the report itself explicitly contains
`localSamplingEligible=false` and `releaseEligible=false` because the edits
case and five repeats of each case are absent. Compared with the single
previous `v1-safe-final` report, whose SHA-256 is
`313b907deaf6aa55386a4a3d621d516b5088f6da50dfa16740d1229ca9d6b3b9`,
duration is 13.81% lower, throughput is 16.02% higher, and wire requests are
63.57% lower. Single sequential runs do not prove causality or a release
improvement.

This report also predates bounded sync-base admission (`responsive-stage-231`) and the
identity-bound desktop reader (`responsive-stage-232`). The reader change requires a new
native run, and the path still requires Windows/macOS qualification.

## Result boundaries

- The native adapter operates only on its own private canonical `/tmp`
  fixture. It verifies types, paths, links, and native completion. Additional
  `lstat/open/fstat/close` guards are included in the counters and wall time;
  this is not an exact model of Obsidian/Electron IPC or Windows/9p cost.
- Native timestamps are rounded down to the millisecond convention in the
  same way for cache and adapter stat. This is an explicit host-shell
  translation, not invented metadata success or stable identity under
  same-stat drift.
- Client write/readback means completion of the native API. The harness does
  not add `fsync`, does not prove power-loss behavior, and does not kill the
  server for durability qualification. Actual server durability is not
  weakened.
- The current baseline is HTTP-only, tree v1, and known-hash small notes. WS
  switching, CRDT/editor binding, workers, the native large-file pipeline,
  initial scan, and device suspend/resume are not tested by this run.
- Desktop tuning is fixed; feed min=max, and the governor does not run.
  `perfTrace` retains its normal per-operation lag sampler. Source hashes are
  computed during preparation; during the drain the client verifies native
  metadata/size, and the server validates the hash of uploaded content bytes.
  WASM metadata/intent hashing runs on the Node main thread, with no worker
  pool.
- The 16-ms heartbeat measures an interval between Node callbacks, **not
  input-to-paint**. Process memory and the capacity of one specific WASM
  linear memory are sampled; this is not allocator-used memory, a guaranteed
  maximum RSS, or a mobile-memory gate. The shared transient-pool peak
  includes seed work; the before/after snapshot exposes that boundary rather
  than presenting the whole-lifetime peak as the measured-phase peak.
- Cold readback after orderly completion is not crash recovery. A separate
  fresh-process harness already covers SIGKILL after object ACK but before the
  root, and after an accepted root but before the local receipt (`responsive-stage-210`,
  `responsive-stage-218`), but does not replace a complete power-loss/cut matrix. The other
  restart boundaries, real iOS/Android/macOS/Windows host bridges, long-term
  memory, a reference pipeline with the same durability, and the roadmap's
  70% throughput gate remain open.

The smoke run, single-run pilot, frozen baseline, and two historical
median-of-three series passed. They predate the current requirement of five
runs per case and do not count as a new local qualification. The authoritative
v2 repeat directly recorded 1/2/2 accepted retentions in the edits runs, zero
terminal failures, and completion of every run with one plan/explicit drain
and an exact cold root. The results, rejected intermediate candidate, old
counter boundary, and source/raw identities are recorded in the
[report](responsive-sync-progress.md) and
[derived evidence](native-root-read-concurrency-2026-09-05.json). This remains
historical actual packaged-WASM/native-FS throughput and cold evidence for
that snapshot; the existence of the harness and a Node heartbeat does not
close the new qualification, device UI/RSS/reload, or overall release
readiness.
