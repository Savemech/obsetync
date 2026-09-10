# Responsive sync: implementation progress

Updated: 2026-09-10. Published baseline: `1.11.4` (`d5df76fe613d509a64c5ff22d9cc9f27845cc4f5`).
Related document: [implementation roadmap](responsive-sync-roadmap.md).

## Current state: stabilization candidate

The enabled P0–P3 file-sync scope has no known implementation backlog after
independent audits. The current tree also adds bounded initial-review
preemption: a separately captured and reviewed durable recent-edit prefix can
publish before a 25,000-file review completes, while deletion review, journal
ownership, dependency cuts, and the mandatory complete review remain
fail-closed. P4/Yjs/CRDT remains dormant by explicit scope decision.

The implementation is staged as `1.12.1`. Local release gates and exact-build
identity checks are required before deployment; remote CI, release artifacts,
tagging, publication, and real-device numerical evidence remain separate gates.

### Current exact-tree local evidence

The mandatory 25,000-file local qualification used five isolated `quiet` and
five isolated `edits` processes in alternating order. Raw report:
`/tmp/obsetync-native-root-results-qualification-preemption-r16.json`,
1,270,479 bytes, SHA256
`0f78198e6a6186e03f30d65f9895daabca1ff935affe21183f83e1d9ddb12d99`;
instrumentation bundle SHA256
`949c49c5c8a66c62b305a79ed58ef8b525bea01db0378021ebd3efcee05f1708`.
It passed the local gates:

- exact saved-generation callback p95: 1,279.184 ms, limit 2,000 ms;
- exact root-admission opportunity p95: 3, limit 3;
- quiet median: 115,252.600 ms, 216.915 files/s, first root 5,975.005 ms;
- edits median: 119,894.049 ms, 208.517 files/s, first root 6,042.569 ms;
- every run ended with an exact rebuilt root, empty journal and root intent,
  and zero source/generation drift, terminal failure, unexpected failure,
  observer miss, or observer overlap.

`localQualificationEligible=true` records the deterministic local gate only.
`releaseEligible=false` remains correct because this is neither a real-device
input/RSS run nor a same-durability comparison against the best safe reference
pipeline.

The separate 25,000-file `audit-edits` diagnostic injects 73 sequential saves
inside the first 256-stat cooperative cut. Raw report:
`/tmp/obsetync-native-root-results-audit-preemption-r15.json`, SHA256
`501d0c00eac9d3d8414db9f4e3505b11451b251a71548a44270f8d907b3eb39e`.
It published bounded live roots `[64, 6]` before rebuilding the mandatory full
review, completed the 25,000 static entries exactly once, and finished with an
exact cold root and no unfinished owner. Exact callback p95 was 2,432.294 ms,
down from the earlier 7,325.154-ms diagnostic but 432.294 ms above the local
two-second target. Callback-to-initial-review-return p95 was 2,077.230 ms: the
synthetic injector deliberately awaited all sequential journal writes inside
one host yield. This diagnostic is retained as an honest stress boundary, not
substituted for the passing normal-save qualification or for real Obsidian
input-to-paint evidence.

Current-tree automated gates completed so far:

- full plugin `npm test`, production build, TypeScript, and `git diff --check`:
  PASS;
- Rust format, workspace Clippy, workspace tests, sync-core WASM-feature tests,
  wasm32 check, and release server build: PASS; the server binary is 4,618,384
  bytes;
- native WS data, fragmentation/interleaving/cancellation/fallback, and both
  fresh-process crash boundaries: PASS. The tested release server binary
  SHA256 is
  `dba812c0d0269aa1a01dbc68e55c9b278a8eb826c71db328defac1d0d3deb8f3`;
- fresh packaged scalar/SIMD WASM reproduction, parity, 25,000-entry memory
  lifecycle, browser worker/probe, and desktop-worker regressions: PASS. All
  eight generated artifacts are byte-identical to the tracked files; scalar
  is 807,655 bytes and SIMD is 793,402 bytes;
- `nix flake check --no-build`, focused Nix WASM build plus parity/memory,
  complete `nix flake check`, and `nix build .#server`: PASS. The focused Nix
  output is `/tmp/obsetync-sync-core-wasm-post-preemption`;
- isolated Docker `just e2e`: PASS, including concurrency, conflict, enrollment,
  history export, realtime WS, stale-tree rejection, text merge, transport
  security, and one-device/two-device scenarios; teardown left no containers;
- current source and documentation outside the untracked user-owned `temp/`
  directory contain no Cyrillic text.

Remote CI passed for the initial `1.12.0` deployment commit. The tag-triggered
release stopped before publication because YAML folding passed a leading space
in the server provenance version argument; the corrected literal command is
covered by the normal CI workflow. Release artifacts and the broader real-device
qualification remain open.

### 1.12.1 field stabilization

The first Windows field run completed a 25k full scan in 1.6 minutes with
foreground lag normally at or below 8 ms, then retained all 24,655 dirty paths
after a deterministic root-plan admission refusal. No root mutation or journal
acknowledgement occurred after that refusal. The failure was independent of disk,
server, transport, and V2 output pressure: the configured 16 MiB metadata model
left room for only about 60 unescaped ASCII path characters per hashful row at
that queue size.

The root-plan ceiling is now 32 MiB without reducing any row, path, hash,
dependency, or workspace charge. The mobile shared arbiter retains an 8 MiB
interactive reserve; root review, root plan, and prepared-manifest ownership use
48 MiB of the 72 MiB background capacity and leave 24 MiB for other admitted
background work. The existing 25k hashful test now uses production-depth paths,
peaks at 18,058,752 modeled bytes, must fail under an explicit 16 MiB ceiling,
and drains under the new ceiling. This is still a deterministic model rather
than a measured heap/RSS or Jetsam guarantee.

## Previous freeze checkpoint (historical evidence)

The sections below retain the contracts and measurements recorded at each
historical slice. Where they mention a 16 MiB root-plan ceiling, that value is
superseded by the 1.12.1 field-stabilization note above.

The current verifiable code-freeze checkpoint before documentation edits is
`responsive-stage-235`. After separate audits, this cut
has no known implementation backlog left in the enabled P0–P3 file-sync
scope. This marks the end of feature development and the start of
stabilization, **not completion of P0–P3 and not release readiness**: their
numerical acceptance/device gates and P5 remain open. The version remains
`1.11.4`; there is no new tag, GitHub release, release assets, Nix SHA/pin,
remote push, or deployment.

After cut `responsive-stage-133`, the following groups were implemented and verified by
local integration/regression suites:

- compact 32-byte missing-index hash spool (`responsive-stage-135`), durable bounded scan,
  download, object-confirmation, and mobile transfer checkpoints; a restart
  continues confirmed work instead of requiring a global rehash;
- responsive scheduler lanes, urgent uploads, and root preemption; bounded
  preparation, mobile ranged/oversized streaming, browser/mobile hash workers,
  shared admission owners, and lifecycle quiescence;
- WS bulk multiplexing, control priority, cancellation ordering, semantic
  probes, and measured adaptive WS/HTTP routing with recovery; server writer
  fairness and sealed-HTTP memory admission/backpressure;
- truthful progress/milestones, path redaction, build provenance, version and
  artifact manifest checks;
- fail-closed legacy compatibility handoff without mixed-format authority;
- the same pre-allocation/admission/retirement contract for Tree V1/V2
  replacement, candidate-open, and mutation output. The full integration suite
  additionally caught rejection of a valid V1 plan barrier (`responsive-stage-200`) and
  stale synthetic ABI fixtures (`responsive-stage-201`) without weakening production
  validation.

After `responsive-stage-201`, the code-freeze cut additionally received:

- a bounded and amortized candidate mutation driver, preservation of reviewed
  batch ownership, and elimination of repeated wide-prefix sorting (`responsive-stage-209`,
  `responsive-stage-214`, `responsive-stage-215`, `responsive-stage-222`), followed by reuse of canonical V1 leaves
  without changing serialized tree bytes (`responsive-stage-223`);
- process-crash recovery before root and after an accepted root (`responsive-stage-210`,
  `responsive-stage-218`), a native WS data lane and WS-v2 end-to-end (`responsive-stage-211`, `responsive-stage-213`),
  protocol fuzzing and adaptive carrier chaos (`responsive-stage-207`, `responsive-stage-208`, `responsive-stage-227`,
  `responsive-stage-229`);
- responsive content-pack coalescing without mixing root/dependency cuts
  (`responsive-stage-228`), bounded deferred segmented-store cleanup (`responsive-stage-230`), and
  count/byte admission of all queued and in-flight sync-base mutations
  (`responsive-stage-231`);
- desktop whole-file reading bound to one no-follow descriptor and a
  revalidated identity path/ancestors, with fail-closed drift and a portable
  adapter fallback (`responsive-stage-232`); the actual crash fixture accounts for this
  path (`responsive-stage-234`);
- a reproducible scalar/SIMD build chain with a single Binaryen ≥117,
  checksum-pinned CI/release tools, amd64/arm64 Docker mapping, an explicit
  scalar-no-SIMD contract, and artifact tests after a fresh build (`responsive-stage-235`);
  Nix applies the same feature/version contract and explicitly builds the WASM
  derivation in flake checks;
- mandatory root-export, mutation-complexity, cancellation, lifecycle,
  transport-pressure, and benchmark-order regressions in the actual test/CI
  entrypoints (`responsive-stage-206`, `responsive-stage-212`, `responsive-stage-224`, `responsive-stage-226`, `responsive-stage-225`,
  `responsive-stage-233`, and related test-only commits).

Confirmed local freeze checks for `responsive-stage-235`:

- full plugin `npm test`: PASS, `exit_code=0`; helper TAP — 73/73, followed by
  all 122 declared source suites, 30 actual-main lifecycle scenarios / 311
  assertions, and final metadata/parser TAP 52/52. A single fabricated
  aggregate assertion count is not used;
- `cargo test -p sync-server`: 323 passed, 1 ignored, 0 failed,
  `exit_code=0`;
- `cargo build --release -p sync-server --bin sync-server --locked`:
  `exit_code=0`; local binary — 4 618 512 bytes;
- `cargo clippy --workspace --locked --all-targets`: `exit_code=0`, 0 errors,
  242 warnings. A separate diagnostic run with `-D warnings` was not a gate
  and stopped on the existing warnings;
- both fresh-process crash scenarios — PASS: stopping after object ACK before
  root and after an accepted root before the local receipt;
- fresh scalar/SIMD build using an exact local equivalent of the CI toolchain —
  PASS: `wasm-pack 0.13.1`, Binaryen 117; scalar 811 717 bytes, SIMD 797 076
  bytes. SHA256: scalar
  `dc82bf8576057a9dca1c3b67c093fb8e01f09282ae1a0b13ec7bd67a8abefd2a`,
  SIMD `8dd6cba12488483fb40f8772b2b04e954b9c109b4844954e5a60431565a87239`;
- packaged scalar/SIMD parity — PASS (`parity=true`, payload 10 485 883 bytes,
  5 hash and 9 chunk feed sizes). Packaged memory lifecycle — PASS: all 24
  scalar/SIMD × narrow/wide × V1/V2 ×3 cycles, zero allocation/reallocation
  failures; this is requested-allocation telemetry for one WASM instance, not
  RSS;
- `nix flake check --no-build` and the actual build of
  `checks.x86_64-linux.sync-core-wasm` — PASS; parity directly on the Nix
  output — PASS. Locked Nix Binaryen 126 satisfies the minimum contract;
- browser hash worker, browser probe, TypeScript, production build, build-WASM
  contracts 4/4, runner contracts 18/18, and release helper suites — PASS.
  Remote CI and actual release artifacts have still not been run.

### Latest 25k diagnostic, not qualification

One isolated `quiet` run of 25 000 × 4 KiB was performed on a code cut that
already included content-pack coalescing `responsive-stage-228` and bounded store cleanup
`responsive-stage-230`, but did **not** yet include sync-base admission `responsive-stage-231` or the
desktop identity reader `responsive-stage-232`. Raw local report:
`/tmp/obsetync-native-root-results-content-pack-final.json`: 124 509 bytes,
SHA256 `4a13a42a7c078b4c2fccddcfd7f9b05af176e90392c7b913ad83cdd1e45365c9`;
instrumentation bundle SHA256
`82268c8afdb30d0c389ba2cf16e70cc386b4af3219ece0f2e7aa5df3049d1bf9`.

The drain took 136 674.463 ms, with useful throughput of 182.916 files/s; the
first root was accepted after 6 148.768 ms. The run recorded 98 check and 98
put calls with at most 256 objects, 784 wire requests, heartbeat p95 ≤ 32 ms,
and a maximum of 143.866 ms. Cold fresh-process readback obtained the exact
root, empty journal/root-intent, and zero source drift/terminal failures. The
report explicitly contains `localSamplingEligible=false` and
`releaseEligible=false`: `edits` and at least five runs of each variant are
absent.

The previous single safe diagnostic had raw SHA256
`313b907deaf6aa55386a4a3d621d516b5088f6da50dfa16740d1229ca9d6b3b9`,
158 570.595 ms, 157.658 files/s, and 2 152 wire requests. The new run differs
by −13.81% wall duration, +16.02% throughput, and −63.57% wire requests, but
single runs on different code cuts do not prove causality, the population
tail, or a release performance gate.

What actually remains before release:

- iPhone/iPad/Android/macOS/Windows matrix: input-to-paint, UI lag, RSS/Jetsam,
  plugin reload, suspend/resume, OS kill, and native-filesystem behavior;
- LAN/WAN/loss/reorder/WS-reset testing of automatic WS ↔ HTTP selection,
  mixed-client upgrade, and prolonged soak/canary;
- fault/power-loss evidence on real storage, remote CI on the exact commit, and
  threshold calibration. Increasing concurrency for pull/reconcile is
  intentionally excluded without comparable admitted stage-demand evidence;
- only after these gates: version assignment, tag, release assets,
  provenance/SHA256, Nix pin update, and staged deployment.

P4/Yjs/CRDT remains a frozen dormant track: simultaneous editing of one file
is not being optimized now, while the existing conflict-copy, revision/root,
and journal safeguards remain in place. The JS plugin also cannot strictly
bound native allocation created by the host `requestUrl` before the body is
returned to JS; telemetry and server/client admission must not be presented as
a complete RSS ceiling.

## Historical implementation chronology

The history rewrite consolidates the unpublished development range into one
reviewable implementation commit. Labels such as `responsive-stage-043` below
preserve the order of historical local measurements and design checkpoints;
they are documentation identifiers, not Git object names. Published baseline
tags and their commit identities remain unchanged.

Every section below preserves the evidence and limitations of its particular
cut at the time it was completed. Statements such as "not yet implemented"
and "next step" in an older cut are not the current backlog: they must be read
together with the later sections and the current status above. They are
intentionally not rewritten retroactively.

First slice: `1c03979a59e784172c6fbd61645d7326987c25a6` (runtime/tests), `004d40fa37e318e220e79fa3d0dde7d84ebf22c8` (test runner/CI).

Second slice: `responsive-stage-002` (worker recovery), `responsive-stage-003` (active windows, adaptive batches, hash source admission). The commits are local; GitHub workflows have not yet run for them.

Third slice: `responsive-stage-005` (bounded FastCDC window, index size query), `responsive-stage-006` (shared transfer admission, native ownership, deferred work). These are local commits, not a new tag/release/deploy.

Fourth slice: `responsive-stage-008` (journal checkpoints), `responsive-stage-009` (bounded repair, rename recovery, browser probe), `responsive-stage-010` (browser-WASM CI/preflight). The commits are local; the version and Nix pins were not changed by this slice, and no publication was performed.

Fifth slice: `responsive-stage-012` (diagnostic checkpoint queue), `responsive-stage-013` (segmented sync-base, recovery/GC, filesystem benchmark), `responsive-stage-014` (local event guards, engine replay, and regression entry point). Local commits; version `1.11.4`, Nix pins, and production deployment are unchanged.

Sixth slice: `responsive-stage-016` (source-proven migration/retry), `responsive-stage-017` (segmented batched journal, partial rename ACK, and filesystem benchmark), `responsive-stage-018` (startup capture before negotiation, stale-init fences, and regression entry point). Local commits; tag/release/push, version, Nix pins, and deployment are unchanged.

Seventh slice: `responsive-stage-020` (bounded prepared storage and verified logical publication), `responsive-stage-021` (desktop reuse with full-file verification, journal-bound cleanup, and regression entry point), `responsive-stage-022` (native/WASM preparation benchmark). Local commits; version `1.11.4`, Nix pins, tag/release/push, and deployment are unchanged.

Eighth slice: `responsive-stage-024` (actual engine drain and continuous capture handoff), `responsive-stage-025` (native worker retirement), `responsive-stage-026` (plugin retirement registry, serial settings, tree disposal, and mandatory actual-main lifecycle harness). All three commits are local; version, Nix pins, tag/release/push, and production are unchanged.

Ninth slice: `responsive-stage-028` (atomic server root outcomes, strict ordered replay, incarnation fence, native process/fault tests, and ADR). Local commit; the client does not yet activate the new root protocol. Version, Nix pins, tag/release/push, and production are unchanged.

Tenth slice: `responsive-stage-030` (durable terminal cancellation and sealed method fences), `responsive-stage-031` (client wire codec/API, authoritative root intents, atomic base publication, epoch-bound journal ACK, and replayable conflict-free settlement). Local commits; the engine does not yet activate the new path. Version `1.11.4`, Nix pins, tag/release/push, and deployment are unchanged.

Eleventh slice: `responsive-stage-033` (root outcome recovery, bounded verified conflict preservation, durable copy receipts, and recovery/lifecycle/native-IO regressions). Local commit; engine integration is not yet activated. Version, Nix pins, tag/release/push, and production are unchanged.

Twelfth slice: `responsive-stage-035` (isolated regression runtimes), `responsive-stage-036` (actual engine root recovery/settlement, bounded dependency-safe transactions, scope/mutation/lifecycle guards, and integration tests). Local commits; the new path is activated in source when both server capabilities are present. Version `1.11.4`, Nix pins, tag/release/push, and production are unchanged.

Thirteenth slice: `responsive-stage-038` (immutable dirty captures, bounded claims, active lazy readiness, reusable cooperative graph planner, and work-count/regression fixtures). Local commit; engine cache activation is still ahead. Version, Nix pins, tag/release/push, and production are unchanged.

Fourteenth slice: `responsive-stage-040` (activated reviewed queue, compact hashful planner, bounded live overlay/urgent roots, source retry/deletion guards, fresh dispatch fence, and regression fixtures). Local commit; version `1.11.4`, Nix pins, tag/release/push, and production are unchanged. Native/device qualification and the remaining roadmap are not declared complete.

Fifteenth slice: `responsive-stage-042` (explicit listener bind for the dedicated loopback fixture), `responsive-stage-043` (native root benchmark/cold readback), `responsive-stage-044` (bounded concurrent metadata/source reads), `responsive-stage-045` (drift-safe urgent continuation), `responsive-stage-046` (versioned direct native drift diagnostics). Version `1.11.4`, Nix pins, and running clients are unchanged.

Sixteenth slice: `responsive-stage-048`
(complete transient-memory admission for root outcome HTTP, lazy canonical
encoding, bounded capability/server-eph responses, and exact recovery sizing).
Local commit; version `1.11.4`, Nix pins, tag/release/push, running clients,
and production are unchanged.

Seventeenth slice: `responsive-stage-050`
(cooperative deferred partition, bounded dependency closure, and preparation
revision fence). Eighteenth slice:
`responsive-stage-052` (cooperative admitted dependency
snapshot capture and actual-engine fence regression). Local commits; version
`1.11.4`, Nix pins, tag/release/push, and production are unchanged.

Nineteenth slice: `responsive-stage-054`
(stable cooperative priority sorting, bounded indexed random copy, and
preparation/actual-engine regressions). Local commit; version `1.11.4`, Nix
pins, tag/release/push, and production are unchanged.

## Implemented in the first slice (P0)

- PR01, client diagnostics: active operations, open phases and their durations, and live scan counters. Scheduler wait, WS connect, credit wait, send queue, socket buffer wait, and ACK wait are shown separately.
- Responses and bytes from a reused WS connection are attributed to the current RPC, not to the already completed operation that opened the socket.
- Closing a socket immediately closes its diagnostic buffer wait. A late decrypt of an old HELLO_ACK cannot install limits in a new WS session; both cases are covered by regression tests.
- Lag samples from a hidden window and intervals crossing known visibility transitions are excluded from foreground UI lag estimates. Actual terminal lag in a visible window remains measurable.
- PR01, server diagnostics: `ws_data.accepted/completed/errors`, `inflight_rpcs`, `responses_sending`. Inflight remains until local response sending completes; cancellation releases gauges. A local flush is not described as confirmation of client receipt.
- PR01, CI: client regression tests run in CI and release preflight. A bundler or child-test failure is no longer masked by a shell pipeline.
- PR02: metadata regular file, confirmed absence, directory, and IO error are distinct. Incomplete enumeration of `.obsidian` after an error does not become an authoritative snapshot for file deletion.
- Old journal hints for untracked folders are ignored. An actual tracked file → directory transition passes through existing guards as deletion of the tracked file. Folder delete events do not create file changes.
- Partial-download read errors do not turn it into an empty prefix; folder deletion/creation errors are not swallowed without checking the result.
- PR03: bounded FIFO scheduler with `setImmediate` on supported desktop, `MessageChannel`, and timer fallback. One native callback, bounded pending queue, cancellation/disposal, and diagnostics for the selected backend.
- Scan, push, chunking, and renderer-fallback hashing use task boundaries; hashing preserves hash verification and cleanup. Whole-source-file reading on mobile remains the existing limitation for now.
- Cancellation before root publication stops new publication. If the server has already accepted an in-flight root, the result is persisted locally rather than declared nonexistent.

This is the code and local verification for the first stage, **not complete closure of P0/P1**. The plan's real-device gates remain mandatory.

## Second slice: PR04–06

- PR04: worker startup no longer depends on a global `process`; capability checks use the available Node bridge. Debug output shows fixed path-free rejection reasons without internal error text.
- Constructor/init/protocol errors do not trigger recursive restart attempts. A runtime crash/exit permits up to three restarts per slot over the pool's lifetime; changing governor limits does not reset this bound.
- Worker replacement waits for confirmed termination of the old worker. Failed termination or timeout disables the slot; the queue does not continue waiting for an absent executor. A timeout does not mean the native heap is guaranteed to have been released.
- PR05 foundation: `ResourceBudget` with reserve-before-allocation, a bounded FIFO queue, byte/count limits, AbortSignal, and an explicit lease lifetime. Capacity reduction/close/abort does not forget already-issued reservations.
- `hashFileStreaming` now reserves the workset before reading. The whole-file fallback estimate is `3 × source + 2 × maxFeed + 64 KiB`; for a native stream it is `6 × feed + 64 KiB`. The reservation lives until reading, hashing/cleanup, and native stream closure complete.
- A preliminary admission ceiling was selected for this subsystem: mobile/unknown 32 MiB, desktop 128 MiB, additionally bounded by the current policy target. This estimates concurrently live buffers, **not RSS and not a guarantee against Jetsam**. It does not yet cover push payloads, crypto, worker heaps, or apply.
- Whole-file fallback checks stat before admission and detects source/backing-buffer growth before hashing. Native allocation caused by a file growing between stat/read cannot yet be prevented without a ranged API.
- PR06: the governor consumes short active-operation windows instead of repeatedly analyzing completed totals. Counters are deltas; overlapping operations do not multiply time/control signals. Hidden/crossing/unbounded intervals are not treated as healthy foreground work.
- Read/hash/network/apply are controlled independently. Network pressure reduces network; foreground UI lag reduces read/hash/apply. A network-limit change does not cancel a feed-size reduction previously selected by the hashing loop.
- A single-axis probe is implemented and tested under flat throughput: it requires measured healthy UI, real backlog, capability, fully accounted memory for the relevant stage, and headroom. Production currently supplies truthful partial coverage: automatic limit growth waits for complete memory accounting.
- Scan rereads read concurrency between groups. Push lazily selects byte/count batches after visibility wait/task yield and rereads read concurrency between groups; shrink/grow neither skips nor repeats files.

This slice **does not fully close PR04–06**: the mobile worker spike, complete memory admission across all stages, pending-without-blocking for oversized files, and actual device/performance gates remain ahead. This change does not implement a durable transfer plan, short root commits, or an adaptive transport router.

## Third slice: shared admission, transfer, and deferred files

- Hash source admission, scan workers, push, owned API downloads, and apply use one shared ledger rather than independent limits. The complete workset is reserved before source reading/copying; nested transport/hash/apply draws quota from the issued reservation without waiting for the same global budget again.
- Push retains the source through hash/check/upload ACK. A small unknown file is read once; the same bytes are used for upload. Backing-allocation size is accounted even when only a small view is passed. Metadata drift is checked before and after reading; this does not prevent initial native allocation during a growth race.
- Native ranged upload preserves existing FastCDC chunks, including 4 MiB. Under a reduced budget, the range queue shrinks to 4 MiB. Whole-file fallback after worker refusal receives separate admission if the old reservation does not cover its owner/work portions.
- Worker feed is bounded by the issued batch ceiling, and the number of concurrent job buffers is accounted. Worker `run()` may report an error before stream completion, but its memory remains occupied until confirmed native cleanup. Failed/timed-out termination prohibits renderer fallback; a later actual exit releases the reservation.
- Streaming FastCDC no longer accumulates a consumed prefix before growing the Vec: the payload window is fixed at 4 MiB and does not grow with feed size. Its memory comes from reusable work quota separately from the source. The feed bridge, hasher state, and ChunkRef metadata are not claimed as part of this fixed window.
- Index upload gained a query for exact byte length from WASM without copying the payload. JS copies are created after admission of a bounded group and released after its ACK instead of being collected for the entire vault. This does not bound internal tree traversal, candidate/store, or metadata arrays.
- `putObjects` reserves encode/crypto/request workspace. Re-encoding after 413 starts after release of the original pack; chunks → manifest ordering is preserved. Automatic WS selection and fallback to HTTP remain enabled.
- WS bounds the size and number of queued ciphertext frames before decrypt. A timeout does not release child quota for encrypt/decrypt that is still running; HTTP fallback waits for the old cryptographic work to finish. Initial native allocation of an incoming WebSocket/requestUrl body still precedes the JS size check.
- `getObjectsOwned` gives the caller an explicit release boundary: small-file bytes are retained through every completed/failed native apply and checkpoint; large chunks are retained through hash, append, and transfer checkpoint. Receive workset size accounts for recovery capacity, not only the platform label.
- Append is selected based on actual availability of adapter `appendBinary`. The old fallback reserves the full prefix/replacement before reading and checks size/mtime before/after the read. Admission refusal does not reset staging/checkpoint. The public adapter does not make the final external stat/write race atomic.
- An oversized source remains pending without a source read; independent notes are published. Deletions are held conservatively so a deferred rename destination cannot destroy the old path. ACK applies only to completed journal generations; old single-row rename records and newer edits retain linked dependencies through recovery.
- Retry of unchanged deferred work is suppressed for 60 seconds, but a new hint/metadata, a change in actual capacity/capability, or an explicit Sync now/Full Rescan permits an immediate attempt. Bulk review checks the full batch before this filtering. Approval is not cleared while deferred/new work remains.
- Debug output shows queued/deferred work and the required workset. Matching root hashes are no longer labeled as completed sync when changes are pending; startup no longer overwrites engine status with an unconditional check mark.

PR05 remains **partially implemented**. The ledger consists of estimates for managed concurrently live buffers, not RSS: tree metadata/traversal, retained WASM/worker heaps, host allocations, and reconcile/control/legacy paths not yet migrated are not fully covered. Automatic governor growth remains disabled. On mobile without a ranged source API, an oversized upload is truthfully deferred rather than reported as synchronized successfully.

## Fourth slice: reconcile, journal recovery, and mobile spike

- Reconcile transfers missing index payloads through bounded pre-copy admission. Small/large repair reserves the complete workset before reading, retains native ownership until IO/worker cleanup completes, and uses shared transport quota without nested waiting on the global budget.
- Repair compares the actual content hash with the already committed hash. Source drift, an oversized workset, read errors, and upload errors remain incomplete objects; independent content repair and normal pending notes may continue. Missing-hash responses are checked for request membership and deduplicated.
- Debug output, idle status, and the Sync Now result no longer declare completed sync when repair is incomplete, changes are pending/deferred, or safety guards have stopped work. A repair-check error is distinguished from an honest result with zero missing objects.
- PR07, first part: strict journal replay, monotonic ID after empty compaction/reset, a sealed checksum checkpoint, verifiable `.next`/`.bak` generations, and prohibition of new append after an ambiguous write error. An IO error does not become an empty journal; corrupt complete/middle rows and unknown schema are not skipped. Details and boundaries are in the [journal ADR](journal-checkpoint-adr.md).
- A new rename is recorded as one existing `renamed` row with a shared generation for two path hints. The callback captures the destination before an await because Obsidian may mutate the same TFile object during the next rename. An append error preserves both session hints but does not fabricate a durable ACK.
- Rename links account for generation and append operations that have not yet completed. An old detached push does not remove new links even if the paths match. Retirement runs separately, only after journal ACK of both covered endpoints; a conflict-copy/base-save/ACK error preserves links for retry. An independent in-memory replay reproduces the original loss of the old-path deletion and confirms its elimination, including pending registration and restart.
- The browser/mobile spike is implemented as a separate opt-in command and settings button. One synthetic Blob worker verifies actual scalar/SIMD hashes and transfer of buffer ownership; there is no vault IO, resource URL, or network. The deadline includes the admission queue; hidden/pagehide/unload cancels the probe, and indeterminate completion prohibits retry in the same session. Production browser workers are **not enabled**. The contract and device matrix are in [mobile capability spike](mobile-capability-spike.md).
- IO/desktop worker selection depends on app-host flags, not mobile/desktop UI. An unknown host receives a conservative policy; an unknown architecture is not declared ARM64 solely because of mobile policy.
- CI receives a separate fresh scalar/SIMD build → parity → actual browser-worker bundle tests → production bundle gate. Release preflight also verifies the browser bundle; the fast pure-source suite does not depend on generated WASM.

This is **not closure of PR04/05/07**. The journal is still whole-file, without segments or a store epoch; sync-base/checkpoint queues still require equivalent hardening. A historical binary cannot protect the new checkpoint metadata: downgrade is not supported by this slice. Explicit group identity after coalescing is required before moving to short root transactions. Browser termination does not provide a native heap/GC ACK, and the small probe does not measure sustained mobile UI/RSS. Governor growth remains disabled for incompletely accounted stages.

## Fifth slice: segmented sync-base and local recovery queues

- PR07: sync-base uses immutable WAL segments and snapshot pages with epoch, sequence, and a verifiable publication head. The cursor belongs to the same confirmed batch as the preceding entry mutations. Replay does not skip a corrupt row to apply a newer cursor; unavailable storage does not become an empty base.
- Pages are bounded to 128 KiB payload / 256 rows, outer frames to 512 KiB, and the head to 128 KiB. There is a task yield between pages. The descriptor bounds the number of snapshot pages/replay segments; checkpoint starts compaction after 128 segments. These are bounded new IO frames, not streaming legacy import or a complete RSS/admission budget.
- The live index is now an immutable AVL with O(log n) path-copy updates and O(1) root capture. Snapshot reads a stable version without copying the entire entry map or mixing in mutations that arrive during an await. An O(vault) index and an as-yet-unbounded queue of authoritative mutations remain in memory.
- Publication verifies readback of every segment and only then publishes the head through `.next`/`.bak`. Recovery verifies the entire selected cut before exposing the base. A lone newest stage is not overwritten during recovery; an ambiguous error prohibits further mutations until validated reload. Cached metadata is separated from mutable caller objects.
- GC after verified publication deletes only the bounded closure of the previous backup while preserving everything referenced by the current and actual backup heads. Unexpected backup replacement or a cleanup error does not turn successful publication into an error or permit unsafe deletion. Orphan discovery, durable cleanup retry, and a separate maintenance lane are still required; stalled cleanup may currently delay commit return.
- Legacy snapshot/WAL are imported only after strict validation, and the originals are preserved. Conflicting snapshots, a snapshot with a corrupt WAL, and unknown schema require recovery. A WAL-only torn final append permits a validated prefix. The legacy read cap is 8 MiB per file; large legacy stores and interruption of the very first migration require a separate procedure. Downgrade to historical main.js is unsupported. Details are in the [segmented sync-base ADR](segmented-sync-base-adr.md).
- The diagnostic checkpoint no longer accumulates one promise/write per progress message when the adapter stalls: it retains one native write and one replaceable pending progress item. Begin/fail/complete preserve FIFO lifecycle barriers; late progress does not resurrect a completed operation. This is best-effort diagnostics, not a durable TransferPlan.
- Local callbacks are protected by reference counts and latest-event tokens. An overlapping callback does not remove another callback's guard; a mutable TFile does not carry an old hash to a new path. Above 4 096 path guards, protection conservatively extends to all paths instead of forgetting events.
- Engine startup first attaches listeners and restores journal hints in batches of 256 while preserving newer live edits. Replay neither reads nor publishes contents; this engine's ping/pull follows it. Replay failure leaves capture active and blocks pull/push/scans/probes. The earlier `main.initSync` still waits for tree negotiation before creating the engine, so plugin startup is not yet fully independent of the network.
- A confirmed rename pull echo now retires only ACK-covered generation links, preserving newer and incomplete registrations. Deferred records are not removed. The debug count uses index size without materializing every path.

PR05/07/08 are **not closed**: the change journal is still whole-file; there is no durable TransferPlan, cross-store commit intent/outcome, or shared base/journal ACK cut. Native durability, mobile memory/UI, and safe downgrade require separate implementation and verification.

## Sixth slice: segmented journal, migration proof, and startup fences

- The journal now uses shared bounded immutable segments/pages. Concurrently pending append/ACK operations are combined into a verifiable batch; public IDs/index become visible after its complete publication cut, not after writing the first segment. Compaction/clear/load remain ordered barriers; IDs are preserved even with an empty base.
- The immutable journal index coalesces normal edits to one path through the latest generation. A rename retains independent group identity and two pending flags. ACK of the destination does not remove an unsent old-path delete; ACK of the source does not resurrect it after restart. Compaction no longer flattens groups. Partial ACK projects only the remaining endpoint; newer edits/groups are not lost.
- ACK uses the endpoint index and cooperative steps rather than scanning the entire journal. Replay retains a captured index iterator instead of a complete array of records; the pull guard receives indexed membership instead of a Set of every path. An unavailable guard does not permit overwriting unknown offline edits. Pending normal paths, the unresolved rename graph, and queued callers still require complete memory admission.
- The first journal/base migration persists source/metadata proof before creating the target directory. Exact retry after interrupted initial pages is permitted only with the same verified source and no advance fence. A durable advance fence appears before the first new commit/snapshot; loss of every head after new changes does not permit importing the old originals again.
- Original legacy files remain byte-identical. The fingerprint includes roles, absence/presence, and lossless chunked strings; crypto inputs are bounded. The legacy journal parser yields to the event loop every 256 rows, including empty ones, but the adapter read itself remains whole-file with an 8 MiB cap. Partial/unknown markers, a changed source, an old unmarked directory, and an oversized legacy store require recovery rather than an assumption of emptiness.
- Engine local preparation now idempotently attaches listeners/replay before awaiting tree negotiation that has already started. Capture runs while network/apply/scans remain closed; tree format is selected before activation. Repeated start does not duplicate registrations; stop is terminal.
- Init-generation/unload fences cover publication of WASM exports and governor/profile, engine/tree/worker ownership, and late errors. Old WASM compilation / HTTP requests are not physically claimed as cancelled, but their result cannot change the new runtime. Capture still starts after local WASM/cache rather than at the very beginning of plugin onload.

The detailed contract is in the [segmented journal ADR](segmented-journal-adr.md). PR07 remains incomplete with respect to full memory/native/rollback gates. PR08 TransferPlan, prepared content/object progress, and a joint base/journal commit outcome are not yet implemented; short root transactions and adaptive transport are not replaced by the new local WAL.

## Seventh slice: durable prepared manifests and verifiable reuse

- The first integrated PR08 preparation stage stores desktop manifests in a bounded segmented store: header, ordered chunks, and logical seal are published under one verified head. The final closure check occurs before recovery promotion; incomplete records do not become available. The public mutation ID appears after publication; ambiguous IO requires validated reload.
- Scope fixes the vault, endpoint, server/enrollment identity, negotiated tree version, and ignore/config/content policy. A digest is persisted without the bearer token. This is not a server storage epoch or proof that objects survived a server restore.
- Default ceilings: 128 records, 16 384 chunks, 8 MiB estimated retained metadata, 8 MiB estimated queued copies, and 32 queued requests. Capacity-based retention refusal neither discards the journal nor prohibits a fresh upload. This ledger does not cover every caller/result copy, page buffer, or native heap; there is no complete RSS/admission gate.
- On every reuse, the worker fully reads and hashes the current file even when size/mtime/fingerprint match and the server says that all objects are present. Only a matching content hash permits use of the manifest with a new verification fingerprint. A mismatch removes only the observed hint by CAS and preserves the new preparation through content checks/uploads. Verification read/hash time is accounted separately.
- Missing ranges from the stored manifest are additionally verified by a local hash before the send queue. Their ownership continues through hash, send, and native reader close. Pending/in-flight local edits are checked around preparation/range/candidate and before the root request. A root already accepted by the server is not declared cancelled because of a late abort.
- Push returns generation-specific cleanup tokens without removing hints before settlement. The engine first performs durable journal ACK, then retires covered hints in one CAS batch; retained rename peers are preserved. Cleanup failure after ACK does not resurrect confirmed dirty work; the next attempt first reloads plan storage.
- Plan load runs after local listeners/replay are installed while network publication/apply/scans remain closed. Scope is computed after selecting the actual tree format. Slow/failing initialization preserves capture and a diagnosable reason; retry does not duplicate listeners, and stop prevents late activation. Debug output shows only aggregate capacity/state.

The contract and remaining limitations are in the [prepared-transfer ADR](prepared-transfer-adr.md).
This is **not a complete TransferPlan**: scan cursor, durable object confirmations,
root transaction intent/outcome, and a joint base/journal cut remain ahead. Fresh
presence checks are mandatory; zero-rehash/zero-read resume is not claimed. This
slice does not replace small-file and mobile paths, and governor growth is not
enabled.

## Eighth slice: old-engine retirement and continuous capture handoff

- The old engine no longer hands shared base/journal to its successor based only on `stop()`/`isBusy`. Synchronous admission closes while registered operation owners live through the actual root outcome, base/cache save, journal ACK, prepared cleanup, and checkpoint completion. A server-accepted ACK is not discarded. Bounded scan groups await every sibling job even if one fails.
- `quiesceAndDrain` preserves capture while an old HTTP request and replacement preparation are running. Shared dirty/deferred/local-guard state is then transferred by reference without copying the vault. The transfer preserves not only durable journal rows but also failed-append session hints, uncertain rename links, and latest-event tokens. The incoming owner closes traversal of the old callback cut through A → B → C or unload.
- Partial listener-registration failure retains already-started callbacks of the failed replacement through the old bridge owner until actual completion. Aborted mobile echo classification remains dirty without losing its hint after append refusal. A stale startup helper now quiesces rather than terminally stopping capture needed by the replacement.
- Reload of shared persistence runs after the old callback cut, is itself an operation owner, and does not open the activation gate after failure. New edits continue to be captured during reload/replay. A late checkpoint begin after stop does not start a new transport preflight. A queued mobile visibility wait can be cancelled separately from actual IO without waiting for foreground to return.
- Worker `closeAndDrainNative` is separate from the previous bounded `close`: it waits for actual exit/terminate completion of every native owner, including retired/replaced/startup slots and a reentrant factory. Timeout/rejection is not treated as release. A failed constructor transfers the actual cleanup promise even when the factory result is `null`; the plugin retains it across init generations.
- After operation/native drain, the old packaged WASM tree is explicitly freed exactly once. Debug getters no longer call a retired wrapper; main clears the matching reference. A destructor error stops runtime reuse rather than initiating a second unproved free. This is not a promise of reduced linear memory/RSS.
- Synchronous unload registers actual retirement; scheduler/storage/memory services needed for settlement remain available until owners complete. A same-renderer registry keyed by the host vault object delays storage/logging of the new bundle until this cut. Settings are persisted through a serial native queue with one active and one coalesced latest pending detached snapshot; an old `saveData` cannot complete after a newer write.

Detailed contracts and limitations are in the [engine lifecycle ADR](engine-lifecycle-adr.md).
This eliminates a specific same-session/reload race, not a complete durable
TransferPlan or proof of real-device lifecycle/memory gates. This slice does
not convert capture while the plugin is disabled or volatile hints after a
failed append across process death into durable data.

## Ninth slice: server root outcomes and unified root/receipt publication

- PR08/10 server prerequisite: new sealed HTTP `root-commit`/`root-outcome` endpoints and capability `root-outcome-v1`. Root acceptance persists the latest receipt for the authenticated device/vault; application sequence is separate from the transport replay counter. An exact sequence/ID/request digest returns the prior result without repeating validation/merge/publication. Old numbers are not executed again; gaps and payload substitution are rejected.
- Current root and receipt are published by one native head replacement with file/directory barriers. The rich head contains scope/schema/checksum and preserves receipts across legacy/admin/rollback/tree-version changes. Before the first receipt, the record remains legacy 64-hex. Current read errors, corruption, and unknown schema no longer become an empty vault in API/admin; inventory distinguishes Empty from Read error.
- Encoded ceilings: head 2 MiB, receipt 64 KiB, 128 device streams, commit request 704 KiB, and decoded root 512 KiB. Caps are checked before root publication, while deterministic retention refusal occurs before consuming the one-time deletion bypass. Streams are not eviction-cleared for revoked/re-enrolled devices; a larger scope requires a separate workflow rather than resetting high-water marks.
- Validated receipt replay repeats the durability barrier after a possible ambiguous rename/dir-sync failure. Reusing root history synchronizes the same verified file descriptor and directory ancestry; readable bytes after failed sync are not considered sufficient proof. A typed validation refusal differs from native `EINVAL`: the latter does not receive a false deterministic 413.
- Random server incarnation changes on every startup, including restore through restart. An old persisted receipt remains available; absence of a result after an incarnation change does not permit blind republication. An accepted root is a historical result, not a promise that it matches the current root or that objects remain present after restore.

The contract, wire binding, and mandatory next client coordinator are in the
[root outcome ADR](root-outcome-adr.md). The client does not yet send the new
root protocol. Durable intent/terminal cancellation, a joint local base+journal
cut, short transactions, and transport outcome routing remain work for later
slices. This is not complete PR08/10 or a ready rollout of the new server head.

## Tenth slice: terminal cancellation and local root-intent settlement

- The server persists `cancelled` under the same vault lock and in the same head publication as acceptance. Cancelling the first request for an empty vault persists an explicit checksummed `root_hash: null`; a late commit of an already cancelled operation cannot change the root. Cancellation after acceptance returns the prior accepted result. Other streams, the current root, and the one-time deletion bypass are preserved; no extra root notification is emitted.
- The new commit/query/cancel endpoints require authenticated semantic POST specifically. `root-cancel-v1`, a strict 4 KiB request, and matching replay before the incarnation fence were added. The legacy root path does not change its wire format.
- The client codec reproduces the Rust BLAKE3 binding and verifies byte/structure limits, canonical base64, duplicate JSON keys, safe integers, and exact response identity. The additive API uses sealed HTTP, does not substitute the old operation identity during transport retry/rotation, and does not fall back to legacy PUT after an error. The future coordinator requires both capabilities; a cached incarnation does not authorize resending an unknown request.
- `RootIntentStore` persists one exact pending request, candidate publication, journal epoch/path cut, and separate terminal result; the number remains after retirement. Bounded header/root pieces/entry/cut/seal are published under one segmented head. The full digest is verified before writing and during recovery; foreign scope, an incomplete cut, and an advanced store with lost heads do not become a new empty stream.
- `commitRootPublication` atomically persists entries + candidate root + timestamp + applied marker. Schema-2 WAL/snapshot block old readers. Submission barriers preserve ordering between the previous save and new setters; even a late same-visible-root setter is not lost. Receipt replay does not overwrite a newer base. The new queue is bounded to four payloads / 1 MiB serialized metadata; the old ordinary-setter backlog is separately unbounded.
- `acknowledgeOwned` verifies the original validated journal epoch and already-issued generations inside the writer queue. An old exact ACK neither suppresses a new edit nor acknowledges a separate rename peer. Local settlement coalesces concurrent callers, performs base → exact ACK → retirement, and returns candidate/observed roots separately.
- This is not yet the new production sync path: the engine does not call the intent store/coordinator. A receipt with conflicts remains `conflicts-pending` without writes/ACK; cancelled does not change base/journal. The next mandatory parts are verifiable repeatable conflict copies, startup/outcome recovery, tree/cache repair, and short transactions. Adapter completion and Node fault tests are not claimed as fsync/mobile/power-loss guarantees. The contract is in the [root outcome ADR](root-outcome-adr.md).

## Eleventh slice: outcome recovery and exact conflict copies

- `RootRecoveryCoordinator` preserves one original request and resolves a lost response through lookup/conditional terminal cancellation. Only a newly prepared intent within the current call permits initial submission; an existing intent is not considered "definitely not submitted." Server restart does not change the original digest, and an expired/gapped stream does not become a new local sequence. Credentials are checked before every new request; negotiated limits and both capabilities are mandatory.
- A received terminal outcome is persisted until local settlement. Close starts no subsequent network request but awaits the actually started request and the receipt/base/ACK persistence tail. A new owner does not receive storage before this drain; callbacks/reentrant close are tested separately.
- The losing version is obtained only by hash/size from the accepted intent, not from the current local file. A small blob or bounded manifest/chunks is fully verified; publication requires a fresh whole-file hash. Source feeds are ≤64 KiB, one owned object per request, and the sink uses child quota until actual IO completion. Large staging requires native append without prefix-copy fallback.
- A deterministic sibling copy is bound to the complete operation identity/path/hash. Publication uses SDK `DataAdapter.copy`, which must refuse an occupied destination; there is no overwriting fallback. Ambiguous completion may be accepted only after full-content verification. Desktop excludes symlink/hardlink aliases; on mobile, a large existing destination without a qualified ranged reader remains pending. After waiting for source budget, a fresh stat filters already observable growth before the whole read.
- A separate strict `conflict-copy` receipt is persisted in the root-intent store after the copy completes. It is bound to the exact accepted conflict and candidate upsert; every required receipt is mandatory for retirement. An already recorded copy is not recreated after a later user edit/deletion. A newer journal generation of the source file is not ACKed by an old copy.
- Fresh staging does not trust an old `.part` even if size matches. Old ambiguous attempts are preserved; after four files per conflict, new writes/downloads stop until explicit recovery/cleanup. Only a fresh owned stage is deleted after its receipt is persisted. There is not yet a complete aggregate staging quota/maintenance process.

These coordinators and the conflict path are **not yet connected to the
production engine**. SDK-level copy completion does not imply atomic visibility,
fsync, mobile/native RSS, or protection against an external writer during a
native copy. The portable adapter provides no inode/link evidence; native
conformance remains mandatory. The next slice must replace split saves/full-
queue ACK in the actual push and restore tree/cache/dirty hints before the first
startup pull, rather than merely replacing `putRoot`. The contracts are in the
[root outcome ADR](root-outcome-adr.md).

## Twelfth slice: engine activation and short verifiable roots

- `RootSyncRuntime` is connected from the actual `main.ts`/engine: local capture and journal replay occur before intent loading and outcome recovery. Startup, pull/push, Full Rescan, metadata audit, and content repair do not bypass an unresolved outcome. New submission requires both capabilities and an exact stream high-water; negotiation failure does not enable legacy fallback. A transient initial storage-load failure permits a new validated retry and is not cached forever.
- Root-stream identity fixes endpoint, long-term server pin, vault/device, and API owner, but not ignores/tree version/incarnation/transport cache. Bearer replacement preserves the digest and prevents dispatch by the old client. Scope/policy are captured before asynchronous WASM/cache work; changing vault ID during initialization no longer creates an engine for the new vault with the old API. Ordinary operations also verify scope. An already accepted native response completes its durable tail even if settings change.
- In the actual `push`, the durable branch persists the intent before submission and uses receipt → one atomic base publication → verified conflict copies → exact epoch-bound journal ACK → retirement. The old separate base/root/timestamp saves and full-queue ACK are not called on this branch. ACK cuts include only entries actually published from the original detached snapshot. Candidate C, historical merged M, and current base are not mixed.
- The planner selects ≤256 paths/cuts under a conservative metadata ceiling after full materialization and shared bulk/deletion review. Explicit rename links and a shared journal generation form indivisible components. An unknown peer, uncertain/stale registration, or oversized component remains pending; independent components continue. A late oversized source first holds the existing shared delete set, then the full selected dependency group before candidate mutation.
- `DirtyPathSet` distinguishes its own durable generation from a watermark transferred onto a newer no-ID edit. Bounded capture/commit retirement uses identity of the original hint and does not remove the edit that replaced it. Validated journal replay introduces provenance separately. A same-renderer error after recorded retirement and handoff uses a bounded attempted cut; only path absence in the same validated epoch plus own provenance permits clearing the old hint.
- An explicitly excluded standalone path and a confirmed old untracked-directory hint may complete through a separate bounded local ACK without server/root/base mutation. This requires complete successful materialization/review, the original epoch/generation, and absence of any dependency/shared-ID peer. An IO error, including unhandled EISDIR, does not become an omission. A new no-ID edit is not removed by an old ID. Mixed excluded rename and unproved volatile omissions remain pending; no artificial remote deletion is created solely to empty a counter.
- Recovery performs a full rebuild of the derived graph from the CURRENT validated immutable base cut. A root-only load without child chunks is not used; an honest persisted parent remains separate from the rebuilt semantic hash. Two passes yield every 256 entries; cap 65 536 entries / 8 MiB JSON, controlled input reservation `6 × JSON + 128 KiB` without retaining the first scratch lease during the large reserve. The final native rebuild remains synchronous, and the native graph/retained WASM heap is not fully accounted; oversize/drift does not open the gate.
- One tracked iterative push drains the ready queue in short transactions, including manual Sync Now with auto-sync disabled. Successful batches are separated by a real task yield and mobile visibility gate, without new three-second debounce pauses. No progress means no continuous retry loop. Materialization yields every 256 paths, including excluded paths. Stop/re-init awaits actually started root/base/journal operations; the automatic callback handles a rejected recovery/continuation promise.
- Metadata audit now holds normal mutation exclusion through hash/refresh and releases it before its own push. The live callback continues journal capture, but a new push/pull/scan/recovery does not overlap metadata base mutation. After scan, edits whose debounce fired while scan was busy are also submitted.
- Debug output shows pending intent/derived repair and fixed-shape last-batch holds without paths/credentials. Settings do not declare "in sync" from matching old roots when queued/busy/root-recovery work exists.
- The regression runner uses a static import manifest and a separate sequential child runtime for every declared suite; every bundle must build successfully before the first child starts. This eliminates a proven global `maxBatchFiles` conflict between pull and push fixtures without weakening bulk assertions. The optional focused/benchmark entrypoint remains. The runner does not claim that a bare pending Promise without a completion contract is detectable.

PR05/07/08/09/10 are **not fully closed**. Full re-stat/sort/planner of the remaining backlog still repeats between roots: a short commit is not yet a complete pipeline prioritizing the open editor. Bounded incremental planning, durable scan/object progress, storage epoch/leases, adaptive transport, admission for backlog/native heaps, and native-device qualification are still required. Legacy fallback under explicit incompatibility does not receive the new durable-root/conflict guarantees. This is a local implementation, not a release/deploy or proven native-throughput increase.

## Thirteenth slice: lazy dirty queue and incremental-planning foundation

- `DirtyPathSet` uses immutable path/order indexes: O(1) capture, lazy detached metadata rows, and bounded all-or-none `claim` of up to 256 paths. Exact internal identity, rather than matching ID/fingerprint, preserves newer events; claim/restore does not turn an inherited no-ID watermark into its own durable generation. Sequential insertion priority is preserved. A claim is neither review nor ACK.
- `hasRunnablePendingChanges` no longer performs a full `take/restore` between roots. The read-only iterator stops at the first runnable hint; an empty queue does not become ready even under force. A fully cooled cut is still traversed to the end for now: the cached global readiness/deletion aggregate is not yet connected.
- `createRootBatchPlan` cooperatively builds one graph for an already reviewed immutable cut; repeated `next()` selects ≤256 paths without recreating the complete retained/unselected tail. Shared validation/group/hold rules are also used by the previous synchronous selector, which retains greedy packing. The new planner uses a strict component prefix: the next eligible group that does not fit starts the next root, while oversized/missing/stale groups are classified once. Selection verifies scalar witnesses for the entire group before advancing the cursor; dispose releases plan references without ACK or dirty-state mutation. This is a prerequisite, **not a connected engine review cache**.
- The planner has preflight and a repeated charge check before retaining metadata, bounded count/string units, and an actual-host cooperation port; an ordinary step no longer creates a separate resolved Promise. The experimental ceiling is 65 536 paths/nodes/dependencies and a 16 MiB charge model that the caller may only lower. This is not a measured JS heap/RSS bound or a shared `ResourceBudget` lease. The hashless 25k fixture fits the model at 14 658 752 bytes; a hashful full-scan corpus of the same size is honestly rejected. Activation requires a compact/paged index or qualified profile budget, without bypassing admission.
- A separate work-count diagnostic runs the actual engine with a synthetic JSON tree/server and real local-store classes over `MemorySegmentedIO`. It verifies actual stat/materialize/planner/dirty-queue calls, exact final roots/base/journal, and completed host yields; it is not a native throughput gate. The measured 600/2 500 paths produce 1 032/13 480 repeated stat/planner inputs. The expected 1 233 232 visits for 25k is a calculation for the current algorithm, not an executed 25k engine benchmark.
- The [incremental root-planning contract](responsive-root-planning.md) defines the following integration gates: a complete review epoch, bounded exact claims, a new tracked missing state before deletion audit, global hold-all-deletes for a cooling source, dependency/base/policy invalidation, and a separate urgent/bulk service window. One shared revision per keystroke is not considered a solution to repeated traversals.

The main repeated materialization/sort/partition on every root still remains.
Persistent indexes retain O(N) live metadata and caller-held historical roots;
the current full restore recreates them with O(log N) updates. Without the next
reviewed-cut integration, this is **not elimination of O(N²)** and not a proven
native speed/RSS gain. Version, Nix pins, working vaults, and production are
unchanged.

## Fourteenth slice: one reviewed plan and separate urgent roots

- `RootReviewedQueue` is activated in the durable engine for one serial drain. The first cooperative typed-stat audit covers the entire captured queue, including cooled sources/deletions and positive omissions. The engine then claims only the selected ≤256 hints and rechecks their current type/metadata; the unselected remainder is no longer detached, restored, and sorted before each root. Exact WAL/base/outcome settlement remains with the existing durable coordinators.
- `DirtyPathSet.captureTracking` installs the initial cut together with a bounded observer before the first await. New hints coalesce across ≤1024 paths; an equal-looking add remains a new event, while trusted unchanged restore/claim/ACK does not. Overflow/observer replacement invalidates review while preserving authoritative dirty/WAL. `claimHints` verifies owner-local provenance and current entry identity without retaining a separate historical AVL capture for each edit; an inherited watermark does not become its own durable generation.
- Independent live upserts update the ledger and complete review counts through a bounded overlay. Urgent-first and bulk-first roots alternate; an urgent root contains up to 64 paths with the open note first, while bulk preserves components of up to 256 paths. A new tracked missing state, deletion, linked generation, structural registration, or policy/retry/base change rebuilds the review. Fully incremental component repair does not yet exist; frequent renames may again cause full audits.
- The global cooling set holds deletes even when the source is outside the selected root. After accepted removal of the last cooling source, the previously withheld delete graph must be rebuilt. Explicit local omissions receive a bounded owned ACK; a new no-ID edit remains pending, while independent actual changes may continue the same pass without a no-progress retry loop.
- Each selected component is detached by an atomic bounded claim. Late dirty notifications are reread before declaring exhaustion, with a finite refresh ceiling; a continuous event stream does not become a hot infinite loop. Classification requires exactly one materialized/explicit-omission result per captured path. An empty result, raw IO error, and unknown path do not authorize ACK.
- Review is bound to a validated journal epoch, immutable base witness, API scope, and policy/retry key. Only its own accepted successor updates the base witness; the dependency witness after its own ACK is updated synchronously before the next await. The fresh-send guard additionally verifies review and selected pending/in-flight generations after asynchronous intent hash/persistence/final negotiation, immediately before the initial commit API call. Failure preserves the intent for query/cancel recovery; an accepted native tail is not interrupted by a late settings change or stop.
- The compact graph planner shares matching queued/ready witnesses and canonical strings and discards union-find/duplicate membership arrays after build. The previous 16 MiB cap was not raised: the matching hashful 25k fixture now passes with an experimental model of 15 058 752 bytes; hashless uses 10 358 752. The 25k differing-stat overrides may still not fit. A separate review ledger is bounded to 65 536 paths / an 8 MiB model; this is not yet a shared resource lease, measured JS heap, or native RSS.

Quiet actual-engine fixtures on 600/2 500 paths verify `2N` stat visits, one
graph, and N bounded claims: 1 200/5 000 instead of the previous 1 032/13 480.
For the small queue, the stat count **increases** because of separate selected
revalidation; the repeated full traversal of large tails was eliminated, not
proven to accelerate every workload. The JSON tree/server and in-memory adapter
are synthetic. The 25k actual packaged-WASM/native-FS throughput, editor/UI,
and device-memory gates have not yet run. The slice does not close
PR05/07/08/09/10 and is not a release.

## Fifteenth slice: real files, WASM, and sealed root server

- A [reproducible native root fixture](responsive-native-root-benchmark.md) was added: actual engine/IO/base/journal/intents/AEAD and packaged SIMD WASM, with a separate release Rust server for each run. Only the Obsidian host shell is a test port; the production runtime is not rewritten for measurement. The native sync source and all index/content objects actually exist on the filesystem/server.
- The server CLI accepts `--bind-address`; default `0.0.0.0` and normal ports are preserved. The loopback fixture uses OS-assigned ports and actual bound addresses, its own enrollment, rejection of foreign origins/redirects, bounded response, and join before stopping the server owner.
- The native adapter verifies the private canonical fixture, regular files, links, typed errors, and actual native completions. Counters distinguish vault sources/client state/mixed, logical bytes, and additional validation syscalls. The Linux child owner manages only the detached process group it created; timeout/signal/a surviving descendant are not considered success. When absence of a running owner cannot be confirmed, fixtures are preserved and an error is returned.
- Instrumentation is bounded by method/histogram/plan count and preserves actual Promise/result/error identity. Full/selected review, claims, graph work, completed yields, synchronous WASM stacks, filesystem waits, root/base/WAL/API latency, wire body bytes, sampled Node memory, and WASM capacity are separate. Peak shared transient pool explicitly includes seed rather than being attributed only to the measured phase.
- `quiet` is 25k unique 4 KiB known-hash notes; `edits` additionally performs 70 actual create callbacks and three superseding generations after the first commit API admission. Exact accepted generation is identified by epoch/ID/hash; a coalesced successor is not described as publication of intermediate text. An explicit second drain after writer completion is included in timing and is not presented as automatic continuation by the production scheduler.
- Seed/start/source preparation are excluded from drain timing. After an actual paged server snapshot, a new process loads base/WAL/intents, verifies every source hash/stat, and rebuilds the complete WASM graph. Exact final root/sequence and an empty backlog are required, not merely matching old roots.
- This is a Linux/Node `/tmp` integration baseline, **not** Obsidian IPC/Windows 9p/mobile host, full-scan/worker/large-file throughput, WS routing, CRDT, input-to-paint, fsync/power-loss, or release readiness. Native callbacks during the first full audit and `autoSync=true` retrigger after terminal generation drift in actual Obsidian remain separate required scenarios; in-owner retained continuation is already measured directly below.

## Sixteenth slice: complete memory scope for root-outcome HTTP

- `commitRootOutcome`, `queryRootOutcome`, and `cancelRootOutcome` synchronously
  capture and validate caller-owned primitives, then await the shared budget.
  The canonical encoder is called only inside the issued complete scope; a
  queued caller mutation cannot change the body or expected identity.
- For a large commit, exact encoded length is computed from a small canonical
  header and an already verified ASCII base64 length without materializing the
  complete JSON. Recovery uses the same helper instead of encoding the entire
  root solely for `.byteLength`. After admission, the actual length must match
  exactly; query/cancel are checked against a strict 4 KiB protocol maximum.
- One scope and one child transport quota are retained through initial channel
  refresh, tree negotiation, lazy encode, replay/stale retries, actual native
  response, decrypt, `arrayBuffer`, duplicate-key parse, and terminal
  validation. `sealed` does not receive an automatic global reserve, so already
  admitted content paths do not acquire a nested self-deadlock. The scope
  closes before conflict/base/journal settlement.
- Tree capability and bootstrap `/server-eph` receive a 64 KiB plaintext cap;
  ciphertext wire above that cap plus a 31 B envelope is rejected before
  decrypt/copy. The root-response cap remains 64 KiB + 512 B. The owner model is
  `3B + 6R`, and the transport child is
  `estimateTransportWorkset(max(B, R))`; the complete protocol-cap workset at
  `B = 704 KiB` is **7 343 104 bytes**, below the 32 MiB mobile admission
  ceiling.
- This is controlled byte-workspace accounting. The original persisted
  root/base64 string, parsed JS object graph, fixed crypto/WASM heaps, initial
  native receive allocation by `requestUrl`, and process RSS receive no false
  guarantee from this number. A separate recovery
  `negotiateRootOutcomes(true)` runs before these commit/query/cancel operations
  and does not receive their scope. No new transport benchmark or real-device
  memory plateau is claimed.

## First-slice verification

Local Node: 22.23.2. The workflow uses Node 20; its separate run has not yet been performed.

- Full plugin regression suite, including 4 checks of the runner itself, new filesystem/scheduler/telemetry cases, and 147 push-transaction assertions.
- TypeScript `tsc --noEmit`.
- Production bundle built; scalar/SIMD WASM parity confirmed across five feed sizes and a tree fixture of 2 048 entries. The bundler emits `import.meta` warnings in generated WASM glue; that code was not changed, and loading parameters are passed explicitly. Verification in actual Obsidian is still required.
- `sync-server`: 222 passed, 1 previously existing ignored benchmark. Cancellation guards and WS data tests are among the checks.
- Rust formatting and whitespace-diff check.
- The overhead benchmark was extended with actual lifecycle `phase()`, per-batch progress, and periodic snapshots/debug formatting. The existing 2% threshold was not changed.

Clean comparative Node run: 8 192 × 64 KiB per run, 7 paired samples in two opposite orders; identical copy/hash in both branches. In the live scenario, batch = 4 files, snapshot every 1 024, and debug every 4 096 files.

| Scenario | Baseline median | Instrumented median | Median paired overhead |
| --- | --- | --- | --- |
| Sampled phases | 687.3036 ms | 692.0413 ms | 0.6893% |
| Live batches | 668.7554 ms | 673.1805 ms | 0.7371% |

Paired overhead samples, %:

```text
sampled: [1.7850, -0.1695, -0.3477, 0.8241, 0.6128, 0.6893, 1.6206]
live:    [1.0011, 1.0440, 0.3843, 0.7371, 1.3052, 0.6617, 0.6576]
```

This is a CPU-only Node benchmark with lag timers disabled. It does not measure mobile memory, input-to-paint, disk, network, or the absolute speed of actual sync. Parallel phases may overlap; their sums do not equal wall time.

## Second-slice verification

- Full plugin suite: including 142 worker assertions, 82 allocator assertions, 47 source-budget assertions, and 231 push-transaction assertions. It verifies an actual Node stream, closure after errors/cancellation, lifetime of independent parallel reservations, and dynamic batches/read groups.
- Deterministic integration of actual `PerfTrace → observeWindow`: response before scan completion, counter deltas, overlap/lifecycle filtering, and prohibition of probes under partial memory coverage.
- Callback ordering after a long pause is verified: an old lag probe that runs after the window callback does not contaminate the new foreground window. Actual new foreground lag remains visible.
- TypeScript, production build, and whitespace checks passed. Scalar/SIMD parity: five feed sizes, payload 10 485 883 bytes, tree fixture 2 048 entries. Generated WASM glue warnings during build remain unchanged.
- Server code did not change in this slice; a new server-suite run is not claimed here. The first-slice result is listed above.

The CPU overhead benchmark was extended with a third scenario: live batches, demand updates, sampling windows, and controller/snapshot calls. Windows are invoked manually every 2 048 files; actual lag timers are disabled, and unconfirmed memory coverage does not permit probes. This measures instrumentation/control plumbing cost, not dynamic-ramp effectiveness. The latest full run on the current code, with the unchanged median paired overhead threshold `< 2%`:

| Scenario | Baseline median | Instrumented median | Median paired overhead |
| --- | --- | --- | --- |
| Sampled phases | 604.5225 ms | 605.3287 ms | 0.1334% |
| Live batches | 615.7411 ms | 620.3314 ms | 0.7455% |
| Active-window control | 622.4471 ms | 630.3338 ms | 1.3002% |

Paired samples for the new scenario, %: `[1.6338, 1.4751, 1.0417, 1.4032, 1.2671, 1.2325, 1.3002]`. The preceding full run before the final timer-ordering regression also passed: the new scenario was 1.2512%. These values do not mean sync became faster by the corresponding percentage.

A separate `bench:governor` passed on Linux x64/Node 22.23.2: production SIMD workers, 32 × 8 MiB, warm cache, three alternating runs of each fixed profile. Median with 2 workers: 1 634.8562 MiB/s; with 4 workers: 3 288.8278 MiB/s; ratio 2.0117; maximum p95 lag across runs 0.9991 ms. The throughput ≥ 95% conservative and lag ≤ 16 ms gates passed, with actual lag samples present. The report now explicitly describes this as a comparison of **fixed startup profiles**, not a run of the adaptive-window controller. It does not measure an actual network, Obsidian UI, or the complete vault; this run did not overwrite old-release evidence files.

## Third-slice verification

- The final full plugin suite passed: 5 runner checks, 196 worker assertions, 255 push-transaction assertions, 56 WS assertions, 8 actual-API scenarios with a local fake transport, 47 shared-scope assertions, 39 bounded-append assertions, and 37 large-transfer assertions. Pure admission/index/deferred tests, journal recovery, and all previously existing suites also passed. API test transport without an explicitly installed fake fails instead of accessing the network.
- Fault injection verifies parent **and** child quota during late native cleanup after timeout; a failed parallel source read does not release a running sibling. Tests cover stopping before root publication, ACK-loss/resume, same-length prefix drift, oversized-fallback refusal without reread/write, and preservation of the linked rename WAL after partial ACK.
- `sync-core --features wasm`: 175 tests passed. `sync-server`: 222 passed, 1 previously existing ignored benchmark. TypeScript, production bundle, Rust formatting, and whitespace checks passed.
- Both WASM variants were rebuilt from current Rust. Parity: scalar 384 649 B, SIMD 387 764 B; payload 10 485 883 B, 5 hash feed sizes, 9 FastCDC feed sizes (including larger than the window and the whole payload), 2 048 tree entries, v1/v2, and candidate commit/abort/sweep. Hashes and protocol did not change.
- Local build obstacles were not masked: the default sandbox did not execute the child test runner, and the full suite passed with subprocess execution allowed; a stale SIMD cache was replaced with a new `/tmp` target; generated artifacts were copied without unsupported 9p `chmod`. No working vault was used. Generated glue still emits known `import.meta` warnings; the native Obsidian gate was not replaced by a Node check.
- A repeated instrumentation benchmark with the same 7 paired samples and unchanged gate `< 2%`: sampled 0.1029%, live batches 0.8349%, active-window control 1.3302%. Corresponding baseline/instrumented medians: 620.5054/621.3842 ms; 618.5833/624.3239 ms; 621.5889/629.8571 ms. This is CPU instrumentation cost, not measurement of the new end-to-end admission pipeline or mobile UI.
- Repeated fixed-startup-profile worker benchmark: median with 2 workers 1 648.6580 MiB/s; with 4 workers 3 584.5726 MiB/s; ratio 2.1742; worst p95 event-loop lag 1.0151 ms. Both existing gates passed. The corpus and Node/Linux environment are unchanged; the benchmark uses hash mode and does not measure FastCDC throughput, the adaptive controller, network, or actual Obsidian.

## Fourth-slice verification

- Full local plugin suite: 5 runner checks, 122 journal checkpoint/recovery assertions, 28 actual-engine rename assertions, 66 reconcile-admission assertions, 17 actual-engine repair status/mutex assertions, browser lifecycle/transfer/admission, and host/UI classification. Existing push/worker/transport/apply regressions pass together with the new tests.
- A separate 5 browser-bundle tests use actual scalar/SIMD WASM in development and minified IIFE through a Node worker bridge. They verify hashes, detachment in both directions, malformed input, and independent scalar refusal; this is not a test of an actual CSP/WebView.
- TypeScript and production build passed. Scalar/SIMD parity was reconfirmed: 5 hash feed sizes, 9 FastCDC feed sizes, 2 048 tree entries. Known generated-glue `import.meta` warnings remain; byte loading is explicit, and fetch/importScripts are prohibited by the packaging test.
- Workflow YAML and mandatory-step ordering were verified locally; remote CI and the native-device matrix were not run. Rust code did not change in this slice; Rust-suite results are listed in the third slice rather than claimed as a repeated run.
- No new end-to-end throughput/RSS/input-to-paint result is claimed. Working devices/vaults were not used for these checks.

## Fifth-slice verification

- The final full plugin regression suite passed: 5 runner tests, 350 storage restart boundaries, 324 authoritative IO rejections, 26 accepted maintenance failures, and 12 torn writes. Before verified publication, only a complete old/new cut or an explicit recovery error is permitted; afterward, an exact new cut is strictly required, including after IO failure and the next successful commit.
- Separate GC coverage: 121 normal publications, 12 restart boundaries, 4 actual-backup guard skips. Current and backup are independently replayable; shared snapshot/WAL closures are not deleted, and failed cleanup does not poison a confirmed writer. The first attempt at the combined run exposed an old requirement for rejection on post-publication maintenance; the harness was corrected to use actual rename + exact readback rather than weakened to accept any outcome.
- AVL was verified across 5 000 reference operations, rotations/deletions, and preservation of a captured iterator across an await. Sync-base retains 26 compatibility assertions and adds strict migration, poisoned IO, atomic cursor cut, concurrent snapshot, and mutable initialization-metadata regressions. Paged pull: 32 assertions, including cold restart through actual referenced frames and orphan rejection.
- Local event callbacks: 25 assertions; engine startup replay: 19; rename journal: 35. Tests cover overlapping append/hash/ACK, mutable TFile.path, bounded guard overflow, preservation of newer live edits, and refusal of scans/probes during incomplete replay. Existing push/worker/apply/transport and diagnostic-queue suites passed with them.
- TypeScript, production bundle, and whitespace checks passed. Separate browser-worker bundle tests: 5 passed; scalar/SIMD parity: 5 hash feed sizes, 9 FastCDC feed sizes, 2 048 tree entries. Known generated WASM glue warnings remain. Production bytes are passed explicitly; native Obsidian/WebView was not run.
- Rust and network protocol formats did not change in this slice; Rust suites were not rerun here. Remote CI, the real-device recovery/memory/UI matrix, and rollout/rollback gates are not marked complete.

New reproducible native-filesystem sanity benchmark: `cd plugin` →
`node scripts/bench-sync-base.mjs`. Three runs on Linux x64 / Node 22.23.2,
25 000 synthetic metadata entries, seeded in batches of 256. It uses the actual
`ObsetyncSyncBase` and a separate temporary directory, not a working vault. Cold
load occurs in a new process without the old index/JS heap; the filesystem cache
is not cleared.

| Operation | Three runs, ms | Median, ms |
| --- | --- | --- |
| Checkpoint | 376.611 / 372.912 / 373.879 | 373.879 |
| Snapshot save | 335.661 / 336.393 / 334.621 | 335.661 |
| Cold load | 260.914 / 255.884 / 265.296 | 260.914 |

Maximum individual read/write: 56 510 bytes; snapshot/load: 47 822 bytes.
On disk: 4 665 556 logical bytes / 4 812 800 allocated bytes, 101 files:
98 snapshot pages, one post-cut WAL segment, and two heads. All 25 000 hash
identifiers, cursor, root, separate treeMtime, and newer generation were
verified: sequence 25006 / snapshot cut 25003. Synthetic hashes do not require
content hashing.

Median raw Node event-loop p95 sampler values: 12.042 / 10.723 / 11.444 ms,
respectively, with a 10 ms sampling interval. This is **not renderer UI lag,
RSS, or input-to-paint**. There is no comparison with the old implementation,
fsync guarantee, or new performance gate. Result checks run outside the measured
phases. The run required permission for a child Node process because of sandbox
restrictions; its own `/tmp` fixtures were removed.

## Sixth-slice verification

- The final full plugin suite passed: 339 journal assertions, 10/10 actual-engine journal-semantics cases (38 assertions), 36 rename assertions, 11 hot-endpoint concurrency assertions, 45 startup-replay, 7 startup-activation, 31 local-event, and 19 reconcile-status assertions. The test specifically verifies a yield at the 256th ACK candidate out of 300: a late append survives full publication, compact, and restart, while the intermediate index does not become public.
- Shared store: 364 restart boundaries, 338 authoritative IO rejections, 26 accepted maintenance failures, 12 torn writes. New migration suite: 142 initialization/retry restart boundaries, 142 IO failures, 7 partial writes, 32 advance-fence failures. Async replay callbacks are checked before the public cut. Existing GC: 121 publications, 12 restart boundaries, and 4 backup guard skips.
- The journal is verified with 200 concurrent long-path submissions across several bounded segments under one head, failure before/after WAL and lost confirmation of an already accepted publication; strict legacy import, partial rename ACK, monotonic ID, poisoned queue, and explicit reload. Index: 10 000 edits to one path, 3 000 reference operations, and path membership without a full walk. Sync-base retains compatibility/atomic-cut tests and source-proven retry after a partially written initial page.
- The lossless legacy fingerprint is compared with an independent SHA-256 reference, including Unicode surrogate boundaries, roles/absence, mutable caller inputs, and cooperative decoding of blank lines. Unknown/corrupt markers and a changed source do not become permission to overwrite; original files remain unchanged.
- The full run also includes the previous push (255 assertions), workers (196), WS (56), owned API transport (8 scenarios), resource/admission/apply, and deferred-generation suites. Old actual-journal fake adapters were updated for immutable `write`/`rename`; WAL failure is still injected, including torn bytes, without weakening generation/ACK expectations.
- TypeScript, production build, whitespace checks, and separate 5 browser-worker bundle tests passed. Scalar/SIMD parity was reconfirmed: 384 649 / 387 764 B, payload 10 485 883 B, 5 hash feed sizes, 9 FastCDC feed sizes, 2 048 tree entries. Known generated-glue `import.meta` and Node module-type warnings remain. Rust/protocol code did not change, Rust suites were not rerun here, and remote CI, the native-device matrix, and rollout/rollback gates are incomplete.

New reproducible benchmark: `cd plugin` → `node scripts/bench-journal.mjs`.
Three sequential runs on Linux x64 / Node 22.23.2, the actual
`ObsetyncJournal`, and a native filesystem in a separate `/tmp` fixture. 25 000
distinct synthetic paths, followed by 10 000 edits to one path; submissions in
batches of 256. File contents are neither read nor sent over the network.

| Phase | Three runs, ms | Median, ms | Confirmed WAL heads |
| --- | --- | --- | --- |
| 25 000 distinct paths | 866.565 / 843.150 / 835.603 | 843.150 | 98 |
| 10 000 edits to one path | 677.208 / 652.108 / 681.967 | 677.208 | 40 |
| Explicit compact | 297.163 / 297.552 / 305.701 | 297.552 | 0 |
| Fresh-process load | 304.856 / 317.931 / 309.870 | 309.870 | 0 |

The second phase includes one automatic snapshot; explicit compact publishes one
more snapshot head. Median mutation-submission rates are 29 650.712/s and
14 766.518/s respectively, not file-sync throughput. Maximum individual
read/write: 42 943 B; 103 retained files, 3 861 440 logical / 4 022 272 allocated
bytes. Every returned ID and pending identity was verified; a separate linked
rename with a new destination edit and partial ACK survives compaction and a new
process. Subsequent untimed ACK/compact/reload verifies retirement and monotonic
ID.

Median raw Node event-loop p95 values are 13.001 / 12.272 / 11.805 / 12.444 ms
across the four phases, at a 10 ms interval. Cold load neither clears the
filesystem cache nor includes module startup; verification is outside timings,
while native IO counters are inside. This is not renderer UI lag, RSS,
fsync/power-loss proof, a comparison with the previous journal, or a real-device
performance gate. The first run in the default sandbox terminated its child Node
without a report; the benchmark refused to emit a result. After subprocess
execution was allowed, all three runs completed and their own fixtures were
removed.

After the journal benchmark, `node scripts/bench-sync-base.mjs` was repeated
sequentially on the current store with migration markers. Checkpoint: 393.724 /
390.872 / 397.107 ms (median 393.724); save: 336.794 / 341.700 / 339.194 ms
(339.194); fresh load: 271.789 / 269.016 / 272.748 ms (271.789). Maximum IO
remains 56 510 B; 103 files, 4 666 448 logical / 4 820 992 allocated bytes. All
25 000 identities, hashes, cursor/root/treeMtime, and the newer generation were
verified. Raw p95 medians: 12.378 / 10.920 / 12.337 ms; the same limitations of
the Node/native `/tmp` measurement apply. Post-compact journal append (median
30.522 ms) includes 112 cleanup remove calls, so it is not steady-state single-
append latency. Maintenance is currently awaited rather than placed in a
separate fully budgeted queue.

## Seventh-slice verification

- The final full plugin suite passed: prepared storage — 720 assertions, 52 rejected retain-publication boundaries, and 6 accepted maintenance faults; actual push/prepared integration — 344 assertions; ranged upload — 61; startup/replay — 57. Prepared helper — 7 groups and 10 invalidation/abort native-wait boundaries. Scope capture and generation-specific retirement are also included in the shared entrypoint.
- Generic segmented recovery passed again: 364 restart boundaries, 338 authoritative IO rejections, 26 accepted maintenance faults, and 12 torn writes. Migration: 142 initialization/retry boundaries, 142 IO rejections, 7 partial pages, and 32 advance-fence checks. Journal and existing push/worker/transport/admission/apply regressions are also green.
- Actual `engine.pushPending` with real journal/plan stores verifies failed journal ACK, accepted root/base/ACK with failed hint cleanup, and explicit plan reload before the next drain. Fake transport/host/worker ports are not described as native Obsidian proof. Native temporary source/range tests verify offline same-stat changed bytes, fresh fingerprint, wrong cached range hash, lost ACK, and same-key server reset; synthetic SHA256 test manifests do not replace an actual FastCDC benchmark.
- TypeScript `tsc --noEmit`, production build, 5 packaged browser-WASM bridge tests, and scalar/SIMD parity passed. WASM remains 384 649 / 387 764 B; parity payload 10 485 883 B, 5 hash feeds, 9 FastCDC feeds, and 2 048 tree entries. Known generated-glue `import.meta` and Node module-type warnings are not hidden. Rust did not change and was not retested; remote CI/device gates were not run.

### Native prepared-manifest sanity benchmark

`node scripts/bench-prepared-manifest.mjs` passed after the shared regression
gate, with no parallel heavy jobs. `--check` separately verifies only bundles
and does not run fixtures/timings. Linux x64 / Node 22.23.2, an actual desktop
worker, and packaged SIMD WASM; deterministic mixed 64 MiB source, three pairs
ordered fresh/reuse → reuse/fresh → fresh/reuse. Report:
`/tmp/obsetync-prepared-benchmark-report-GxxXZx/prepared-manifest.json`.

| Preparation | Median wall time | Actual source read |
| --- | --- | --- |
| Fresh worker manifest | 142.388 ms | 64 MiB |
| Prepared helper with complete verification hash | 63.004 ms | 64 MiB |
| Reuse + loading a new plan instance, actual total for each pair | 68.352 ms | 64 MiB |

Separate plan load / lookup medians: 4.643 / 0.046 ms. Worker-read medians:
31.098 / 29.900 ms; hash/FastCDC versus verification hash: 109.484 / 30.723 ms.
Both branches performed 256 actual completed `FileHandle.read` calls of 256 KiB;
this is not physical disk IO. All 33 chunk hashes/offsets/sizes and the file hash
matched exactly. Reuse does not repeat preparation/retention. The plan contains
one record: 5 files, 8 320 logical / 24 576 allocated bytes; replay — 6 reads,
8 728 B, maximum individual read 6 583 B, and no writes.

Initial source generation (252.775 ms), worker startup (55.702 ms), and bootstrap
retention (9.328 ms) were measured separately once and are not called medians.
The fresh comparison branch does not include plan persistence. The worker and
filesystem cache are warm; a new plan instance is not a new process/cold disk.
Native read instrumentation is identical in both branches and exists only in
the fixture worker bundle. Raw Node p95 median is 10.117 ms in both branches at
a 10 ms sampler interval, not input-to-paint/UI lag. There is no new speed gate.
This measures savings from repeated FastCDC/chunk-hash work, **not acceleration
of the entire sync or proof that iPhone/Obsidian/9p works**. Source/plan/generated
bundle fixtures were deleted; only the path-free JSON report was retained.
Production/vault were not used.

## Eighth-slice verification

- The separate mandatory `test-main-lifecycle.mjs` is included in `npm test`: actual bundled main/startup-activation/runtime-retirement/settings writer, 13 scenarios / 113 assertions. It verifies quiesce/native/free/handoff/reload/activation ordering, a third init generation, two independent bundle evaluations in one global, unload during onload, failed constructor, actual settings serialization, and tree-disposal failure. Host/engine/native/storage here are explicit synthetic ports; actual engine/base/journal are verified separately by the next suite. `--check` performs bundling only, truthfully reports 0 executed tests, and does not replace a CI run.
- The final full `npm test` passed after the actual-main harness was added to the mandatory entrypoint; separate TypeScript and production builds also exited 0. Actual engine/shared segmented base/journal control reproduces the prior server B / cold base A / empty journal divergence under unsafe stop without drain. The new path retains root, base WAL, journal ACK, and checkpoint gates until actual completion; a subsequent cold load returns B. The 59 lifecycle assertions include callbacks, third replacement, failed append/rename, registration failure, persistence reload/replay, and prohibition of debug calls on an already freed tree wrapper.
- Startup/replay — 65 assertions, including stale completion during preparation/negotiation without capture loss. Worker — 255 assertions: timeout/rejection and late native exit, retired/replaced slots, constructor/listener/factory reentrancy, and failed-construction lifetime. Engine work — 6 groups; runtime registry — 4; serial settings — 6 groups with 10 000 coalesced submissions and four combinations of native-write failures. Aborting one hidden visibility waiter does not unblock its sibling.
- Existing prepared storage 720, push/prepared 344, ranged upload 61, journal 339, segmented restart/migration/GC, and push/transport/apply/admission suites also pass. TypeScript, production build, 5 packaged browser-WASM bridge tests, and scalar/SIMD parity passed. WASM bytes remain 384 649 / 387 764; parity payload 10 485 883 B, 5 hash feeds, 9 FastCDC feeds, 2 048 tree entries. Known generated-glue/module-type warnings remain. Rust did not change and was not retested; remote CI/device gates were not run.

After terminal regression/build/browser/parity and separately from the main
harness, the fixed-startup-profile SIMD worker benchmark was repeated
(`npm run bench:governor`): Linux x64 / Node 22.23.2, 32 × 8 MiB, warm cache,
three alternating runs/profile. Median with 2 workers: 1 654.0186 MiB/s; with
4 workers: 3 264.2147 MiB/s; ratio 1.9735; worst p95 event-loop lag 1.0051 ms
with actual samples. Both unchanged gates (throughput ≥ 95% conservative,
lag ≤ 16 ms) passed. These are hash-mode fixed profiles, not the adaptive-window
controller, FastCDC, network, or Obsidian/iPhone UI. RSS samples in such a short
fixture are not a memory plateau/native-reload proof. Old release evidence files
were not overwritten (`--record` was not supplied); the benchmark itself removed
temporary source/runner/result fixtures.

## Ninth-slice verification

- Final `cargo test --workspace --locked --quiet`: sync-server 261 passed / 1 previous ignored benchmark, sync-core 174 passed (default features), sync-schema 5, perf-harness 8. The E2E crate without the feature runs 0 tests; compose/remote E2E is not claimed by this result.
- New actual sealed-router tests — 13: exact replay, changed exact bytes with the same semantic root, lower/gapped sequence, authenticated device/vault scope, new transport session, real AppState restart/incarnation, actual merge/conflicts/counts after the next root, concurrent duplicates with one notification, malformed fields/caps, and fail-closed corrupted/unknown head.
- The retention/bypass case passed separately with `OBSETYNC_GUARD=enforce`: 256 actual fixture deletions and a triggered blast-radius guard, 128 retained device receipts, and refusal 413 without changing head/root or losing the one-time grant. The global test environment was unchanged: enforcement was set only for the separate process.
- Storage suite — 37 tests (including child helper), rich codec — 4, protocol binding — 3; admin suite — 15. Fault checks cover selected head, temp write/sync/rename, directory ancestry, replay/history-reuse barriers, readonly preflight, scope/schema/canonical/checksum validation, per-receipt/device/aggregate caps, and sequence exhaustion. Native `EINVAL` is not classified as a pre-publication refusal.
- Two storage parent tests execute 12 separate native child processes: six cut points each for replace and first publish. Each must reach the designated boundary and exit with code 73 without stack destructors; a new store then verifies an intact old/new root+receipt and retained orphan temp. The OS page cache is not cleared; this is not a power-loss/controller/production-restore gate.
- `cargo fmt --all -- --check`, normal workspace Clippy, and native `cargo build -p sync-server --locked` passed. Clippy leaves 13 existing warnings; an additional stricter `-D warnings` run stopped on five baseline server findings outside the new storage/protocol branches. The native debug build leaves the existing unused `crdt::split_frames`; this is not release/container artifact qualification.
- Plugin source/dependencies/WASM did not change, so TypeScript/plugin build/parity are not presented here as a new run; the latest verified result is listed in the eighth slice. This server slice claims no new throughput/UI/memory benchmark or real-device result. Version `1.11.4`, Nix pins, remote CI, tag/release/deploy are unchanged.

## Tenth-slice verification

- Final `cargo test --workspace --locked --quiet`: sync-server 276 passed / 1 previous ignored benchmark, sync-core 174, sync-schema 5, perf-harness 8. The E2E crate without the feature again runs 0 tests, not compose qualification.
- Cancellation regressions verify both commit/cancel orders, a concurrent winner, late delivery, restart/current incarnation with the original digest, scoped device/vault/session, strict method/schema/byte/sequence limits, corrupt heads, and capacity refusal without bypass/notification. The native parent suite starts 24 separate child processes: the previous 12 acceptance cuts and 12 cancellation cuts, with verifiable exit 73. This is process interruption with the OS cache retained, not power loss.
- The full `npm test` passed: 5 runner checks, shared plugin suite, and actual-bundled-main lifecycle harness (13 scenarios / 113 assertions). In the new suite: root-intent 249 assertions, owned journal ACK 15, local settlement 42, atomic base 9 groups / 50 actual adapter fault boundaries. Recovery stages receipt/base/ACK/retire were verified with a newer edit to the same file and a newer base; wrong journal epoch refuses before accepted base mutation. A cancelled intent can finish with an unloaded/foreign journal without changing any journal or base.
- The root codec is checked against two shared Rust/client BLAKE3 vectors (sequence 1 and MAX_SAFE_INTEGER), malformed/canonical/duplicate/identity boundaries. API — 5 actual `sealed`/`sendEncrypted` scenarios with synthetic crypto/requestUrl, including transport retries, outer-incarnation change, response-byte refusal before decrypt, and no legacy fallback. Per-origin test-transport coexistence is verified alongside the previous 8 API memory cases; this is not native server/client E2E.
- `npx tsc --noEmit`, production plugin build, native debug server build, `cargo fmt --all -- --check`, normal workspace Clippy, and diff check passed. The existing generated-WASM `import.meta` build warnings and 13 baseline Clippy warnings remain; clean `-D warnings` is not claimed. The WASM algorithm/artifacts did not change; this slice claims no separate parity/performance/native-device run.
- Root-intent and settlement tests use a deterministic injected SHA-256 provider for persistence bookkeeping; they are not described as BLAKE3 parity. The actual BLAKE3 oracle is used in a separate codec test without production dependency/lock changes. Caller metadata/native `requestUrl` receive allocation and complete heap/RSS coverage remain outside the new controlled-byte ceilings.

## Eleventh-slice verification

- Final `npm test` passed with frozen source: 5 runner checks, shared suite, and mandatory actual-bundled-main harness — 13 scenarios / 113 assertions. A fail-closed `Platform` export was added only to the Node test stub so the actual desktop/mobile IO classes can run; it provides no implicit host capabilities.
- Root recovery — 355 assertions: lost accepted response/restart, accepted/cancelled winner, original identity/current cancellation incarnation, newer local generation and newer base, wrong scope/epoch/response, negotiated limits, gaps/expiry/unsupported, real pending-promise drain, and reentrant scope close. Adapter failures at terminal/base/ACK/retire stages preserve recovery through a repeated load. The network in these coordinator tests is a synthetic port, not server/client E2E.
- Root-intent — 1 041 assertions with strict conflict-copy schema/binding/count/order, duplicate API/no-IO, malformed WAL/snapshot closure before promotion, compaction/fault boundaries, and close/drain. The atomic-base suite still passes 9 groups / 50 actual adapter failure boundaries; existing local settlement — 42 assertions.
- Root-conflicts — 129 assertions: small/zero/chunked content, exact losing generation rather than a newer source, historical receipt after user edit, copy completed-but-rejected, occupied/partial destination, copy-before-receipt ENOSPC/reload, foreign old `.part`, four-attempt ceiling, and refusal without native append. Close during verifier/download permits no new heavy work; copy tail and receipt await actual completion; repeated close from an abort listener receives the same drain promise. These integration storage/IO fixtures use a synthetic SHA-256 hasher and are not described as WASM/BLAKE3 parity.
- Verified-content — 9 bounded portable suites: exact chunk threshold/4 MiB chunk/16 384-row manifest, duplicate/malformed/unknown fields, UTF-8, contiguous/exact sizes, invalid individual and assembled hashes, cooperative feeds, owner retention across delayed native consume/error/cancel, and constructor/hasher cleanup. The suite does not measure native `requestUrl` initial allocation or device RSS.
- File verification — 53 assertions with actual temporary native files/ranged reads and the Noble BLAKE3 oracle: empty/small/3 MiB content, replacement during read, symlink/hardlink and link creation during hashing, and hash/constructor/cancel failures. Linux separately verifies that no actual file descriptors remain. Portable synthetic tests check growth/mtime/missing/abort after waiting for an isolated budget: no native whole read before a fresh stat. IO wrappers add 10 actual desktop/mobile class suites with a synthetic adapter, no overwrite fallback, and waiting for the real copy promise. This is not native Obsidian/Windows/macOS/mobile qualification.
- Final `npx tsc --noEmit`, production plugin build, and diff checks passed. Generated-WASM `import.meta` warnings remain unchanged; the new coordinators are not yet enabled in the production entrypoint. Server/Rust, WASM bytes/algorithm, and dependencies did not change in this slice; no new Rust/parity/performance/browser/device/remote-CI run is claimed. Tests removed temporary native/test-runner fixtures; working vaults, the user's `temp/`, and production were not touched.

## Twelfth-slice verification

- Final `npm test` with frozen source — PASS: 13 runner subtests, all 86 declared source entrypoints in isolated child runtimes, mandatory actual-bundled-main harness — 15 scenarios / 133 assertions. The initial aggregate run exposed actual interference: a push fixture temporarily set global `maxBatchFiles=2` while a concurrently running pull expected one GET for three objects. The isolated pull was green; runner isolation eliminated the cause while preserving the original bulk assertion. Child assertion/rejection/exit/signal and build-before-any-execution are verified; a suite without a completion contract around a bare pending Promise is not declared qualified.
- Actual engine integration — 18 suites: one push of 600 dirty paths with an actual callback edit N+1 publishes `[256,256,89]`; the journal cut changes `345 → 89 → 0`, and three atomic base publications perform no legacy root/timestamp saves. Tests cover a held ACK and real host task between roots, recovery before normal entrypoints, unsupported pending without fallback, exact current-base rebuild after candidate/cache failure, startup capture during a pending query, and actual stop/re-init drain. Transport, source adapter, and transactional JSON tree are synthetic here; this is not native server/client E2E or a device benchmark.
- The engine fault matrix additionally contains 18 retirement IO cases (`wal` orphan / staged head / promoted head × old-only / N+1 / no-ID × same-renderer / handoff), six ambiguous local-omission ACK cuts, callbacks during local ACK, 257 omissions with `[256,1]`, a retained mixed rename, and complete review of 600 tracked deletes before any ACK. A held actual hash-read/config-list preserves the metadata mutex, live journaling, and subsequent push; EIO/abort does not release the native owner before actual completion. The automatic debounce case passes in an actual `--unhandled-rejections=strict` child without a global rejection handler, persistence, or network under stale scope.
- Planner — 167 assertions with a deterministic 25k metadata fixture and take/restore provenance; drain — 1 068 assertions with 257 successive attempts, both eligibility cuts, failure/close, and actual owner cleanup; local-omission selector — 119. Dirty set — 75; materializer — 61, including positive-omission classification, raw IO refusal, and cooperation at 256/512 skipped paths. Actual push/root-context — 183 assertions: exact candidate bytes/hash/version/parent/entries, C/M, no legacy base writes, accepted/cancelled/deferred, late source/dependency hold, guards, and native candidate failure. These counts are not throughput/RSS measurements.
- Runtime bridge — 11 real-store suites with portable Noble BLAKE3 and synthetic tree/transport: strict preflight/floor/capability, durable-before-send, lost response, cancellation, guarded conflict IO, load/reload, repeated initialization after an actual injected storage-read failure, and close/preparation ownership. Stable scope — 6 suites with a fixed digest, canonical endpoint, and secret-free framing. Root repair — 95 assertions, including an actual packaged scalar WASM v1/v2 complete graph, reproduction of root-only missing children, and atomic failure of the old native graph. Status — 25 assertions, without materializing the journal for a counter.
- `npx tsc --noEmit`, production build, and staged diff check — PASS. Separate packaged scalar/SIMD parity — PASS: 384 649 / 387 764 bytes, payload 10 485 883 bytes, five hash feed sizes, nine chunk feeds, and a 2 048-entry tree v2. `npm run test:browser-probe` — PASS (Node bundle/worker fixtures, not browser/mobile host conformance). Generated-WASM `import.meta` and Node typeless-module warnings remain unchanged; WASM artifacts were not rebuilt in this slice.
- Root-intent/recovery/conflict/base/journal and previous regression suites passed again. Server/Rust sources, dependencies, and the WASM algorithm did not change; this slice claims no new workspace Rust run, real-device UI/RSS, native full-pipeline throughput, remote CI, rollout/rollback, or power-loss test. The tests removed native temporary file-verification/runner fixtures themselves; the user's `temp/`, working vaults, and Syncthing were not touched.

## Thirteenth-slice verification

- Full `npm test` — PASS: 13 runner subtests, 87 declared source suites in separate strict child runtimes, and actual-bundled-main lifecycle — 15 scenarios / 133 assertions. Focused dirty-set — 174 assertions; actual engine/root — 19 suites, including four readiness checks over 25k live hints: exactly four visits, no take/restore/IO, and no insertion-priority change.
- New planner — 91 139 assertions (a substantial portion is exhaustive UTF16/JSON/UTF8 parity), previous selector — 167. Independent 25k **hashless metadata** fixture: one preflight of 50 000 rows, one graph build of 125 000 steps, then 98 selections and 25 097 component visits; `next()` neither rebuilds the graph nor creates full-tail arrays. Model charge — 14 658 752 bytes; hashful 25k is rejected at 16 MiB and is not declared admitted. Real held cooperation wait, abort, repeated admission after row growth, original provenance, modified witness, custom iterator bounds, and disposal are tested separately. This is a scheduling fixture, not 25k actual-engine sync or RSS qualification.
- Read-only differential review of the old HEAD and shared synchronous selector over 1 000 seeded graphs — PASS: identical groups/holds/limits/order and original hint references. Native `JSON.stringify`/UTF8 reference matched the scanner for all 65 536 UTF16 units and boundary/pair strings; the corresponding exhaustive scanner test is included in the new suite.
- Final isolated `node scripts/diagnose-root-drain.mjs` — PASS after freezing runtime sources: for 600/2 500 paths, materialize/stat/planner/take visits remain 1 032/13 480, readiness 5/19 calls and only 4/18 hint visits, with no take/restore inside readiness. Actual roots are `[256,256,88]` and nine groups of 256 plus 196; completed host yields — 75/740, continuation boundaries — 2/9. Synthetic single-run whole durations — 358.004/5 010.771 ms; this is not a baseline comparison or native-throughput gate. The fixture has one-byte sources, all objects present, a synthetic JSON tree/server, and an additional cloned local-store readback on every root.
- `npx tsc --noEmit`, production build, diff check, packaged scalar/SIMD parity, and `npm run test:browser-probe` — PASS. Parity: 384 649/387 764 bytes, payload 10 485 883 bytes, five hash feeds, nine chunk feeds, tree v2 with 2 048 entries. The probe remains a Node bundle/worker fixture, not a native browser. Generated-WASM `import.meta` and Node typeless-module warnings remain unchanged; WASM was not rebuilt.
- Server/Rust, dependencies, version `1.11.4`, and Nix pins did not change; no new Rust run, remote CI, real-device UI/RSS, native full-pipeline throughput, release/deploy, or power-loss evidence is claimed. Owned temporary test bundles were removed by the fixtures themselves; the user's `temp/`, Syncthing, and working vaults were not touched.

## Fourteenth-slice verification

- Final `npm test` after freezing source — PASS: 13 runner subtests, all 89 declared source suites in separate strict child runtimes, actual-bundled-main lifecycle — 15 scenarios / 133 assertions. TypeScript `tsc --noEmit`, production build, and diff check — PASS.
- Actual engine/root — the previous 19 suites plus 15 new reviewed-plan suites. Quiet 600/2 500 checks require exactly `[N, ...selected batches]`, 2N stat, N claimed paths, no full take/unbounded restore, and exact final/cold roots/base/WAL. A new edit among 600 initial paths changes scheduling to `[256,256,1,88]`: the old test now verifies four separate ACK gates and exact intermediate journal cuts rather than a weakened aggregate root count. 70 urgent callbacks produce `[256,256,64,88,6]`; the active last-inserted path belongs to the first urgent window.
- Actual callbacks during full review, selected stat, object upload, native ACK, and stop/handoff preserve the new generation. A lost accepted reply is resolved through an actual outcome query without resend; pull/base mutation between roots requires a new full-review epoch. An unexpected 200 tracked missing paths trigger a full audit of the remaining 344 paths and a review guard before new deletes. EIO during selected stat preserves the entire queue for later review, and stop does not forget a native read promise.
- Fresh-dispatch matrix: request hashing, intent WAL prepare, and final capability negotiation × callback/rename/priority, plus tree-format change during final negotiation. All 10 cases prohibit the initial commit, preserve the exact durable intent and unchanged file WAL/base; normal UNKNOWN → conditional cancel does not ACK files, and a new sequence is published after fresh review. Two manual oversized-retry cases (old 1 GiB → 1 GiB and old hint 512 MiB → fresh stat 1 GiB) publish only 598 independent paths after local-omission ACK, actually retry source-memory admission, and leave source/delete in WAL and dirty. Huge source stats are synthetic; gigabyte files are not read or claimed as a native transfer test.
- Reviewed queue — 195 assertions: late overlay before exhausted, finite refreshes, missing/duplicate/unknown classification, dependency/overflow, known cooling and finite forced/fresh-stat slots in normal/manual modes, positive omissions, atomic cooling-group CAS, source-order priority, and restoration of partially detached components upon exclusion without losing a new no-ID edit. Dirty set — 279; selector — 167; compact planner — 141 169 (many are exhaustive UTF16 scanner checks), including actual interned hashes/provenance, 25k matching-hash admission under the existing 16 MiB, differing-stat refusal, and release of the union graph.
- The deferred tracker gained five revision-lifecycle cases: opaque witness, creation/advance/no-op, pending/uncertain confirmations, exact ACK/chain cleanup, held actual Journal ACK, and torn-WAL failure/reload. Recovery — 440 assertions; runtime — 12 real-store suites. The invocation-local fresh guard is not called during existing-intent resolution or after a response has already been sent; reentrant close permits no new dispatch and awaits the actual tail.
- Three sequential isolated `diagnose-root-drain.mjs` runs after stopping all other project tests/builds — PASS; [complete raw results](root-drain-reviewed-counts-2026-09-05.json). In all three runs, 600/2 500 paths produce 1 200/5 000 stat, one graph (3 000/12 500 build steps), 602/2 509 selection-component visits, and N actual claim visits. The synchronous selector is not called; take and restore input visits are 0. Host yields — 117/895, all actually completed; continuation boundaries — 2/9. Median whole synthetic drain — 365.993/5 067.486 ms, materialization — 3.041/7.965 ms, graph creation — 6.990/17.897 ms. This is not an old/new timing comparison: the fixture includes a synthetic JSON tree/server and cloned intent readback on every root. The separate native 25k gate remains open.
- Packaged scalar/SIMD parity — PASS: 384 649 / 387 764 bytes, payload 10 485 883 bytes, five hash feeds, nine chunk feeds, and tree v2 with 2 048 entries. `npm run test:browser-probe` — PASS (Node bundle/worker fixtures, not browser/mobile conformance). Generated-WASM `import.meta` and Node typeless-module warnings remain unchanged; WASM artifacts were not rebuilt in this slice.
- Server/Rust, dependencies, version `1.11.4`, Nix pins, and running clients did not change. No new Rust run, remote CI, actual packaged-WASM/native-FS 25k full pipeline, real-device UI/RSS, power-loss, or release/deploy evidence is claimed. Owned temporary test bundles/native fixtures were removed by the tests; the user's `temp/`, Syncthing, and working vaults were not touched.

## Fifteenth-slice verification

- Server CLI — three new tests PASS; debug/release builds PASS. Release binary: 4 535 456 bytes, SHA256 `fab3b82a2775ea5cb7defd639e9a8e6a6e994576f428532cb6525cd40c47e77e`. Actual loopback AEAD smoke verified enrollment, capabilities/outcome stream, content hash/upload/download, reload of persisted transport reservation, origin refusal/redirect policy, and held native response join. This is not a new full Rust workspace run.
- Final explicit helper/runner gate — 53 tests PASS: runner 14, native IO 13, metrics 10, instrumentation 7, native child ownership 9. Tests cover held native tails, real signals, ignoring/orphaned descendants with inherited pipes, bounded output, absence of continued writes after confirmed cleanup, and observer transparency under false/throw/poisoned counter. No actual kernel-blocked unkillable process was created; the unsafe-deadline branch is not claimed as native fault proof.
- Final `npm test` on frozen source — PASS: 53 first-stage tests, 89 declared source suites in separate strict runtimes, actual-bundled-main — 15 scenarios / 133 assertions. Reviewed queue — 205 assertions; actual engine reviewed-root — 17 suites; push/root transaction — 183 assertions. `tsc --noEmit`, production build, packaged scalar/SIMD parity, browser probe, and diff check — PASS. Two independent read-only reviews found no data-safety/liveness blocker.
- Packaged scalar/SIMD — 384 649 / 387 764 bytes; parity payload 10 485 883 bytes, five hash feeds, nine chunk feeds, tree v2 with 2 048 entries. SIMD SHA256 `9a4189fdfef1ea4fc477e334dce4c3901d71a04d2be07a03cb032c5e0658b86f`. Existing generated-WASM `import.meta` and Node typeless-module warnings are unchanged. WASM was not rebuilt.
- Bundle-only check and native 32-file smoke for both scenarios with separate cold children — PASS; the edits smoke was repeated separately after adding v2 counters. Diagnostics v2 calculates total as exact accepted-retention decisions + terminal `recordSyncFailure`; old raw output without the marker shows only terminal failures, and its absent fields cannot be read as zeroes. Accepted retention alone does not mean the next attempt completed; final queue/cold invariants confirm that separately. The production engine does not leave a false `Last error` for internally recovered bounded drift; an exhausted retry budget and other errors remain visible and terminate the drain.
- Bounded concurrency does not change review/ACK authority: materialization stat and already admitted small-source reads receive dynamic concurrency from the current profile, an abort-aware `allSettled` join, and the existing byte/count ceilings. Quiet 25k work remains exactly 100 000 source stat, 50 000 materialization stat, 25 000 claims, 98 roots, and one graph; source IO peak increased from 1 to 4. No file/hash/review work needed for the result was skipped.
- The first frozen baseline series on `responsive-stage-043` — 3× quiet + 3× edits, all PASS. Median quiet: 98 753.623 ms, 253.155 files/s, first root 11 692.872 ms. Median edits: 111 862.406 ms, 223.489 files/s, first root 11 939.108 ms, callback→accepted p95 1 813.156 ms. Raw report: 700 478 bytes, SHA256 `ff6cb83b854a3c83a0a12dc5e28233cd74c10df27bebf1718338b9594570d585`.
- Intermediate `responsive-stage-044` was honestly rejected: median quiet had already become 62 632.868 ms / 399.151 files/s, but one pre-dispatch drift destroyed the reviewed owner in two edits runs. The next explicit drain built a second plan for the remaining old tail; callback→accepted p95 rose to 61 114.511 / 59 934.687 ms, plan/drain 2 instead of 1, RSS samples — 353 939 456 / 349 335 552 bytes. A third run without drift produced 1 145.631 ms. This is scheduler starvation, not a UI/event-loop freeze: heartbeat p95 remained ≤20 ms. Raw rejected report: 709 065 bytes, SHA256 `950eda263a289483f43acf9e82459accd32d9d83c467b02214a80d1e07825800`.
- The fix preserves a failed independent upsert selection and live overlay as urgent only after exact restore, only before a root attempt, and only under the current scope/dependencies. Linked/cooling/deletion paths are ineligible. A complete previously encountered selection is not retried, while the entire drain shares a budget of four even across owner rebuild; after exhaustion, work remains in WAL/dirty queue for the next trigger. A new journal ID with same bytes/same mtime during actual object upload does not publish the old cut. A separate test forcibly alternates five drifts with policy rebuild: exactly four continuations, zero roots before bounded exit, then the next explicit drain completes the latest generation.
- The first final series on `responsive-stage-045` — all 6 runs PASS. Median quiet: 61 203.144 ms / 408.476 files/s / first root 5 620.234 ms; median edits: 66 744.478 ms / 374.563 files/s / first root 5 918.945 ms / callback→accepted p95 1 321.899 ms. The old counter showed zero because it observed only `recordSyncFailure`; the combination of exact continuation and restore/claim counters unambiguously derives 0/1/2 accepted retentions in the edits runs. This discrepancy is not hidden: the raw report is retained as legacy-counter evidence, and the direct v2 repeat below is authoritative for drift counts.
- The authoritative v2 series on `responsive-stage-046` — all 6 runs PASS. Median quiet: **65 503.511 ms**, **381.659 files/s**, first root **5 966.495 ms**. Median edits: **63 001.282 ms**, **396.817 files/s**, first root **5 418.087 ms**, callback→accepted p95 **1 161.922 ms**. Relative to the sequential baseline, this is −33.670% / +50.761% / −48.973% for quiet and −43.680% / +77.556% / −54.619% / −35.917% for edits. Callback latency improved by 51.582× relative to the rejected drift median. Variation relative to the first final series remains in the evidence and is not selected by best run.
- Across all v2 runs: unexpected failures 0, terminal drift failures 0, explicit drains 1, plan creates 1, pending WAL 0, pending intent false, exact cold rebuilt root true; cold verified 25 000 quiet or 25 070 edits source hashes. Edits runs directly recorded **1/2/2 accepted retentions**, after which the same drain completed; roots 101, continuations 101/102/102, restore visits 23/50/48. Callback→durable ACK p95 — 1 780.316 / 1 764.486 / 1 783.272 ms. RSS samples: edits 259 891 200–311 529 472 bytes, quiet 291 110 912–314 384 384; Node heartbeat p95 bucket ≤32 ms, max ≤48.664 ms. This is native Node evidence, not Obsidian input-to-paint/mobile RSS. Complete derived comparison and raw identities: [native-root-read-concurrency-2026-09-05.json](native-root-read-concurrency-2026-09-05.json). V2 raw: 703 133 bytes, SHA256 `326ae53b8f9b5fa95e34be8610292902fadaae4138e2e781b74cfcd598200206`.
- Working vault, Syncthing, production, and external clients were unchanged. Tag/release/push/deploy, Nix pin update, remote CI, and real-device qualification were not performed.

## Sixteenth-slice verification

- Final `npm test` after the lazy-encoding refactor passed with exit 0: 53
  first-stage helper tests, 89 declared source suites in separate runtimes, and
  actual bundled main — 15 scenarios / 133 assertions. The root-outcome API now
  runs 8 admission/capability/retry groups alongside the previous 8 API memory
  groups; recovery — 440 assertions, transient memory — 56.
- Focused codec/API/recovery/transient suites and `npx tsc --noEmit` passed.
  The production bundle was built; the existing generated-WASM `import.meta`
  warnings remain. Rust/server, WASM source/bytes/algorithm, and dependencies did
  not change, so this slice claims no new workspace/parity result.
- Deterministic budget gates verify an occupied pool before transport, an
  immutable caller snapshot, exact body after admission, one scope without a
  nested global reserve, retention across tree negotiation/native response/
  replay retry, and release after success/decode/capability failures. Capacity
  one byte below the requirement refuses before encrypt/HTTP.
- A separate bootstrap test uses actual client X25519 bootstrap and synthetic
  per-origin `requestUrl`: an oversized `/server-eph` wire response is rejected
  before decrypt, persistence, and root dispatch. Tree-capability and root-
  response bounds are verified by the same actual `sealed`/`sendEncrypted`
  code; this is not native server E2E or proof of a bound on the initial host
  receive allocation.
- Two independent read-only reviews found no data/lifetime/deadlock blocker.
  They separately confirmed that the upload scope closes before root publication
  and settlement begins after release of the root HTTP scope. Working vaults,
  Syncthing, production, tag/release/push/Nix, and real-device clients were not
  touched.

## Seventeenth-slice verification

- The full-review cooldown/deletion partition was moved to the shared stepped
  core. The synchronous compatibility path preserves the previous
  policy/order/reference semantics inside the new fail-closed admission, while
  `RootReviewedQueue` invokes the cooperative path for initial normal/manual
  passes and the live overlay. Input indexing, cooldown scanning, the global
  delete hold, FIFO closure through missing paths, shared journal IDs, and output
  projection are counted as separate work units; a real host yield is invoked
  after 256 units, without a Promise per row.
- Reachable closure no longer creates the previous
  `related=[...adjacency,...sameId]`. Separate caps limit 131 072 visited
  endpoints and the same number of directed explicit-edge visits; exceeding a
  cap in the preparation path produces a typed admission failure without a ready
  cut, dirty claim, or ACK. A small partition finishes without an extra host
  round trip. The new limit also applies to the synchronous partition
  compatibility path: this is a safe refusal, not unbounded old-policy parity.
  Post-commit settlement preserves the previous synchronous closure semantics:
  it does not acquire a new failure after the root/ACK has already occurred.
- A separate opaque `planningRevision` covers cooldown records and structural
  dependencies. Root review holds it only from normal partitioning through the
  eligibility/forced pass to the ready graph plan; accepted settlement does not
  invalidate the cached queue forever. The revision changes before the first
  actual record mutation. The private cooldown hint is separated from the
  returned `retained`, so caller mutation can no longer bypass the witness.
  Abort, rejected yield, mutable array length, and late cooldown/rename mutations
  do not produce a partial cut.
- The final full `npm test` passed: 53 first-stage helper tests, all 89 declared
  source suites in separate strict runtimes, and the actual bundled main with
  15 scenarios / 133 assertions. Deferred direct tests verify sync/async parity
  before and after a completed cooperative checkpoint, force, nonqueued bridge,
  high-degree closure, dependency admission, abort/yield failure, revision
  lifecycle, and the detached retained hint. The reviewed queue has 211
  assertions, including mutation inside partitioning and between partition/plan.
- The actual-engine reviewed-root matrix passed 19 suites. Two new scenarios hold
  a real partition host wait after full materialization: before release there is
  no full take, claim, root intent/request, base publication, or journal ACK. A
  late callback preserves durable N+1 and no root publishes N; stop remains
  pending until actual release, preserves all 300 WAL/dirty owners, while the
  replacement performs a new full review and finishes with a cold-consistent
  root. The tree/transport in the fixture are synthetic; this is
  ordering/ownership evidence, not native Obsidian responsiveness.
- `npx tsc --noEmit`, the production build, and the diff check passed. The build
  left only the existing generated-WASM `import.meta` warnings. Three independent
  read-only reviews found no data-safety, ordering, or preparation-fence blocker
  after the input lengths/options and edge cap were fixed. Scalar-row
  immutability remains an explicit caller contract; the current engine owns
  these arrays locally.
- Rust/server, WASM artifacts/algorithms, dependencies, version `1.11.4`, Nix,
  working vaults, Syncthing, and production clients were unchanged. This slice
  does not claim remote CI, native-device UI/RSS, tag/release/push/deploy, or a
  new 25k throughput/cold run.

## Eighteenth-slice verification

- Dependency snapshot capture was moved to the shared stepped core: the
  synchronous compatibility API preserves the previous insertion order,
  orientation, and uncapped result, while production `RootReviewedQueue` uses
  the cooperative admitted path. Every outer endpoint and every directed
  adjacency entry is a separate work unit; after 256 units, a real caller-owned
  host yield is invoked. The originally ordered detached/frozen rows are returned
  without a mutable alias to the rename graph.
- The cooperative path fail-closed limits the snapshot to 65 536 pairs, 131 072
  endpoints, and 131 072 directed adjacency visits, with refusal occurring before
  the next row is copied. A separate adversarial early-hub fixture verifies a
  long reverse-only prefix: traversal admission triggers before the first
  checkpoint at test limit 16, not after the full traversal. A smaller caller can
  only lower the common limit; an invalid/exceeded limit does not produce a
  partial snapshot.
- Capture runs inside a temporary preparation fence between partitioning and the
  graph planner. A structural rename mutation invalidates its own witness;
  cooldown-only settlement does not break an independent structural capture, but
  correctly rejects the entire reviewed cut through the shared
  `planningRevision`. Admission is converted into a typed fail-closed preparation
  error; abort and a rejected host yield preserve the original error. None of
  these outcomes returns a partial snapshot; no dirty claim, WAL ACK, or
  server/root/base mutation occurs before preparation completes.
- The actual-engine reviewed-root matrix passed 20 suites. A new scenario holds
  the host wait inside real dependency capture after full materialization: before
  release there is no take/claim, root intent/request, base publication, or
  journal ACK. A callback during the wait preserves durable N+1; N enters no root;
  after release and, when necessary, an additional drain, the WAL/dirty state is
  empty and the base is cold-consistent. Tree/transport remain synthetic, so this
  is ownership/ordering evidence, not an Obsidian UI measurement.
- The final `npm test` outside the sandbox passed: 53 first-stage helper tests,
  all 89 declared source suites in separate strict runtimes, and the actual
  bundled main with 15 scenarios / 133 assertions. Focused deferred tests, 225
  reviewed-queue assertions, `npx tsc --noEmit`, the production build, and the
  diff check passed. The build left only the existing generated-WASM
  `import.meta` warnings. The first full-suite run inside the sandbox stopped at
  `native-root-child.test.mjs` and `run-tests.test.mjs` without child diagnostics;
  the same frozen source passed completely outside the sandbox.
- Three independent read-only reviews after the adversarial fix found no blocker.
  Review found and closed the initial gap where the cap counted emitted pairs but
  did not limit the early reverse-only adjacency tail. The final
  `Object.freeze(snapshotArray)` remains one native JS call; Node work counts do
  not prove a ≤4 ms renderer slice, GC/RSS plateau, or mobile UI. Rust/server,
  WASM artifacts/algorithm, dependencies, version `1.11.4`, Nix, working vaults,
  Syncthing, production, remote CI, tag/release/push/deploy, and real-device
  qualification were unchanged.

## Nineteenth-slice verification

- Production reviewed preparation for `oldest`, `newest`, `smallest`, `biggest`,
  and `alphabetic` no longer invokes one native whole-array sort. A stable
  bottom-up merge sorts the original `FileChange` references: indexed copy and
  every merge/tail output count as a work unit, and owner host cooperation is
  invoked after every 256 units. Comparators were transferred literally;
  `undefined` remains zero, `localeCompare` was not replaced with ASCII ordering,
  ties preserve insertion order. `sequential` returns the original array without
  copying.
- Cooperative admission of ≤65 536 rows is checked before allocation. The
  deterministic path uses two reference arrays and random uses one; this is a
  fixed count, not a shared memory lease or heap/RSS measurement. The public
  contract requires a dense immutable array/scalar rows until return: a length
  fence does not detect same-length replacement or a change to an already
  inspected field. Production passes its own detached materialized rows, while
  queued journal proofs are neither sorted nor copied by the sorter.
- The historical `random` comparator was not silently replaced with another
  distribution. Its input is now copied cooperatively by captured indices, so a
  custom `Symbol.iterator` cannot bypass admission, but the native
  `sort(() => Math.random() - 0.5)` itself remains a synchronous residual. The
  legacy fallback and synchronous compatibility helper preserve the previous
  behavior; Fisher–Yates requires a separate decision and tests of scheduling
  semantics.
- A new direct suite verifies exact native-oracle parity for five deterministic
  modes, stable ties, original identity/input order, Unicode/composed paths,
  undefined/zero/extreme finite keys, empty/single/small inputs, sequential and
  seeded-random compatibility, hostile iterator, abort, input resize, rejected
  yield, and lowered/exceeded/invalid admission. 25 000 duplicate-heavy rows
  produce exactly 400 000 copy/merge units and 1 562 cooperation calls; this is a
  deterministic work-count fixture, not a timing/UI benchmark.
- The sort await is inside the previous temporary preparation fence before
  dependency capture/graph planning. Dependency and cooldown mutation both
  during a checkpoint and immediately after the sorter returns invalidate the
  provisional owner before the dirty claim; admission becomes typed
  `RootReviewInvalidated`, while abort/yield/comparator errors preserve the
  original error. The reviewed-queue suite now contains 239 assertions.
- The actual-engine reviewed-root matrix passed 21 suites. A new deterministic
  priority case holds a real host wait inside the sorter after full
  materialization. Before release there is no take/claim, root intent/request,
  base publication, or journal ACK. A live callback preserves durable N+1, and no
  root publishes N; after release and, when necessary, the next drain, the WAL,
  dirty queue, and pending intent are empty, and the cold base matches the server
  root.
- The final `npm test` outside the sandbox passed: 53 first-stage helper tests,
  all 90 declared source suites in separate strict runtimes, and the actual
  bundled main with 15 scenarios / 133 assertions. `npx tsc --noEmit`, the
  production build, and the diff check passed; the build left the existing
  generated-WASM `import.meta` warnings. Three independent read-only reviews
  found no blocker after their identified random/custom-iterator cap bypass was
  fixed.
- Initial allocations, every `localeCompare` call, the native random comparator,
  and GC remain uninterruptible primitives; Node work counts do not prove a ≤4
  ms renderer slice, input-to-paint, mobile RSS plateau, or the absence of reload.
  Rust/server, WASM artifacts/algorithm, dependencies, version `1.11.4`, Nix,
  working vaults, Syncthing, production, remote CI, tag/release/push/deploy, and
  real-device qualification were unchanged.

## Continuation: tree jobs and bounded cold-rebuild input

Subsequent native scheduler slices are described in section 5 of the
[roadmap](responsive-sync-roadmap.md#5-scheduler-the-ui-has-priority-bulk-does-not-starve):
private candidate jobs, chunk/root export, byte-budgeted root work, constant-time
revision witnesses, and admitted durable candidate publication. These are
implemented local changes, not a newly published release.

Cold-rebuild input: `responsive-stage-074`
(`perf(sync): page cold rebuild input`).

The new cold-recovery input job sends sync-base to WASM in pages of up to 256
entries / 256 KiB UTF-8 instead of a complete JSON `join`. Shared admission is
acquired before native entry allocation and held until actual finish/cancel; a
real host task runs between pages. The exact tree wrapper, committed/candidate
revisions, immutable base capture, and AbortSignal are checked around waits and
immediately before finish. The scope callback is pinned at startup. A failed or
canceled private input does not replace the live graph; a ready finish consumes
the job even on a late build/revision error. The legacy API remains available.

Final local checks for this slice:

- `cargo test -p sync-core --features wasm --locked`: 230 passed, including five
  new native replacement regressions. `cargo test --workspace --locked`: 499
  passed, 1 ignored. `cargo fmt --all -- --check` passed.
- Scalar/SIMD WASM was rebuilt. Full packaged parity passed, including exact root
  bytes/hash and reachable chunks for v1/v2 × scalar/SIMD × pages 1/256,
  Unicode/canonical ordering, format transition with a candidate, and terminal
  failure.
- Nine new host suites passed: count/byte pages with native-valid Unicode paths,
  the full input lease, queued admission, task fairness, drift/cancel, final-await
  microtask, and preservation of the original scope callback. The old repair
  suite passed 95 assertions with actual packaged scalar v1/v2.
- The full `npm test`, TypeScript, production build, browser-probe tests, and
  `git diff --check` passed. The final `npm test` was repeated after review
  hardening; child-process suites ran outside the spawn-restricting sandbox. The
  existing generated-WASM `import.meta` build warnings remain.

At the boundary of this input slice, precisely the delivery of **input** is
bounded and split. Sort, leaf/internal codecs, closure validation, and dropping
the old graph remain synchronous inside finish. The input ledger does not cover
the complete replacement graph or allocator/GC/RSS. Full Rescan and the other
bootstrap/reconcile/tree-format call sites have not yet been migrated. This run
does not claim new measurements on the 25k corpus, native-device
input-to-paint/RSS/reload, remote CI, or production qualification. Version, Nix
pins, tag/release/push/deploy, working vaults, and Syncthing were unchanged.

## Stepped private native v1/v2 construction

Commit: `responsive-stage-076`
(`perf(sync): resume private tree rebuilds`).

The next slice connects optional start/step after paged input. The native builder
performs grouping/sort/segmentation in parts, builds private chunks, and verifies
their full closure. The old committed/candidate pointers and revisions do not
change before a successful finish. Without the optional pair, compatibility with
paged-input/atomic-finish WASM is preserved; a partial API fails closed before
admission.

- Up to 256 work units per native turn, with at most one node codec/hash or graph
  validation step. Owned input rows, obsolete ranges, and closure descriptors
  are released incrementally before readiness. The old canonical builder remains
  an independent oracle, including v1 stable duplicates and lexical decimal
  labels, v2 duplicate rejection, and path-only boundaries.
- The host retains input admission until finish/cancel, strictly validates the
  progress own data fields, and yields to a real host task after every step,
  including done. Final done does not authorize publication after cancellation
  or a base/scope/wrapper/revisions change. Start/step errors leave the owner for
  cancel; only the correct ready finish has a terminal-drop contract.
- Native suites include the 25k differential, both budgets, every exact chunk map,
  cancellation at each phase, revision exhaustion, and fault injection of the
  private closure. The host suite has 15 sets; the actual packaged scalar helper
  has 95 assertions. Scalar/SIMD parity matches legacy in bytes/hash/complete
  reachable chunks; early/ready cancellation, failed-state persistence, and an
  empty graph are verified.
- In the packaged fixture, budget 256 performs 84 v1 / 76 v2 build turns for
  1034/1033 entries. Budget 1 produces 16548/16547 turns with the same result.
  This is a count of explicit work units, not a renderer-latency or iPhone-RSS
  measurement.
- Final `cargo test -p sync-core --features wasm --locked`: 244 passed;
  `cargo test --workspace --locked`: 510 passed, 1 ignored. The full `npm test`
  passed with the old compatibility artifacts and again with new scalar/SIMD
  artifacts. TypeScript, the production build, browser-probe suite, fmt, and the
  diff check passed. The build retains the existing generated-WASM `import.meta`
  warnings. After the final WASM build, only native test-only closure fault cases,
  not new production logic, were added; they are also included in the final Rust
  gates.

Remaining atomic work includes codec/parse, individual string operations,
allocator/growth, drained backing arrays/hash tables, cancellation dropping the
private graph, and dropping the old resident graph during the final swap. A v1
leaf retains the 1000-row boundary without a new byte cap; a v2 node is limited
to 256 KiB. The shared ledger currently covers input, not the complete
sort/replacement/resident heap. Full Rescan, bootstrap/reconcile, and tree-format
call sites have not yet been migrated. Real-device qualification, release, Nix
pins, working vaults, Syncthing, and production deployment were unchanged.

## Stepped retirement of the replacement and previous resident graph

Commit: `responsive-stage-078`
(`perf(sync): retire replacement trees cooperatively`).

The next slice moves cleanup for startup root repair to the optional deferred ABI.
Cancellation, private-build failure, and successful replacement no longer have to
release every row/descriptor/node buffer in one native call. The old ABI is
preserved.

- `cancel_replacement_rebuild_job_deferred` changes only the owner of private
  input/build. `finish_replacement_rebuild_job_deferred` requires an already
  ready stepped build; every preflight/take failure preserves a cancellable job.
  Success changes the live graph/revisions once, while the previous graph and
  residual builder remain in retirement under the same exclusive token.
- `step_tree_retirement` releases up to 256 units. Rows, sparse sort slots,
  root/closure/range descriptors, candidate baseline hashes, and node buffers are
  cleared incrementally. Every nested grouped vector is cleared before its key is
  removed. New jobs/mutators and generic cancel cannot bypass unfinished cleanup.
- The host retains admission until actual cleanup, yields to a host task on every
  step (including final done), and does not cancel cleanup along with the original
  signal. A cleanup error preserves the strong owner/token/lease; the singleflight
  drain can be retried without a second reservation. A new repair first waits for
  such an owner. Re-init/unload wait for drain before native free and scheduler/
  pool shutdown. After the await, init rechecks the generation; a throwing free
  is not retried. Before returning a successful result after cleanup, the helper
  rechecks base/signal/scope and a fresh published-tree witness. This closes the
  late-drift window before subsequent cache/base writes without rolling back the
  installed graph.
- Native regression covers input/partial/failed/ready cancellation, every builder
  phase, exact destructor counts for 25k sparse sort, O(1) ownership transfer
  without copying buffers, revision exhaustion, both tree formats, and stale
  tokens. The host has 22 suites; the actual bundled main has 17 scenarios/149
  assertions. Packaged scalar/SIMD parity passed at cleanup budgets 1/256 with
  exact legacy root bytes/hash/full reachable chunks, not merely the final count.
- Gates: `cargo test --workspace --locked` — 518 passed, 1 ignored;
  `cargo test -p sync-core --features wasm --locked` — 258 passed. The full
  `npm test` passed first with compatibility artifacts and then again with rebuilt
  scalar/SIMD. TypeScript, the production build, 5 browser-probe tests, fmt, and
  the diff check passed. Scalar/SIMD artifacts are 577053/566547 bytes. The
  existing generated-WASM `import.meta` build warnings remain. No new
  real-device/performance/remote-CI/release gates are claimed.

Atomic codecs and allocator/GC remain: releasing one large v1 buffer, hash
iterator scans, drained backing allocations, and direct Drop/free outside the new
contract. A complete private/sort/resident memory ledger was not added: the
previous input workset is retained, rather than claiming accounting for the whole
WASM heap. This is also not real-device latency/RSS/reload proof. Other rebuild
call sites, version, release/Nix/deploy, working vaults, and Syncthing were
unchanged.

## Indexed sorting and observation of WASM allocations

Commit: `responsive-stage-080`
(`perf(sync): reduce sort memory and observe wasm allocations`).

This is the next memory slice, **not completed admission for the private/resident heap**.

- The shared native `StableSort`, used by candidate mutation v1 and replacement
  v1/v2, now stores the original `Vec<T>` once. Two pre-reserved `Vec<usize>`
  values sort indices; the inverse permutation changes the original array in
  place. There are no additional full-record merge/output arrays, T clones,
  realloc of the original array, or geometric growth of index buffers. One tick
  performs one index unit, no more than one comparison or one swap. Equal-key
  order remains stable. For 25k, the index payload is 200000 bytes on wasm32,
  separate from input records/strings and allocator overhead.
- On cancellation, at most one T is released per unit; each Copy-index backing
  allocation is then released in one unit, without 50k separate pop/yield steps
  for integer slots. Allocator/free itself and comparison of long strings remain
  atomic primitives, not a wall-clock bound. The earlier sparse-sort descriptions
  refer to the previous commit and are superseded by this representation.
- `wasm_memory_snapshot()` observes requested-live/peak bytes, live/success/
  failure allocation counters, and linear-memory bytes for one WASM instance.
  The wrapper delegates `alloc/alloc_zeroed/dealloc/realloc` to `System` and does
  not introduce quota/OOM refusal. Hooks do not allocate memory, log, or panic;
  saturation/underflow makes the counters invalid. The native executable/test
  global allocator is unchanged. On the JS ABI, unsafe integers are explicit
  null + invalid, not rounded Numbers. Debug info shows scope and exclusions,
  while a missing/invalid/throwing export does not block other diagnostics.
- Counters include allocations from this module through GlobalAlloc, not one tree;
  they exclude allocator metadata/fragmentation, internal realloc overlap,
  stack/static, JS, other workers/instances, and RSS. Linear memory need not
  shrink after free. The first report initializes serializer state; the verifier
  performs one explicit warmup before baseline, with no subsequent adjustment.
- `scripts/verify-wasm-memory.mjs` was added to CI/release after fresh build and
  parity. Actual scalar/SIMD × v1/v2 × one-prefix/25000 distinct-prefix produce
  8 scenarios with 3 cycles each. A 25k committed graph and a modified candidate
  are already live; input/graph-emitting partial/ready cancellation, failed
  duplicate v2, a rejected 257-row bounded JSON page, deferred swap, and
  retirement are verified. The legacy oracle is a separate instance; v1 clocks
  are fixed, and root bytes/hash and reachable chunk bytes are compared exactly.
  Native errors are not replaced with errors from the verifier itself. Real
  Hasher/tree-handle ownership and isolation of the second instance are present;
  a work guard bounds completed units.

All 24 cycles passed on the final rebuilt artifacts: after free, exactly the
initial 216 requested bytes / 1 serializer allocation remained; after each
cancellation, exactly the pre-job live/count remained, without tolerance or a
baseline reset. Scalar/SIMD produced identical observed lifetime peaks (bytes):

| Corpus, 25k entries | Tree | Requested peak | Linear-memory peak |
| --- | --- | ---: | ---: |
| One prefix | v1 | 8714343 | 14876672 |
| One prefix | v2 | 7732347 | 12713984 |
| 25000 distinct prefixes | v1 | 21320881 | 29425664 |
| 25000 distinct prefixes | v2 | 7982347 | 12845056 |

This observes specific fixtures, not a universal ceiling, renderer RSS, or a
native-device gate. Verifier `elapsed_ms` includes the corpus, a separate legacy
oracle, all three lifecycle cycles, and diagnostics, not the time of one rebuild.

An additional Node A/B microcheck used the previous local retirement artifacts:
25k reverse-order `wide/NNNNNN.md`, 256-entry pages/256-unit steps, an empty seed,
fixed clocks, one warmup, and the median of five complete begin→free runs. Linear
memory v1: 15335424 → 7733248 bytes; v2: 11862016 → 5111808 bytes. Canonical roots
matched. Final scalar/SIMD median v1: 39.63/36.95 → 40.41/38.16 ms;
v2: 40.92/36.74 → 40.60/37.77 ms. **No wall-time speedup is claimed**: this is a
memory reduction, while the cost of instrumented allocation and additional index
steps requires further profiling. This is not a device/host-yield timing gate.

Release LTO initially replicated hooks across allocation sites: WASM grew by
approximately 220 KiB per variant. `inline(never)` on four allocator hooks
preserved shared instrumentation and removed this growth: final scalar/SIMD sizes
are 579192/568934 bytes versus the previous 577053/566547 bytes. The repeated
memory/parity/build gates used these compact artifacts specifically.

Gates: `cargo test --workspace --locked` — 528 passed, 1 ignored;
`cargo test -p sync-core --features wasm --locked` — 266 passed. They include 6
isolated allocator tests (aligned/zeroed/realloc failure/concurrency/overflow),
JS integer conversion, and 25k stability/pointer/no-growth/drop tests. The full
`npm test` passed with the previous artifacts, rebuilt artifacts, then compact
rebuilt artifacts; 12 actual host diagnostic tests are included in the default
command. TypeScript, production build, 5 browser-probe tests, and fmt/diff check
passed. Packaged parity verifies budgets 1/256 and exact v1/v2 roots/full chunk
maps; the new memory verifier ran directly on the final compact build and passed.
An additional attempt through the child-process output wrapper returned empty
stdout and was not counted as gate evidence. The existing generated-WASM
`import.meta` warnings are unchanged. Remote CI was not run.

Remaining gaps: there is no pre-allocation admission for all
private/resident/sort allocations, no per-tree allocator attribution, and long
paths/codec scratch plus all other tree call sites still require qualification.
V1 still retains drained input backing on top of grouped rows; hash/path strings
and serde page scratch are not yet included in component accounting. Real-device
UI/RSS/reload, reader/worker, pipeline, adaptive transport, and CRDT gates remain
open. Version, tags/releases/Nix/deploy, working vaults, and Syncthing were
unchanged by this slice.

## Component accounting for tree stores and the foundation for resident admission

- `MemoryChunkStore` maintains O(1) exact counters for the number of chunks it
  owns, payload bytes, and `Vec::capacity()` bytes. Insert/replace/delete,
  verified overlay promotion, and reachability retain update the map and counters
  under one `RefCell` borrow. Overflow/underflow is sticky-invalid; diagnostics
  must not turn it into zero. `HashMap::capacity()` is published only as logical
  element capacity, not allocator bucket bytes.
- `into_chunks()` now transfers the original map to `ChunkRetirement` without
  copy/collect. After every `next()`, the buffer belongs to the caller and is no
  longer counted as cursor-owned; map backing remains visible until the cursor
  itself is dropped. Equal hashes in separate stores count as separate
  allocations.
- V1/V2 replacement builders, live trees, and retirement forwarding report
  `resident`, `replacement`, and `retiring` components according to actual
  ownership. Committed/candidate share one resident store and are counted once.
  Cancel transfers the private store into retirement; publish transfers the new
  store into resident and the old one into retirement. A late V2
  closure-validation failure with already-created nodes also retains the owner
  until explicit deferred cleanup.
- Optional `chunk_memory_snapshot()` has schema/scope and fail-closed counters.
  Debug info validates only own data/safe integers and explicitly states that this
  is a subset of the already observed module allocator, not an additional total.
  Root strings/arrays, input/sort/closure, codec scratch, map buckets/metadata,
  JS, other instances, and RSS are excluded. Other private tree jobs set
  `other_private_jobs_unmeasured` rather than a fabricated zero.
- The replacement page parser preserves exact legacy decode/error semantics for
  0..=256 rows, but a custom serde seed rejects the 257th entry before decoding
  its fields. The native 256 KiB check precedes parsing; host preflight remains
  the first protection before the wasm-bindgen crossing. This change does not
  eliminate the first 256 `RawEntry` values, path/hash strings, or serde scratch.
- `NativeResidentLedger` adds a separate synchronous fail-fast ownership
  foundation: reserve/grow/resize, conserved split/transfer, explicit idempotent
  release, shrink-to-overcommit, and close without a FIFO waiter or finalizer.
  Forged, foreign, and released leases cannot change the count. This is arithmetic
  over caller-provided estimates; the ledger is not yet connected to production
  tree jobs and is neither a heap/RSS ceiling nor a passed
  reserve-before-allocation gate.

Native tests verify 1500 mixed store mutations against a complete oracle, spare
capacities, collision/validation rollback, pointer-preserving retirement,
partial/empty cleanup, and independent arithmetic failure. The ownership matrix
covers previous v1/v2 × replacement v1/v2 × partial/ready cancel/publish, one
retirement unit at a time, plus a late failed build. Page tests confirm legacy
parity and early refusal of a malformed 257th row. The host ledger has five groups
of ownership/bounds tests; the formatter has 18 path-free fail-closed tests.

Fresh scalar/SIMD artifacts are 585287/575075 bytes. Packaged parity preserved
exact roots/chunk maps for v1/v2 and budgets 1/256. The updated memory verifier
passed scalar/SIMD × one/wide-prefix × v1/v2 × 3 cycles, 24/24 total: an existing
committed+candidate shared store, input/partial/ready/failed cancel, publication,
exact component handoff, monotonic retirement, release of tree/hasher, and
isolation of instances. Every final snapshot returned to the warm baseline of 424
requested bytes / 1 allocation; this is the lifetime of one synthetic WASM
instance, not RSS. The compact report retains per-corpus allocator/component
maxima; full phase/cycle output remains opt-in through
`OBSETYNC_WASM_MEMORY_DETAILS=1`.

Scalar/SIMD matched on these measured maxima (bytes):

| Corpus, 25k entries | Tree | Requested peak | Linear peak | Resident payload max | Replacement payload max |
| --- | --- | ---: | ---: | ---: | ---: |
| One prefix | v1 | 8714807 | 14876672 | 2397080 | 2303308 |
| One prefix | v2 | 7732811 | 12713984 | 1624948 | 1603548 |
| 25000 prefixes | v1 | 21321345 | 31391744 | 4000160 | 4000000 |
| 25000 prefixes | v2 | 7982811 | 12845056 | 1749348 | 1729344 |

Payload maxima equal the measured node-buffer capacity maxima for this corpus;
map capacity remains logical in the report and is not converted into a byte
total. The increased wide-v1 linear high-water includes frequent component
diagnostics and does not mean that the live requested peak or production RSS
grew; linear WASM memory does not shrink after expansion.

Gates for this slice: `cargo test --workspace --locked` — 532 passed, 1 ignored;
WASM-feature — 280/280 passed; `cargo check --workspace --locked` passed with the
existing default-feature dead-code class of warnings; fmt/diff check passed. The
full `npm test`, TypeScript, production build, and browser-probe 5/5 passed on
fresh artifacts. Build/probe retained the existing generated-WASM `import.meta`
warnings. Remote CI and real-device memory/UI/reload checks were not run.

## What remains unproven and unchanged

- The late-ACK replacement race found in the seventh slice is closed in code and in the eighth-slice regression tests through the actual drain/capture handoff; mock/native-promise call-order evidence does not replace Obsidian reload/device gates. The same-renderer registry requires a stable host vault identity; process death does not wait for promises. Final crash-log flush and exceptional listener teardown still have separate diagnostic/retained-closure limitations.
- The one-minute timer-throttling hypothesis requires a foreground → hidden > 6 min → foreground A/B in native Obsidian, also without DevTools. Merely having a backend does not guarantee hidden throughput.
- iPhone reloads require a real memory/UI run and inspection of the system report. A new scheduler does not bypass background OS suspension.
- The governor already receives short windows, but eligibility based on visibility/monotonic bounds is not an exact measurement of CPU active time or OS suspension.
- Shared admission already covers the primary push/download/apply and root
  outcome HTTP paths; complete accounting of all live buffers, the mobile ranged
  reader, review/graph/WASM heaps, scan/object TransferPlan, and transport router
  is still absent. Full-review partitioning, dependency snapshot capture, and
  deterministic priority sorting are now cooperative and bounded by work count,
  but native random sort and synchronous settlement are not yet split into host
  slices; review/graph/sort arrays are still excluded from shared memory
  admission. The twelfth slice activates outcome recovery, verified conflict
  preservation, and short root commits in the engine, but does not complete the
  entire restart/pipeline workflow or native memory qualification.
- Complete CRDT/editor binding, durable document operations, and a materializer are not enabled. These WS metrics do not declare the server live-editing scaffold fixed.
- Worker-startup reasons and the opt-in browser probe are available; native mobile capability results, the production mobile worker/reader, and build-provenance/manifest mismatch still require work. Version `1.11.4` here is the source baseline, not a newly published release.
- Native adapter integration tests and scenarios where a file changes to a directory between stat and read are still needed. Such a read error remains an error for retry rather than becoming an inferred deletion.
- Working vaults, clients, Syncthing, and production services were unchanged. This phase did not perform a new tag/release/deploy.
- The new journal/sync-base limit the size of individual IO/parse operations, not the entire backlog/index or native-host memory. Legacy migration has explicit limits/recovery stops; a safe user rollout/rollback is not ready. The advance fence is neither a latest-generation witness nor an fsync/power-loss guarantee.

## Next step

The next bounded local slice is to extend pre-allocation accounting from the
already counted node buffers to root/candidate metadata, input/sort/closure, and
codec scratch, then expose conservative planned bytes before the first
corresponding allocation. Only after that should the existing fail-fast resident
ledger be connected to production owners. A long-lived graph cannot be placed in
the FIFO transient pool: otherwise a new build waits for memory from the old
graph, which will be released only after that same build. The replacement must
reserve simultaneous R+N in advance, transfer the new graph's charge to the
resident owner and the old graph's charge to retirement during the swap, and
reduce the charge only after actual cleanup. A throwing native free requires a
strong retained owner/charge, not a WeakMap or fake timeout release. No shared
WASM quota is claimed until candidate open/mutation and direct build/update call
sites are covered. Stepped builders, component counters, and allocator
measurements do not close the native UI/RSS gate. After qualification, migrate
the remaining rebuild call sites with their own bootstrap/fresh-upload and
lifecycle contracts. Batch/step boundaries must not change v1/v2 bytes.

Separately, decide whether to retain random as a compatibility residual or
explicitly replace it with cooperative Fisher–Yates, and split safe pre-commit
settlement. Initial allocations, `localeCompare`, native random sort,
synchronous settlement, the final array freeze, and residual native codecs/drop
prevent claiming that overall CPU ≤ 4 ms or mobile responsiveness has been
proven.

Verify the implemented retirement/handoff on native clients: a delayed root
response during re-init/unload, live edits with a pending callback, failed native
termination, and repeated tree allocations. Node ordering tests do not authorize
declaring the platform memory/UI/reload gates passed.

Continue PR05/07/08/09/10 after passing the actual packaged-WASM/native-FS 25k throughput/cold gate: shared resource accounting is needed for review/graph/WASM heaps, the real CPU slice must be bounded during full audit/partition/sort, and local dependency-component repair must replace a full rebuild on every rename. Separate incremental metadata/prepare/upload/commit and urgent editor edits by bytes/time without creating a promise backlog or splitting rename groups. Then add durable scan/object progress, storage epoch/leases, storage/control admission without nested deadlock, shared staged quota/cleanup maintenance, and adaptive WS/HTTP. Mixed excluded renames and no-ID policy omissions require a separate explicit generation-safe workflow. Scope-change/lost-stream reconciliation, understandable migration/recovery/server downgrade, and native server/client integration tests are needed. On mobile, run the voluntary native probe, separately approve the reader-fixture experiment, and define a budget for retained WASM/worker heaps. Verify pending/restart/capability transitions in native Obsidian, not only with mocks. The Node 25k throughput/cold result does not close device input-to-paint/RSS/reload gates PR01/03/04.

The seventh slice implements the first safe desktop reuse with mandatory full
content verification. The prohibition on restoring `FileChange.hash` from only
`mtime + size` remains: otherwise an offline edit could publish an old object.
The current adapter cannot rule out unobserved same-stat drift after verification,
especially for ranges already present on the server. Portable skip-verification
requires proven source identity/capability or an immutable source; the current
mobile adapter provides neither.

## Exact V2 node-output plan and pre-encode barrier

- After sort/entry validation, `ReplacementRebuildV2` now first builds a
  cooperative dry plan for the entire tree. Leaf and internal boundaries are
  computed from UTF-8 path bytes, endpoint indices, and fixed hash/scalar sizes;
  no node encode/hash/store occurs before `plan ready`.
- The plan is exact for logical serialized output: `nodeCount`, `leafCount`,
  `internalCount`, `nodePayloadBytes`, `maxNodeBytes`, `storedRootBytes`.
  Root ID/path limits and exact root length are checked allocation-free before
  the first node allocation. At the barrier, the component snapshot confirms
  0 private chunks and 0 payload bytes.
- A new token-scoped WASM ABI separates read-plan from resume. Resume requires
  an exact safe-integer payload witness; a wrong token/phase/value leaves the
  plan and empty store unchanged. A repeated step without resume remains at
  `plan ready` with zero work instead of performing a hidden encode.
- The host repair driver pins both functions as an all-or-none ABI, strictly
  reads only own data fields without getters, validates count/limit arithmetic,
  and pins the optional `onNodePayloadPlan` before the first await. Base/scope/
  tree are rechecked after reading the plan and after the hook. An explicitly
  configured V2 policy without the plan ABI is rejected before admission, and
  a declared plan ABI without exactly one pre-`done` barrier is rejected before
  finish, so an old or corrupted adapter cannot bypass authorization.
  Policy rejection goes through the existing strong deferred-retirement owner;
  the live graph is unchanged.
- Before every internal codec, emission checks span/height against the saved
  compact dry schedule, then checks each codec length against the exact span,
  aggregate node count/bytes before the root, and stored root length after
  assembly. Natural-anchor and byte-cap differential fixtures pass budgets
  1/256; an artificial schedule mismatch is rejected before the first internal
  allocation. Metadata-only changes with the same paths produce the same plan;
  canonical hashes/chunks still depend on metadata.

Checks: `cargo test --workspace --locked` — 540 PASS, 1 ignored;
WASM-feature — 284/284 PASS; `cargo check --workspace --locked`, TypeScript,
fmt/diff check, full `npm test`, production build, and browser probe — PASS.
Host root-repair suite — 24/24. Fresh packaged scalar/SIMD artifacts —
597000/589851 bytes; parity preserved exact v1/v2 roots/full chunk maps at
budgets 1/256, requires exactly one V2 `plan ready`/zero V1 barriers, and checks
all six plan fields against canonical chunks/root. For the 1033-entry V2
fixture: 3 nodes / 66309 logical payload bytes / 195-byte root. Real JS→WASM
resume rejects `NaN`, negative, fractional, unsafe, and wrong exact witnesses.
The memory verifier passed 24/24 lifecycle cycles; all instances returned to
the baseline 424 requested bytes / 1 allocation.
Node process-group tests passed outside the filesystem/process sandbox; inside
the sandbox their pipe delivery falsely produces `TIMEOUT`/`INVALID_JSON`.
Remote CI and native-device UI/RSS/reload were not run.

This is not total heap/RSS admission. The exact plan covers serialized node
payload, but not allocator capacity rounding, entry/sort/planner/range/closure/
root/map owners, codec/hash scratch, WASM linear high-water, or JS. By default,
the optional policy hook is not connected to `NativeResidentLedger`; a partial
charge cannot be presented as a general memory ceiling. V1, candidate mutation/
open, and direct builders remain outside this preflight for now.

Next local slice: O(1) root/candidate/tree-ID metadata sidecars and baseline
logical ownership without copying cached capacity through `RootNode::clone`,
then input/sort/plan/closure peak bounds, and only then fail-fast resident lease
handoff for simultaneous old resident + private replacement + retirement.

## Exact ownership of accepted replacement input

After the separate exact root/candidate/tree-ID metadata slice, the next
component ABI covers the owner of paged replacement input before the native
builder starts:

- A new `replacement_input_memory_snapshot()` returns in O(1) the physical
  owner `Vec<FileEntry>`, its length/capacity, actual slot size and backing
  capacity bytes, plus aggregate length/capacity of path Strings. Arithmetic is
  checked, and invalid state is sticky and does not turn into zero.
- The meter is created after successful `try_reserve_exact`. An accepted page is
  added only after all byte/offset/count/parse checks and the actual move into
  the primary Vec; a rejected page does not change the snapshot. A partial feed
  reflects only accepted strings while retaining backing for the declared
  cardinality.
- Deferred cancel transfers the sidecar with `Vec::IntoIter` without copying or
  double ownership. Each row decrements String counters before drop; an empty
  iterator still owns the original Vec backing until a separate cleanup unit.
  A zero-entry owner is likewise not presented as idle empty.
- After the v1/v2 builder starts, input rows are redistributed among sort/group/
  plan containers that this slice does not yet account for. The snapshot
  honestly returns `null` and `other_input_owners_unmeasured`, then becomes exact
  empty again after actual retirement.
- The debug formatter uses an optional ABI, calls the export once, and strictly
  validates own fields, safe integers, nested validity, `length <= capacity`,
  `backing == capacity * slot size`, and matching row/path owner counts. Errors
  are path-free; the component is labeled as an additive subset, not allocator
  total, heap/RSS, or admission.

Native tests include Unicode/astral paths, rejected feeds, full and partial
accepted input, zero-entry owner, transfer, per-row retirement, and separate
backing drop for both tree formats. Host formatter — 27/27, including iteration
over all new numeric/boolean fields with fractional/unsafe/wrong-type values and
nested invalid counters.

The fresh packaged scalar/SIMD gate passed v1/v2 × 25k one-prefix/wide-prefix ×
three full lifecycle cycles, 24/24 total. It now performs empty/partial/full
input cancellation, checks exact owner transfer, remaining row/path bytes and
capacity on every retirement tick, prevents premature empty, and separately
observes Vec backing release. All final requested-live/count values returned to
the fixed warm baseline; canonical scalar/SIMD parity was preserved.

Checks: WASM-feature Rust — 303/303 PASS; workspace — 555 PASS, 1 ignored;
workspace check, fmt, and diff check — PASS with the existing default-feature
dead-code warnings. Full `npm test`, TypeScript, production build, and browser
probe — PASS; generated bindings retained the known `import.meta` warnings.
Fresh scalar/SIMD binaries — 619178/611626 bytes, SHA-256
`100be5baeed8df2761936602c8d447dfe58a5dcd18e1a24ad8b3520ee09e4198` /
`7ee4950a63d3f8babe4f496cd283662b36eac8a154d73f924c4f4b760dbd581c`.
Remote CI and real-device UI/RSS/reload gates were not run.

This is not a general native memory ceiling. Builder-owned indirect-sort arrays,
v1 groups, v2 plan/range/closure, codec/hash/serde scratch, map buckets,
candidate mutation, and direct rebuild funnels are not counted. Therefore,
`NativeResidentLedger` remains disconnected from production admission. The next
bounded local slice is exact ownership of indirect-sort buffers and rows
transferred to them, followed by plan/range/closure owners and conservative
pre-allocation bounds. Only after covering all live/private/retirement
transitions can R+N safely be reserved and fail-fast resident lease handoff be
connected.

## Exact ownership of replacement indirect-sort backing

The next bounded memory slice was implemented in code commit
`responsive-stage-090`
(`feat(sync): account replacement sort backing`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- A new `replacement_sort_memory_snapshot()` shows in O(1) only the top-level
  Vecs actually owned by the active indirect sorter: `values`, `source_indices`,
  and `target_indices`, with owner/length/capacity, native slot size, checked
  backing bytes, and sticky validity. Nested path/label Strings inside
  `FileEntry` and `(String, FileHash)` are not attributed to this component.
- V1 separately marks the entry sorter and child-label sorter; the schema rejects
  simultaneous presence of both. V2 observes the same entry sorter before and
  after its transfer into the internal retirement cursor. The snapshot
  disappears only after the owner is actually dropped, not after sorting
  logically completes.
- Deferred cancellation transfers the exact summary from `replacement` to
  `retiring` without copying or double counting. Cleanup retains the original
  values backing after the last row pop, destroys at most one row per unit, and
  then frees at most one index backing allocation per unit. Native tests verify
  spare capacity, source/target role swaps, empty values with still-live backing,
  and separate entry/child slot sizes.
- Candidate mutation owns other sort worksets. Until they are instrumented, the
  ABI returns `replacement: null` and `other_sort_owners_unmeasured: true`; a
  real packaged mutation job verifies that this gap cannot be represented as
  exact zero.
- The host formatter is optional, path-free, and fail-closed: own fields, safe
  integers, owner cardinality, `length <= capacity`, exact
  `capacity * slot_size`, equal index slot sizes, and all nested validity flags.
  The host diagnostic suite is now 30/30; debug explicitly labels the additive
  scope and excluded owners.

Fresh `scripts/verify-wasm-memory.mjs` passed scalar/SIMD × v1/v2 × 25k
one-prefix/wide-prefix × three lifecycle cycles, 24/24 total. Each corpus now
includes cancellation directly inside entry sort, exact replacement→retirement
transfer, monotonic per-unit row/backing cleanup, and return of requested-live/
count to the fixed baseline 840 bytes / 1 allocation. Separate scalar/SIMD v1
fixtures of 1001 rows reached `sort-internal` with two child labels, passed
cancel/transfer/9-unit retirement, and returned to baseline. Failure cleanup
preserves the real accepted-row oracle and does not mask the verifier's original
error.

Checks: WASM-feature Rust — 306/306 PASS; workspace — 557 PASS, 1 ignored;
workspace check, fmt, staged diff check, TypeScript, production build, full
`npm test`, and browser probe — PASS. Process-group plugin tests were again run
outside the sandbox; the sandbox-only run completed without their child
diagnostics. Fresh canonical scalar/SIMD parity — PASS. Verified binaries —
623419/615974 bytes,
SHA-256 `6f8df41eaa9ac3451afb3fc0c237da9a61b768bae7fee2b669138530f10bcfb3` /
`2aa836adebb1add0e04a1b97e28aa596059ce36206da0c0e324440a48ac949e4`;
all eight WASM/binding artifacts were copied byte-identically into the packaged
directory. Production build/probe retained the known generated-WASM
`import.meta` warnings. The final read-only audit found no P0/P1/P2.

This is not a general heap/RSS ceiling or production admission. Nested Strings
after builder start, v1 groups, sorted v2 entries, plan/range/closure,
candidate-mutation clones, codec/hash/serde scratch, and real map/HashSet
buckets are not yet counted; infallible sort workspace allocations have not yet
been replaced with recoverable pre-reserve. `NativeResidentLedger` remains
disconnected. The next bounded local slice is exact plan/range/closure and
post-sort entry ownership plus conservative fallible pre-allocation before
transferring authoritative input. Only then can R+N lease handoff safely be
proved. Remote CI, real-device UI/RSS/reload, and mobile worker gates were not
run; the user's `temp/`, working vaults, Syncthing, and production were untouched.

## Exact ownership of V2 planning descriptor backing

The next bounded memory slice was implemented in code commit
`responsive-stage-092`
(`feat(sync): account v2 planning backing`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- A new `replacement_v2_planning_memory_snapshot()` shows in O(1) the exact
  top-level backing of six POD `Vec`s in the V2 replacement builder: `spans`,
  `leaf_spans`, `planned_ranges`, `next_planned_ranges`, `planned_spans`, and
  `planned_internal_spans`. Native slot size comes from the actual Rust type;
  the host does not guess it. Entries/path Strings, `RangeRef` endpoints,
  closure sets/maps, and codec scratch are not part of this additive component.
- `clear()` retains capacity and charge. Replacing with `Vec::new()` frees the
  backing, but an active builder still honestly reports the existing empty Vec
  owner. Deferred cancellation takes the snapshot before `mem::take` and
  transfers the same owner from `replacement` to `retiring` without copying or
  double counting.
- Six retirement iterators are released explicitly. One unit removes at most
  one descriptor or frees at most one already-empty Vec owner; after the last
  descriptor, capacity remains observable until a separate release unit.
  Failure, cancel, ready, and successful publish preserve this ownership
  contract until actual drain.
- The debug formatter is optional, path-free, and fail-closed. It calls the
  export once with the original receiver, accepts `null` only with an explicit
  incomplete coverage flag, and strictly validates own data fields, safe
  integers, nested validity, `length <= capacity`, and exact
  `capacity * native slot size`. The host formatter suite is now 36/36.

The fresh packaged memory gate again passed scalar/SIMD × v1/v2 × 25k
one-prefix/wide-prefix × three lifecycle cycles, 24/24 total. An additional
scalar/SIMD V2 probe of 5000 rows switched to budget=1 after sort, actually
observed all six planning Vecs as nonzero, and then performed 5029 unit-step
retirement units. The strengthened verifier limits the sum of destroyed
descriptors and freed owners to the units actually issued; all instances
returned to the fixed warm baseline 1672 requested bytes / 1 allocation. Fresh
canonical scalar/SIMD parity — PASS; the previous v1/v2 roots and full chunk
maps were preserved.

Checks: WASM-feature Rust — 310/310 PASS; workspace — 559 PASS, 1 ignored;
workspace check, fmt/diff check, TypeScript, production build, full
`npm test`, and browser probe — PASS. Process-group suites of the full plugin
gate were run outside the sandbox after the expected sandbox-only failure.
Fresh binaries —
630826/623218 bytes, SHA-256
`a8eac193420759be57f99b06fcffd8fe7406ccc5d3a7a8b33e1c8877b485748b` /
`6fa42ba0bef8d89be8f907d8b5bba2dae8422e4a49143da5ac593a51c2236a24`;
all eight WASM/binding artifacts were copied byte-identically into the packaged
directory. Build/probe retained the known generated-WASM `import.meta` warnings.
The final read-only audit found no P0/P1/P2.

This is still not a general heap/RSS ceiling or production admission. The next
unmeasured owners are sorted V2 entries and their Strings, actual `RangeRef`
endpoint Strings, closure HashSet/HashMap, V1 groups/rows/leaf/children
worksets, candidate-mutation clones, and codec/hash/serde scratch. Fallible
pre-reserve and R+N resident/private/retirement lease handoff are also not yet
connected. Remote CI, native filesystem race, real-device UI/RSS/reload, and
mobile worker/reader gates were not run; working vaults, Syncthing, and
production were untouched.

## Exact ownership of V2 post-sort entries and path Strings

The next bounded memory slice was implemented in code commit
`responsive-stage-094`
(`feat(sync): account v2 post-sort entries`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- A new `replacement_v2_post_sort_memory_snapshot()` reuses the exact
  `ReplacementInputMemory` payload for the original `Vec<FileEntry>` and the
  aggregate actual length/capacity of its path Strings. Production start
  transfers the already-computed accepted-input sidecar into the V2 builder
  without another O(n) scan.
- While `StableSort` owns the same values Vec, the post-sort owner is explicitly
  `null/other_post_sort_owners_unmeasured`; the sort ABI continues to count the
  top-level values/source/target backing, with no double ownership. After
  `finish()`, the sidecar becomes exact without a clone/reallocation gap. V1
  and input-only jobs report exact empty for this V2-only component.
- Actual ownership of entries is stored separately from the diagnostic meter.
  Normal cleanup removes one entry/path allocation per unit, then reports an
  empty Vec with retained capacity, and a separate unit frees the backing before
  `Ready`. Deferred cancellation after sort transfers `Option<IntoIter<_>>` and
  the meter together; variable-length Unicode paths and spare String capacity
  are subtracted according to the entry actually destroyed.
- Cancel inside sort remains nullable while the sorter destroys strings.
  Drained values backing now has its own retirement unit and cannot free the
  first planning owner in the same unit. The verifier similarly fences post-sort
  backing release from changes in neighboring sort/planning components.
- Duplicate validation failure retains the exact owner until explicit deferred
  retirement. The host formatter is optional, path-free, and fail-closed: own
  fields, safe integers, `length <= capacity`, exact
  `capacity * native slot size`, row/path conservation, and all nested sticky
  validity flags. The host suite is now 39/39.

The fresh packaged memory gate passed scalar/SIMD × v1/v2 × 25k
one-prefix/wide-prefix × three lifecycle cycles, 24/24 total. It checks accepted
input against the post-sort owner, normal retained-empty/release/Ready, cancel
during and after sort, duplicate failure, exact replacement→retirement transfer,
and full drain. A separate 5000-row V2 planning probe retained all six actually
nonzero planning Vecs and completed 5030 retirement units. All instances
returned to the fixed warm baseline 1672 requested bytes / 1 allocation. Fresh
canonical scalar/SIMD parity — PASS.

Checks: WASM-feature Rust — 314/314 PASS; workspace — 561 PASS, 1 ignored;
workspace check, fmt/diff check, TypeScript, production build, full
`npm test`, and browser probe — PASS. The full plugin suite was again run outside
the sandbox for real process-group/child diagnostics. Fresh binaries —
634585/626854 bytes, SHA-256
`05eba9186be9412d27c879e4d68628fe29b922a4b885db23d545fbb95de7c697` /
`6053a4469e38ccefbd9a87964956b6b7646a6bb0042d781941fef704b0ad1ead`;
all eight WASM/binding artifacts were copied byte-identically into the packaged
directory. Build/probe retained the known generated-WASM `import.meta` warnings.
Two final read-only audits after fixes found no P0/P1/P2.

This is still not a general heap/RSS ceiling or production admission. The next
unmeasured owners are V2 `RangeRef` endpoint Strings and closure HashSet/HashMap,
V1 input/groups/rows/leaf/children redistribution, candidate-mutation clones,
codec/hash/serde scratch, and real map/HashSet buckets. The next bounded local
slice is V1 original-entry redistribution with exact path-meter transfer,
grouped Vec aggregate, and cooperative release; after that come V2 ranges/
closure and conservative fallible pre-allocation before R+N lease handoff.
Remote CI, real-device UI/RSS/reload, and mobile worker/reader gates were not
run; working vaults, Syncthing, and production were untouched.

## Exact ownership of V1 original-entry redistribution

The next bounded memory slice was implemented in code commit
`responsive-stage-096`
(`feat(sync): account v1 replacement entries`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- A new `replacement_v1_entries_memory_snapshot()` transfers the already-exact
  accepted-input meter into the V1 builder without another O(n) path scan. It
  counts retained input, aggregate backing of all grouped `Vec<FileEntry>`s,
  nested path ownership of the active entry sorter, `rows`, `leaf`, the
  encoded-retirement iterator, and actual length/capacity of all path Strings.
- Top-level sorter backing remains exclusively in the existing sort ABI;
  `sorting_rows` links its values length to path owners without double byte
  charge. An empty live sorter is valid while its source/target/values backing
  is released in separate units.
- Group add/grow/remove/transfer are updated in O(1). At every transition, the
  sum of all V1 row owners equals the number of path Strings; differing
  FileEntry slot sizes, an impossible owner/capacity aggregate, and overflow/
  underflow fail closed and cannot later spontaneously become valid.
- Normal build explicitly releases exhausted input/rows/encoded backing before
  Ready. Deferred cancel destroys one entry/path or one backing owner per unit.
  Physical `Option`/Vec owners control cleanup independently of diagnostic
  validity. `total_files` overflow is checked before extracting the group.
- After drain, the child sorter now consumes a separate tail-release unit itself.
  The packaged mixed fixture retains the next group of seven rows in parallel
  and proves that the sorter tail does not change V1 entries, V2 post-sort/
  planning, chunks, or metadata. The native oracle records retained capacity/
  backing until actual drop, Unicode/spare String capacity, failure, and
  cancellation.
- The host formatter is optional, path-free, and fail-closed. The verifier checks
  strict schema/own safe fields, row/path conservation, and sums only
  non-overlapping exact backing components; their sum must fit within the more
  general WASM requested-live sample.

The fresh packaged memory gate passed scalar/SIMD × v1/v2 × 25k
one-prefix/wide-prefix × three lifecycle cycles, 24/24 total, including exact
replacement→retirement transfer, partial/failure/ready cancel, publication, and
full drain to the fixed baseline 1672 requested bytes / 1 allocation. The
separate mixed child-sort fixture has 1008 rows, two child values, and seven
still-live rows in the second group. Fresh canonical scalar/SIMD parity — PASS.

Checks: targeted V1 Rust — 13/13 PASS; WASM-feature Rust — 320/320 PASS;
workspace — 566 PASS, 1 ignored; workspace check, fmt/diff check, TypeScript,
host formatter 42/42, production build, full `npm test`, browser probe, and
canonical parity — PASS. Clippy: 0 errors, 142 existing/shared warnings.
Fresh binaries — 644046/636263 bytes, SHA-256
`4aac56c342c26ad95efb66f94df8bd2bc749807c1a96db1b85027907bbd0f853` /
`c68552458ad62939d4fe73c361a44eebbe7bc37bcb60ee20e4ddc75620808eab`;
all eight WASM/binding artifacts were copied byte-identically into the packaged
directory. `install` on the Windows mount reported the expected chmod `EPERM`
after writing; the subsequent SHA-256 check confirmed byte equality. Build/probe
retained the known generated-WASM `import.meta` warnings. Two final read-only
audits after fixes found no P0/P1/P2.

This is still not a general heap/RSS ceiling or production admission. V1
BTreeMap nodes/prefix keys, child/root labels and hash vectors, V2 `RangeRef`
endpoint Strings and closure HashSet/HashMap, candidate-mutation clones,
codec/hash/serde scratch, and actual allocator/map buckets are not measured.
The next bounded local slice is either the remaining V1 graph workset or V2
ranges/closure with conservative fallible pre-allocation before R+N lease
handoff. Remote CI, native filesystem race, real-device UI/RSS/reload, and
mobile worker/reader gates were not run; working vaults, Syncthing, and
production were untouched.

## Exact ownership of V1 graph assembly

The remaining V1 graph workset was implemented in code commit
`responsive-stage-098`
(`feat(sync): account v1 graph assembly`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- A new `replacement_v1_graph_memory_snapshot()` exactly counts physically held
  graph owners: group keys, current prefix, generated child/root labels,
  builder vault/device IDs, and the backing of leaf hashes, moved hash iterator,
  unsorted children, encoded-retirement children, and root children.
- Active child-sorter backing still belongs only to the replacement-sort ABI.
  `sorting_children` is a logical link, so the same allocation cannot appear in
  both graph and sort totals.
- Immediately after assembly, the provisional root is transferred to the
  existing metadata ABI; the graph owner is cleared at the same point. Tests
  record exact graph→metadata and replacement→retirement handoff without double
  counting or a gap.
- All transitions and retirement sidecars are updated in O(1). An exhausted
  iterator retains backing until a separate physical drop; cancellation frees
  one String, hash, child, or backing owner per unit. Sticky-invalid telemetry
  does not control cleanup: actual Rust owners are destroyed even after a
  counter error.
- The host parser and debug output remain path-free, strict-schema, and
  fail-closed. The packaged verifier checks the additive component fence, no
  overlap between graph and sorter/metadata, V2-empty states, and bounded
  transfer, cancellation, and retirement for all graph Vec states and the
  separate child-sort fixture.

The fresh packaged memory gate passed scalar/SIMD × v1/v2 × 25k
one-prefix/wide-prefix × three lifecycle cycles, 24/24 total. Additional
fixtures: child sorter 1008 rows and V2 planning 5000 rows. After full drain,
all eight instances returned to the baseline 1672 requested bytes / 1
allocation; allocation/reallocation failures — 0. Fresh canonical scalar/SIMD
parity — PASS.

Checks on frozen source: targeted graph Rust — 3/3 PASS; WASM-feature Rust —
323/323 PASS; workspace — 568 PASS, 1 ignored; cargo check, fmt/diff check,
TypeScript, host formatter 45/45, main lifecycle 17 scenarios / 149 assertions,
production build, full `npm test`, browser probe, and canonical parity — PASS.
Clippy: 0 errors, 142 existing/shared warnings. Fresh scalar/SIMD binaries —
658340/650683 bytes, SHA-256
`79477f8608756624e1f3f742411368b5fc364ea2c3be94c745313b7a731615ff` /
`0ee74fc494239a284b59aed6095718afc65fdff46df5e58293b6527ece9daf6a`;
all eight WASM/binding artifacts were copied byte-identically into the packaged
directory. The known generated-WASM `import.meta` warnings remain. The final
read-only audit found no P0/P1/P2.

This is still not a general heap/RSS ceiling or production admission. V1
BTreeMap nodes and the reachability closure are not measured; group keys and
child/root labels are now measured. V2 `RangeRef` endpoint Strings, closure/map
buckets, candidate-mutation clones, codec/hash/serde scratch, and actual
allocator/map buckets also remain unmeasured. The next bounded local slice is
V2 ranges/closure: first a move-only closure without clone amplification, then
exact endpoint/backing reservation. It can authorize only explicitly bounded
endpoint/node admission, not general heap admission. Remote CI, native
filesystem race, real-device UI/RSS/reload, and the device matrix were not run;
working vaults, Syncthing, and production were untouched.

## Exact ownership of V2 replacement ranges and closure

The next bounded memory slice was implemented in code commit
`responsive-stage-100`
(`feat(sync): account v2 range worksets`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- The replacement closure now expands move-only: child `RangeRef`s move from
  `IntoIter` to pending without cloning endpoint String buffers. Exhausted
  pending/expanding backing is freed by a separate cooperative unit during both
  normal cleanup and deferred retirement.
- A new `replacement_v2_ranges_memory_snapshot()` exactly counts the backing of
  four physical Vec owners (`ranges`, `next_ranges`, `closure_pending`,
  `closure_expanding`) and actual length/capacity of all retained min/max
  Strings. Map descriptors are counted logically and their endpoint Strings
  exactly; bucket and inline descriptor bytes are intentionally not attributed
  to the component.
- Leaf/internal emission, range retirement, closure expansion/pending/map,
  cancellation, and replacement→retirement handoff update the sidecar in O(1).
  An empty but still allocated Vec remains visible until physical release; a
  zero-capacity Vec is treated as absent. Checked arithmetic and nested String
  validity are sticky fail-closed.
- During root handoff, the original child RangeRef leaves the range component and
  moves into the metadata ABI. A separate clone required for closure validation
  remains separately accounted in `closure_pending`; the transfer creates
  neither double count nor an undeclared gap.
- `RootBuildRetirement` now destroys child/vault/device String owners separately
  and boundedly. Even an empty device ID is transferred as a real owner and gets
  its own retirement unit. A regression test also records that numerical zero
  after cleanup does not restore previously lost validity.
- The host formatter is optional, path-free, and strict: only own data fields,
  with no accessors/inherited/extra keys, safe-integer arithmetic, exactly two
  endpoints per physical descriptor, and sticky native validity. Debug output
  reports scope boundaries; the host suite is now 52/52.

The fresh packaged verifier passed scalar/SIMD × v1/v2 × two 25k corpora ×
three lifecycle cycles, 24/24 total. In all three V2 repetitions it observed
four range vectors, root transfer, descriptor peak, move-only expansion, and
retained empty pending/expanding tails. Scalar/SIMD returned to the fixed warm
baseline 1672 requested bytes / 1 allocation; allocation/reallocation failures
— 0. Worst-case wide-prefix V1 took 190.778/195.607 s, V2 — 10.068/10.174 s;
one-prefix V1 — 6.791/6.587 s, V2 — 10.820/10.133 s. Additional probes:
1008-row V1 child sort and 5000-row V2 planning. Fresh canonical scalar/SIMD
parity — PASS.

Checks on frozen source: WASM-feature Rust — 335/335 PASS; workspace —
579 PASS, 1 ignored; cargo check — 0 errors / 126 warnings; clippy — 0 errors /
143 existing/shared warnings; fmt/diff check, TypeScript, host formatter 52/52,
main lifecycle 17 scenarios / 149 assertions, production build, full `npm test`,
browser probe, and canonical parity — PASS. Fresh scalar/SIMD
binaries — 671112/663195 bytes, SHA-256
`f4d22be5e5285726131f12cda2dc383abc090617f5fd6fdf686ffd5eef87d14d` /
`3a32dc9865545b12480946ccc14277e176a014b65a0817a833dc9d56267e44e6`;
all eight WASM/binding artifacts were copied byte-identically into the packaged
directory. Build/probe retained the known generated-WASM `import.meta` warnings.
Two final read-only audits after fixes found no P0/P1/P2.

This is still not a general heap/RSS ceiling or production admission. HashMap/
HashSet buckets and inline descriptor bytes, the temporary node clone of at
most 256 KiB from `MemoryChunkStore::get_chunk`, endpoint clones in
`descriptor_for_node`, clone amplification of the shared `V2ReachabilityCursor`,
parent-boundary endpoint clones, candidate-mutation range owners, allocator,
serde, and codec scratch are not measured. The next bounded local slice is a
move-only shared `V2ReachabilityCursor` and allocation-free descriptor
validation scratch; then parent-boundary move and exact scoped admission, but
not a claim of a complete heap bound. Remote CI, native filesystem race,
real-device UI/RSS/reload, and the device matrix were not run; working vaults,
Syncthing, and production were untouched.

## Move-only shared V2 reachability cursor

The next allocation-amplification slice was implemented in code commit
`responsive-stage-102`
(`feat(sync): move v2 reachability ranges`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- The shared `V2ReachabilityCursor`, used by candidate-open,
  candidate-chunk-plan, and ordinary `reachable_hashes`, now stores decoded
  child expansion as `Vec::IntoIter<RangeRef>`. Each child moves to pending
  without cloning min/max String buffers; direct transfer order, subsequent
  LIFO validation order, and the existing work units are preserved.
- `descriptor_for_node`, which created a temporary `RangeRef` and cloned two
  endpoint Strings for every loaded node, was replaced with a borrowed
  validator. It still fail-closed checks min/max, checked file count, serialized
  bytes, and height, while the content hash is checked before decode. The
  returned `V2Node` and its ownership were unchanged.
- A Rust regression moves decoded children first with one unit, then the
  remainder, and compares pointer/capacity for each endpoint buffer. Budget
  1/256 produces not only the same reachable set but also the same cumulative
  work. A separate mismatch matrix covers leaf/internal min, max, count, height,
  and byte length.
- The packaged-WASM allocator probe warms reporting and the first progress
  codec, then isolates exactly one internal-root validation step. On scalar and
  SIMD fixtures with four root children, it produced exactly 14 = `2N + 6`
  successful allocations and 0 reallocations. The former temporary descriptor
  would have added two more String allocations and failed the gate. The same
  probe verifies cancellation after one moved child, complete traversal in
  10 = `2 × 5 nodes` units, a ready no-op without allocations, and return to the
  resident/final baseline.
- The verifier completion loop has an exact finite guard; cleanup always calls
  `tree.free()` even if the native step already removed the failed job and
  cancel returned an error. The cleanup error does not mask the primary error.

The fresh packaged verifier passed scalar/SIMD × v1/v2 × two 25k corpora ×
three lifecycle cycles, 24/24 total, plus the new scalar/SIMD 2000-row
reachability probe, 1008-row V1 child sort, and 5000-row V2 planning. All full
corpus instances returned to the fixed warm baseline 1672 requested bytes / 1
allocation; allocation/reallocation failures — 0. One-prefix V1 took
6.631/6.541 s, V2 — 10.035/9.908 s; wide-prefix V1 — 192.838/195.093 s, V2 —
10.021/10.071 s. Fresh canonical scalar/SIMD parity — PASS.

Checks on frozen source: tree-v2 Rust — 18/18 PASS; WASM-feature Rust —
337/337 PASS; workspace — 581 PASS, 1 ignored; cargo check — 0 errors /
126 warnings; clippy — 0 errors / 143 existing/shared warnings; fmt/diff check,
TypeScript, host formatter 52/52, main lifecycle 17 scenarios / 149 assertions,
production build, full `npm test`, browser probe, and canonical parity — PASS.
Fresh scalar/SIMD binaries — 670707/662853 bytes, SHA-256
`49bcb737fe900e75747b839e79de2c5231aa86c79aae94fc70e2e8a82d26e85a` /
`ffc9c092f22114ead359ba14589451e7bc6dd007fcc4ae0382506d6f684c6bba`;
all eight WASM/binding artifacts were copied byte-identically into the packaged
directory. Build/probe retained the known generated-WASM `import.meta` warnings.
The final read-only audit found no P0/P1/P2.

This does not mean an allocation-free node step: decode, node-byte clone,
pending growth, and hash tables still allocate memory. The root seed clone
remains a separate owner from resident metadata. The most important remaining
limitation is that `finish`, `cancel`, and execution error of the shared cursor
can still synchronously destroy the entire descriptor map/reachable set (up to
the protocol cap), while candidate-chunk finish still synchronously assembles
and sorts hash arrays. The next bounded local slice is deferred retirement of
the shared reachability cursor through an owner-local job slot and host drain
contract; only then can work proceed to parent-boundary endpoint moves/scoped
admission. This is still not a general heap/RSS ceiling or production admission.
Remote CI, native filesystem race, real-device UI/RSS/reload, and the device
matrix were not run; working vaults, Syncthing, and production were untouched.

## Cooperative retirement of the shared reachability cursor

The deferred/bounded cleanup slice was implemented in code commit
`responsive-stage-104`
(`feat(sync): retire reachability jobs cooperatively`). Version, Nix pins,
tag/release, remote push, and deployment were unchanged.

- V1 and V2 reachability cursors transfer all large owned worksets to a separate
  retirement owner. `finish`, `cancel`, and execution error no longer have to
  synchronously destroy all descriptor/reachable state; V2 separately frees
  pending descriptors, hashes, and iterator backings, at most one physical
  owner per work unit.
- The WASM deferred ABI retains the same job token through complete cleanup:
  deferred step/cancel/finish publish the terminal result and open replay-safe
  retirement; `step_reachability_retirement` accepts an exact completed witness,
  while `finish_reachability_retirement` is a strict acknowledgement/tombstone.
  Generic tree retirement and reachability retirement are isolated; stale/wrong
  tokens and repeated terminal calls are tested.
- The host retains a strong owner for exact tree/token/API/candidate revision,
  forbids new work and `free()` until complete drain, and uses a separate
  uncancellable host yield independent of operation signal/visibility gate.
  Concurrent callers join one cleanup flight; direct synchronous callback
  reentry is rejected. Permanent cleanup failure intentionally leaves the
  wrapper quarantined rather than losing the native owner.
- Candidate rollback is transferred as exact revision debt and never aborts a
  newer candidate. Push records the revision immediately after each successful
  native mutation; a missing legacy reader and a throwing exact reader are
  distinguished, so an indeterminate revision does not cause blind abort.
  Lifecycle retirement now drains reachability, then root replacement, and only
  then calls `free()`.
- CI now separately and obligatorily runs
  `cargo test -p sync-core --features wasm --locked`. Packaged parity checks
  scalar/SIMD × v1/v2, deferred finish/cancel/error, same-token replay, family
  isolation, absence of publication/revision/root drift, and a real V2
  Internal-root transition with a partially transferred child list before
  cancel.

Checks on frozen source: WASM-feature Rust — 341/341 PASS; workspace —
582 PASS, 1 ignored; cargo check — 0 errors / 130 warnings; clippy — 0 errors /
147 existing/shared warnings; fmt/diff check, local TypeScript, 97 connected
source-test suites, main lifecycle 20 scenarios / 208 assertions, production
build, full `npm test`, browser probe, packaged memory gate, and canonical
scalar/SIMD parity — PASS. The memory gate passed 24/24: scalar/SIMD × v1/v2 ×
two 25k corpora × three lifecycle cycles, returning to the warm baseline 1672
requested bytes / 1 allocation with no allocation/reallocation failures.
Fresh binaries — 688782/675916 bytes, SHA-256
`1648a5d903148ab0b92b16e47284208e8e729c70f5e92fbfae1112b8c4bc2466` /
`157d362d35de5e8eddd925f20e78d83b75fb0c6733a2ce016254fcb11cdc6e8b`;
all eight WASM/binding artifacts in the packaged directory are byte-identical
to the fresh build. `main.js` — 3462589 bytes, SHA-256
`46a5229401bb4426cc37818a2bdec4b7c54272201ca8232599fce54df86515ff`.
The known generated-WASM `import.meta` warnings remain. Final independent
reviews found no P0/P1/P2.

Residual synchronous sections are not hidden: candidate begin still clones the
root/all-resident baseline; chunk-plan finish sorts, creates fresh/hex
representations, and serializes the result; V1 iterator backing and a separate
V2 `RangeRef` endpoint String are destroyed atomically within one unit. This is
not a general JS/WASM heap or RSS ceiling and not production admission. A
cleanup callback must not asynchronously await its own drain. The next bounded
local slice is move-only parent-boundary endpoints and exact scoped admission
for already measured owners, after which general admission can be expanded
without falsely promising a complete heap bound. Remote CI, native filesystem
race, real-device UI/RSS/reload, and the device matrix were not run; working
vaults, Syncthing, and production were untouched.

## Move-only V2 parent boundaries

The slice eliminating unnecessary endpoint copies was implemented in code
commit
`responsive-stage-106`
(`perf(sync): move v2 parent endpoints`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- After all fallible encode/hash/validation/store operations, the generic V2
  builder, replacement rebuild, and candidate mutation now move `min_path` and
  `max_path` from private child descriptors into the parent through native
  ownership transfer. An unsuccessful stage does not consume boundary owners
  and preserves safe retry/cancel.
- The replacement sidecar exactly accounts for actual moved-from empty `String`
  values until their bounded retirement: the number of String owners is
  preserved, while endpoint length/capacity move to the parent without double
  counting. Cancellation immediately after partial parent emission frees donors
  and parents by units and returns counters to zero.
- Direct and stepped candidate updates no longer clone and discard the entire
  candidate root. Vault/device identity buffers remain the same; fallible tree
  computation and cursor finish complete before in-place install. The
  `no active candidate` contract is preserved for an empty update as well.
- Native Unicode regressions verify exact pointer/capacity reuse and failure
  atomicity. The packaged memory verifier separately records preservation of
  endpoint ownership during parent emission and cancellation at this
  checkpoint; native pointer tests prove the move, while packaged end-state
  honestly proves only preservation of ownership/capacity.

Checks on frozen source: WASM-feature Rust — 345/345 PASS; workspace —
586 PASS, 1 ignored; cargo check and clippy — 0 errors; fmt/diff check,
JavaScript syntax, local TypeScript, main lifecycle 20 scenarios / 208
assertions, production build, full `npm test`, browser probe, fresh packaged
memory gate, and canonical scalar/SIMD parity — PASS. The memory gate passed all
scalar/SIMD × v1/v2 × one/wide-prefix lifecycle cycles, returned to the warm
baseline 1672 requested bytes / 1 allocation, and recorded no allocation/
reallocation failures. Fresh binaries — 688408/675728 bytes,
SHA-256
`e02995379426556137abdb8537555fe4c913b93945b367112a7a5571589a33de` /
`2851e21a3876560324865139cb43306d5002fae0a0c64652f111e97d9f4f2f17`;
all eight WASM/binding artifacts in the packaged directory are byte-identical
to the fresh build. `main.js` — 3461598 bytes, SHA-256
`bc95b81fd98341a146bbfc0ffc57467b1224f117187e7e6c696dc7291c49c7d3`.
The known generated-WASM `import.meta` warnings remain. Three independent
read-only reviews found no P0/P1/P2.

This slice does not claim a general heap/RSS ceiling or production admission.
Leaf descriptors and closure decode still create endpoint buffers; Vec/HashMap
backing, empty String structs, input/sort, scratch, allocator overhead, JS heap,
and WASM linear memory remain outside the future bound. The next slice is a
versioned conservative V2 replacement-output requested-buffer plan and a
separate fail-fast `NativeResidentLedger`: the lease must survive cancel/error,
deferred retirement, and simultaneous ownership of old+new tree, and be released
only after proven drain/free. Remote CI, native filesystem race, real-device
UI/RSS/reload, and the device matrix were not run; working vaults, Syncthing,
and production were untouched.

## Scoped admission of V2 replacement output

The exact fail-fast admission slice was implemented in code commit
`responsive-stage-108`
(`feat(sync): admit v2 replacement output`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- An additive versioned WASM ABI stops V2 replacement at `PlanReady` before the
  first output allocation and returns seven strictly validated fields. `P` is
  exact serialized node payload, `D` is UTF-8 endpoint requests for all emitted
  descriptors, and `R` is resident root-child endpoints; admission uses
  `peak=P+D+R`, and `resident=P+R` remains after Ready. All sums are checked
  `u64`; a stale token/phase or any of the three witnesses does not change the
  job or create output.
- Leaf/internal buffers and endpoint copies first make a recoverable exact-size
  request. An independent verifier decodes the canonical `OVR2`/`OVI2`/`OVL2`
  graph, computes `P/D/R` itself, and compares scalar/SIMD and budgets 1/256
  without trusting native counters or the plan.
- A separate `RootTreeResidentAdmission` counts only this versioned
  requested-buffer region. The old resident and full new peak are counted
  simultaneously; peak-only bytes are released only at Ready, the new resident
  is published before fallible post-checks, an old or cancelled private owner is
  released only after proven deferred drain, and the current resident only
  after successful native `free()`.
- The cleanup slot and output capability are preallocated/registered before
  native begin/publication/detach. An active slot is not yet drainable; a
  concurrent repair cannot overwrite another owner. Cancellation/free errors
  leave a strong quarantine for retry. V2→V1 replacement transfers the prior
  tracked resident through a zero-byte lifecycle without presenting V1 output
  as measured.
- A repeated automatic admission refusal is cached only by exact refusal-time
  base/tree/ledger facts: an unchanged interval does not repeat load/feed/sort/
  plan or pollute error history. Sync Now and Full Rescan explicitly allow retry;
  capacity/used/base/tree changes automatically clear the cache. A capacity
  change during cleanup cannot preserve a stale refusal.
- The production profile defines an independent resident-ledger capacity, and
  debug honestly reports scoped private/resident/retiring requested bytes.
  Unload closes new admissions; reinit/free releases allowance only after all
  native drains and a successful destructor return.

Checks on frozen source: WASM-feature Rust — 346/346 PASS; workspace —
587 PASS, 1 ignored; cargo check and mandatory clippy — 0 errors; fmt/diff check,
JavaScript syntax, TypeScript, root-repair 27 suites, resident-ledger 11 groups,
actual-engine/root 20 suites, main lifecycle 20 scenarios / 211 assertions,
production build, full `npm test`, browser probe, fresh memory gate, and packaged
canonical parity — PASS. The memory gate passed 24/24 scalar/SIMD × v1/v2 ×
one/wide-prefix lifecycle cycles, returned to the warm baseline 1672 requested
bytes / 1 allocation, and recorded no allocation/reallocation failures. Fresh
binaries — 695698/682471 bytes,
SHA-256
`0227c1b0946a34ef8230bfd2c87a74c3cfad9c389c99dc94863c72d43bb24da4` /
`ce71d6a348d9ee15c465cfedf19f07d7fd48e68909d21e8fa38cd86df5164bdf`;
all eight WASM/binding artifacts in the packaged directory are byte-identical
to the fresh build. `main.js` — 3507341 bytes, SHA-256
`2122c10458182b0e59a42b8ece2a7b00d4e4f8623227d531c7fbacd7e6b30306`.
The known generated-WASM `import.meta` warnings and existing clippy warnings
remain; an additional experiment with `-D warnings` predictably rejected them,
but the mandatory roadmap command without raising warning policy passed. Three
independent final read-only reviews found no P0/P1/P2.

The scope is intentionally narrower than a complete memory bound: it counts
requested node payload and endpoint buffers only for replacement cohorts created
through the new ABI. The initial/pre-existing graph, root IDs/serialized root,
Vec/HashMap backing, empty String structs, input/sort/codec/hash scratch,
allocator overhead, subsequent candidate/push/pull growth, JS heap, WASM linear
memory, and RSS are excluded. Fatal host OOM is not declared recoverable; a
violated native/host ownership contract remains fail-stop quarantined. The next
bounded local slice is versioned admission/adoption of the initial V2 committed
graph before activation, with the same old+new/free proofs; coverage can then be
expanded to candidate and pull/push mutations. Remote CI, native filesystem
race, real-device UI/RSS/reload, and the device matrix were not run; working
vaults, Syncthing, and production were untouched.

## Admitted first-pull rebuild and durable base fence

The safe initial-tree slice was implemented in code commit
`responsive-stage-111`
(`feat(sync): admit first-pull tree rebuilds`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- The first pull no longer activates an empty or V2 tree synchronously built in
  `pull.ts`. The engine accepts the applied sync-base and builds the committed
  graph through the same resumable replacement job, exact output admission, and
  deferred retirement as recovery. A server-root read error now also falls
  within the operation-cleanup boundary and does not leave the engine at
  `syncing=true`.
- The observed remote head is separated from the verified diff fallback. A
  deferred or refused first-pull rebuild does not record the target root as
  locally applied, so the next pull does not skip files that have not yet been
  received. A cross-format V1↔V2 target is preserved through admission/
  refinement failure; the refusal witness includes the required tree version.
- The sync-base WAL gained a durable `base-fence`: the first nonempty legacy diff
  and first snapshot page set `verifiedBaseRequired` before files are applied.
  The fence survives cold load and is cleared atomically only with verified
  parent/root publication. Push is fail-closed even for an empty sync-base when
  remote state is known but no verified parent exists.
- Pull explicitly counts `remoteOmissionCount`. An ignored remote upsert does not
  allow base adoption, does not advance the persisted snapshot cursor past the
  omitted entry, and does not become a local deletion on first publication.
  Such bootstrap intentionally remains closed until there is an agreed model
  for preserving excluded remote leaves.
- During V1→V2 publication, the prepared transfer scope is recomputed before the
  repair gate is cleared. If scope refresh fails after native publication, retry
  retains old+new resident ownership, repeats admission, and only then permits
  ordinary pull/adoption.

Checks on frozen source: `git diff --check`, TypeScript, production build, full
`npm test`, and focused pull, actual-engine/root, sync-base compatibility,
migration/publication, paged-pull, root-repair, and engine-root-plan suites —
PASS. The actual-engine regression covers a nonempty fresh V2 first pull:
PlanReady refusal → durable fence without root ACK → native V2 publication →
prepared-scope error → repeated old+new admission → V2 scope refresh → ordinary
pull/adoption. Two independent read-only reviews found no remaining P0/P1.

This slice does not claim admission for all tree mutations. Full Rescan still
builds the committed graph directly from the pre-scan sync-base cut; initial
push and the existing pull candidate-tree update also remain separate growth
owners. Review/index/queue backing, codec/sort scratch, worker heaps, JS heap,
WASM linear memory, and a general RSS ceiling are not yet covered. The next
bounded local slice moves the Full Rescan rebuild of the exact sync-base cut
under resumable replacement/output admission while preserving bulk approval,
the live-edit journal, and parent/base invariants. Remote CI, native filesystem
race, real-device UI/RSS/reload, and the device matrix were not run; working
vaults, Syncthing, and production were untouched.

## Admitted Full Rescan rebuild

The Full Rescan slice was implemented in code commit
`responsive-stage-113`
(`feat(sync): admit full rescan rebuilds`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- The direct `allPaths().map → JSON.stringify → build_from_entries` was removed
  from Full Rescan. Before stat/hash, the committed tree is rebuilt from an
  immutable sync-base cut through paged replacement, V2 PlanReady output
  admission, old+new resident ownership, and deferred retirement. The rebuilt
  semantic root is not presented as a verified parent: the engine retains
  `capturedBaseRoot`.
- A pending root outcome is still resolved before the operation checkpoint and
  any scan-body actions. If entry recovery already built a cold or interrupted
  graph, Full Rescan does not perform a second 25k rebuild. A separate cold
  regression permits capacity only for the initial peak and proves exactly one
  begin/finish before stat.
- The full-scan checkpoint no longer materializes `statBulk()` only for text: it
  uses an O(1) tracked count, while the full vault map is built once after the
  admitted rebuild. Runtime recovery also checks the resource/visibility gate
  between feed/build units; mandatory retirement after abort/stop remains
  ungated and guarantees owner release.
- `pushBlocked` is cleared only after a complete successful stat/hash/delete scan
  and durable flush. Admission, stat, or hashing failure restores the previous
  publish block and bulk-review requirement. Pending native retirement is
  excluded from the recovery fast path.
- Manual Full Rescan retry clears the exact refusal cache; an unchanged automatic
  retry does not repeat feed/sort/plan. The no-runtime compatibility path uses
  the same bounded helper and, after successful retry, clears its own repair gate
  instead of remaining permanently paused.
- The actual-engine lifecycle holds the new Full Rescan after output admission,
  records live N+1 through the vault callback, and calls `stopAndDrain()`. Stop
  waits for the native owner; after abort there is no stat/root/ACK, private/
  retiring charges are zero, and the old resident and new WAL/dirty generation
  are preserved.

Checks: `git diff --check`, TypeScript, production build, root-repair 27 suites,
resident-ledger 11 groups, actual-engine/root 28 suites, engine-lifecycle 59
assertions, startup-replay 65 assertions, bundled main lifecycle 20 scenarios /
211 assertions, and full `npm test` with explicit `exit_code=0` — PASS. The known
generated-WASM `import.meta` warnings remain. The final independent read-only
review found no P0/P1.

The scope remains narrower than a general memory/RSS bound: stat/toHash/deleted/
sample arrays, review/index/queue backing, candidate mutations after the scan,
codec/sort scratch, worker heaps, JS heap, and WASM linear memory are excluded.
The next direct owners are reconcile bootstrap, initial push bootstrap, and the
existing push/pull candidate mutations; candidate growth specifically requires
a separate pre-allocation model, not moving the old direct call under the label
of replacement admission. Remote CI, native filesystem race, real-device UI/
RSS/reload, and the device matrix were not run; working vaults, Syncthing, and
production were untouched.

## Admitted reconcile bootstrap

The reconcile bootstrap slice was implemented in code commit
`responsive-stage-115`
(`feat(sync): admit reconcile tree bootstrap`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- Reconcile no longer materializes the entire sync-base through
  `allPaths().map → JSON.stringify → build_from_entries`. An empty committed
  graph is rebuilt through paged replacement, V2 PlanReady output admission,
  old+new resident ownership, and deferred retirement before content repair is
  enumerated. The honest parent remains `capturedBaseRoot`; reconcile does not
  publish a root or clear the existing `pushBlocked`.
- The compatibility engine without a durable root runtime retains the repair
  version after refusal and clears the gate only after a successful admitted
  retry. Cache/root summary are updated after full validation of the replacement
  result.
- Incomplete retirement can no longer be skipped because the root is already
  nonempty. `rootRepairRequired` and the strong pending-retirement owner each
  independently route repeated reconcile into the helper even with a nonempty
  graph or empty base.
- The regression publishes a V2 replacement, crashes the first retirement, and
  verifies quarantine. The next reconcile first finishes draining the same
  token 201, then starts token 202, releases the retiring owner, retains one
  resident allowance, and only afterward performs content checks.

Checks: `git diff --check`, TypeScript, production build, root-repair 27 suites,
reconcile-upload 66 assertions, actual-engine/root 30 suites, and full `npm test`
with explicit `exit_code=0` — PASS. The known generated-WASM `import.meta`
warnings remain. The repeated independent P0/P1 review after fixing
post-publication retirement retry — PASS.

The scope is still not a general memory/RSS bound: content-index arrays,
candidate mutations, codec/sort scratch, worker heaps, JS heap, and WASM linear
memory are excluded. The next direct owners are initial push bootstrap and
compatibility bootstrap in pull; after that, a separate admission/retirement
model is required for actual push/pull candidate graph growth. Remote CI, native
filesystem race, real-device UI/RSS/reload, and the device matrix were not run;
working vaults, Syncthing, and production were untouched.

## Admitted initial push bootstrap

The initial push bootstrap slice was implemented by code commit
`responsive-stage-117`
(`feat(sync): admit initial push bootstrap`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- The production engine no longer builds a parentless committed graph through
  `allPaths().map → JSON.stringify → build_from_entries` inside push. Before the
  candidate, it invokes an engine-owned asynchronous hook that takes the
  sync-base cut through paged replacement, V2 output admission, and deferred
  retirement. Direct callers without the hook retain the previous compatibility
  fallback.
- The parentless-publication flag is captured before graph preparation. Thus the
  first push checks all candidate chunks, not only the fresh subset, and publishes
  an honest empty parent. A hook error does not open a candidate, fall back to a
  compatibility rebuild, or reach content/index/root work.
- An unchanged output-admission refusal is cached by the exact base/tree/ledger
  witness: periodic retry does not repeat feed/sort/Plan or duplicate the same
  error. Capacity, tree/base facts, Sync Now, or Full Rescan permit another
  attempt; WAL/dirty remain pending.
- A late retirement failure preserves the exact native token and resident charge.
  The next recovery first retries the same token, then opens a new replacement.
  Parentless recovery does not write a cached root: the locally built graph does
  not become remote authority before server acceptance. A regression also breaks
  source stat after recovery and verifies cold persisted state:
  `treeBaseRoot=null`, journal unsynced, no root intent, and no advance of
  cache/base/local root.
- Stop after the bootstrap hook returns rechecks the operation signal and does
  not open a candidate. Together with the Full Rescan native
  cancellation/retirement matrix, this covers the new lifecycle call site and
  preservation of the parentless WAL.

Checks passed: `git diff --check`, TypeScript, the production build, 509 push
transaction assertions, 27 root-repair suites, 32 actual-engine/root suites, and
the full `npm test` with explicit `exit_code=0`. Known generated-WASM
`import.meta` warnings remain. Two independent final reviews found no P0/P1.

The scope remains narrower than a shared memory/RSS bound. Candidate graph
growth after baseline admission, candidate/root export buffers, review/index
arrays, codec/sort scratch, worker heaps, JS heap, and WASM linear memory still
lack a unified pre-allocation/retirement model. Non-blocking P2 coverage gaps are
a positive rootless test of the direct no-hook compatibility fallback and the
deferred chunk-job variant of parentless all-chunk enumeration. The production
engine always passes the hook, and the all-vs-fresh selector is shared. The next
local slice is to separately design and introduce admission for actual push/pull
candidate-graph growth without presenting the replacement baseline ledger as a
complete bound. Remote CI, native filesystem race, real-device UI/RSS/reload,
and the device matrix were not run; working vaults, Syncthing, and production
were untouched.

## Admitted V2 push candidate mutations

The V2 candidate mutation output slice was implemented by code commit
`responsive-stage-119`
(`feat(sync): admit v2 candidate mutation output`). Version, Nix pins,
tag/release, remote push, and deployment were unchanged.

- V2 update/delete no longer begin encoding a new candidate graph without host
  admission. Before the first output allocation, the native job reaches strict
  `PlanReady` and reports the exact scoped peak: node payload plus the maximum
  simultaneously live range endpoints. Continuation accepts the same witness;
  wrong token, budget, phase, or numbers fail closed and retain the owner for
  deferred cleanup.
- After construction, `Ready` reports actual retained output: only the node
  payload actually staged after resident dedup plus root endpoints. The host
  shrinks the peak reservation to actual and, after native publication, attaches
  the cohort to the existing resident tree without replacing the baseline
  allowance. Resident-equal nodes are not cloned into the staged overlay and
  produce zero staged payload.
- Promotion of staged chunks is two-phase: fallible hash/collision/map-reserve
  preflight does not mutate the overlay; the subsequent move has no fallible
  work. Finish/cancel transition into bounded same-token retirement. Exact abort
  debt and the published owner survive cleanup error, and lifecycle drains run
  before the next bootstrap, replacement, and `free()`.
- Push uses admission for both delete and update. Candidate revision is checked
  at both mutation boundaries and after the final retirement yield: a foreign
  newer candidate revision is not aborted by the old owner. Initial push first
  drains earlier mutation debt and only then prepares the committed bootstrap, so
  retry cannot build a new graph over a quarantined candidate.
- The JS bridge fully creates the callback owner before reserve, and registers the
  raw owner before reading fallible getters. Thus even an accessor exception after
  successful reserve does not lose the allowance. Two independent reviews found
  exactly these P1 classes—bootstrap-before-debt and stranded reserve; both were
  closed with regressions before the commit. A repeat host and native/WASM audit
  found no P0/P1/P2.
- V1 deliberately preserves the previous mutation path and rejects the new opt-in
  output policy. No-op V2 passes through an exact zero plan/progress while still
  respecting the deferred owner lifecycle. Scalar and SIMD bindings export the
  same complete ABI.

Checks on frozen source: Rust workspace—590 passed, 1 ignored; sync-core with the
WASM feature—352/352 passed; fmt and mandatory workspace Clippy passed (only
existing warnings); TypeScript, production build, JavaScript syntax,
`git diff --check`, and the full `npm test` with explicit `exit_code=0` passed.
Bundled-main lifecycle: 21 scenarios / 216 assertions; actual engine/root: 32
suites; candidate driver: 21 suites; admission bridge: 3 groups; actual push
transaction: 1280 assertions. Fresh packaged scalar/SIMD parity verifies budget
1/256, the canonical oracle, refusal/cancel/finish/retirement, no-op, V1, and
resident dedup. The memory gate repeated the previous 24 lifecycle cycles on 25
thousand entries and four candidate-output runs; both independent WASM instances
returned to the baseline of 1672 requested bytes / 1 allocation without
allocation failures. In the synthetic mutation corpus, update took 82 units,
delete 81, and retirement 12; dedup kept staged payload at 0.

The scope is not a shared heap/RSS ceiling. Only requested V2 mutation-output
node buffers and range/root endpoint strings are counted. Candidate-open baseline
clone, input/planning owners, HashMap buckets/backing, hashing/codec/serde scratch,
allocator overcapacity, JS heap, WASM linear-memory capacity, and process RSS are
excluded. Final finish still synchronously verifies hashes, reserves map backing,
and moves prepared buffers; this is a separate bounded-work owner, not a hidden
part of the current byte plan. Published mutation cohorts are conservatively
retained until whole-tree replacement/free and may therefore overstate the ledger
over time; exact ownership reconciliation remains the next local memory slice.
Pull candidate mutations are not connected yet: its current orchestration
swallows `rebaseTree` errors and requires a separate fail-closed asynchronous
ownership transition, not mechanical activation of the policy. Remote CI,
native filesystem race, real-device UI/RSS/reload, and the device matrix were not
run; working vaults, Syncthing, and production were untouched.

## Fail-closed pull tree rebase

The pull tree rebase fence slice was implemented by code commit
`responsive-stage-121`
(`fix(sync): fence failed pull tree rebases`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- A candidate rebase error after an already applied remote delta no longer turns
  into `treeParity=false`, which the metadata/count fallback could accept as a
  permissible mismatch. Pull throws typed `PullTreeRebaseError`, does not request
  the server root, and does not checkpoint a new merge base.
- The engine captures the tree version before pull and, on typed failure, sets
  `rootRepairRequired` with that exact version. Old local/base/cached roots do not
  advance, the committed tree is preserved, candidate abort remains atomic, and
  the next normal pass must first perform an admitted base-derived replacement.
- Any valid paged checkpoint found on entry, whether partial or completed, is
  considered insufficient evidence of the volatile tree state in the current
  renderer. Pull returns `requiresTreeRebuild` and does not calculate parity from
  a potentially stale prefix; the engine reconstructs the graph from the
  WAL-recovered sync-base before root verification/adoption. The verified
  completed cursor is then removed through the normal base-adoption path without
  reloading pages.
- Regression covers the original rebase failure after disk/base apply, prohibition
  of root fetch/cache/base advancement, preservation of the committed tree,
  partial resume, and completed resume with an intentionally nonempty stale tree.
  The actual engine/root set grew to 34 suites and paged-pull to 34 assertions.

Checks passed: `git diff --check`, TypeScript, the production build, and the full
`npm test` with explicit `exit_code=0`. Known generated-WASM `import.meta`
warnings remain. An independent repeat P0/P1/P2 review after strengthening the
partial-checkpoint rule passed. Rust/WASM source was unchanged in this
TypeScript-only slice; the frozen native/parity/memory-gate results from the
previous slice are not represented as new.

The next local slice is to move pull candidate mutations themselves to the
asynchronous V2 admission/retirement driver. Pending mutation and reachability
owners must be drained before any new pull network/disk work; then every
delete/update rebase must preserve exact revision ownership through yield,
refusal, stop, and cleanup failure. The current fail-closed exception remains a
mandatory external safety fence, not a replacement for the admitted mutation
path. Remote CI, native filesystem race, real-device UI/RSS/reload, and the
device matrix were not run; working vaults, Syncthing, and production were
untouched.

## Admitted V2 pull candidate mutations

The pull candidate mutation admission slice was implemented by code commit
`responsive-stage-123`
(`feat(sync): admit pull candidate mutations`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- Production pull of a nonempty graph now sends delete/update through the same
  asynchronous V2 `PlanReady → admission → Ready → deferred retirement` as push.
  Direct/V1 callers without engine context retain the synchronous compatibility
  path. Output reservation shrinks to the actual Ready cohort, and the published
  resident owner remains attached to the tree store.
- Candidate ownership is preserved by exact revision after begin and every
  mutation. There is no `await` between the final witness and atomic commit; the
  operation signal is checked both before and after synchronous scope/native
  getters. Thus stop inside the final witness itself does not permit a stale
  commit, and ABA with a new candidate revision does not blindly abort another
  candidate.
- Pending mutation and reachability owners are drained before the pull capability
  probe or any network/disk work. The same two drains were moved into the shared
  root-recovery entry, so startup, push, reconcile, and Full Rescan cannot begin
  reload/probe/replacement over a quarantined native slot. Cleanup uses the
  original uncancellable host cooperation, while new continuation rechecks stop,
  wrapper identity, and durable root scope.
- Refusal or cleanup failure leaves the already applied remote disk/sync-base rows
  as the source of truth but does not advance server root, timestamp, or cache.
  Exact abort debt survives a retirement error; retry first drains the old
  token/candidate and begins a new mutation only after capacity changes. The
  external typed `PullTreeRebaseError` remains a fail-closed repair fence.
- Paged pull publishes the next cursor only after the admitted tree mutation
  finishes. A regression holds retirement after durable page apply, forbids the
  next page/root fetch, then verifies the exact cursor with `complete=false` after
  a cold reload of sync-base.
- Actual-engine regressions cover stop during nonzero V2 retirement and
  `stopAndDrain`, plus failed pull cleanup before the first vault inspection in
  Full Rescan. The already applied base deletion is preserved; the committed
  graph, authoritative roots, and cache do not advance.

Checks on frozen TypeScript source passed: `git diff --check`, TypeScript, the
production build, pull regression, 56 paged-pull assertions, candidate driver,
21 mutation-driver suites, 36 actual-engine/root suites, and the full `npm test`
with `exit_code=0`. Known generated-WASM `import.meta` warnings remain. The final
independent P0/P1/P2 review passed. Rust/WASM source was unchanged in this slice;
the prior native/parity/memory results are not represented as a new run.

The scope is still not a shared heap/RSS ceiling. Admission counts output node
payload and range/root endpoints only for the specific V2 mutation. Candidate
begin clone, mutation input/planning backing, map buckets, finish/commit GC,
codec/sort scratch, worker/JS heaps, WASM linear-memory capacity, and process RSS
are excluded. Published mutation cohorts currently live conservatively until
whole-tree replacement/free; exact reconciliation with physically reachable
chunks requires a native provenance/owner protocol and remains a separate memory
slice. Suppression of an identical automatic mutation refusal has not yet been
introduced either: it must depend on exact base/tree/ledger and dirty generation
without suppressing manual retry or a new journal cut. Remote CI, native
filesystem race, real-device UI/RSS/reload, and the device matrix were not run;
working vaults, Syncthing, and production were untouched.

## Suppressed unchanged V2 mutation refusals

The suppression slice for an unchanged automatic mutation refusal was implemented
by code commit `responsive-stage-125`
(`fix(sync): suppress unchanged mutation refusals`). Version, Nix pins,
tag/release, remote push, and deployment were unchanged.

- After the first V2 candidate output-admission refusal, periodic auto-sync no
  longer repeats the same native mutation, source read, and preflight for an
  unchanged complete durable cut. Dirty/WAL remain pending, the UI receives the
  stable reason `candidate memory admission`, and the suppressor is neither an
  ACK nor evidence that memory was released.
- The marker is bound to owner-local dirty identity, exact base capture, journal,
  runtime scope, tree/root/count and committed/candidate revisions, policy and
  deferred planning revisions, the resident ledger, and its monotonic mutation
  epoch. Even an equal-looking reserve/release ABA, capacity change, or repeated
  event for the same path invalidates the marker. If the safe-integer epoch is
  exhausted, optional suppression is permanently disabled without interrupting
  post-native ownership cleanup.
- Push attests only the actual mutation input after late oversized and dependency
  deferrals. Thus a mixed cut and selections larger than 256 do not receive a
  global block. Attestation is stored outside the public Error shape and is
  best-effort: its allocation/validation failure cannot skip candidate abort or
  replace the original refusal.
- Capacity/base/dependency/policy changes automatically permit a new attempt;
  Sync Now, deferred retry, and Full Rescan explicitly change the opaque retry
  epoch. The marker is set only after exact abort/retirement and only when the
  committed root did not advance, there were no publications, and the entire
  original cut without newer callback events returned to the dirty set.
- Regression covers a positive cut of exactly 256, suppression prohibition for
  257 and late mixed deferral, manual retry, capacity change before and during
  retirement, same-root base reload, dependency/policy drift, equal-looking N+1,
  and WAL preservation. Dirty capture has separate owner/ABA/O(1) tests on 25
  thousand paths.

Checks on frozen TypeScript source passed: `git diff --check`, TypeScript,
production build, 320 dirty-set assertions, 3 candidate-admission groups, 21
candidate-driver suites, 19 root-admission groups, 509 push-transaction
assertions, 1280 push/root-transaction assertions, 27 root-repair suites, 36
actual-engine/root suites, and the full `npm test` with `exit_code=0`. Known
generated-WASM `import.meta` warnings remain. The final independent P0/P1/P2
safety review passed. Rust/WASM source was unchanged in this TypeScript-only
slice; prior native/parity/memory results are not represented as new.

The next local memory slice is exact stable V2 settlement reconciliation.
Successfully published mutation cohorts currently accumulate conservatively in
the resident ledger until whole-tree replacement/free, although commit/abort
already removes unreachable native chunks. A versioned native post-sweep report
with exact graph provenance, branded host settlement capability, and O(1) shrink
of the resident lease is required; fail-closed debt must survive a report or
reconciliation error. This is still not a shared heap/RSS ceiling: candidate
clone, planning/map backing, codec/sort scratch, worker/JS heaps, WASM
linear-memory capacity, and process RSS remain separate owners. Remote CI, native
filesystem race, real-device UI/RSS/reload, and the device matrix were not run;
working vaults, Syncthing, and production were untouched.

## Exact stable V2 output settlement

The exact stable V2 settlement reconciliation slice was implemented by code commit
`responsive-stage-127`
(`fix(sync): reconcile stable tree memory after settlement`). Version, Nix pins,
tag/release, remote push, and deployment were unchanged.

- The additive native commit/abort ABI returns a report from the same operation
  that performed the reachability sweep: exact `before/reachable/removed/after`,
  removed payload bytes, post-sweep node payload, and the actual UTF-8 length of
  surviving root range endpoints. Every counter and their sum are checked for
  JS-safe exactness before the sweep; a corrupt store, invalid counters, or
  revision exhaustion does not change root, candidate, or physical chunks. The
  legacy ABI shape is unchanged.
- The host parser accepts only an exact own-data schema without accessors,
  extra/missing/symbol fields, unsafe numbers, or inconsistent GC, revision, or
  `payload + endpoints` values. The authority proof is created inside the module
  only from a direct successful native return and is consumed once by the exact
  tree/outcome/revision owner. A malformed/null report after native success does
  not turn the already completed commit/abort into an error and does not release
  the charge.
- Resident output cohorts are now combined in O(1) into one tree lease. After
  commit/abort, the ledger shrinks exactly to the proven physically reachable V2
  payload plus root endpoints; target zero releases the lease. A foreign/replayed/
  stale proof, getter reentrancy, partial ABI, failed reconciliation, and the
  legacy path only invalidate provenance and conservatively retain the old charge.
- Production root rebuild marks complete V2 provenance only after publication,
  retirement, and repeated base/scope/wrapper fences. Push and pull advance exact
  candidate revisions; their accepted/refused/cleanup commit and abort paths use
  the shared settlement adapter. The uncancellable tail after server ACK and
  deferred abort debt preserve the original ownership ordering.
- The regression matrix covers commit/abort, UTF-8 endpoints, an empty graph,
  corruption, invalid/overflow counters and their sum, active jobs, V1 rejection,
  both revision-exhaustion boundaries, proof replay/foreign owner, reentrant
  fences, and adversarial report schemas. Packaged scalar/SIMD parity uses an
  independent canonical wire/chunk oracle; the memory gate warms the serializer
  and returns every 25k lifecycle fixture to the allocator baseline.

Checks passed: a fresh scalar/SIMD WASM build, both packaged parity/memory
verifiers, `cargo fmt`, workspace Rust tests, wasm-feature Rust tests, the wasm32
check, workspace Clippy, TypeScript, the production build, browser probe,
lifecycle suite, and the full `npm test` with `exit_code=0`. Clippy and
generated-WASM `import.meta` warnings remain known. The final independent
P0/P1/P2 review after adversarial additions passed.

This result covers only stable V2 output payload/endpoints and is not a shared
heap/RSS ceiling. Candidate-open clone, replacement/candidate sort workspace,
mutation input/planning/map backing, codec/hash scratch, JS/worker heaps,
allocator capacity, WASM linear-memory high-water, and process RSS remain
separate owners. The next local memory slice is recoverable preallocation and
transient admission of the two index backing buffers for the V2 replacement
sorter before the input owner is transferred to the native job. Remote CI,
native filesystem race, real-device UI/RSS/reload, and the device matrix were not
run; working vaults, Syncthing, and production were untouched.

## Admitted V2 replacement sort workspace

The transient-admission slice for the V2 replacement sorter's index workspace was
implemented by code commit `responsive-stage-129`
(`feat(sync): admit v2 replacement sort workspace`). Version, Nix pins,
tag/release, remote push, and deployment were unchanged.

- The versioned native plan for fully accepted V2 input returns two exact
  allocation requests for `Vec<usize>` and their sum. On wasm32 these are
  `source = 4N`, `target = 4N`, `peak = 8N`: 200000 B for 25 thousand rows and no
  more than 512 KiB at hard cap 65536. The plan excludes input rows/path Strings,
  ID strings, other builder buffers, allocator metadata, linear memory, and RSS.
- Both index buffers are now created through `try_reserve_exact` before
  `tree_job.take()`. The first or second allocation failure removes only the
  already prepared local workspace and preserves the same input token, Vec,
  meter, live root/candidate, and revisions for retry or deferred cancel. A
  successful start transfers the buffers move-only into the sorter; subsequent
  ticks do not realloc them. Legacy V2 start uses the same recoverable allocator
  without a false admission claim; the V1 path is unchanged.
- The host adds exact `8N` in advance to the single paged-rebuild transient
  reservation before native begin. There is no second FIFO reserve while the
  input lease is already held, so self-deadlock is impossible. After the full
  feed, a strict own-data parser verifies the schema, wasm32 index width, count,
  both requests, and their sum, then echoes the pinned witnesses into admitted
  start.
- A partial/non-callable ABI is rejected before admission. A missing whole pair
  preserves the compatibility path and is not presented as measured. A
  malformed, accessor/proxy, wrong-token/phase, unsafe/fractional, or `±1`
  witness does not cross the allocation boundary. Base/scope/revision/abort
  fences are repeated around the observable plan; start failure remains
  `jobOpen` and holds the shared input+workspace lease until successful native
  retirement. Failed-cleanup quarantine is retried without a new reservation.
- Native regressions verify exact request sequence `[N]`/`[N,N]`, first and
  second real `TryReserveError`, same-token retry/cancel, 0/1/25000/65536 rows,
  pointer/capacity stability, and V1/phase fences. Host regressions cover exact
  combined reserve, queue/abort/refusal before begin, the adversarial report
  matrix, pinned methods, reentrant drift, and retirement retry.

Checks passed: fresh scalar/SIMD WASM build and parity; the packaged memory gate
ran scalar/SIMD × V1/V2 × one-prefix/wide-prefix for three full 25k cycles plus
separate `N=0/1` boundaries and returned to baseline `1672 B / 1 allocation`.
`cargo fmt`, workspace Rust tests, 359 wasm-feature tests, wasm32 check, Clippy,
TypeScript, the production build, browser probe, and the full `npm test` with
`exit_code=0` passed. Clippy and generated-WASM `import.meta` warnings remain
known. The final independent P0/P1/P2 review passed.

This result covers only the requested bytes of the two V2 replacement index
buffers. Candidate-open root clone/all-resident baseline, candidate mutation
sort/input/planning/map backing, remaining replacement descriptors/maps,
codec/hash scratch, JS/worker heaps, allocator usable size, WASM linear-memory
high-water, and process RSS remain separate owners. The next local slice removes
the atomic candidate-open root-clone/all-resident-key-snapshot residual by adding
exact preflight/admission and recoverable ownership before candidate publication.
Remote CI, native filesystem race, real-device UI/RSS/reload, and the device
matrix were not run; working vaults, Syncthing, and production were untouched.

## Bounded V2 candidate root opening

The candidate-open root/baseline slice was implemented by code commit
`responsive-stage-131`
(`feat(sync): bound v2 candidate root opening`). Version, Nix pins, tag/release,
remote push, and deployment were unchanged.

- The V2 all-resident baseline no longer clones `HashSet` keys whenever a
  candidate is opened. Every physical chunk receives a scalar insertion
  generation; open pins one cut, new owner insertions become fresh, and
  replacement of an existing hash preserves its birth generation. While the cut
  is active, delete/sweep of that store is forbidden; commit/abort removes the pin
  before terminal sweep. Generation overflow and failed promotion are atomic. The
  V1 baseline and wire/chunk semantics are unchanged.
- After bounded reachability, native returns a strict versioned plan with exact
  requested UTF-8 bytes for two identities and, for a nonempty root, two endpoint
  Strings. The baseline snapshot request is zero regardless of resident chunk
  count. `resume` verifies three host witnesses, makes a fallible
  `try_reserve_exact` clone, and stores the prepared root in the same job/token;
  publication remains an O(1) move. Refusal, cancellation, or a stale committed
  root transfers the prepared owner into the same cooperative retirement rather
  than leaving it to a hidden destructor/GC.
- The host accepts only an exact own-data schema, safe integers, `I + E = C`,
  `peak = C`, 2/4 Strings, and strategy `insertion-generation-v1`. Resident
  reservation occurs before the native clone. Private, ready, published,
  detached, and retired transitions are synchronous, pinned, and retry-safe; a
  callback that did not return an owner cannot skip admission. The V1 route is
  explicitly separated by `tree_version`: additive wasm-bindgen exports are not
  invoked for V1.
- The root resident ledger stores no generation history, only at most two
  physical clone slots: committed and candidate. Commit removes the previous
  committed clone and promotes candidate; abort removes candidate. Exact graph
  settlement retains proven payload/endpoints plus the surviving root identity.
  Even without complete-graph proof, a known terminal root transition is applied
  conservatively, while the remaining bytes stay charged. Replacement/free
  absorb or release these slots together with the aggregate lease.
- Push and pull use one bridge and exact revision-bound abort settlement. If
  handoff and the first terminal cleanup fail together, the already completed
  reachability owner is registered again as durable abort debt; the next drain
  completes the same abort instead of leaking the candidate/root lease. Actual
  `push()`/`pull()` fixtures verify refusal before resume, charge through
  uncancellable retirement, terminal abort/commit, and ledger return without
  publishing an unacknowledged root/base.

Checks on frozen source passed: fresh scalar/SIMD build; packaged parity and the
memory matrix `0/1/25000`, V1/V2, one-prefix/wide-prefix, with three lifecycle
cycles each; every instance returned to the fixed warm baseline. Workspace Rust:
304 tests; wasm-feature Rust: 364 tests; `cargo fmt`, wasm32 scalar/SIMD checks,
and workspace Clippy passed. TypeScript, 582 push-transaction assertions, pull,
focused candidate/root/settlement suites, the production build, browser probe,
and the final full `npm test` with `exit_code=0` passed. The runner has 53
subtests; actual bundled-main lifecycle has 21 scenarios / 216 assertions. Known
Clippy and generated-WASM `import.meta` warnings remain nonblocking. Two
independent final P0/P1/P2 reviews found no remaining concrete findings.

This removes the candidate-lifetime heap snapshot of keys but is not a shared
heap/RSS ceiling. Inline generation metadata and HashMap buckets, actual
allocator usable size, candidate mutation/replacement planning, sort and map
backing, codec/hash scratch, JS/worker heaps, WASM linear-memory high-water, and
process RSS are not summed by this admission. Cloning at most four root Strings
remains a small synchronous fallible step. The next local slice removes
synchronous sort/hex/result-array allocations from candidate chunk-plan finish
while preserving exact all/fresh semantics and cooperative ownership. Remote CI,
native filesystem race, real-device UI/RSS/reload, and the device matrix were not
run; working vaults, Syncthing, and production were untouched.

## Bounded candidate chunk-plan pagination

The cooperative-sort and paged candidate chunk-plan slice was implemented by code
commit `responsive-stage-133` (`feat(sync): page candidate chunk planning`). Version, Nix pins,
tag/release, remote push, and deployment were unchanged.

- After bounded reachability, the additive same-token WASM ABI first returns the
  exact requested workspace for two `Vec<FileHash>` values:
  `32N + 32N = 64N` bytes. Both `try_reserve_exact` calls occur before consuming
  the ready traversal, so allocation failure leaves the original job retryable.
  HashSet backing, allocator metadata, and page-bridge output are explicitly
  marked unmeasured rather than reported as zero.
- Stable LSD radix sort performs exactly `N` collect + `N` scratch init + `32N`
  count + `32N` scatter, or `66N` cooperative units. One host turn is limited to
  4096 units. For `N=0`, the state is immediately READY and repeatedly reports
  zero progress without a special unsafe branch.
- The sorted native owner is read in pages of at most 256 hashes. Every page
  contains canonical lowercase `all` and sorted `fresh ⊆ all`; V1 uses the
  original all-resident baseline, while V2 uses the pinned insertion-generation
  cut. The cursor advances only after successful page construction, and finish
  is allowed only after exact EOF.
- The strict host parser verifies complete own-data schemas, safe integers, exact
  `64N` witnesses, `66N` progress, offsets/EOF, global page order, and subset
  semantics. The workspace reservation is held until same-token cooperative
  retirement on success, abort, sink failure, and cancellation. A partial
  additive ABI fails closed; the legacy API remains compatible and delivers the
  already materialized result to the sink in pages of 256 with yield/fences.
- Production push calls `checkChunks` for every page: incremental checks only
  `fresh`, while bootstrap checks `all`. The server response must be a canonical
  subset of that exact page without cross-page duplicates. The native plan is
  fully retired before chunk export, so the single tree-job slot does not overlap
  upload. Candidate revision, applicability, and abort fences repeat around every
  await; the root is not published after a stale/cancelled page.

Checks on frozen source passed: a fresh scalar/SIMD WASM build; packaged parity
for V1/V2; and the packaged memory matrix scalar/SIMD × V1/V2 × 25 000 entries ×
three lifecycle scenarios. The memory gate confirmed exactly `64N` requested
bytes and two native allocations, pages ≤256 without retained native growth, and
return of every tree to the warm baseline `3336 B / 1 allocation`. Workspace
Rust, 366 wasm-feature tests, wasm32 scalar/SIMD checks, workspace Clippy,
`cargo fmt`, TypeScript, the production build, browser probe, focused collector,
actual push regressions (2293 assertions), the full `npm test`, parity, and the
diff check passed. Known Clippy and generated-WASM `import.meta` warnings remain.
Two independent final P0/P1 reviews found no concrete findings.

This historical slice removed complete native sort/hex/result arrays from
production finish but did not claim a shared heap/RSS ceiling. The next step
stated here—a compact 32-byte missing-hash spool—was subsequently implemented in
`responsive-stage-135`; later feature and freeze checks are listed in the current status at the
start of the document. A shared heap/RSS ceiling is still unproven: allocator
overhead, bounded page serialization, JS/worker heaps, WASM linear-memory
high-water, and process RSS remain separate residuals. Remote CI, native
filesystem race, real-device UI/RSS/reload, and the device matrix were still not
run; working vaults, Syncthing, and production were untouched.
