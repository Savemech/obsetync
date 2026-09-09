# ObsetyNC: fast sync, a responsive editor, and recovery after interruption

Date: 2026-09-05. Status: engineering plan, not a report of completed implementation.

Implementation progress and the boundaries of checks already completed: [implementation progress report](responsive-sync-progress.md).

Checkpoint 2026-09-09: the enabled P0–P3 file-sync scope is a stabilization
candidate after the known implementation backlog and independent code audits
were completed. Bounded initial-review preemption now lets a separately
captured and reviewed durable recent-edit prefix publish before a 25,000-file
review completes without weakening deletion review, journal ownership,
dependency cuts, or the mandatory complete review. The exact-tree local 25k
qualification passes the two-second saved-generation and three-admission
placement gates. This is not release-ready status: real runs on
iPhone/iPad/Android/macOS/Windows, the network/fault/soak matrix, remote CI,
and the P5 release/tag/artifact/Nix gates are not yet closed. P4 (CRDT/live
editing) remains dormant: PR15–20 were not executed, there is no verified
Yjs/CM6/Yrs selection, and that track is not part of the current release
scope.

Historical `responsive-stage-NNN` labels in this plan preserve the order of
local implementation and measurement checkpoints after the unpublished
development history was consolidated. They are documentation identifiers, not
Git object names. Published baseline tags retain their original commit IDs.

Investigated baseline: `1.11.4`, commit `d5df76fe613d509a64c5ff22d9cc9f27845cc4f5`.
The user's measurements apply to an installed build reporting `1.11.3`; the manifest version does not prove that its `main.js` is identical to the release artifact.

Navigation:

- [Observations and source map](#2-what-we-actually-know)
- [Success criteria](#3-verifiable-success-criteria)
- [Architecture](#4-overall-architecture-choice), [scheduler](#5-scheduler-the-ui-has-priority-bulk-does-not-starve), [memory](#6-memory-budget-before-reading-and-for-the-buffers-entire-lifetime)
- [Batches](#7-a-real-pipeline-and-batches), [resume](#8-durable-resume-instead-of-another-full-rescan), [transport switching](#9-automatic-switching-semantics-and-transport-are-separate)
- [Live editing and data safety](#10-real-live-editing-is-a-separate-durability-layer)
- [25 PRs and implementation order](#13-implementation-plan-and-dependencies)
- [Devices and workloads](#14-device-and-workload-matrix), [failures](#15-fault-injection-and-invariants), [release and Nix](#16-verification-artifacts-and-nix)
- [Open decisions](#17-open-decisions-and-explicit-boundaries), [first three deliverables](#18-first-three-concrete-deliverables)

## 1. The outcome we are building

The user can open Obsidian and start writing immediately while tens of thousands of files catch up in the application's background. The phone must not freeze because of plugin work. A restart must not turn completed preparation into another full rescan. A fast network and powerful device must be used without manual concurrency tuning.

"The same" means the same durability, conflict, acknowledgement, and resume rules on iPhone, iPad, Android, macOS, and Windows. It does not mean identical buffer sizes or identical filesystem access.

Objective: **maximize useful, reliably acknowledged changes per second while respecting the editor-latency and memory budgets**. The objective is not maximum CPU use, thread count, or batch size in isolation.

Required properties:

- Local editing does not wait for bootstrap, Full Rescan, remote pull, or completion of an attachment upload.
- A new edit to the open note does not sit behind a queue of 25 thousand files.
- Batches are bounded simultaneously by bytes, object count, and time since the first item arrived.
- Memory is reserved before reading, decoding, and queueing, not after large buffers already exist.
- Losing WS changes the delivery mechanism, not the meaning of edits or their order relative to document versions.
- Stopping the process at any point leaves either the old consistent state or a recoverable new state.
- An acknowledgement means that a specific durability milestone was reached, not merely that `send()` succeeded.
- An incompatible old client neither makes every other device wait forever nor gains permission to silently overwrite the new format.

Capability boundary: an ordinary Obsidian plugin cannot promise continuous JS execution after iOS suspends the application. We therefore require rapid persistence of progress and automatic continuation on return, not an "eternal background daemon." UIKit limits background execution; this is not a setting on our timer. [Apple: background execution](https://developer.apple.com/documentation/uikit/extending-your-app-s-background-execution-time)

This plan does not change Syncthing, its configuration, services, or data. Neighboring plugins are treated as external load; we do not blame them without measurements.

## 2. What we actually know

### 2.1. Observations and confidence

| ID | Observation | What follows from it / what we do not yet know |
| --- | --- | --- |
| E01 | Full scan processed about 21 thousand files quickly, then often added exactly two files per minute at roughly the same second of each minute | Strong evidence of scheduler waiting; insufficient to conclude anything about disk or network |
| E02 | `yieldToUI()` in push and chunking uses `window.setTimeout(0)`; debug reports the window as `hidden` | Throttling must be tested in this exact Electron/Obsidian version |
| E03 | The lag probe is also timer-based; aggregate `p95<=10001ms` moved the governor into recovery | Execution delay after hiding/sleep cannot automatically be counted as busy CPU |
| E04 | `desktop hash workers unavailable`; processing fell back to the renderer | The accelerated path did not start in this run; the common error handler hides the reason |
| E05 | During a large push, the committed tree and sync-base contained 330 files while the queue contained 25,440 changes | Matching old roots does not mean the local vault is already synchronized |
| E06 | The platform `stat()` loses the object type and turns every error into `null` | This dangerously conflates "file absent," "this is a directory," and "metadata could not be read" |
| E07 | An early journal push encountered `EISDIR` | Reproduction in a native client is required; WSL/9p errors do not prove Windows file corruption |
| E08 | Mobile `readFile()` reads the entire file; the current limit is 128 MiB | Splitting it for WASM afterward does not make the initial read streaming |
| E09 | Credit-based WS bulk, HTTP bulk, paged pull, WAL, and a candidate tree already exist | Integration and boundary repair are needed, not replacement of the entire protocol |
| E10 | The client WS has no complete handler/editor binding for `ops`; the server can store an opaque update | Presence and root notifications are not yet character-level sync |
| E11 | Server `ops` fanout runs even after append failure; append does not trigger sync; compact replaces the whole log | This path cannot be enabled as reliable live editing without separate durability and race work |
| E12 | A marker for the previous unfinished operation exists | It is evidence of an unfinished exit, not proof of Jetsam or of a specific reload cause |

Chromium documentation describes a mode that checks chained timers once per minute for a sufficiently long-hidden page under additional conditions. This makes E01–E03 a plausible hypothesis, **but does not confirm it for this specific Obsidian runtime**. A controlled foreground/hidden A/B longer than five minutes is required. [Chrome: timer throttling](https://developer.chrome.com/blog/timer-throttling-in-chrome-88/)

Electron handles visibility differently across platforms; for example, a fully occluded window on macOS may be `hidden`. We do not automatically transfer Windows results to Mac. [Electron: page visibility](https://www.electronjs.org/docs/latest/api/browser-window#page-visibility)

Low aggregate CPU and disk utilization is compatible with serial waits on IPC, timers, credit, ACK, and reads. A small delta in server counters does not rule out an in-flight request, an unknown WS phase, or delay before reaching the server. The current measurements are insufficient to honestly name a single bottleneck.

### 2.2. Source map

| Area | Sources | Main work |
| --- | --- | --- |
| Scheduling and lifecycle | [sync.ts](../plugin/src/sync.ts), [main.ts](../plugin/src/main.ts) | Separate long-running bulk from urgent work, cancellation, and resume |
| Reading and file types | [platform.ts](../plugin/src/platform.ts) | Typed stat, capability-based reader, errors without false deletes |
| Push pipeline | [push.ts](../plugin/src/push.ts) | Remove inter-batch barriers, redundant reads, and the global final commit |
| Pull and apply | [pull.ts](../plugin/src/pull.ts), [pull-echo.ts](../plugin/src/pull-echo.ts) | Protect the editor, bounded apply, and resumable staging |
| Governor and measurement | [resource-governor.ts](../plugin/src/resource-governor.ts), [perf-trace.ts](../plugin/src/perf-trace.ts) | Active windows, independent limits, and honest UI lag |
| Worker/WASM | [desktop-hash-workers.ts](../plugin/src/desktop-hash-workers.ts), [wasm-runtime.ts](../plugin/src/wasm-runtime.ts) | Verifiable startup, memory limits, and portable fallbacks |
| Journal and base | [journal.ts](../plugin/src/journal.ts), [dirty-set.ts](../plugin/src/dirty-set.ts), [sync-base.ts](../plugin/src/sync-base.ts) | Preserve generation/ACK semantics and add a durable transfer plan |
| Diagnostic marker | [operation-checkpoint.ts](../plugin/src/operation-checkpoint.ts) | Do not confuse a breadcrumb with full resume; bound the write queue |
| Transport | [api.ts](../plugin/src/api.ts), [ws.ts](../plugin/src/ws.ts), [ws-data.ts](../plugin/src/ws-data.ts) | Shared router, bounded tx/rx, health, and fallback |
| Framing | [ws-data-codec.ts](../plugin/src/ws-data-codec.ts), [bulk-codec.ts](../plugin/src/bulk-codec.ts) | Versioned multiplexing/fragmentation without changing content hashes |
| Server bulk | [ws_data.rs](../crates/sync-server/src/ws_data.rs), [storage_writer.rs](../crates/sync-server/src/storage_writer.rs), [pack_store.rs](../crates/sync-server/src/pack_store.rs) | Preserve durable group commit and add end-to-end observability |
| Live-editing skeleton | [crdt.rs](../crates/sync-server/src/crdt.rs), [ws.rs](../crates/sync-server/src/ws.rs), [api.rs](../crates/sync-server/src/api.rs) | Durable ops protocol, projection, and safe compaction |

### 2.3. What we preserve

- Content-addressed objects, hash verification, and the existing FastCDC boundaries.
- AEAD, transport-sequence verification, and protection against silent downgrade.
- A candidate tree with an honest parent root and conflicts on concurrent commit.
- A compact per-path representation of dirty changes and protection of newer journal generations.
- Paged pull of a fixed snapshot root and existing chunked-download recovery.
- A bounded server writer queue and group durability; the server already need not perform a separate fsync for every file.
- SIMD/scalar parity and a self-contained installable plugin bundle.
- Persisted mass-rescan approval from 1.11.4; extend resume without removing protection against mass deletion.

## 3. Verifiable success criteria

The numbers below are **proposed release gates, not already measured performance and not a promise for every vault**. P0 fixes the corpus, OS/Obsidian versions, network, and real baselines. If a gate cannot be met, document why and change the architecture or supported scope instead of substituting a different measurement.

| Area | Initial target | Measurement conditions |
| --- | --- | --- |
| Typing | input-to-paint p95 ≤ 50 ms, p99 ≤ 100 ms | iPhone 16 Pro Max and the other primary devices; foreground bulk; notes up to 100 KiB, with larger notes measured separately |
| Renderer work | Normal plugin CPU slice ≤ 4 ms; no repeated plugin-owned tasks > 50 ms | Also measure serialization, WASM calls, and CRDT apply, not only the outer async loop |
| Fresh file edit | Publication of a small saved note p95 ≤ 2 s while the server is available | Does not require bootstrap completion; LAN RTT ≤ 30 ms, with no artificial failure/conflict |
| Live editing | Remote paint p95 ≤ 250 ms, server durable ACK p95 ≤ 500 ms | After CRDT is ready; same LAN; short transactions, not megabyte-scale paste |
| Warm start | No repeated content read of unchanged files with a valid cache; sync starts without Full Rescan review | Corpus of about 25 thousand files; metadata audit is allowed; correctness with a questionable cache matters more than speed |
| Metadata audit | Initial target ≤ 5 s on the reference corpus | Measure cold and warm cache separately using the metadata API actually available; do not treat this as a universal guarantee |
| Resume | Acknowledged objects/commits are retained; no repeated global rehash | Process kill, reload, network loss; an unacknowledged window may be repeated idempotently |
| Memory | All managed queues bounded; reserve before allocation; plateau in a long-running test | Task buffers, WASM, editor/CRDT, and native overhead accounted separately |
| iPhone stability | 10 × 30 min foreground editing + bulk with no plugin-induced reload; 100 suspend/resume cycles without loss of acknowledged data | Real device; record system reports, not just JS exceptions |
| Bulk speed | At least 70% of the best measured safe reference pipeline on the same device and corpus | Same durability, network, and warm/cold state; unique and already-present objects measured separately |
| Idle | No continuous content polling or CPU busy loop | Notes unchanged, queue empty, connection healthy |

Also record absolute files/s, useful MiB/s, wire MiB/s, total wall time, active time, stage p50/p95/p99, and energy consumption where possible. Percentage of the reference does not replace these numbers.

Three distinct text-status levels:

1. **In the editor** — displayed locally; persistence may still be in progress.
2. **Persisted on the device** — the selected local persistence operation completed; crash/power-loss guarantees depend on the API and are verified separately.
3. **Acknowledged by the server** — durable ACK for the required version/operations, not merely a socket write.

We cannot promise durability of the last character not yet persisted when the process is killed instantly. The goal is a minimal measured local-persistence window, no network wait in an editor transaction, and no false "saved" status.

## 4. Overall architecture choice

### 4.1. Three options

| Option | Advantages | Limitations | Decision |
| --- | --- | --- | --- |
| Identical JS/DataAdapter only on every platform | Minimal platform and packaging differences | No guaranteed ranged read on mobile; large files and the renderer remain constraints | Safe baseline and fallback |
| Shared sync core + platform accelerators | Identical semantics, Node workers/ranged IO on desktop, verified Web APIs on mobile | Requires capability tests and a shared conformance suite | Primary path |
| Native mobile bridge/host-application changes | True ranged IO and potentially different lifecycle capabilities | Not available to an ordinary JS plugin; a different product and deployment scope | Separate branch if the mobile large-file gate cannot be met through public APIs |

Do not begin by rewriting the whole client in another language, adding a native helper on every device, or replacing the transport stack. The native server and WASM already exist; the main gaps are between stages and guarantees.

### 4.2. One control loop, multiple bounded stages

```text
Editor / vault events / remote cursor
                 |
        durable per-device journal
                 |
        priority scheduler + generations
          /            |              \
 live documents    urgent files     background reconcile
          \            |              /
       resource reservations + bounded stage queues
                 |
    read -> hash/prepare -> check -> transfer -> durable ACK
                 |
       single logical commit sequencer
                 |
        root / doc watermark / local ack

Transfer router: WS interactive | WS bulk | HTTP bulk/ops
```

"One sequencer" means one order for changes to authoritative vault state, not a global mutex held throughout reads and network waits. Independent objects are prepared concurrently; root commits, rename groups, and CRDT checkpoints are serialized as short, consistent transactions.

The module names below are proposed boundaries, not existing APIs: `work-scheduler.ts`, `resource-budget.ts`, `transfer-plan.ts`, `transport-router.ts`, `document-session.ts`.

## 5. Scheduler: the UI has priority, bulk does not starve

### 5.1. Work queues

- `interactive`: local text persistence, incoming editor operations, and required ACK/control frames.
- `urgent-file`: the current note and recently saved small files before CRDT is introduced and for file mode.
- `bulk`: bootstrap, historical backlog, attachments, and catch-up upload.
- `maintenance`: metadata audit, compaction, cache checks, and cleanup.

The scheduler services them with priorities and aging/weighted fairness. Initial experimental policy: a reserve for interactive, a bounded urgent window, the remainder for bulk, and maintenance only when capacity remains. Continuous input must not permanently stop normal sync.

Short jobs can be reordered before they start; a large synchronous WASM/JSON call already running cannot be preempted. Therefore split the work itself, not merely the outer promises. A remote replacement of the open note goes through editor-aware reconciliation rather than directly overwriting the file behind the editor.

A large local paste, opening a heavy note, or mass application of remote files temporarily reduces bulk CPU/read/apply. Attachment upload remains useful as long as it does not consume resources required for live work.

### 5.2. Yield without the timer trap

Introduce `yieldWork({ lane, deadline, signal })` with enqueue → actual-start measurement. Select the mechanism from working capabilities: Node scheduling on desktop, available browser scheduling/message mechanisms, and a bounded timer fallback. Every mechanism must pass tests for UI fairness and hidden-window progress.

- `Promise.resolve()`/continuous microtasks do not count as yielding to rendering.
- Do not use `requestAnimationFrame` as the only bulk engine: hidden pages may not render.
- Do not treat `MessageChannel`, a scheduler API, or a worker as a way to defeat OS suspension.
- Do not globally disable Electron throttling or use silent audio, synthetic WebRTC, or busy-spin to bypass energy saving.
- In desktop hidden mode, choose a verified bounded continuation path; if the runtime cannot provide one, show the limitation honestly instead of diagnosing a "slow disk."
- In mobile hidden mode, stop accepting heavy work; an already persisted plan permits continuation without a callback when the application is terminated.
- visible/hidden/suspended/unloaded transitions cancel unnecessary queued jobs, release buffers, and invalidate connection state; `AbortSignal` reaches stages that actually support cancellation.

First E01 test: the same prepared backlog, with the window foreground → hidden > 6 min → foreground. Compare scheduler wait, read service time, hash CPU, transport wait, visibility, and throughput. Open DevTools may change behavior; also run without it.

### 5.3. A governor that does not remain at one forever

The current governor receives the final operation result, while a large operation lasts tens of minutes. It needs short bounded windows of active work, initially 1–2 s, with event-triggered response to backpressure. Windows use real delta counters rather than repeatedly attributing the entire file size to a small phase.

Control independently:

- hash workers and feed size;
- read concurrency and prefetch bytes;
- network in-flight bytes/requests and pack size;
- remote-apply concurrency and CPU slice;
- maintenance share.

A decision to reduce hashing because of UI lag need not reduce network concurrency to one. Growth is allowed by experimentally increasing one parameter after several healthy windows, even if throughput at the current limit stopped rising. After degradation, roll back and apply cooldown; do not oscillate on every sample.

Exclude intervals of OS suspension and background timer throttling from UI pressure. When reliable samples are unavailable, report the state as unknown, not healthy. Also measure the real delay after return; it cannot be hidden entirely by filtering.

Do not infer capabilities from the marketing name of the SoC. Test the iPhone 16 Pro Max as a real runtime; start with a conservative profile and increase it based on measurements. An orphan marker causes a temporary cautious start, not a lifelong penalty or a label of "definitely OOM."

## 6. Memory: budget before reading and for the buffer's entire lifetime

### 6.1. Ownership model

The shared `ResourceBudget` issues reservation tokens. Before read/receive/decrypt, a job reserves its worst-case managed footprint. After ownership transfer or release, the token is returned exactly once; cancellation/error/timeout leaves no leaks.

Account not only for the payload, but also for simultaneously live copies:

| Memory | Where it appears | Control |
| --- | --- | --- |
| Source input | DataAdapter/Node read | Bounded read/range; whole-file read only after admission |
| Chunk views | `subarray` of a large input | A view retains the entire parent; this is not released memory |
| Pack plaintext | Bulk encode | Reserve before encode; bound batch bytes |
| Ciphertext and decrypt result | WebCrypto/AEAD | Separate reserve, released after handoff/ACK by the real owner |
| WS tx/rx buffers | Browser/native + Promise chains | Bounded admission, receive credits, and a limit on not-yet-decrypted frames |
| WASM heaps | Main instance and every worker | Separate counters/limits and no uncontrolled pool growth |
| Metadata/tree/CRDT | JSON, maps, candidate state, open docs | Pagination, bounded apply, LRU sessions, and cardinality limits |
| Native bridge/host | Copies outside JS | Measured safety margin; do not pretend ArrayBuffer sizes provide exact RSS |

Initial mobile hypothesis: a shared pool of temporary managed byte buffers with a 16 MiB soft budget and a 32 MiB admission ceiling, including a reserve for interactive work. This is **not an Obsidian RSS limit**, not a guarantee against Jetsam, and not permission to read a 128 MiB file. Exact values are approved after P0/P2 measurements. Desktop receives a separate measured range.

Track memory high-water and buffer retention duration. A large retained heap may not shrink immediately after references are dropped; JS GC cannot be invoked as part of the algorithm. Worker recycling is acceptable for a drained stateless hash worker, but not if it loses authoritative tree/CRDT state.

Root commit/query/cancel HTTP requires a separate complete scope rather than an
automatic reserve inside shared `sealed`: content callers may already own a
parent scope, and a nested global reserve would create a self-deadlock. Before
a large canonical encode, calculate the exact size `B`; the request, tree
capability, server-eph refresh, transport retries, native completion, decrypt,
and terminal decode sequentially use one child quota. Initial conservative
managed-workspace model: owner `3B + 6R`, transport work
`estimateTransportWorkset(max(B, R))`, where `R` is the larger strict response
cap. It accounts for simultaneously live JSON/UTF-8 and repeated bounded
receipt validation, but is not presented as a limit on the original persisted
base64 string, parsed JS object graph, the first native `requestUrl` allocation,
or process RSS. A separate preliminary capability observation by the recovery
coordinator must not be counted as part of this scope without explicit transfer
of the same owner.

### 6.2. Mobile large-file reading is a separate mandatory spike

In the installed Obsidian typings, `DataAdapter.readBinary(path)` returns the entire `ArrayBuffer`, not a stream or range. Newer APIs include `appendBinary`, but support by the concrete adapter is checked at runtime. [Obsidian API declarations](https://github.com/obsidianmd/obsidian-api/blob/master/obsidian.d.ts)

Check in order:

1. Whether a documented and actually working ranged/stream reader exists in the target host/adapter versions.
2. Whether a public resource URL can provide a stream/range on the specific platform without prior full materialization. This is an experiment, not an assumption; verify status, `Content-Range`, real allocations, and origin/auth constraints. A `200` response to a range request cannot simply be read in full and called a range.
3. If not, use whole-file admission only for a safe size that fits with copy overhead. A size that changes between stat and read must also be accounted for; without a bounded reader, we do not promise a strict upper bound on host allocation.
4. Leave an oversized local file pending with a specific reason and a safe way to upload it from another device. The rest of the vault continues syncing. Do not delete the file, mark it synchronized, or bypass the limit automatically.
5. If arbitrary large local uploads directly from iPhone are mandatory and the public API does not pass the gate, make a separate decision about native/host integration. Adding a worker does not solve this.

A large remote download may remain chunked staging + append without holding the whole file in JS. But an old append fallback that rereads the previous partial must not silently become quadratic copying; in an unsupported runtime, retain a clear limitation.

### 6.3. Worker strategy

- Desktop: restore Node worker startup and expose a diagnosable fallback reason; jobs pass a path and expected generation/stat, not the whole file from the renderer.
- Mobile: test Web Worker startup, CSP/Blob URL, scalar/SIMD WASM, and transferable buffers in real Obsidian. The presence of the `Worker` class is insufficient.
- A worker removes CPU work from the UI but does not eliminate DataAdapter reads/copies. The number of simultaneously active WASM heaps must be controlled.
- Shared fallback: small CPU slices, bounded reads, and persisted progress, with no infinite worker-restart churn.
- Do not require SharedArrayBuffer/cross-origin isolation for normal sync.
- Measure the startup cost of inline scalar/SIMD/worker source. Do not break BRAT/mobile installation by extracting a mandatory asset that the updater does not deliver.

## 7. A real pipeline and batches

The current push has known serialization points: sequential outer batches, per-operation tuning snapshots, additional reads after check, and a final aggregate list of tree updates. Replacing them with one large parallel `Promise.all` would not solve memory or priority handling.

The target pipeline has bounded queues between stages:

1. Lazily enumerate metadata/dirty generations; do not create jobs/promises for the entire vault in advance.
2. Reserve memory, read, and prepare the hash/manifest once where safe.
3. Perform batched existence checks; combine up to a cap and short flush deadline without waiting for a full batch for a fresh note.
4. Transfer only missing objects; verify every returned hash/record mapping.
5. While transport waits for ACK of batch N, prepare N+1 within the shared budget.
6. After ACK, persist per-object progress and release byte buffers; store commit metadata compactly.
7. Perform a short root commit for a ready dependency-complete group; unfinished files do not block independent ones.

The current full-review path performs cooldown/deletion partitioning through a
shared incremental iterator: the synchronous compatibility API exhausts it
immediately, while the reviewed queue yields to the real host after every 256
elementary units. An input/index/output row, explicit dependency edge, and
shared-journal peer each count as a separate unit; spreading a whole adjacency
list before a yield is forbidden. Reachable closure fails closed at 131,072
path endpoints and 131,072 directed-edge visits, corresponding to at most
65,536 dependency pairs for the following graph planner. Normal partitioning,
eligibility audit, optional forced pass, and plan creation belong to one
temporary `planningRevision`; a cooldown or rename-graph change across an
await invalidates the entire cut before dirty claim. The revision changes
before the first private-record mutation, and returned retained hints share no
mutable object with the cooldown record.

Dependency snapshot capture now also has a shared synchronous/cooperative
iterator. The production reviewed queue counts every outer endpoint and every
directed adjacency entry as a separate work unit, yields to the host after 256
units, and keeps capture inside the same temporary `planningRevision`. The
cooperative path fails closed above 65,536 pairs, 131,072 endpoints, or 131,072
directed entries; snapshot rows are detached from the mutable graph and frozen.
The old synchronous compatibility API preserves its previous uncapped result
and is not used by reviewed preparation.

Reviewed priority sorting for `oldest`, `newest`, `smallest`, `biggest`, and
`alphabetic` now uses a stable bottom-up merge over the original row
references. Every indexed-input copy row, merge output, and tail row is a
separate work unit; after 256 units it invokes the same owner cooperation inside
`planningRevision`. Admission of ≤65,536 rows happens before allocating the two
working arrays, ties preserve input order, and the comparator/`undefined → 0`/
`localeCompare` behavior matches the previous synchronous path. `sequential`
remains O(1) and no-copy.

Reviewed `random` no longer passes the entire array to native
`sort(() => Math.random() - 0.5)`: it uses Fisher–Yates over one planner-owned
indexed copy. Each of the `N` copied rows and `max(N−1, 0)` swaps is a separate
unit; the RNG is captured before the first host turn, and every sample is
verified as finite `0 ≤ x < 1` before indexing. An invalid source fails closed
and invalidates preparation without claim/root/ACK. For 25,000 rows this is
49,999 units, 24,999 draws, and 195 host yields. This deliberately changes the
distribution and scheduling semantics of the reviewed path; the synchronous
compatibility helper still preserves the old biased native comparator.

This is not yet fully cooperative preprocessing: synchronous settlement,
candidate-mutation JSON admission/final promotion, chunk-byte/root export, and
part of the native tree lifecycle remain uninterruptible sections. Caller-owned
dense arrays and scalar rows must remain immutable during partition/sort;
checking revision/array lengths is not a deep clone of the input. Initial array
allocations, each individual `localeCompare`/RNG call, final
`Object.freeze(snapshotArray)`, and GC are not preemptible work units. Work
units and Node tests do not prove a renderer slice ≤ 4 ms, a heap/RSS bound, or
mobile UI responsiveness.

The root limit of 256 paths/cuts and the 256 KiB selected-batch planner metadata
charge bound the plan, but not the actual root-wire size or native graph work:
v1 may load and rebuild an entire large top-level prefix, while v2 may traverse
a wide leaf range up to the next canonical boundary. Splitting the existing
`candidate_update_batch()` into several JS calls cannot be presented as resume:
every nonempty call is a separate logical update, may change parent/history/
serialized root bytes, and repeats structural work.

The first native job contract is now implemented for opening a candidate in a
packaged push. `begin_candidate_job()` creates an owner-local job with a
monotonic token; `step_tree_job(token, budget)` accepts only `1..=256` units,
and the production driver deliberately performs exactly one unit between real
host yields. Root-child seed, moving one decoded child onto the pending stack,
and validating one pending descriptor/node each count as separate units. The
synchronous compatibility `begin_candidate()` and committed-reachability
getters exhaust the same cursor, so there is no parallel validator with
divergent semantics.

Until `finish_candidate_job()` succeeds, no candidate exists and the committed
root and store remain authoritative. Nested jobs and all tree mutators—format/
load, committed/candidate update/delete, rebuild, commit/abort—are rejected;
committed reads are allowed. A wrong/stale token does not remove another
owner's job. A native step error atomically discards only the provisional
cursor; host cancellation calls `cancel_tree_job()` and does not sweep the
store. `finish` first completes full validation, then in one residual section
installs a clone of the committed root and the old baseline of **all resident
hashes**, including unreachable history: replacing it with the reachable set
would change `new_candidate_chunk_hashes()`.

The JS driver verifies exact `completed += units`, a monotonic reachable count,
and abort/applicability before every step and before finish. Cleanup ownership
is transferred to push through a synchronous callback only after the candidate
has actually opened. A partial/non-callable API fails closed and does not fall
back to legacy begin. Cancel/abort failure is diagnosed separately and does not
replace the original typed cancellation/source-drift failure. On the job path,
push no longer calls full `wasm_tree_committed_chunk_hashes()` before begin, so
one of the two previous full graph traversals per root is eliminated; the
validated reachable count is used for performance accounting. Legacy
structural ports preserve a compatible synchronous begin after a mandatory
host yield.

The shared v2 cursor simultaneously closed a validation hole: reuse of one
content hash under a different `RangeRef` is now rejected even if the first
descriptor was already checked. Packaged scalar/SIMD parity executes the job,
not an optional mock, for v1/v2, budgets 1/256, and empty and missing-chunk
graphs; it verifies no candidate or committed-byte change before finish, stale
tokens, mutator fences, cancel, exact candidate/root bytes, and identical
progress. An actual-push regression stops the owner after the first native step
and confirms the absence of candidate mutation, upload ACK, and root
publication.

The second native job contract now covers planning candidate index chunks after
mutation. `begin_candidate_chunks_job()` uses the same owner-local token and
shared `step_tree_job()`, but its purpose fixes the candidate graph rather than
the committed graph. Until ready/finish, the job only reads the immutable
candidate/store; wrong finish kind, stale token, nested job, and mutators fail
closed, while cancel removes only the cursor and preserves the entire
candidate.

`finish_candidate_chunks_job()` receives the single validated reachable set,
sorts it once, and returns `{all, fresh}` together. `fresh` is a strict sorted
subset of `all` relative to the original baseline of **all resident hashes**
captured when the candidate opened. Therefore a normal incremental push checks/
sends only `fresh`, while bootstrap sends all of `all`, without changing
candidate lineage, content/index ACK ordering, or the root-publication boundary.
On the packaged non-bootstrap path this replaces two former full traversals,
`candidate_chunk_hashes()` + `new_candidate_chunk_hashes()`, with one resumable
traversal; bootstrap still has one traversal but now yields a host turn between
bounded steps. A wholly absent new API preserves the synchronous legacy
fallback after a host yield; a partially declared API is rejected.

The driver verifies exact `completed += units`, monotonic reachable counts,
canonical lowercase unique hashes, sorted `fresh ⊆ all`, candidate presence,
and abort/applicability before and after cooperation, including a separate
post-await fence before `checkChunks`. It copies the result using a fixed length,
not overridable `Array.slice`/Species. An actual-push test cancels the operation
after the first chunk-job step: the job is cancelled, then the transaction owner
aborts the candidate; index/root ACK, committed root, and durable base do not
advance. Separate incremental/bootstrap regressions distinguish `fresh` from
`all`. Packaged scalar/SIMD parity runs v1/v2 jobs with budgets 1/256, compares
exact plans to the old synchronous oracle, and checks candidate/committed root
bytes at every step, cancel, stale token, and wrong-kind finish. Native v1/v2
tests separately cover a pre-resident but unreachable future chunk: it remains
part of the legacy baseline and does not become a false fresh upload.

The third native job contract makes one candidate mutation resumable.
`begin_candidate_delete_job()` and `begin_candidate_update_job()` accept
exactly one serialized logical operation and use the shared owner-local token
and `step_tree_job(1..=256)`. The production output driver requests quantum 64
and combines at most 32 fast native calls in one turn while one captured
monotonic clock confirms a CPU slice below 4 ms. An equal, nonnumeric, backward,
or throwing clock fails safe by allowing exactly one native primitive before a
mandatory host yield; the next valid turn may combine calls again. The v1
canonical-shape fast path additionally returns after checking every legacy
leaf, so one call cannot hide tens of thousands of path comparisons. Push still
finishes delete before starting a separate update: operations are not fused and
preserve the previous parent/history, serialized root bytes, and clock
semantics.

Mutation-driver freeze trace: `responsive-stage-222` bounded and safely coalesced native
mutation steps, `responsive-stage-223` added reuse of canonical v1 leaves without a full
rebuild for every batch, and `responsive-stage-225` enabled the native mutation-complexity
gate in CI/release workflows. These checkpoints establish the boundary of the
implemented optimization but do not replace the remaining device/network/soak
gates.

The v1 cursor incrementally groups prefixes, loads the entry/node graph,
performs stable merge-sort and normalization, merges states, and builds leaf/
internal chunks and the root parent hash. The v2 cursor preserves the previous
localized canonical segmentation/resynchronization and legacy-update
statistics. Both write new immutable objects only into a job-local overlay;
the candidate root and resident store remain unchanged until finish. Finish
first verifies all content hashes and resident collisions, reserves all missing
hash-map capacity, and only then advances the overlay and changes the candidate
in one atomic section. Any finish error poisons the cursor in advance: the
retained token is valid only for cancel, so a repeated finish cannot publish a
root after the overlay was lost.

The host driver verifies exact progress, candidate/policy/source ownership
before and after every await, stale tokens, and cancellation. A wholly absent
API falls back to one old synchronous mutation call after a host yield; a
partially declared API fails closed. A native step error has already removed
the job, while a finish error retains it for mandatory cleanup. An actual-push
regression covers successful mixed delete→update and stopping after the first
update step: the completed delete remains only in the outer candidate, then the
transaction owner aborts it entirely; committed root/base, index/root ACK, and
publication do not advance.

Native differential tests compare v1/v2 with the old update functions at
budgets 1/256, including duplicates, missing deletes, empty/nonempty no-op,
wide-prefix, and multi-level/resynchronization corpora. They verify complete
resident `all_chunks` before finish/cancel, exact emitted bytes, and atomic
promotion. Packaged scalar/SIMD parity runs 32 format/scenario/backend/budget
combinations and 128 sequential mutation outcomes: exact root bytes/hash/count,
reachable/fresh maps, independent plain-map semantics, both delete/update
orders, stale/wrong-kind/mutator fences, and eight cancellations after real
staging. The packaged API does not export all resident keys, so known future
reachable hashes are checked additionally; intermediate unreachable staged
chunks remain evidence from native `all_chunks` tests.

The fourth native job contract eliminates whole-chunk cloning when exporting
index objects. `begin_tree_chunk_export_job(hash)` pins a resident object from
the current tree—candidate when open, otherwise committed—and returns exact
`{token,length}`. The hash already belongs to a previously validated chunk
plan: begin does not present itself as a new reachability traversal and
deliberately does not accept an orphan as proof of graph membership.
`read_tree_chunk_export_job()` requires the exact next offset and, under one
store borrow, copies at most 64 KiB; no borrow/view of resident or WASM memory
survives the call. Finish is allowed only at exact EOF, cancel removes only the
metadata job, and the shared tree-job token blocks all mutators during page
export. A missing object, changed length, or wrong token/purpose/offset/budget
retains the owner for explicit cancel and neither repairs nor sweeps the store.

The host first reads only the lengths of a bounded pack and obtains shared
transient-memory admission, then opens native export, allocates one exact final
JS buffer, and copies pages. There is a real host turn between pages and between
neighboring objects; no native page is retained across `await`. Pack hash slots
are copied before the first such turn, so a mutable caller array cannot bind the
bytes to another hash. Candidate push rechecks semantic root/count and selected
generations; committed reconcile forbids a suddenly appearing candidate and
holds the captured committed root/count. The shared scope remains occupied
until the actual transport ACK/error, not merely dispatch; stop during ACK does
not release bytes or publish root/base before the result.

A wholly absent export API remains compatible with the old wasm-bindgen getter,
which still copies the entire object in one call; a partially declared API
fails closed. An actual-push regression covers a three-page object, cancel/
ownership loss after the first page, export-cancel → candidate-abort ordering,
and retaining admission through a late ACK. Packaged scalar/SIMD parity for
v1/v2 and candidate/committed scope compares exact page concatenation with the
old getter and verifies offset/token/mutator fences, cancel/finish, and the
independence of already returned pages after the next read, `memory.grow()`,
and `tree.free()`.

The fifth native job contract makes root export two-phase and explicitly
selects either `candidate` or `committed`, never substituting one for the other.
For v1, Plan incrementally validates vault/device/prefix, calculates a
conservative FlatBuffer arena bound and the semantic root hash, but does not
clone the root, children, arena, or offset vector. After host admission, Build
separately reserves the arena and offsets, fills the arena with zeros in pages
no larger than 64 KiB, creates string/hash/ChildRef records in exact legacy
order, and writes one reverse-vector offset per unit. `collapse()` preserves
the backing allocation and returns its suffix without another full native
output copy; room/capacity are checked before and after builder calls. The new
export API deliberately limits every v1 vault/device/prefix UTF-8 string to
64 KiB and rejects it during Plan; the old serializer and wire format are
unchanged. For v2, allocation-free preflight returns the exact length, while
Plan and Build remain one atomic unit each within the existing 16 KiB codec cap.

The host driver requires the API to be either complete or wholly absent and
independently verifies Plan and Build progress with reset counters, token,
exact descriptors, semantic hash/version, and sequential pages no larger than
64 KiB. One native turn is bounded by two independent budgets: at most 4096
primitives and governor `feedBytes`; every turn is followed by a real host-task
yield. Before Build, it reserves the native maximum + offset metadata + final
JS maximum + two bridge pages; a separate caller quota covers the actual
consumer. Native output is released at exact EOF before the reverse semantic
parse through WASM. The returned JS buffer and shared scope live until explicit
`release()`. The legacy getter is called exactly once after a host yield; its
whole allocation inevitably precedes admission and is labeled accordingly.

The sixth scheduler slice replaces the old shared `step_tree_job` for root work
with dedicated `step_root_export_job(token,maxUnits,maxBytes)`. Progress reports
per-call `units/bytes` independently from cumulative `completed/processed`, and
both counters reset between Plan and Build. Here `bytes` is a conservative
scheduling/CPU charge, not a measurement of allocator traffic, heap, or RSS: a
cheap primitive costs at least 256 B, string/fill accounts for payload, and one
indivisible tick may exceed an undersized byte budget only by itself and by no
more than 64 KiB + 128 B. Therefore a 256 KiB mobile feed groups up to roughly
1024 cheap operations per turn, while a long prefix and arena fill remain
byte-bounded. A native differential test with 25,000 short children requires
fewer than 150 Plan+Build turns instead of the previous roughly 31,000.

Begin now receives a separate `maxArenaBytes`. V1 checks this ceiling
incrementally while calculating the conservative FlatBuffer bound, before
arena/offset allocation; v2 compares bounded descriptor preflight against it.
The cache writer derives the ceiling from total transient-pool capacity using
the proven `offsetBytes <= arena/32` and current owner+consumer accounting. This
rejects a knowingly impossible large Plan early but does not replace later
admission that accounts for competing owners. `maxOutputBytes` separately
limits only the actual sealed/legacy output and is no longer mixed with the
arena maximum.

The first production consumer of this path is committed `cached-root.bin`.
The cache writer pins hash/version and absence of a candidate, rechecks the
owner around every await, performs real task yields, and holds the child quota
and root bytes until actual `writeBinary` settlement, without a `Promise.race`
on abort/timeout. Export/parse/write remains a best-effort tail: failure after
an accepted root does not undo already durable terminal/base/journal state or
restore an old generation. Server-provided `newRootBytes` after pull is stored
through the previous separate path: it is authoritative received payload, not
a reason to serialize the current tree again.

Native differential tests include v1 with 25,000 children, alignment/Unicode/
64-KiB strings, both scope/format variants, cancellation at every phase, wrong
token/kind/EOF, and the shared mutator fence. Actual-engine regressions verify
a two-page cache, host-task fairness, stop and failure while an adapter write is
held, drift with a new WAL generation, and accepted-outcome recovery. Rebuilt
scalar/SIMD parity runs v1/v2 × candidate/committed with operation/byte budgets
`1/1`, `4096/256 KiB`, and `4096/16 MiB`, compares exact bytes with the old
getter, and verifies early arena refusal, independent cumulative counters,
`memory.grow()`, detached lifetime, cancellation, and fences on the real
wasm-bindgen API.

The boundary of this slice is explicit. JSON stringify on the host and parse in
WASM remain synchronous; the 32 MiB byte cap precedes parse but is not a heap/
RSS guarantee. One v2 node codec/hash remains atomic even though a node is
bounded to 256 KiB; a v1 node has no analogous byte cap, and mutation may still
hold an entire large top-level prefix. There is not yet shared governor
admission for the overlay. Final validation/reserve/promotion, v1 clock/root
clone, and release of cursor maps are also uninterruptible. Chunk-job finish
still synchronously sort/filter/hex-materializes two JS arrays. Page export
still requires one final JS buffer the size of the object; the wasm-bindgen
bridge temporarily copies one page, and an exception from the JS allocation/
OOM itself provides no ordinary cleanup guarantee for the temporary native
Vec. The previously admitted hash plan, exclusive tree owner, and immutability
of same-length resident bytes remain a contract, not repeated hash/reachability
proof on every page. Synchronous bulk encode/AEAD copies, resident tree heap,
pull rebase through legacy begin, and native cold rebuild are not yet resumable.
V1 root export still retains the full admitted arena and offsets until finish,
while the host retains one final JS root; reserve, allocator bookkeeping/drop,
JS allocation, and the parser bridge remain atomic sections and do not prove an
RSS limit.

The seventh scheduler slice removes repeated v1 `root_hash_hex()` calls from
production owner checks around every export await. The outer `WasmTree`, which
is not replaced together with `TransactionalTree`, stores a monotonic `u64`
committed revision in the exact-JS-integer range. The next value is checked
before mutation and published after successful native commit/load/rebuild but
before converting the result to JS; exhaustion refuses without wrap or tree
change. The revision also increases on same-hash reload and an A→B→A
transition, because a semantic hash does not prove previous history/graph
ownership. Candidate-only mutation, abort, and export jobs do not change the
committed revision.

The cache writer captures the exact JS tree instance, revision, version, and
one initial semantic hash. Every later await is checked in O(1) by identity,
revision, version, and absence of a candidate; a new wrapper with the same
revision is not accepted. Old WASM without the getter preserves a hash-based
compatibility fallback, while an existing but invalid getter fails closed. The
final parse still verifies the semantic hash/version of exported bytes. An
actual-engine regression with a multi-step two-page root confirms one initial
`root_hash_hex()` for the entire cache export; scalar/SIMD parity covers all
committed mutators, same-root reload, ABA, and absence of candidate/export
noise. This slice does not eliminate the first O(n) semantic hash or final
parse/hash, so the early arena ceiling still follows one full hash.

The eighth scheduler slice moves durable candidate publication onto the same
two-phase byte-budgeted root export. Production no longer calls
`candidate_root_bytes()` on the new API: after Plan/admission, Build returns the
root in pages with real host yields, while one owner retains the native arena,
offsets, pages, and final JS root. The complete child quota for the base64
request, hash preimage, and streaming BLAKE3 is derived in advance from the
negotiated root cap; if the arena and intent preparation cannot fit
simultaneously, the operation is refused before Build. Legacy WASM preserves
the old whole-buffer getter only as a compatibility fallback.

An immutable `RootCommitIntent` and the first request hash are created under the
owner. When the helper returns, neither raw bytes nor a root view pass into
recovery: the owner is released **before** store verification/WAL, HTTP, and
conflict settlement. Those stages obtain independent global admission again,
so the mobile pool cannot deadlock itself through a nested reserve behind an
already held root lease. Intent-hash revalidation first reserves the complete
scope and only then decodes base64 and builds the preimage. Exact-size decoding
does not use iterable `Uint8Array.from`, which could additionally materialize a
list with one JS element per byte.

Native `candidate_revision()` adds a separate monotonic `u64` fence within the
exact-JS-integer range. It increases on every successfully visible candidate
open, mutation, abort/commit, and on committed replacement that removes an open
candidate; provisional job steps, cancel, failures, empty batches, and
`MutationNoop` do not move it. A nonempty update raises the revision even when
the semantic hash is unchanged. Push captures identity/version/revision before
export and checks them around every cooperative turn and at the final
fresh-send boundary after prepare/negotiation. Thus ABA with the same hash/count
does not publish a stale intent. Old WASM uses the previous hash/count fallback;
an invalid getter fails closed.

Memory regressions pin the exact owner+preparation formula, a 512 KiB root,
base64 padding/cap/digest failures, and absence of a hidden iterable copy.
Actual-push/runtime tests cover a two-page root, releasing the owner exactly
once on success/defer/error, absence of the lease during WAL/HTTP/conflict work,
and final-send ABA. Rebuilt scalar/SIMD parity verifies revision transitions and
v1/v2 candidate/committed export; Rust, TypeScript, and packaged gates execute
the real wasm-bindgen API.

The boundary remains explicit: semantic parse, base64 encode/decode, bounded
preimage, and one final JS root are synchronous inside a precomputed scope;
accounting describes controlled byte copies, not complete GC/RSS, allocator
metadata, native bridge, or WAL storage. Legacy publication remains
whole-buffer. Cold rebuild separately requires a replacement job with bounded
entry feed and swap only after full validation, not incremental mutation of the
live tree. An ordinary `async fn` through the current single-poll `run_local`
does not provide that cooperation.

The ninth scheduler slice bounds transfer of cold-start recovery input into
`root-tree-repair`, which is also invoked during normal startup after durable
publications. New `begin_replacement_rebuild_job(version, count, jsonBytes)`
occupies the shared exclusive tree-job slot and reserves private entry slots.
`append_replacement_rebuild_job(token, expectedOffset, pageJson)` accepts at
most 256 entries and 256 KiB of UTF-8 at a time, validating the exact offset and
cumulative full-JSON size. Aggregate limits are 65,536 entries and 8 MiB of
input. Neither feed nor cancel changes the previous committed/candidate roots,
chunks, or revisions.

The host preserves a first pass over the immutable sync-base capture for
validation and counting. After one shared admission, the second pass collects
only the current page; a real host yield occurs between feeds. The new path has
neither a full array of JSON fragments nor a final vault-sized `join`. Input
allowance is
`2 × jsonBytes + 128 × entryCount + 6 × min(jsonBytes, 256 KiB) + 128 KiB` and
is retained until actual finish/cancel. It covers managed input and page copies;
the replacement graph, sort/validation scratch, native allocator, and GC/RSS
require separate accounting.

Before final finish, base capture, signal, exact engine tree owner, and native
revisions are checked again. Only complete input permits finish: it builds and
validates a separate tree using the old canonical builder, then replaces the
live graph and raises the committed revision exactly once. An existing
candidate is removed only by successful replacement and receives its revision
bump. A valid ready finish consumes the input job even on revision/build error;
invalid token/kind, incomplete finish, and erroneous feed retain the owner for
cancel. This is an explicit terminal-drop contract, distinct from ready
candidate-mutation finish.

Native canonical sorting remains unchanged: JS UTF-16 capture order differs
from Rust string ordering for part of Unicode, and a feed boundary is not a
leaf boundary. A legacy API without the new methods preserves the old atomic
rebuild; a partially declared API refuses. Honest `capturedBaseRoot` remains
separate from the rebuilt semantic hash; the helper does not write base/journal
or publish a root.

Five native regressions verify private input, cancel/mutator/token fences,
stable entry allocation across feeds, revision exhaustion, and atomic refusal
of a late v2 duplicate. Nine host suites cover count/byte paging, including
Unicode paths, exact reconstruction of JSON offsets/bytes, one complete input
lease for the entire native lifetime, real task fairness, queued admission
without a second pass, drift/abort, and a separate microtask window before final
finish. Rebuilt scalar/SIMD parity compares v1/v2 byte-for-byte with the legacy
oracle, including all reachable chunks, pages of 1/256 entries, a format change
with an open candidate, Unicode ordering, and the 1000/1024-entry boundaries.
The v1 clock is fixed only in the differential fixture; production clock and
canonical format are unchanged.

This slice makes ingest itself cooperative. Native sort, leaf/internal codecs,
closure validation, and release of the old graph still run in one synchronous
finish. Splitting them requires a separate private builder cursor. Full Rescan,
push/pull bootstrap, reconcile, and tree-format alignment still retain their
old rebuild call sites.

The tenth scheduler slice adds two optional operations to the same input owner:
`start_replacement_rebuild_job(token)` and
`step_replacement_rebuild_job(token, maxUnits)`. Start only transfers complete
input to the private builder; the live graph, candidate, and both revisions
remain unchanged. Without these two methods, paged-input/atomic-finish
compatibility is preserved; a partially declared pair refuses before input
admission. A wholly old API still has the previous whole-JSON fallback.

The v1 builder incrementally groups entries by prefix, stably sorts paths, and
moves strings into leaves of 1000 entries. Decimal internal labels are sorted
lexically as before—`0, 1, 10, 11, 2`—not numerically. The v2 builder
incrementally sorts and validates strings, performs the previous path-only
segmentation, and builds levels of internal descriptors. Duplicate paths remain
stable v1 entries but are a v2 error rather than becoming a last-wins upsert.
Both builders validate the complete content-addressed closure of their own
store before readiness, without repeated full validation in finish. Neither
page size nor work budget changes canonical leaf boundaries or bytes.

One turn performs at most 256 work units and ends after an individual node
codec/hash or graph-validation step even when unit budget remains. The shared
stable merge sorter moves rather than clones strings; input-sized sorting/
grouping/segmentation loops are no longer hidden inside one native finish.
Temporary owned rows and obsolete v2 range/closure descriptors are released
one per unit before readiness. Progress is an exact descriptor with four own
data fields: `done`, `units`, `completed`, `phase`. The host rejects accessors,
missing/extra fields, unsafe/nonmonotonic counters, and stalled non-done steps.
After every step, including final done, it performs a real host yield and
rechecks signal, base capture, exact wrapper/scope, and revisions.

Start/step misuse and a failed build retain the private job for matching cancel;
a failed cursor cannot become ready. Incomplete finish also retains the owner,
while a valid ready finish consumes it even on revision exhaustion. Only a
successful finish replaces the committed graph, removes the previous candidate,
and raises the corresponding revisions. The input lease is retained until this
actual finish/cancel but is not presented as a complete native-graph memory
ledger.

The remaining boundary is substantial: an individual codec/hash/parse, one
string comparison/copy, allocator/reallocation, and dropping drained backing
arrays are atomic. A v2 node is bounded to 256 KiB; v1 preserves the legacy
1000-row leaf without a new byte cap. At the tenth-slice boundary, cancel
destroys the private graph in one call and final swap synchronously releases the
old resident graph; the next slice below adds a deferred path. Complete
accounting of sort/graph heaps and real-device UI/RSS gates remains open. Other
rebuild call sites are not migrated.

Native differential testing covers 25,000 entries, v1 groups/duplicates/decimal
labels, v2 Unicode/path caps/path-only boundaries, cancellation, and corruption
of the private closure (missing/corrupt object and conflicting repeated
descriptor). The host suite contains 15 sets, including a real task between
final done and finish, base/scope/revision changes, strict data descriptors,
and cleanup. Rebuilt packaged scalar/SIMD compares the new build with the
legacy oracle by root bytes/hash and all reachable chunks at budgets 1/256 and
both tree formats. On a fixture with 1034 v1 / 1033 v2 entries, budget 256 gives
84/76 native turns instead of 16548/16547 at budget 1; this is work-count
evidence, not device timing or a UI gate.

The eleventh scheduler slice makes replacement **retirement** cooperative as
well: the optional trio `cancel_replacement_rebuild_job_deferred(token)`,
`finish_replacement_rebuild_job_deferred(token)`, and
`step_tree_retirement(token, maxUnits)` is connected to startup root repair.
Without the trio, the previous ABI is preserved; a partial API is rejected
before admission.

- Deferred cancel transfers an input/partial/failed/ready builder into
  retirement without touching the live graph. Deferred finish requires an
  already ready stepped builder; preflight/take errors retain a cancellable
  owner, while success changes graph/revisions once and transfers the previous
  committed/candidate graph into cleanup. Both the residual builder and old
  graph belong to the same token.
- Retirement occupies the shared exclusive job slot. Until `done`, new jobs
  and mutators are forbidden; generic cancel/step and wrong token/kind/budget
  cannot destroy the cleanup owner. Up to 256 owned rows/slots/descriptors/node
  buffers are released per native step; `done` consumes the token. Sparse sort
  slots count one at a time without a hidden search through an arbitrary run of
  holes.
- The host yields after every step, including final done, and holds input
  admission until actual cleanup. The original abort and scope/base changes do
  not cancel release of the exact old wrapper. Cleanup failure retains the
  wrapper/token/lease in a strong owner registry; repeated drain is singleflight
  and obtains no new reservation. Re-init/unload waits for drain through free
  and pool/scheduler closure, and new init checks generation after that await.
  After cleanup, the helper rechecks base/signal/scope and the new
  post-publication tree witness: a stale result does not authorize subsequent
  cache/base writes, but the already published graph is not rolled back.
- Native suites verify every partial/failed/ready phase, 25k sparse sort with
  destructor counters, exact resident payload counts, no buffer copying,
  revision exhaustion, and both pairs of tree formats. Actual packaged
  scalar/SIMD passes cancellation/finish/error fences at budgets 1/256 and
  preserves root bytes/hash/full reachable chunks from the legacy oracle. The
  host suite has 24 sets; bundled main lifecycle has 17 scenarios/149
  assertions, including fault/retry.

This is a work-count/lifetime bound, **not a complete memory or latency bound**.
One v1 node buffer, allocator/free/coalescing, hash-table iterator scans,
drained backing arrays, and atomic codec/parse remain residuals. Input
accounting does not include the complete private/resident/sort heap and does not
prove lower WASM RSS. Legacy callers, direct wrapper `free`, and other rebuild
paths did not become cooperative automatically. Native-device foreground/
hidden/UI/RSS/reload gates remain mandatory.

The next completed memory slice replaced the shared native stable sort with an
indirect merge: one original `Vec<T>`, two pre-reserved index arrays, and
bounded inversion/permutation. Equal-key order, v1/v2 bytes, and ownership are
preserved; constructor allocations, string comparison, and backing free remain
atomic. During cleanup, T rows are processed one at a time and Copy-index
backing one allocation at a time, instead of alternating release of sparse
full-record slots.

Optional `wasm_memory_snapshot` was added: requested-live/peak/counters for one
WASM instance and linear-memory size separately. The diagnostics are not
admission, a per-tree charge, or RSS. Snapshots around an async job cannot be
subtracted and the delta declared property of one tree: the instance is shared
by trees/hash/serde/exports. Unsafe JS integers and invalid bookkeeping are
explicitly not presented as exact results.

`scripts/verify-wasm-memory.mjs` is a new locally executed gate connected to
CI/release: scalar/SIMD × v1/v2 × 25k one-prefix/distinct-prefix, three cycles
each, an existing committed+candidate graph, input/partial/ready/failed cancel,
rejected page scratch, publish→retirement→free, and a separate legacy oracle
with exact root/chunk bytes. All 24 cycles returned requested-live/count to
baseline; linear memory appropriately remains large. Numbers and limitations
are in the progress report; this is not real-device or universal memory-ceiling
proof.

The next admission requires separate fail-fast resident ownership rather than
a long-lived reservation in the FIFO transient pool. The old graph remains
charged simultaneously with the private replacement; on publication the new
charge becomes resident and the old one enters retirement until actual free.
O(1) component summaries
must account for the store/root/candidate baseline/private jobs and all mutation
funnels. Separate node codecs, bridge memory, and the remaining build callsites
remain uncovered; telemetry does not authorize governor growth by itself.

The first component slice counts the physically owned `MemoryChunkStore` buffers
in O(1): chunk count, aggregate `Vec` length/capacity, and logical
`HashMap::capacity()`. The latter is not a bucket-byte count; an empty map or
retirement may retain its backing. The same hash in two independent stores is
counted twice because they are two allocations. Counters transfer at the real
`replacement -> resident/retiring` boundaries and remain with retirement until
the corresponding buffer is actually removed. The optional tree snapshot shows
resident/private replacement/retiring separately, is not added again to the
allocator snapshot, and fails closed on invalid arithmetic/ABI.

The second additive component ABI, `metadata_memory_snapshot()`, now separately
reports the physical root/identity owners in O(1): String length/capacity for the
v1/v2 root, v2 endpoint paths, the backing capacity of the v1 root children
array, separate ID pairs in `TransactionalTree` and the outer `WasmTree`, plus
the candidate baseline length and logical `HashSet::capacity()`. The latter
remains a count of reported element slots, not bucket bytes;
`logical_hash_bytes` describes the keys, not another allocation. The public
serializable root contains no copyable cache: v1 uses an indivisible private
owner+sidecar and measures the actual allocation of a new clone, while v2
updates private sidecars at every lifecycle funnel. Commit transfers the
candidate meter, abort removes it, and retirement retains the Vec/HashSet
capacity charge until the backing is actually destroyed.

The old `chunk_memory_snapshot()` schema 1 is unchanged. The new host parser is
optional, path-free, and fail-closed: it checks own data fields, safe integers,
and nested invariants, while an unknown partial replacement or other private job
appears as `null`/`other_private_jobs_unmeasured`, not zero. At every lifecycle
stage, the packaged scalar/SIMD 25k gate reconciles both ABIs, the exact
`replacement -> resident` and `old resident -> retiring` transitions, and then
the absence of a retained owner after drain. A canceled input-only rebuild keeps
`IntoIter<FileEntry>` as the exact retirement owner until a separate bounded drop
of its backing allocation; nullable coverage cannot become exactly empty before
that drop. All 24 cycles and canonical parity passed.

The third additive component ABI, `replacement_input_memory_snapshot()`, counts
the `Vec<FileEntry>` accepted before the builder starts: one physical owner,
length/capacity slots, the actual `size_of::<FileEntry>()`, backing
`capacity * slot size`, and the aggregate length/capacity of all path Strings.
The counter is created after the actual reserve; a rejected, malformed, or
oversized page does not change it, and an accepted page is added only together
with the strings already moved into it. A partial feed is therefore measured by
the strings actually accepted, not the declared total.

Deferred input cancellation transfers the sidecar without copying it, together
with `Vec::IntoIter`. Each destroyed row decrements the String owners before its
drop, but the original Vec backing remains charged even when the remaining
length is zero and disappears only in a separate cleanup unit after the iterator
is dropped. This is verified for both a partial feed and a live zero-entry owner.
After the strings transfer to the builder, the input ABI no longer owns them;
subsequent owners appear in the corresponding V1 entries or V2 post-sort
components below, while an unavailable phase remains explicitly
`null/unmeasured`, not fabricated zero. The optional host parser is path-free and
fail-closed: it checks own fields, safe integers, capacity times slot size,
row/path count consistency, and all native validity flags.

The packaged scalar/SIMD 25k gate now includes empty/partial/full input cancel
for v1/v2, exact transfer without duplicate ownership, an exact remaining
row/path oracle on every retirement tick, and separate backing release. All 24
complete lifecycle cycles returned to the fixed allocator baseline; canonical
parity is preserved. This is an additive subset of an already observed WASM
allocation, not heap/RSS, a memory quota, or proof of mobile responsiveness.

The fourth additive component ABI, `replacement_sort_memory_snapshot()`, counts
in O(1) the top-level backing of the active indirect stable sorter: the original
`values: Vec<T>` and the two `source/target` index arrays. The entry sorter and
v1 child sorter are separate because their native slot sizes differ; the builder
cannot report both simultaneously. An empty but live sorter retains three Vec
owners even at capacity 0, whereas an absent sorter has 0 owners. Nested String
allocations inside rows/child labels are intentionally excluded from this
component.

The snapshot follows the actual owner across `replacement -> retirement`, does
not count it twice, and observes bounded cleanup: no more than one `T` is
destroyed per unit, followed by one index backing allocation at a time. The
original values backing remains charged after the last `pop` until the sorter is
actually dropped. Candidate mutation uses different sort worksets and therefore
currently returns explicit `null/other_sort_owners_unmeasured`, not fabricated
zero. The optional host parser strictly checks own fields, safe integers, native
validity, owner cardinality, slot sizes, and `capacity * slot size`; debug output
does not call the component the total heap.

The fresh packaged gate cancels within entry sort for scalar/SIMD × v1/v2 × 25k
one-prefix/wide-prefix, verifies exact transfer, and performs per-unit retirement
to the allocator baseline. A separate v1 fixture of 1001 one-prefix rows reaches
`sort-internal` with two child labels and verifies the same transfer/cleanup. A
real candidate-mutation job confirms the incomplete-coverage flag; canonical
parity is preserved. This is still not admission or proof about RSS or device
responsiveness.

The component snapshots still exclude v1 BTreeMap nodes/prefix keys, child/root
labels and hash vectors, v2 `RangeRef` endpoint Strings and closure sets/maps,
candidate-mutation private clones, codec/hash/serde scratch, and actual
map/HashSet buckets.
They are also not allocator usable size, linear-memory/RSS, or a quota.
Therefore the added host `NativeResidentLedger` remains only a verified basis
for an ownership API:
synchronous fail-fast reserve/grow, conserved split/transfer, and explicit
release without FIFO waiting or a GC finalizer. It is not yet connected to
production job admission, and the bytes passed to it will be a caller-supplied
estimate, not an automatic heap/RSS measurement. It may be connected only after
all candidate/mutation/direct-build owners and conservative pre-allocation bounds
are covered; a partial ledger must not be represented as a global memory ceiling.

Tree v2 post-sort entries now have a separate additive component ABI. The
already exact accepted-input sidecar transfers with the original
`Vec<FileEntry>` into the builder without another O(n) traversal of the path
Strings. While the indirect sorter owns the values, the post-sort owner is
explicitly `null/unmeasured`: the top-level Vec already belongs to the sort ABI
in this phase and is not counted a second time. After `finish()`, the same Vec
backing and the aggregate actual String length/capacity become exact.

The owner's physical state is separate from the diagnostic meter: invalid
telemetry does not control transfer/drop. Normal `cleanup entries` destroys no
more than one entry and its path allocation per unit, keeps the empty Vec backing
observable until a separate release unit, and releases it before `Ready`.
Deferred cancel after a completed sort transfers the iterator and meter together;
cancel within sort remains unmeasured until the sorter is destroyed, with the
drained values backing released in a separate unit and not combined with the
first planning-cleanup unit. Duplicate/path/closure failure retains the exact
owner until deferred retirement.

The host schema is again optional, path-free, and fail-closed; it checks own
fields, safe integers, row/path conservation, the exact Vec backing product, and
sticky native validity. The packaged gate links accepted input to the post-sort
owner, prohibits simultaneous exact sort/post-sort charges, observes the
retained-empty checkpoint, and verifies that the backing-release unit does not
release an adjacent sort/planning owner. This is still observation of an
additive subset, not production admission or a mobile RSS ceiling: V2
ranges/closure and the remaining V1 graph worksets must be counted separately.

Tree v1 original-entry redistribution now has a separate additive component ABI,
`replacement_v1_entries_memory_snapshot()`. The already calculated
accepted-input meter transfers into the builder with the `Vec<FileEntry>` without
another O(n) traversal. From there, a single O(1) sidecar follows the physical
owners: retained input backing, aggregate backing of all grouped Vecs, logical
rows inside the entry sorter, `rows`, `leaf`, the encoded-retirement iterator,
and the aggregate actual length/capacity of all path Strings. At every observed
transition, the invariant is that exactly one of these phases owns each original
entry and the sum of row counts equals the number of path owners.

The active entry sorter's backing remains only in the sort ABI; the V1 entries
component adds only its nested path ownership and therefore does not duplicate
requested bytes. An empty but still-live sorter is valid: its row count is
already zero, while the three Vec owners are released in separate units. The
group aggregate is updated upon actual reallocation and when the group transfers
to the sorter. `total_files` overflow is checked before `pop_first()` so an
entire group cannot be removed and destroyed outside bounded retirement.

Normal build and deferred cancel use physical `Option`/Vec owners, not diagnostic
validity, to decide what to drop. Exhausted input/rows/encoded iterators, leaf
backing, and sorter values backing are released in separate units. The
child-sorter tail also has its own unit: the mixed fixture simultaneously retains
the next group of seven FileEntry values and prohibits changing V1 entries,
post-sort/planning, chunk, or metadata ownership at that point. Native tests
separately lock down unchanged retained capacity/backing, preflight overflow, an
overflow/underflow sticky-invalid aggregate, and continued build/cleanup under
intentionally invalid telemetry.

The optional host parser is path-free and fail-closed: exact own fields, safe
integers, native validity, row/path conservation, a single native FileEntry slot
size, `group owners <= aggregate capacity`, and exact `capacity * slot size`.
The packaged verifier sums the disjoint exact chunk, metadata, input, sort, V2
planning, V2 post-sort, and V1 entries components and requires that the total not
exceed the global requested-live sample. The fresh scalar/SIMD gate passed v1/v2
× 25k one-prefix/wide-prefix × three lifecycle cycles (24/24), exact transfer,
failure/cancel/publish/drain, and returned to a baseline of 1672 requested bytes /
1 allocation; canonical parity is preserved.

The component intentionally excludes BTreeMap nodes and prefix keys, child/root
labels and hash vectors, codec/hash/serde scratch, allocator metadata/buckets,
and other private jobs. It remains diagnostics for an additive subset, not an
admission policy, a WASM linear-memory/RSS ceiling, or proof of device
responsiveness.

The replacement feed now rejects the 257th row before decoding its fields; the
256 KiB outer byte cap is checked before native parsing. This bounds one page,
but wasm-bindgen may already have copied the input string, while the first 256
`RawEntry` values, their path/hash strings, and serde scratch remain separate
owners.

Tree v2 replacement now has an exact pre-encode barrier. After sorting and input
validation, the resumable builder performs a path-only dry plan of the leaves and
all internal levels. The plan stores only endpoint-entry indices and fixed-width
descriptor metadata: content hashes are unnecessary for determining boundaries
and sizes. Before the `plan ready` state, no leaf/internal codec has run and the
private `MemoryChunkStore` contains 0 chunks/0 payload. The allocation-free root
preflight also checks ID/path limits and the exact stored-root size before node
output.

The token-scoped plan returns exact logical `nodePayloadBytes`, node/leaf/internal
counts, maximum node length, and stored-root length. A separate resume accepts
the same exact safe-integer witness; a wrong token, wrong phase, or differing
size cannot cross the barrier. The host strictly validates the plan shape,
invokes the optional policy hook pinned before the first await, rechecks the
base/scope/tree witnesses, and only then resumes. An explicitly configured V2
policy requires the plan ABI even on an old build adapter and cannot silently
become a compatibility bypass; a declared V2 plan ABI must expose exactly one
not-yet-reached `done` barrier. A missing half of the ABI, accessor/extra fields,
unsafe integers, and an internally inconsistent plan fail closed. A policy error
or rejection leaves the private owner for the existing deferred retirement,
without publishing a live tree.

During emission, every actual internal span/height is checked against the stored
dry schedule **before** the corresponding codec/allocation, each codec length is
checked against the exact span, and aggregate node count/payload is checked
before the root; the final stored-root length must also match. Scalar/SIMD
packaged parity with budgets 1/256 requires exactly one V2 barrier and zero V1
barriers, and compares every plan field against independently read canonical
chunks/root. This is the first genuine reserve-before-node-output seam, but it is
**not yet a production memory quota**: logical payload excludes allocator
capacity rounding, input/sort/plan/range/closure/root owners, map buckets,
codec/hash scratch, linear-memory high-water, and RSS. `NativeResidentLedger` is
not connected to this hook by default; V1 and direct/mutation paths do not yet
have such a complete plan.

Two cases are optimized separately:

- **Known content hash, server already has object:** metadata/check without a content read when the local generation and cache are valid.
- **Unknown new small file:** one bounded read, hash, and short retention through check/upload; when retention is unfavorable, use an explicitly selected reread with a measurable counter. Do not present lower RAM use as "free zero-copy."

For big files, cache fingerprint + manifest + generation; use ranged
preparation/transfer on desktop. §6.2 applies to an unsupported mobile
reader. Do not recompute the hash of an unchanged attachment merely because the
batch transport switched.

Experiment with optimistic put for very small new objects versus check-then-put.
Choose based on dedup ratio, RTT, and server CPU; the operation remains
idempotent and the byte budget is unchanged. Do not blindly send an entire vault
to save one RTT.

An object, a network frame, and a root transaction are different sizes. Reducing
the WS payload cannot change FastCDC boundaries or deduplication of existing
data. A large indivisible object is sent through an HTTP path that supports it or
through a separately negotiated fragmentation capability.

Publishing in small groups gives other devices useful progress before bootstrap
finishes. Initial experiment: flush a ready group after 0.5–2 s or at an
item/byte limit; set exact thresholds after measurement. Rename and dependent
delete/create operations remain one atomic logical group. Do not promise an
atomic snapshot of the entire vault for batch-by-batch bootstrap.

## 8. Durable resume instead of another Full Rescan

### 8.1. What to persist

`operation.active.json` is currently a diagnostic breadcrumb written with throttling, not a durable plan for every file. It cannot be used as proof of persisted hash/upload results. A separate versioned transfer plan is required that stores metadata rather than all file contents in RAM.

Minimum record fields:

- `planId`, schema, vault identity, base root/epoch, and tree/hash/ignore-policy versions.
- Stable `mutationId`, per-path generation, change type, and expected predecessor.
- File fingerprint, known content hash and manifest reference, and file-type check result.
- List or compact bitmap of acknowledged objects/ranges and their server generation.
- Root-transaction state, idempotency key, acknowledged root, and local ACK watermark.
- Per-item error, retry classification, and lease expiration when present.

Do not mix this plan with the committed sync-base: a file whose object was uploaded but whose reference was not published is not yet synchronized.

```text
discovered -> fingerprinted -> prepared -> objects-confirmed
                                         |
                                  commit-intent persisted
                                         |
                                  root/doc commit sent
                                         |
                                  commit-confirmed
                                         |
                                  local-base + journal-ack
```

Interruption between every pair of arrows is covered by a test. `sent, outcome unknown` is a first-class state, not a variant of "not sent."

### 8.2. Persistence and recovery

- Initially use a portable segmented WAL + bounded snapshots through the already available adapter. Do not assume SQLite/IndexedDB is equally available and reliable on every platform.
- Do not reread a multi-year WAL into one `string.split()` at startup. Bound segments, replay window, and live-index size; delete a segment only after a durable checkpoint replaces it.
- Add checksum/length/schema for recovery. Distinguish a corrupt tail from corruption in the middle; do not skip arbitrary records as though the state were known complete.
- Metadata snapshot: temp → verified replace with backup/recovery. On mobile, do not attribute `fsync` guarantees to an adapter whose API does not provide them; test force-kill separately from full power loss.
- Coalesce diagnostic records to the latest value under slow IO. The authoritative journal cannot be coalesced that way without preserving generation/ACK semantics.
- Device ID, secrets, transport-sequence reservations, local watermarks, and the active plan are not replicated as ordinary vault files even when `.obsidian/` is synchronized.
- At startup, make the editor and journal recovery available first; run metadata audit incrementally. Recovery does not depend on a successful network connection.
- If an old client does not know the plan schema, forbid unsafe writes through a clear compatibility gate while preserving the data for returning to the new version.

### 8.3. File changes during processing

Every async stage checks the generation. While upload for generation N is being prepared, the user may create N+1. An ACK for N does not clear dirty N+1. Recheck applicability before commit; already uploaded immutable objects may be reused.

`mtime + size` is a fast fingerprint, not cryptographic proof of immutability. Events, bounded audit, and forced verification on drift/questionable recovery are required. A separate test changes bytes while preserving size and mtime. Do not restore "the entire old plan" over new vault state.

On rename, preserve the old/new link and document identity; a folder rename is decomposed into bounded enumeration, but the logical group has explicit semantics. Do not turn folder events into reading a directory as a file.

Typed stat must distinguish `file`, `directory`, `missing`, and `unavailable/error`. A transient EIO, permission error, or incomplete listing is a reason to defer/retry, not to delete a remote file. Mass deletions are published only after complete verification and within the existing approval.

### 8.4. Publication and lost ACKs

After a lost response to root commit, query the outcome by mutation/transaction ID when the new protocol supports it; for the old protocol, inspect remote state and perform a correct rebase. A mismatch with the current root is not proof of failure: another client may have advanced the root after a successful commit.

On the server, the dedup record and mutation outcome must have durability consistent with the operation itself. After backup restoration/reset, the server generation changes so the client does not trust old ACKs for objects that disappeared.

For a long-lived plan, define the fate of uploaded-but-unreferenced objects. Any current or future GC must respect a bounded staging lease/pin, or the client must revalidate objects before commit. Cleanup must not delete a live upload; abandoned leases have a clear TTL/quota. Do not introduce a new GC as a side effect of performance work.

Full Rescan remains a verification tool, but a normal restart does not require repeated manual approval of the same scope. Changed ignore rules, substantially increased deletions, or a different vault/server identity require renewed risk review rather than automatic reuse of old approval.

## 9. Automatic switching: semantics and transport are separate

### 9.1. What we transfer

| Semantic stream | Small activity | Large backlog | If WS is lost |
| --- | --- | --- | --- |
| Live document operations | Microbatch of editor transactions | Compressed/coalesced operation stream or snapshot + suffix under the CRDT protocol | The same operations and ACKs through the HTTP ops API |
| File/object sync | Urgent small pack | Byte-bounded packs through the pipeline | The same content-addressed objects through HTTP bulk |
| Presence/cursors | Coalesced latest state | Old presence updates are discarded | May be temporarily undelivered; this is not user data |
| Root/control | Short priority messages | Coalesced hints + cursor reconciliation | HTTP query/poll with backoff, without losing authoritative state |

Character-level sync does not mean a separate fsync, packet, and encryption operation for every keystroke. The initial experimental live flush interval is 20–40 ms with early flush by size; editor transactions, IME composition, and paste have semantic boundaries. Under backlog, batch **the same operations** rather than replacing them with an arbitrary text file.

Do not introduce a global "the whole vault now uses WS" or "everything now uses HTTP" mode. The decision is made per lane/operation under a shared resource budget. Live traffic may use WS while a large attachment uploads over HTTP.

### 9.2. WS in the same connection and when a separate one helps

Today notify/presence WS and binary-data WS are separate channels. `OBW1` has no doc-ops frames. The "character edits and batches in the same WS" scenario requires versioned multiplexing: logical channels, independent credits, control priority, and bounded send/receive queues.

Proposed new capability, with a provisional name: `ws-mux-v2`. Server and client explicitly confirm support. Old `ws-data-v1` continues file sync; a new frame type is not sent to an old decoder.

One TCP connection still has head-of-line blocking: a live message cannot overtake an already sent large bulk frame or a lost TCP segment. Therefore:

- Keep few bulk bytes already handed to the socket; priority must take effect before `send()`.
- Measure latency, not merely whether the live queue is first in an array.
- For large objects, support negotiated fragmentation with bounded reassembly/spool and final hash verification, or send them through HTTP.
- For a single WS, reduce fragment size according to the live-latency budget and observed throughput; a supported chunk may be larger than such a fragment.
- A separate bulk socket remains an optional isolation mechanism. It does not remove competition for Wi-Fi/CPU but may reduce blockage by one TCP stream.

The user need not choose the number of sockets. Auto policy selects a measurably useful configuration with clear diagnostics. A shared WS must pass verification but does not become a dogma that makes the editor wait for a large object.

### 9.3. Router and fallback

Maintain rolling estimates per transport: connect/handshake, RTT, credit wait, send wait, ACK latency, useful throughput, failures, retry-after, and in-flight bytes. Separate queueing from service time; server-side handlers use their own durations rather than subtracting timestamps from different clocks.

Example policy to refine through tests:

1. Interactive prefers a healthy WS. Bulk selects an available path with sufficient credit and predictable completion time.
2. A hard close/error triggers fallback immediately; do not wait out the remaining minute of a shared request timeout.
3. After resume, verify half-open state with a short health probe. `state=connected` is not proof of a live server.
4. Define a soft stall by duration without progress relative to RTT/size, not one fixed 60 s limit for every request.
5. A circuit breaker, exponential backoff with jitter, minimum decision hold time, and one half-open probe prevent retry storms and WS/HTTP ping-pong.
6. Use one shared retry owner: lower-level WS fallback plus upper-level HTTP retry must not multiply requests uncontrollably.
7. After WS recovers, perform bounded catch-up; do not open one hundred document sessions or send the entire accumulated log at once.

Timeouts have separate `waiting-budget`, `waiting-credit`, `queued-to-send`, and `sent-awaiting-ack` phases. A deadline may account for size and measured throughput with an absolute cap. Device suspension does not become an artificially poor RTT or a series of aggressive retries.

HTTP in Obsidian may also materialize the entire request/response. Do not design around streaming fetch as an already available replacement for `requestUrl`; use bounded frames/requests and test the API separately.

### 9.4. Exactly-once effect, not exactly-once network

The network may duplicate, delay, and reorder delivery. Stable application mutation IDs, idempotent semantics, and durable outcomes are required; a session sequence/WS request ID does not replace them.

- Retry a content-addressed PUT with hash verification.
- A doc-op retry does not create a second character; the durable ACK names the accepted op range/watermark.
- Root commit includes the expected parent and transaction identity.
- Retry after a lost ACK preserves the mutation ID but uses a correct new transport envelope/sequence; never reuse a nonce merely for "the same bytes."
- Remove work from the local queue only after the required semantic ACK, not when WS credit is released.
- Do not allow old HTTP wire `0x01` as a fallback; show a client-upgrade message instead of silently weakening the protocol.

## 10. Real live editing is a separate durability layer

### 10.1. Selecting the CRDT and editor binding

The candidate implied by the current server skeleton is Yjs for the client with a CM6 binding. Its update protocol permits duplicate delivery and exchange of missing state; this is a useful foundation, not a finished guarantee for our storage and root projection. [Yjs: document updates](https://github.com/yjs/yjs#document-updates)

Current upstream `y-codemirror.next` distinguishes the stable Yjs 13 line from the experimental Yjs 14 line. During the spike, pin exact compatible versions and lockfiles; do not copy examples from `main` as production dependency policy. Test integration specifically with Obsidian Live Preview/Source mode. [CM6 binding](https://github.com/yjs/y-codemirror.next)

Do not keep a `Y.Doc` for each of 25 thousand notes. Active sessions are bounded by a memory budget and LRU; initially allow 1–3 hot documents on mobile, then measure. Cold documents synchronize through a snapshot/log cursor without permanent full materialization in the renderer.

### 10.2. Document identity and the file → CRDT transition

Every activated note receives a stable `docId`, epoch, initial committed content hash, snapshot watermark, and path binding. A path hash as the sole identity does not survive rename correctly.

Activation is a fenced transaction:

1. Establish a consistent initial file version without losing local unsaved changes.
2. Create one initial CRDT state and durable metadata; two clients must not independently import the entire text as two sets of new insertions.
3. Declare the epoch and authority mode through server CAS.
4. Convert new editor changes into the op journal and start bounded catch-up.
5. File projection for this epoch is now published only under materializer rules, not by a parallel blind file push.

Do not automatically leave CRDT for an ordinary file PUT merely because WS became slow. HTTP carries the same document protocol. The reverse transition requires quiesce/fence, persistence of all acknowledged ops, a snapshot, and an explicit compatibility path.

### 10.3. Editor handling

- Integrate through public extension hooks; do not ship a second incompatible CodeMirror copy in the bundle.
- Apply incremental changes rather than `setValue()` for the whole note on every update.
- Define offset mapping: UTF-16 editor positions, emoji, surrogate pairs, combining marks, and CRLF/LF; do not substitute byte offsets for text positions.
- Test IME, dictation, iOS autocorrect, undo/redo, selection, cursor, multiple panes, and switching editor modes.
- Remote apply has an origin marker and does not create an infinite local save → push → remote apply cycle.
- Local undo does not undo another user's edit as its own. Presence may be lost/coalesced; text may not.
- Local-persistence microbatches do not wait for the network. On failure, preserve dirty state and show the problem rather than reporting "everything saved."
- Severe overload reduces live/bulk batch size and the number of hot documents; it does not make input synchronously wait for fsync.
- Large paste and a large remote update are bounded by decode/apply budget. Worker decoding alone does not make the final editor transaction cheap; a separate test/support threshold is required.

### 10.4. Server journal, ACK, and compaction

The current opaque log must be replaced or fenced by a new versioned protocol:

- Serialize append/compact per document and bound the queue; move blocking filesystem work out of the async request handler.
- Durable append with framing/checksum, bounded record count/bytes, and robust tail recovery.
- ACK only after the selected durability guarantee; do not fan out a failed append as a successful change. For the first release, prefer fanout after durable append, after measuring group-commit latency.
- Persist `opId`/watermark and dedup consistently with the record, not only in process memory.
- Snapshot compaction names the covered watermark and epoch, preserves the concurrent suffix, and has CAS/generation. Use a unique temp and recoverable publication; do not replace a log with a snapshot unaware of the latest appends.
- Bootstrap: paged snapshot + suffix, with limits on total bytes, number of structs/clients, and CPU decode; a single 4 MiB limit does not bound the accumulated log or document complexity.
- Authorize every document in the vault, validate schema/epoch, limit fanout, and handle slow consumers with disconnect+resume without accumulating an unbounded queue.
- Coordinate retention of old snapshots/tombstones with offline clients. A client that is too old performs rebootstrap/merge while preserving its pending ops rather than receiving endless history in one buffer.

### 10.5. Who converts operations back into Markdown

Having a reliable op log does not yet update the file root. We cannot depend on whether the last iPhone managed to publish a snapshot before shutting down.

The preferred option for the spike is a server materializer on a compatible Yrs version that builds a `.md` projection at a durable watermark and publishes it through the shared root sequencer. Yrs aims for compatibility with Yjs; the specific version pair, UTF-16 semantics, and update encodings still need verification with a cross-language corpus. [Yrs](https://github.com/y-crdt/y-crdt)

This extends the server implementation; it does not change the current trust model: the server already stores vault content available to it. If client-side content E2EE is introduced later, server-side materialization will need reconsideration; this project does not disguise that fact.

The alternative is a fenced elected-client materializer with a persistent queue. It is more complex when every client is offline and does not provide a timely Markdown snapshot by itself. Choose it only if a demonstrated need requires it, not merely because an opaque relay already exists.

Projection invariant: `(docId, epoch, watermark, contentHash)` are published consistently. After a crash, the materializer resumes from the last watermark. "The server persisted the operations" and "all file-only readers received the projection" are distinct statuses, and both are visible.

### 10.6. Other writers, old clients, rename/delete

An external Markdown write or an old file client is a potential concurrent edit, not a reason to replace the active CRDT entirely. Two safe modes are possible:

- Import it as a diff/three-way change relative to the known projection, followed by a CRDT transaction with a predecessor check.
- Reject the incompatible write for that specific document with a clear reason and preserve the local version/conflict copy. This is the initial fallback for cases where a correct import has not yet been implemented.

The capability gate must apply to the affected semantics. An offline Mac must not block bulk acceleration for everyone else through a global "fleet 1/6" gate. At the same time, an old client must not be allowed to silently overwrite a new document epoch.

A delete creates a tombstone/fence. A late offline operation must not silently resurrect a deleted note; recovery/conflict-copy rules are documented and tested. A rename changes the path → docId mapping rather than starting a new document. Case-only renames, Windows path restrictions, and Unicode normalization must not merge two distinct documents.

## 11. Server performance without sacrificing durability

The existing `storage_writer` already has a bounded queue and grouped writes. We must not begin by disabling sync/hash verification merely to produce attractive throughput numbers.

Server work:

- Unified end-to-end HTTP and WS telemetry: accepted/inflight/completed/failed, queue wait, decode, hash, disk append, durability, and ACK enqueue.
- Measure actual handler service time separately from waiting for the bounded blocking pool/writer queue.
- Prioritize control/live durability over bulk without starvation; group-commit sizes are bounded by latency and bytes.
- Bounded frame parsing, declared-length validation before large allocations, and limits on decrypted bytes and manifest cardinality.
- Propagate backpressure to the client through credits/retry-after rather than allowing RSS/queues to grow until OOM.
- Objects with the same hash may be deduplicated during concurrent PUTs without accepting different content under the same key.
- Limits on active vault/document sessions, fairness between devices, and disk-full recovery.
- CRDT materialization/compaction and pack maintenance do not block the server event loop.

Do not change storage layout, retention, or wire format for a speed tweak without a separate migration and rollback. An idle server CPU is a reason to measure waiting, not proof that the server needs more threads.

## 12. Diagnostics and user interface

Instead of showing only `1266/25440 · 1.0 f/s`, compactly show:

- The current phase and recent seconds of useful progress: prepare / check / upload / commit / apply.
- Prepared, object-ACKed, and committed files separately; deletes separately from uploaded files.
- Content bytes and protocol bytes separately; local hash/read work is not called upload speed.
- The measured rate for the current window plus total elapsed time; ETA only when the stage and backlog are predictable enough.
- When no progress is occurring: `waiting for read`, `waiting for WS credit`, `awaiting ACK`, `paused by OS visibility`, `retry at …`, or `file needs attention`.
- `In sync` only when there are no pending local mutations, unacknowledged document operations, or required remote reconciliation. Root equality may remain a separate technical line.
- The transport choice and a brief reason; actual worker capability/fallback reason, build ID, and effective budgets.

The diagnostic ring buffer is bounded. In normal mode, do not write paths/content/tokens to telemetry; a specific path error requires explicit local opt-in with the ability to redact the export. Do not perform an HTTP health check for every file or write a log entry for every keystroke.

Metrics must be available during an operation. Sampling-overhead gate: compare the same workload with tracing off/on; the initial target is ≤ 3% CPU/wall overhead without queue growth. Fake event-loop tests complement a real input-to-paint measurement; neither replaces the other.

For iPhone reloads, collect the phase/build marker together with the system crash/Jetsam report when available. The Jetsam report must identify the terminated process and reason specifically; whole-system memory or one orphan marker does not establish the cause. Do not request complete reports unnecessarily: they may contain information about other applications. [Apple: Jetsam reports](https://developer.apple.com/documentation/xcode/identifying-high-memory-use-with-jetsam-event-reports)

## 13. Implementation plan and dependencies

The original backlog follows; the current status is maintained in the [implementation progress report](responsive-sync-progress.md). A PR is a bounded unit of review, not a promise of a calendar deadline. Sizes: S is a local change, M spans several related modules, and L is protocol/persistence work with separate review. Estimate schedules after P0 and two capability spikes; do not promise the whole project "in one evening."

### P0. Reproducibility and data protection

Outcome: time loss can be explained, and metadata errors cannot create deletes.
There is no need to wait for CRDT.

| PR | Size | Scope and primary files | Dependencies | Gate |
| --- | --- | --- | --- | --- |
| 01 | M | Active stage tracing, build/capability diagnostics, baseline harness; `perf-trace`, `debug-modal`, server `perf`, CI | None | scheduler/read/hash/credit/ACK/commit are distinguishable; trace overhead is measured; plugin tests actually run in CI |
| 02 | M | Typed stat, file/folder listeners, error classification; `platform`, `sync`, tests | None | EIO/permission/list failure does not produce deletes; folders are not passed to readFile; a newer generation is not cleared |
| 03 | M | Scheduler/yield/lifecycle abstraction; remove timer-chain dependencies from the hot path; `push`, `sync`, `main` | 01 | Foreground/hidden > 6 min A/B; UI fairness; scheduler drift is not labeled as disk latency |

PR01 does not need a perfect UI immediately. It needs a small trace that explains
the current minutes of waiting. The new trace must not retain the contents of the
user's vault.

**Current CI state:** PR01 connected plugin `npm test` after the TypeScript
typecheck; a separate fresh-WASM job additionally checks scalar/SIMD parity, the
browser worker, and the production bundle. New regressions must still enter the
real test entrypoint rather than remain separate files that are never run. A
green remote CI result applies only to the exact commit tested and does not
replace the device gates below.

### P1. Responsiveness and mobile memory

Outcome: the first candidate for user release without new file/CRDT semantics.
The budget is verified on an iPhone 16 Pro Max; any large-file limitation is
shown honestly when the API cannot provide a safe read.

| PR | Size | Scope and primary files | Dependencies | Gate |
| --- | --- | --- | --- | --- |
| 04 | M | Diagnostics/recovery for desktop workers, mobile worker capability spike; `desktop-hash-workers`, `wasm-runtime`, packaging | 01 | Startup success/failure is diagnosable; fallback does not change hashes or loop indefinitely |
| 05 | L | ResourceBudget, bounded allocations, mobile ranged/resource-reader spike, large-file admission; `platform`, `push`, codecs | 01, 02, 04 spike | Byte ledger returns to baseline after cancel/error; oversize does not hang the entire sync; real mobile memory traces |
| 06 | M | Short governor windows, independent limits, suspend-aware lag, controlled probes | 01, 03, 05 | No permanent recovery=1 after hiding; growth on an unconstrained device, reduction under foreground lag |

P1 is not declared complete from Node tests alone. Real-device foreground editing +
bulk is required. Better desktop timers are not proof that the iPhone reload is fixed.

### P2. Resumable batched file sync

Outcome: bootstrap can be interrupted, new notes arrive quickly, and upload does
not wait for global completion before publishing all progress.

| PR | Size | Scope and primary files | Dependencies | Gate |
| --- | --- | --- | --- | --- |
| 07 | L | Segmented durable store/recovery primitives, schema/migration; `journal`, `sync-base`, new storage module | 02, 05 | Torn tail, replace failure, disk full, reload recovery; bounded replay without losing acknowledged generations |
| 08 | L | TransferPlan and persistence of scan/hash/object progress, cache invalidation, approval scope | 07 | Kill at every boundary in §8; restart does not globally rehash confirmed unchanged work |
| 09 | L | Read/hash/check/upload pipeline, lazy batches, bounded prefetch, reread counters | 03, 05, 06, 08 | Preparation overlaps network waiting; memory is bounded; a slow file does not block independent files |
| 10 | L | Short root transactions, commit outcome recovery, urgent-file lane, incremental tree apply | 08, 09 | A note edit passes through bootstrap; lost ACK and a concurrent root preserve data; rename groups are consistent |
| 11 | M | Bounded pull/apply, open-editor reconcile, staging resume/cleanup | 02, 03, 05, 07, 10 | Remote pull does not overwrite an unsaved local editor; a large download resumes; UI gate passes |

Before the P2 release, disabling the new feature returns to the old transport,
but not to the old incorrect interpretation of a persisted plan. Migration and
downgrade have separate tests. "Flag off" must not mean deleting the journal.

### P3. Adaptive transport and shared WS

Outcome: file sync automatically selects WS/HTTP with the same mutation semantics
across disconnects; multiplexing works without degrading urgent delivery.

| PR | Size | Scope and primary files | Dependencies | Gate |
| --- | --- | --- | --- | --- |
| 12 | L | TransportRouter, phase timeouts, idempotency/outcome abstraction, shared retry owner; `api`, `ws-data` | 01, 08, 10 | Close, half-open, and lost ACK do not duplicate effects; fallback does not wait a universal minute |
| 13 | M | Bounded tx/rx/decrypt queues, cancel propagation, credit accounting with a memory budget | 05, 12 | A slow consumer does not grow RAM without bound; cancel releases the reservation and request lifecycle |
| 14 | L | Versioned WS multiplexing and fragmentation when necessary, server parity, same-socket priority | 12, 13 | A large transfer + interactive test messages meet the latency gate; an old decoder receives only old frames |

P3 permits an early release of adaptive HTTP/WS without the new mux if it already
provides a measured improvement. The mux capability need not wait for CRDT
readiness, but doc frames are enabled only after P4.

### P4. Reliable character-level editing

Outcome: real editor ops, durable ACK, projection, and safe transport switching.
This carries the highest data-integrity risk; a separate protocol review is
mandatory.

**Scope decision 2026-09-08:** P4 is frozen as a dormant feature track and does
not block stabilization/release of file-sync P1–P3/P5. The project owner
estimates the practical probability of concurrent editing of one file at below
1%; current protection remains conflict copies, revision/root history, and
fail-closed fences. The existing server live-document scaffold is not activated
or declared ready. PR15–20 were not implemented; no compatible
Yjs/CodeMirror 6/Yrs combination has been selected or verified. Returning to P4
requires a separate explicit decision and the full PR 15–20/gates set; the
accumulated specification below is retained so safety shortcuts cannot be
substituted for complete CRDT semantics.

| PR | Size | Scope and primary files | Dependencies | Gate |
| --- | --- | --- | --- | --- |
| 15 | M | Yjs/CM6/Yrs compatibility spike, exact versions, memory/IME/large-paste bench, ADR | 04, 05; research may precede P3 | JS↔Rust exchange has no drift across the corpus; real Obsidian input test; materializer approved |
| 16 | L | Durable doc log, op IDs/watermarks, HTTP ops parity, safe compaction, server queues | 07 design, 12, 15 | append failure is not acknowledged; concurrent compact does not lose the suffix; server crash preserves ACKed ops |
| 17 | L | Editor binding, local op persistence, limited doc sessions, offline replay | 03, 05, 07, 15, 16 | No network wait in input; IME/undo/remote origin tests; reload preserves locally persisted ops |
| 18 | L | Server materializer and projection commits by doc watermark | 10, 15, 16 | All clients offline after durable ACK: the file is still published; crash/restart is idempotent |
| 19 | L | Fenced file↔doc activation, rename/delete epochs, routing live micro/bulk batches | 12, 14, 17, 18 | Disconnect at any transition point neither duplicates text nor applies an old snapshot over ops |
| 20 | L | Legacy-client fences, external file import/conflict workflow, offline epoch recovery | 11, 18, 19 | An old Mac neither blocks independent bulk nor overwrites an active doc; offline delete/edit is safe |

P4 is initially enabled only on a small test vault and selected notes. Do not
migrate all 25 thousand files to CRDT just to check a "live" box. File-only
documents and attachments continue to work through P2/P3.

### P5. Calibration and release

Outcome: demonstrated platform support, reproducible artifacts, and a clear
rollback.

The stabilization candidate does not complete P5: real device and
network/fault/soak runs, the exact remote-CI result, artifact/upgrade/rollback
verification, tag/release provenance, and a consistent Nix build are mandatory
before release.

| PR | Size | Scope and primary files | Dependencies | Gate |
| --- | --- | --- | --- | --- |
| 21 | M | Runtime policy calibration, energy/thermal scenarios, slow-link fairness, transport hysteresis | 06, 09, 12; 19 when live is activated | Faster than baseline without exceeding UI/RAM gates; no retry oscillation |
| 22 | M | Device/e2e fault harness, golden traces, correction of the old test plan | Built incrementally from 01; finalized after all capabilities being enabled | Matrix §14–15 covers every supported capability; dormant P4 is not represented as verified; real results are attached |
| 23 | M | UI truthful status, actionable pending/error states, compact diagnostics export | 01, 08, 10, 12; live portion only when 18 is activated | No "in sync" with a dirty queue; the user understands the waiting phase without reading source code |
| 24 | M | Release provenance, artifact verification, Nix consistency, upgrade/downgrade tests | All PRs included in the release | Git/tag/release/artifacts/build ID match; clean install and update are verified |
| 25 | M | Soak/canary reports, final gates, and documentation of platform limitations | 21–24 | No unexplained reload/data loss; rollback verified with preserved new data |

These PRs must not become one enormous branch lasting until the end of P5. User
releases: R1 after P1, R2 after P2, R3 after P3, and R4 after verified P4. The
relevant P5 steps are repeated for every release; version numbers are assigned at
release time, not invented in the plan.

Before implementing a new persisted/wire format, record a short ADR covering
invariants, schema/version, capability negotiation, memory/size limits, old-reader
behavior, crash recovery, and rollback. The minimum ADR set is
scheduler/lifecycle, resource ownership/mobile reader, transfer-plan/commit
outcomes, WS mux/fragmentation, doc authority/projection, and local persistence
guarantees. A prototype does not become a production format merely because it
passed a happy-path test.

Critical path:

```text
01 -> 03 -> 05/06 -> 07/08 -> 09/10/11 -> 12/13/14
                     |                         |
                     +-> 15 -> 16/17/18 -> 19/20 -> live release

02 — independent early data protection.
21–25 — verification and release for every enabled capability set.
```

## 14. Device and workload matrix

### 14.1. Devices

| Platform | Required real-device runs | Special focus |
| --- | --- | --- |
| iOS | User's iPhone 16 Pro Max + a less powerful supported iPhone when possible | Input/IME, WKWebView/native copies, app switch/lock, reload reason, whole-file admission |
| iPadOS | Real iPad; exact model/memory recorded in the report | Split View/Stage Manager, multiple panes, memory pressure, large vault |
| macOS | MacBook Pro and MacBook Air; separate Intel verification when support is claimed | Occlusion hidden, Apple Silicon/x64 workers, battery, case-sensitive/insensitive FS |
| Windows | Native Obsidian on the primary Windows PC | Real Windows IO/errors/rename, background timers, worker startup; WSL bench separately |
| Android | Real modern phone + a supported memory-constrained device class | WebView/adapter differences, pause/resume, low power/Doze, storage errors |

Record the Obsidian version, plugin semantic version + build SHA,
OS/WebView/Electron information by whatever means are available, scalar/SIMD,
actual worker capability, power mode, and installed plugin set. Mark an untested
device as such; a simulator does not replace the iPhone memory gate.

Android Doze/App Standby may defer network access and CPU work in the background.
Do not promise that an ordinary plugin can bypass this mechanism; verify recovery
and the absence of false network-error diagnostics after wake-up. [Android: Doze and App Standby](https://developer.android.com/training/monitoring-device-state/doze-standby)

### 14.2. Corpora

- `C-small`: 1 000 markdown files, varied sizes from 0–100 KiB, directories, Unicode, rename/delete.
- `C-vault25k`: approximately 25 000 files with a size distribution close to the observed vault; synthetic content/paths and a fixed seed.
- `C-vault100k`: 100 000 files for testing metadata/WAL/tree memory, not a mandatory promised speed target for the first version.
- `C-attachments`: several types of large files, including files larger than the mobile whole-read budget; unique and repeated chunks.
- `C-live`: Unicode/IME, undo, concurrent edits, long history, many offline producers, paste at 100 KiB/1 MiB and higher thresholds.
- `C-paths`: file→directory, directory→file, case-only rename, NFC/NFD, invalid Windows names, missing/permission/EIO.

For each corpus, retain the seed, file count and distribution, total content
bytes, expected hashes, initial root, and expected final changes. Use a real vault
only in a separately approved safe run; do not perform destructive fault
injection on user data.

Workload modes:

1. Cold initial upload, empty remote.
2. Remote already contains objects; local metadata cache is empty/partial.
3. Warm unchanged startup.
4. Sparse edit: one current note amid a large old backlog.
5. Mass rename/metadata changes without changing bytes.
6. Pull of a large snapshot with a locally open modified file.
7. Live editing between 2–5 devices plus attachment bulk.
8. Network-off offline editing, followed by reconnect catch-up.
9. Slow disk/adapter, slow server durability, and slow consumer, separately.
10. Foreground/hidden/suspend/resume, low-power, prolonged thermal load.

Running with the minimum plugin set establishes a baseline; then repeat with the
user's normal workload. If the indexer is investigated separately, do so only in
a test vault while preserving its configuration. Do not stop or reconfigure
Syncthing; when an external writer is active, record it as a run condition.

### 14.3. Network

Test LAN, RTT 100–250 ms, constrained bandwidth, packet loss, periodic
disconnects, Wi-Fi → cellular, server restart, and silently dropped idle WS.
Inject faults in a separate environment; do not modify the production firewall
or router.

The same number of files does not imply the same network work. Comparisons
separate unique upload, dedup check, root commit, and download; progress counters
separately count "usefully published," "read again," and "transmitted again."

## 15. Fault injection and invariants

### 15.1. Data integrity

| Failure | Required outcome |
| --- | --- |
| Kill after local WAL append, before sending | Persisted mutation is recovered without a Full Rescan of the entire vault |
| Kill after object ACK, before root commit | The object is not considered a published file; preparation is reused after checking server generation |
| Root committed, ACK lost, another client advanced the root | Outcome recovery/rebase without duplicate conflict copies or loss of local changes |
| Local base persisted, journal ack not persisted | Repeated replay is idempotent; the newer generation is not cleared |
| WS→HTTP fallback after server append | The effect is not duplicated; application ID is unchanged and transport sequence is correct |
| CRDT append failed/disk full | No durable ACK; the local op remains pending; no false-success fanout |
| CRDT compact concurrent with append | Snapshot + suffix contain every acknowledged operation |
| Last live client killed after durable ACK | The server journal and materializer retain the text and publish the projection |
| Old snapshot/file update arrives for an active doc | Fence/import/conflict; no blind overwrite |
| Offline edit after delete/rename | Explicit epoch/tombstone policy without accidental resurrection |
| File changes during read/hash/upload | Drift/retry or a separate generation; no manifest assembled from different versions |
| `stat`/listing temporarily broken | No inferred mass deletion |
| Staging replacement interrupted between rename steps | The old or new file is recoverable; the only copy is not silently missing |
| Server restored from backup | Old ACKs are checked against the new server generation |
| Lease expired, unreferenced object deleted | Revalidate/reupload before root commit; the root does not reference a missing object |
| Corrupted/torn local journal | The valid prefix is retained; an unknown middle is not disguised as success |

### 15.2. Responsiveness and boundedness

- One slow read does not hold all independent jobs behind a giant `Promise.all` barrier.
- A slow WS consumer and decrypt queue do not accumulate unbounded ciphertext buffers.
- Cancel in waiting-budget/reading/encoding/sent releases exactly the resources it owns; after sending, the outcome remains unknown until reconciliation.
- Post-resume lag does not trigger a cascade of recovery or increase penalties for the same event.
- Long synchronous tree update, JSON stringify/parse, CRDT bootstrap, and compaction are included in UI/memory tests.
- Repeated note open/close, reconnect, worker crash, and plugin unload do not leave growing retained state.
- Bulk on a fast desktop increases useful parallelism to the measured optimum instead of being limited to one operation by an old profile.
- Reducing the byte limit does not block control frames required to release credit: use a separate reserve and verify the absence of deadlock.
- Negotiated fragmentation does not let one client occupy the entire reassembly spool with unfinished objects; quota/timeout/hash validation are mandatory.
- When the local disk is full, the editor remains available, but the persistence failure is visible; do not claim that the data is "saved."

### 15.3. Protocol and security

- Golden vectors for AEAD, sequence/replay, codecs, scalar/SIMD, Yjs/Yrs, and tree formats.
- HTTP wire, WS envelope, WS data frame, file tree, and CRDT update versions are distinguished explicitly; "v2" for one layer does not mean "v2" for the others.
- Fuzz lengths, flags, frame count, manifest cardinality, and decompression/CRDT complexity if those features are enabled.
- Repeating an application operation under a new envelope is permitted by idempotency; raw envelope replay is rejected according to the current security model.
- Authorization and quota are the same for HTTP, WS, and multiplexed doc channels; fallback does not create a less secure endpoint.
- Device credentials and sequence state do not appear in synchronized `.obsidian` payloads or diagnostics exports.

The current publishable release gate is described in
[transport-security-test-plan.md](transport-security-test-plan.md) and aligned
with [transport.md](transport.md): wire `0x02`, raw envelope replay rejection,
separation of the pre-open decoy from encrypted semantic errors, a 64 MiB body
cap, and separate idempotency for application retry. Local/private test notes are
not release authority; do not regress security to satisfy their old expectations.

## 16. Verification, artifacts, and Nix

### 16.1. What to run during implementation

Use existing tests and add new ones to real entrypoints. Example commands: run
them at the appropriate stage; their presence in the plan does not mean they
have been run.

```bash
# From plugin/
rtk npm test
rtk proxy npx tsc --noEmit
rtk npm run bench:perf
rtk npm run bench:bulk
rtk npm run bench:ws-data
rtk npm run bench:ranged
rtk npm run bench:governor

# From the repository root
rtk cargo fmt --all -- --check
rtk cargo test --workspace --locked
rtk cargo clippy --workspace --locked --all-targets
rtk proxy node scripts/verify-wasm-parity.mjs plugin/wasm
```

E2E uses an isolated compose stack and separate vault IDs. Device
UI/foreground/OS-kill tests cannot be replaced by a Node benchmark. Run WASM
parity against artifacts from the current build, not stale files left behind by
chance.

Every performance report includes raw bounded traces, parameters, and results
from at least five repetitions for short throughput tests. For soak and
lifecycle tests, report the explicit cycle count/duration and every failure, not
only the best attempt. Do not present p99 statistics from a short run as a
reliable tail without enough samples.

### 16.2. What "release ready" means

- Git changes and reviews/tests correspond to the exact release commit; there is no manually fixed `main.js` that cannot be reproduced from the tag.
- The version is identical in root/plugin manifests, package metadata/lock, and the corresponding release declarations; `versions.json` reflects the minimum Obsidian version actually verified.
- `main.js` contains build identity: semantic version, Git commit, protocol capabilities/schema; debug output shows both manifest and bundle identity and detects a mismatch.
- A separate artifact manifest stores hashes of the final files. Do not attempt to embed the final self-hash of `main.js` inside `main.js` itself.
- The Git tag points to the verified commit; the GitHub Release contains the complete required set of flat assets/zip; clean BRAT/manual install and update are verified.
- Nix version/revision corresponds to the same source. `npmDepsHash` is recalculated reproducibly when dependencies change; do not substitute a random hash merely to "make it pass."
- A Git commit SHA-1 and a Nix content hash are different entities. Record the full commit ID, and use the fixed-output hash in the format Nix requires, usually `sha256-…` in this repository.
- The server/container artifact and its digest, plus compatibility of the old/new client, are verified, not merely successful upload of release assets.
- The release workflow must be connected to the required unit/integration gates; the current fmt/tsc preflight does not replace the full test suite.
- Canary devices actually loaded this build rather than merely downloading a new manifest. Coordinate device updates without a forced reload during typing.
- Release notes contain measured changes, supported platforms, remaining limitations, and recovery instructions that do not delete the only copy of data.

### 16.3. Rollback

Transport/policy flags safely disable the accelerator and preserve the journal.
Stateful schema/doc epoch changes require a compatible reader or a fence, not
"just install the old main.js."

Before an irreversible migration, create a recoverable backup/snapshot and
verify restoration in a test environment. Compaction does not delete the only
acknowledged history before the replacement is published and verified. Retain
previous release artifacts and an upgrade/downgrade report.

Stop rollout on any loss of acknowledged changes, increase in unexplained
reloads, memory-bound violation, phantom deletes, or unstable transport retry.
Switch to a safe compatible path while preserving forensic data; do not force
the user to press Full Rescan again as a universal remedy.

## 17. Open decisions and explicit boundaries

| Question | Working decision | When to revisit |
| --- | --- | --- |
| Mobile ranged input | Capability-qualified ranged path, safe whole-read admission, and oversized streaming/pending fallback are implemented | P1 real-device gate: if required attachments fail memory/reload limits, a host/native path or a refined support boundary is needed |
| One or two WS connections | Logical mux + automatic carrier selection; HTTP for disruptive live transfers | P3 based on head-of-line latency and byte budget |
| CRDT | Not selected; a possible Yjs/CM6 + Yrs materializer pair requires separate verification | P4 spike; no implicit switch to an unstable upstream |
| Local store | Portable segmented WAL/snapshot | If real adapter durability/stream limits fail the recovery/memory gate |
| Native mobile background sync | Outside ordinary plugin scope | Only as a separate project with host/native capabilities |
| Compression | Do not enable by default before CPU/copy/ratio measurements | After the bounded pipeline; negotiated limits against decompression bombs |
| Tree format upgrade | Do not couple ordinary batching improvements to a fleet-wide tree migration | Separate compatible migration with its own gate |
| QUIC/WebTransport/P2P | Not required for this architecture | Only with a demonstrated transport bottleneck and portable support |
| Global atomic vault snapshot | Not promised for gradual bootstrap | Separate feature if an explicit snapshot barrier is needed |

Actual data is still missing: the exact cause of iPhone reloads, the native
allocation profile, mobile reader capabilities, the real file distribution,
WS/HTTP phase latency on a specific network, and native Windows reads. These are
P0/P1 measurement tasks, not a reason to immediately require another complete
25-minute rescan from the user.

## 18. First three concrete deliverables

1. **PR01: expose the cause of waiting.** A small diagnostic build + automated test gate. Obtain one foreground/hidden trace on Windows and one foreground editing/memory trace on iPhone; no destructive experiment on the working vault.
2. **PR02: do not turn IO problems into data changes.** File/folder/missing/error, regression coverage for EISDIR/EIO, and preservation of newer generations. This reduces risk whether or not the timer hypothesis is confirmed.
3. **PR03: fix the execution queue.** Verified scheduler/yield, correct distinction between hidden/suspended, and measured UI fairness. PR04–06 then close worker/memory/governor and produce the first release candidate.

After P1, focus shifts from "do not hang" to "do not redo work" and "publish
fresh work immediately": a durable plan, pipeline, and short commits. Live
editing is added on this foundation while preserving the same journal, budgets,
and transport outcome semantics.

Final acceptance is not "batches exist" or "the CPU is busy now." It is a body
of evidence: the user can type during a large sync; fresh text arrives quickly;
persisted work continues after kill/suspend; acknowledged data converges; memory
does not grow without bound; and the release is reproducible from Git/Nix while
reporting status equally honestly on every claimed platform.
