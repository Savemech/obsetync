# Incremental root planning: integration contract

Date: 2026-09-05. Supplement to [roadmap §5/7/8](responsive-sync-roadmap.md)
and the [implementation status](responsive-sync-progress.md).

In the fourteenth slice, `RootReviewedQueue` is connected to the durable
engine: one review session per drain, bounded claims, and separate urgent
roots. The implemented contract and remaining qualification gates are
described below. The legacy branch does not receive this cache or the new
durable guarantees.

## Measured problem

`plugin/scripts/diagnose-root-drain.mjs` runs the actual engine with the real
journal/base/root-runtime classes, but with a synthetic JSON tree/server and
`MemorySegmentedIO`. The corpus consists of independent one-byte notes; every
object is already present on the server, and there are no concurrent edits.

| Queue | Quiet root commits | Old stat/materialize visits | New stat/materialize visits |
| --- | --- | --- | --- |
| 600 | 3: 256, 256, 88 | 1 032 | 1 200 |
| 2 500 | 10: nine of 256, then 196 | 13 480 | 5 000 |
| 25 000 | 98, calculated for the same quiet workload | 1 233 232, calculated | 50 000, calculated; not an engine measurement |

The old repeated traversal is `N + (N − 256) + …`; it is not evidence of a
slow disk or overloaded server. The quiet path now performs one full audit
and one re-stat of selected paths: `2N`. At 600 paths, this is **more** stat
work than before; the benefit is scaling and separate source revalidation,
not a promise that every small push is faster. The graph is built once,
claims visit N paths, and full-queue take/restore is absent. Synthetic
durations and heartbeat measurements do not prove Obsidian speed, editor
operation, native IO behavior, or absence of Jetsam.

The new counters were reproduced in three sequential runs after freezing the
source and stopping all other project tests/builds; the [complete raw
results](root-drain-reviewed-counts-2026-09-05.json) are stored separately.
Median whole synthetic drain time is 365.993/5 067.486 ms. There is no old
paired timing baseline; these durations cannot support a claim of increased
native throughput.

## Ownership and authority

The authoritative queue remains in `DirtyPathSet` and the durable journal.
The plan stores metadata from one reviewed epoch, not contents, a queue of
promises, event history, or a new source of ACK authority.

- `capture()` preserves immutable owner-local entry identities. Equality of
  path/mtime/size/journalId does not replace identity. Two persistent indexes
  preserve the previous insertion order; their O(N) metadata is not an RSS
  bound.
- `claim(capture, paths)` and the engine's `claimHints(hints)` atomically
  detach at most 256 exact entries; a stale endpoint cancels the entire claim.
  The rest of the queue is not detached and restored after every root.
  Returned hints preserve their existing take/restore provenance, including
  the absence of an own durable ID on a scan/live hint with an inherited
  watermark.
- Selection by `next()` advances only the graph planner's scheduling cursor.
  It does not detach dirty entries, acknowledge review, authorize an upload,
  write the base, or ACK the WAL. `null` means "this plan has no selectable
  group," not "the vault is synchronized."
- On a failed/stale claim, the engine returns to live state; on an unknown
  root outcome, it returns to the existing recovery coordinator. The
  ACK/base/conflict-copy tail remains with the current native owner until
  actual completion.

An unbounded number of captures/plans must not be retained. There must be one
owner, one planning session, and one active claim, followed by release after
drain/invalidation. Cache admission bounds cardinality and accounted metadata
before copies; unaccounted runtime/host heaps must not be described as covered
by this limit. An admission refusal preserves dirty/WAL state and does not
authorize legacy protocol fallback. Byte charges are an experimental metadata
model, not a measured worst-case JS heap size or a reservation from the shared
`ResourceBudget`. Caller-owned inputs lie outside this model and must remain
immutable; repeated admission does not protect against arbitrary later
mutation of an already-retained caller-owned object. The compact planner shares
matching queued/ready scalar witnesses and canonical paths/hashes, and releases
the union graph after constructing compact components. The initial 16 MiB
ceiling admitted the short synthetic 25k corpus but rejected a production-shaped
corpus whose modeled path charge exhausted the remaining budget. For 24,655
hashful rows this boundary is roughly a 60-character unescaped ASCII average;
Unicode and JSON escaping change it. The ceiling is now 32 MiB; a 25k matching
hashful long-path fixture must exceed the old limit and remain below the new one,
while distinct ready overrides are charged separately.
This is not a universal memory-admission proof for every vault. A separate
review ledger has
a 65 536-path / 8 MiB experimental charge ceiling; combining the models,
original immutable AVL captures, and native heaps still requires shared
resource accounting.

`captureTracking(1024)` installs the initial cut and a bounded coalesced
observer synchronously. Repeated events for one path do not accumulate
history; an unchanged owned claim/restore and ACK removal do not appear as a
new edit. Overflow or observer replacement invalidates the entire review
without losing dirty/WAL state. Proof-bearing row hints do not require a full
historical AVL capture to be retained for every edit.

## Review epoch

The first complete cooperative typed-stat audit is mandatory. Its receipt
binds exact captured entries, journal epoch, root scope, ignore policy, tree
format, base witness, aggregate changes/deletions, and reviewed omissions.

One shared dirty revision invalidated by every keystroke is insufficient: it
would merely move the repeated full traversal into review. A normal edit must
replace one coalesced row/component and update the aggregates. A scope/policy
change, Full Rescan, recovery/base reload, or external pull terminates the old
review epoch. The engine's own accepted root advances that epoch only through
a verified successor, not through an arbitrary match with the current root.

A new typed stat is required before preparing the selected ≤256 paths. A new
tracked missing/directory state that was not part of the reviewed deletions
requires a complete audit **before that deletion is published**. A mass
disappearance cannot be "approved" in pieces smaller than the review
threshold. EIO/permission/unavailable does not become absence or a policy
omission. Growth in the aggregate change/deletion count beyond the approval
requires a new review, including coalesced new events.

A fresh stat proves applicability and classification, not bytes. Existing
source-generation and content-verification guards remain in force, including
for same-size/same-mtime edits. An old prepared hash cannot be recovered from
the fingerprint alone.

The fresh-send guard also runs after asynchronous intent hashing/persistence
and final negotiation, immediately before the initial commit API call. A
refusal leaves the stored intent for normal outcome recovery instead of
discarding it. The guard does not apply to query/cancel of an existing intent
and does not interrupt an already accepted receipt/base/journal tail.

## Dependencies and cooldown

The graph includes explicit rename links, pending/uncertain registrations,
nonqueued intermediates, and equal-journal-ID membership. A selected group
must be complete; a missing, excluded, cooled, or stale endpoint holds the
entire component. Shared-ID membership includes live and claimed records until
exact settlement. A new link to a claimed path must close applicability.

In this integration, a new structural dependency, a change to a linked
generation, or a new deletion requires a **complete** review/graph rebuild;
local reconstruction of only the affected component is still future work. An
opaque revision tracker distinguishes registrations/confirmations from a link
that has actually finished being removed. Removal owned by an ACK updates the
witness synchronously before the next await; an event during cache persistence
is not absorbed by that update.

If a cooling source exists anywhere in the queue, the current
`DeferredChangeTracker.partition()` holds **all deletes** and the dependency
closure. Calling it only on the selected page violates this guard. The review
session retains the global cooling set; the selected claim additionally obeys
the current source cooldown. Releasing the last cooling source after an
accepted root rebuilds the plan so previously held deletes do not remain
permanently outside the static cursor. An unknown/stale aggregate does not
authorize deletions. A capacity/feed/worker change invalidates the relevant
cooldown, not every source hash.

A manual retry does not clear the known cooling set. Its finite source retry
slots survive a local ACK and independent roots and participate in readiness
for the current drain. A fresh stat whose fingerprint changes without a new
hint receives a separate finite reviewed retry opportunity; the old hint-only
cooldown cannot silently remove that source from an already prepared graph.
The known deletion risk does not disappear until the actual source outcome.
A delete may proceed with retried cooled sources only when **all** such
sources are in the same atomic bounded selection; source-budget deferral
before candidate mutation continues to hold all selected deletes and complete
dependency groups. A positively confirmed independent local omission does not
require rereading the formerly oversized source: its bounded owned ACK is not
blocked by resource cooldown, but it still requires the exact generation and
absence of dependencies.

The new static planner uses sequential whole components: if the next eligible
group does not fit in the remaining root capacity, it starts the next root.
This leaves small unused gaps, but `next()` does not search the entire backlog
for a fitting tail. The existing greedy selector retains its previous behavior
for callers outside the activated reusable plan.

## Urgent service and verification of gains

The engine alternates urgent-first and bulk-first roots. Independent recent
upserts enter a separate root of up to 64 paths, with the active path first;
bulk preserves whole components of up to 256 paths. The overlay is bounded to
1024 coalesced paths. Linked urgent changes still require a complete rebuild
and do not receive the fast path. This is a bound on scheduling opportunity,
not a demonstrated latency in milliseconds or a complete small-file byte/time
scheduler. A complete 256-path bulk component receives a whole root even when
the urgent queue is nonempty. Bulk age is not reset by every new editor event.
A synchronous WASM call that has already begun cannot be interrupted by a
priority change; preparation itself requires slices.

Verifiable invariants and remaining acceptance gates:

1. Quiet 25k queue: one complete review, followed only by selected-path
   re-stat; no whole-queue take/restore/sort/graph on every root. Count
   materialization, graph work, claim visits, and real host yields separately.
2. N+1 at every await boundary (review/stat/read/upload/ACK); a no-ID,
   inherited-ID, equal-looking hint, or obsolete claim must not erase new
   state.
3. New rename links, pending confirm/error, chains/cycles, missing peers,
   component overflow, and late source deferral must not split dependencies.
4. A cooling source outside the selected root holds deletes; unexpected mass
   missing requires a full audit before the first new deletion.
5. Continuous urgent edits have a bounded service opportunity, bulk does not
   starve, and an oversized urgent item does not block independent small
   files.
6. Cache overflow/abort/dispose/unknown outcome preserves the authoritative
   queue; a stale/no-op selection does not start a hot continuation loop.

After deterministic correctness/work-count tests, a separate actual
packaged-WASM + temporary native-filesystem engine fixture is required for
25k paths. Seed and cold final validation stay outside the measurement;
profile/corpus/durability are fixed, with at least three isolated runs and raw
results. Measure callback→durable journal, callback→matching accepted cut,
superseded generations, oldest/root latency, IO bytes/counts, controlled
memory, and heartbeat gaps. Synthetic sealed transport is labeled separately.
Even such a Node/native-FS run does not replace the WebView/Obsidian bridge or
the required iPhone/iPad/macOS/Android/Windows UI, suspension, thermal, and
memory gates.
