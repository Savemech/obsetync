# Segmented sync-base (PR07 implementation slice)

Status: implemented locally, not a release or native durability qualification.
The change journal now shares these primitives; see [segmented journal](segmented-journal-adr.md). TransferPlan,
cross-store commit intent/outcome and joint base/journal ACK cuts are separate.

## Publication and ownership

The committed base now lives under the already excluded internal plugin directory
`sync-base.store-v1/`. A small sealed `head.json` names one epoch, a monotonic
generation/sequence, a snapshot page generation/count/cut, and a contiguous WAL
segment range. Pages contain only entries; head metadata contains root, last-sync
timestamp, diff-page cursor/counters and the original bounded bulk approval.

WAL segments are immutable, checksummed frames. All segments of a checkpoint are
written and read back before publishing its head. A cursor after its entry rows
therefore becomes visible at the same cut, not by skipping a damaged earlier row.
Normal head publication is verified `.next` → rotate main to `.bak` → promote
stage → verify main. A selected recovery stage is promoted without rewriting the
only newest head. IO failure poisons further writes until validated reload.

Recovery compares head identity/generation and validates the selected complete
page/segment closure, including schema, epoch, bounds and contiguous sequence.
It builds a provisional base and exposes it only after successful validation.
Unknown/corrupt authoritative data or unavailable IO never means an empty base.
Unreferenced staged objects are not replayed. An incomplete stage may be ignored
only with another validated publication; an existing store directory with no
valid head requires recovery, not a guessed new epoch.

Checksums detect accidental corruption, not malicious modification. Adapter
completion and rename are not claimed to provide native fsync or power-loss
durability. There is one serialized plugin writer, not cross-process locking.

The live entry index is immutable/path-copy AVL. Capturing a root is O(1), updates
copy O(log n) nodes, and page iteration uses O(log n) stack space. Metadata and
stored entry values are detached from callers. A snapshot root/metadata is
captured in the same synchronous step as the final drained-mutation check; an
await continuation must not substitute a newer uncommitted state for that cut.
Post-cut mutations are published in later WAL segments, including mutations
arriving while snapshot pages are written. The store caches decoded copies of
the exact serialized head, never a mutable initialization metadata object.

## Bounds and maintenance

- Page payload: at most 128 KiB UTF-8 and 256 rows. The inner payload limit is
  checked before decoding it; valid CRC/whitespace cannot bypass the limit.
- Outer page frame/read: at most 512 KiB; head frame/read: 128 KiB.
- Descriptor limits: 8,192 snapshot pages and 2,048 replay segments. Automatic
  snapshot compaction starts after 128 segments at checkpoint boundaries.
- Replay/page writes yield between bounded pages. A full multi-year WAL is not
  read or split as one string in the new format.
- Reclaim considers only the bounded closure of the superseded backup after the
  new head is verified. It preserves every page/segment referenced by current
  and predecessor heads. The actual backup is rechecked after publication;
  a mismatch skips cleanup. No broad directory scan or filename-only deletion.
- That proven unreachable closure is represented as compact numeric ranges,
  never an expanded path list. At most 64 cleanup jobs are retained by default
  (hard maximum 1,024). Overflow leaks files conservatively and increments the
  session counter; it never expands deletion authority or rejects a publication.
- Automatic reclaim waits in the scheduler's separate `maintenance` lane
  outside the serialized writer. Each admitted slice joins the writer for at
  most one exact `exists`/`remove` pair, so a waiting low-priority turn cannot
  hold up a foreground publication. `runMaintenance()` provides an explicit
  bounded-pass trigger for lifecycle owners and tests. A caller that explicitly
  injects the store `yield` hook also owns maintenance admission unless it
  supplies a separate `maintenanceYield`; only ordinary production construction
  reaches the global maintenance lane by default.
- Every production facade fences new work and awaits its store-scoped
  `closeAndDrainMaintenance()` before the shared scheduler is disposed or the
  facade is replaced. Retirement aborts the default pending scheduler admission,
  joins an injected admission hook that cannot be aborted, and joins one
  already-admitted unabortable adapter operation; the retired store cannot
  schedule or perform a later deletion.
- Cleanup IO/scheduler errors retain the compact job and increment a session
  counter. A failed complete pass is dormant until the next publication or an
  explicit maintenance pass, avoiding a hot retry loop. Cleanup never changes
  a successful publication into a failed commit.

These are storage frame/replay bounds, **not full-stage ResourceBudget coverage
or RSS limits**. The live index and pending authoritative mutation queue remain
O(vault/queued work); storage control buffers are not yet integrated into shared
parent/child admission. Without adapter stat, size rejection occurs after read;
even with stat, a native read/growth race remains. Retained index versions and
native/host allocations require real mobile memory evidence.

Retry ownership is process-local, not crash-durable. GC of unpublished/staged
orphans and retry after renderer restart are not implemented: the current IO
contract has no bounded listing API and the on-disk format has no versioned
cleanup-intent publication. Adding either requires a separate persisted schema,
recovery/upgrade rules and fault matrix. Until then, restart or queue overflow
may leak unreachable files; neither limitation licenses deleting a recoverable
publication.

## Legacy migration and rollback

Valid legacy snapshot/WAL metadata is imported once into the new format. Original
files are preserved byte-for-byte and never replayed over an existing new store.
Paths, entries, hashes, roots, cursor fields, approval scope and every complete
WAL operation are validated. Arbitrary malformed middle rows and unknown schemas
are errors. A WAL-only torn final JSON append can retain its valid prefix.

Legacy snapshots have no epoch/cut. Different snapshot copies or snapshot plus
a damaged WAL cannot be safely ordered by filename; migration stops with evidence
intact. Each legacy read is capped at 8 MiB (pre-read when stat is available);
larger legacy data needs a separately implemented bounded migration procedure.
This is not a streaming legacy import. New initialization uses a source-proven
intent and an advance fence: a crash before the first head can retry only the
same revalidated import. Original unmarked interrupted directories, changed
source data and partial/unknown markers still require explicit recovery. The
shared protocol and its limits are described in the journal ADR linked above.

Historical binaries ignore unknown formats and can read stale legacy files.
An on-disk marker cannot retroactively fix them: downgrading by replacing main.js
is unsupported. Release needs an explicit quiesced compatible export/rollback
workflow plus native crash/reload tests. No deployment or legacy deletion has
been performed by this implementation work.

## Related responsive changes

Diagnostic operation checkpoints retain at most one in-flight write and the
latest pending progress. Awaited begin/fail/complete barriers remain FIFO;
diagnostic errors are non-fatal. This is not the authoritative transfer plan.

Local callbacks use bounded per-path reference counts with fail-closed overflow,
immutable event paths and latest-event tokens. Engine replay attaches listeners
first, restores metadata hints in batches of 256 before its startup ping/pull,
and preserves newer live edits. Failed replay gates pull/push/scans/probes while
leaving event capture active. Plugin initialization now prepares the engine
before awaiting tree capability negotiation and activates it only after format
selection. Captured journal iterators avoid its previous full-array replay copy.
Capture still follows local WASM/cache initialization; native lifecycle evidence
is separate from this ordering fix.

Verified rename-echo ACK retirement uses the same generation/pending-registration
fences as push settlement, without altering unrelated deferred records.

Next: bounded authoritative backlog/index ownership, durable transfer plan,
migration/recovery UI, joint commit cuts and outcome recovery, persisted
orphan/retry maintenance and real-device gates. PR07 is not closed by these
local tests alone.
