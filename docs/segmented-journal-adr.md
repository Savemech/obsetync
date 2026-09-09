# Segmented journal, source-proven migration and startup preparation

Status: PR07 implementation slice, not TransferPlan or native release qualification.
The previous [checkpoint format](journal-checkpoint-adr.md) is now a preserved
legacy import format. Committed [sync-base](segmented-sync-base-adr.md) remains
separate from pending mutations and future prepared/uploaded state.

## Pending state and acknowledgements

`change-journal.store-v1/` uses the same immutable bounded pages and verified head
publication as sync-base. WAL rows are schema-1 `append` and per-path `ack`
operations. Snapshot metadata preserves the next mutation ID independently of
the store's epoch, generation and operation sequence. Empty compaction and
explicit clear never reuse IDs. Public pending state advances only after a
complete publication; ambiguous failure poisons further mutations until reload.

The immutable index keeps the latest normal mutation per path plus unresolved
rename groups. Normal coalescing preserves the highest path generation, not
every modify callback as an in-memory history. Rows are projected in ascending
mutation-ID order with a deterministic tie for legacy equal-ID distinct paths.
Captured roots and iterators remain stable through later mutations.

A rename retains its original group identity and independent `oldPending` /
`newPending` flags. Both pending produces one `renamed` projection; only old
pending produces a source deletion; only new pending produces a destination
modification. Neither pending removes the group. A newer normal destination
edit does not erase the group. Compaction preserves these records and flags.

This fixes a concrete old failure: a content-authenticated destination echo ACK
could previously remove the only durable source-deletion row. It also prevents
an already-ACKed source from reappearing after partial ACK/restart. An old ACK
does not affect a newer normal mutation or renewed rename group.

Adjacent pending append/ACK requests share one publication, up to 256 requests
and normally 256 operation rows per group. An individually larger ACK request
remains one transaction across bounded segments, subject to store descriptor
limits; it is not split into independently acknowledged rename halves. Promise
resolution waits for the complete cut, not the first segment write. Compaction
runs at a later mutation boundary before accepting new work when 128 WAL
segments have accumulated. Manual compact/clear/load are ordered barriers.

ACK uses an endpoint index rather than a whole-journal scan. Candidate steps
yield every 256 affected records, including during replay. Storage awaits each
provisional callback; intermediate index state is not publicly exposed. Normal
engine replay uses a captured iterator/count rather than copying an array of
all journal entries. Pull protection captures indexed path membership instead
of constructing a full path Set; unavailable protection is fail-closed.

These are bounded IO/parse steps, not a complete RAM admission guarantee.
Outstanding authoritative request callers and unresolved rename graphs can
still grow. The live index is O(pending paths + groups), not a fixed-size disk
cache. Large ACK input capture and other metadata/control paths still need
full resource accounting. Governor growth remains disabled for partial stages.

## Full-scan source hints

A scan-discovered `created` or `modified` mutation may additionally persist its
lowercase SHA-256 hash and complete source stat (`mtime` plus integer `size`).
These fields let a cold restart publish each already durable scan prefix without
reading and hashing those files a second time. They are optional so existing
journal rows remain valid, and older plugin versions safely ignore them and do
the extra source read.

The hint is never publication authority. Recovery restores it only into the
pending dirty record; push must restat the exact path and reuse the hash only
when both mtime and size still match. A mismatch falls back to the normal source
read and hash. Delete/rename rows cannot carry source metadata, partial stat
tuples, malformed hashes and unsafe numeric values fail closed, and immutable
index equality includes every hint field so contradictory recovered copies are
not silently coalesced.

## Source-proven first initialization

Both journal and sync-base use opt-in migration markers under the excluded
plugin directory. A small sealed sibling `*.init-v1.json` is written and checked
before creating the target directory. It records a random epoch, a versioned
source domain, a SHA-256 exact-source manifest and the detached metadata cut.
Role, presence/absence and every source string are part of the fingerprint.
Lossless fixed-size string chunks keep individual hashing inputs bounded.

If initialization stopped before publishing a head, the caller revalidates its
legacy input and explicitly retries with the same proof. Matching complete
initial pages are reused; only strict prefixes of the expected initial bytes
can be repaired. Different valid pages, unknown formats or changed source data
are not overwritten. Valid heads/stages go through ordinary recovery instead.
Old directories without this intent cannot acquire retroactive proof.

Before the first post-initialization commit/snapshot, the store persists a
matching `*.advanced-v1.json` fence. Its presence forbids replaying stale legacy
originals if all heads later disappear. Partial/unknown intent or fence stops
recovery; an orphaned fence is not ignored. The fence is not a latest-generation
witness: it does not establish power-loss durability or detect every external
loss of only the newest head with a surviving older backup.

Legacy originals are never changed or deleted. Each legacy source read is
capped at 8 MiB. Journal decode cooperatively walks rows/blank lines and bounds
one row at 128 Ki UTF-16 code units; it still begins with a whole adapter string.
The valid torn final append can be imported, but damaged complete/middle rows,
unknown schemas, contradictory copies and impossible staged tails fail closed.
The importer preserves physical mutations and ACKs so rename endpoint state
is reconstructed without the old destination-only filter.

Caller-owned source quiescence and deterministic import are required. These
markers are not cross-process locks, a general repair UI, streaming import for
oversized legacy stores, or a compatible downgrade exporter. Historical binaries
can still consume stale originals and do not understand this protocol; replacing
the plugin with an old binary is unsupported.

## Startup lifecycle

The plugin creates its engine after local WASM/cache preparation, then calls
idempotent local preparation before awaiting the already running capability
negotiation. Listeners/replay are active while network/apply/scans remain gated.
Negotiated tree format is selected before activating ping/pull/push. Repeated
starts share work; stop is terminal, and retry does not duplicate listeners.

Init generation/unload fences protect published WASM exports and profile,
engine/tree/worker ownership and late errors. A stale native compile or HTTP
request is not claimed to be physically cancelled; a stale initialization
result cannot activate or overwrite the newer runtime. Joining an already-active
engine's accepted root settlement and handing off live capture is supplied by
the subsequent [engine lifecycle slice](engine-lifecycle-adr.md), not by the
initialization generation fence alone.
Capture still starts after local WASM/cache,
not at the beginning of all plugin `onload` work. Real Obsidian lifecycle and
foreground editing/memory tests remain required.

## Remaining integration

The first [prepared-manifest stage](prepared-transfer-adr.md) now persists
bounded desktop hints with mandatory current-file verification on every reuse.
Complete TransferPlan scan/object progress, commit intent/outcome, joint
plan/base/journal ACK cuts and short root transactions remain separate PR08–11
work. The new journal does not claim an uploaded object is
synced, bypass bulk approval, or enable CRDT/editor semantics. Full metadata
admission, cleanup maintenance, native adapter/device gates and safe rollout /
rollback remain open. No production deployment accompanies this slice.
