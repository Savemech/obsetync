# Durable prepared manifests and verified desktop reuse

Status: first integrated PR08 preparation slice, not the complete TransferPlan.
The [roadmap](responsive-sync-roadmap.md) still requires scan progress, durable
object/transaction outcomes, joint base/journal cuts, short root commits and
native-device qualification. This stage stores preparation hints only. Neither
retention nor an object upload means a file is synced or its journal is ACKed.

## Storage contract

`prepared-transfer.store-v1/` lives in the always-excluded plugin directory.
The [segmented store](segmented-sync-base-adr.md) publishes a manifest header,
ordered chunk rows and a final logical seal under one verified head. A large
manifest does not become one unbounded JSON row. Its mutation ID is returned
only after the complete publication; a new record cannot expose intermediate
chunks or replace the public previous record before that boundary.

Snapshot metadata preserves the next prepared mutation ID, format and counts.
Replay checks the final logical closure before promoting a selected recovery
head. Corrupt/unknown records, impossible ordering and ambiguous IO require
validated recovery, not a guessed empty cache. First empty initialization uses
the existing source-proven intent/retry protocol; old unheaded directories
cannot acquire evidence retroactively. These checks do not add adapter fsync
or a latest-generation witness against every form of external file loss.

Records are keyed by `(scopeHash, path)`. Scope captures vault, endpoint,
server-key/enrollment device, selected tree format, hash/chunking format and
ignore/config policy. Settings are captured before async work; only the digest
is stored, without bearer tokens. Changing scope gives a miss, not permission
to reuse another vault/policy's preparation. It is not a server storage epoch:
a restored server can have the same endpoint and enrollment key.

A prepared record contains its local mutation ID, optional captured journal
generation and base-root hint, source metadata/fingerprint, whole-file hash and
complete manifest. Retention cannot replace a higher known journal generation;
optional mutation-ID CAS and discard CAS protect a newer replacement. These
fields do not authorize journal ACK or recover a lost root commit outcome.

Default owned-metadata ceilings are 128 records, 16,384 retained chunks,
8 MiB estimated retained metadata, 8 MiB estimated queued copies and 32 queued
requests. Capacity refusal leaves the authoritative journal untouched and
does not prevent the freshly prepared file from uploading. The stage does not
claim every pending file has a saved plan when those ceilings are reached.
Caller-owned inputs, detached lookup/result copies, page buffers, worker heaps
and host allocations are separate; this is not a total RSS/OOM guarantee.

## Production preparation and reuse

The engine loads preparation storage after listeners and journal replay are
installed, while publication/scan/apply remain gated. The scope uses the actual
negotiated tree format. Slow storage or hashing that scope does not stop local
journaling; failed storage remains diagnosable and retry does not duplicate
listeners. A stopped initialization cannot activate from a late result.

For an eligible desktop large-file worker job:

1. Load a detached prepared hint. An unknown or malformed hint is an error,
   not a worker-runtime failure that licenses renderer fallback.
2. Even with identical size/mtime/fingerprint, run a full current-file hash.
   Matching hash permits reuse of the manifest, with the **new** verification
   fingerprint. A remote "all objects present" result cannot skip this step.
3. Mismatch retires only the observed hint by CAS, runs fresh manifest
   preparation, and awaits its retained publication (or explicit capacity
   refusal) before checking/uploading content.
4. Recheck object presence. Use existing bounded ranged upload for missing
   chunks; cached-manifest ranges additionally pass a local hash comparison
   before entering the send queue. The range remains owned through hashing,
   transport completion and native reader close.
5. Publish via the existing root transaction. Return prepared retirement
   tokens without deleting them yet. The engine retires covered tokens only
   after its successful durable journal ACK; retained rename peers keep their
   hints. Matching CAS retirements share a publication. Cleanup failure cannot
   restore work whose root/base/journal already committed; the next prepared
   operation must reload the poisoned storage cut first.

Local pending/in-flight generation checks run around preparation, range sends,
candidate application and immediately before root submission. A newer edit
prevents the older prepared result from entering a new root request. Once a
root request is already accepted, cancellation does not pretend it never
happened; normal generation-bounded settlement still preserves newer edits.

The helper recognizes the versioned canonical FastCDC producer format:
minimum 256 KiB except the final tail, average 1 MiB and maximum 4 MiB. It checks
the possible chunk count before walking/cloning a manifest. This does not
change server wire validation or chunk boundaries. Fresh worker output, the
validated result and the isolated retention argument can coexist; cooperative
cloning requires a future immutable/paged producer ownership contract.

## What the optimization does and does not prove

A reused manifest avoids another FastCDC pass and per-chunk preparation hashes.
It still reads the source and computes the full verification hash. Verification
read/hash time is reported separately from fresh FastCDC time. Local hashes of
missing ranges add work; no throughput improvement is inferred merely from the
existence of a cache. Measurements and their scope belong in the progress log.

Small files/mobile still use their existing admitted paths. Persisted hashes
are not inserted into `FileChange.hash` on the strength of `mtime + size`.
Desktop stat/fingerprint checks are not a durable change token, particularly
on virtual/shared filesystems. An external writer that changes already-present
ranges after verification while preserving every observed stat field is not
ruled out by these APIs. Missing-range hashes alone cannot prove the entire
source stayed unchanged. Strong filesystem change identity or immutable source
support and native fault tests remain necessary for a genuine no-rehash claim.

Object confirmations are not yet persisted as authoritative state. Server
storage epochs, object leases, root idempotency/outcome recovery, policy-scoped
bulk approval and joint plan/base/journal publication remain separate work.
The subsequent [engine lifecycle slice](engine-lifecycle-adr.md) joins accepted
root settlement and hands off live capture before replacement reuses base/journal.
It addresses the old late-ACK race without dropping accepted acknowledgements;
native Obsidian reload and memory qualification are still required.
Historical binaries do not understand this store: a safe downgrade exporter
and user-facing recovery/rollout procedure are not supplied by this slice.
Governor growth is not enabled by partial metadata/native memory coverage.
