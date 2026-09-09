# Root commit outcomes v1

Status: local server protocol and engine-integrated client recovery,
verified conflict preservation and dependency-safe short root transactions.
Native device, full memory/pipeline and release gates remain pending.
No production migration or release.
Related: [responsive sync roadmap](responsive-sync-roadmap.md),
[engine lifecycle](engine-lifecycle-adr.md),
[prepared transfer](prepared-transfer-adr.md).

## Contract

A lost response is not a rejected mutation. Comparing the latest server root
with the proposed root cannot resolve acceptance after another device advances
the vault. Root publication and its durable receipt must share one filesystem
publication boundary, rather than writing the receipt after `current`.

The initial protocol retains the last receipt per authenticated device and
vault. A device submits strictly sequential positive safe-integer operation
numbers and an independent random mutation ID. These are application operation
identities, not HTTP anti-replay counters, WebSocket request IDs or timestamps.
Clients must persist one pending intent before submitting it and finish local
base/journal settlement before starting the next operation on that stream.

- Equal sequence and exact mutation ID/request digest returns the same result,
  without validating or applying the old root again.
- Equal sequence with different bytes/identity is a conflict, never a replay.
- A lower sequence is expired/unknown, never permission to re-execute it.
- A gap is rejected. New streams start at one; the last server sequence is
  discoverable, but learning it does not authorize acknowledging local edits.
- Other devices, legacy root requests and administrative root changes preserve
  existing receipts. They cannot erase an earlier device's accepted result.

The binding includes protocol domain, vault, authenticated device, sequence,
mutation ID, server incarnation and exact parent/root request bytes. Root hashes
alone are not request digests: root history metadata is not in semantic hashes.
The existing tree validation, content-presence walk, honest-parent merge and
blast-radius guard still run before a fresh publication.

## Storage and durability

The server's `current` file acquires a bounded, checksummed, versioned envelope
containing the current root and bounded last-device receipts. Legacy 64-hex
heads remain readable. Root history/merged objects are durably written before
the head. One temp-file write, file sync, atomic rename and parent-directory
sync publishes both root and receipt. Newly created directory entries also
need their parent sync before success is acknowledged.

Reusing an already-valid root history file also syncs that exact validated file
descriptor and its directory ancestry. A prior failed directory sync may have
left a visible file; a successful byte read alone must not skip that barrier.
Head retention is preflighted before consuming a one-time deletion bypass.
Only typed pre-publication validation refusals map to 413; a native `EINVAL`
from sync/rename remains an IO error with possibly unknown publication outcome.

Only a complete validated head is authoritative. Missing, malformed, unknown
schema, truncated, oversized and inaccessible heads are distinct: only actual
absence is an empty vault. An orphan temp is not automatically promoted as an
accepted operation. Ambiguous write errors require validated recovery and a
durability barrier before a replay can acknowledge the observed receipt.

The head, per-receipt result and device count have explicit ceilings checked
before publication. One latest receipt per stream bounds retained history;
older operation numbers remain below the high-water mark and cannot become new
effects. This is not an unbounded UUID dedup table. Exhausting a limit defers
publication; it does not evict a live stream or silently drop conflict details.
No new object GC, leases or cleanup of user data is part of this format.

Concrete storage ceilings: 2 MiB encoded head, 64 KiB complete receipt and 128
retained device streams per vault. Revoking/re-enrolling a device does not evict
its stream. They bound serialized metadata, not total parsed heap, Tokio tasks
or native RSS. Receipt-free writes remain legacy 64-hex; the first receipt
introduces the rich head, and subsequent legacy/admin writes preserve it.
The rich payload binds its vault ID and schema, with canonical JSON and a
domain-separated BLAKE3 checksum. Duplicate/ambiguous keys are rejected.

This is atomic head publication on the existing native filesystem, not a
latest-generation witness against arbitrary deletion or mixed-file backups.
Actual missing `current` without a temp remains absence; retained history alone
cannot distinguish first publication from an externally deleted head. History
is written before first publication and is not proof of an accepted root.

## Sealed HTTP protocol

`POST /api/v1/root-commit/{vault}` accepts strict JSON:
`protocol_version: 1`, current `server_incarnation` (64 lowercase hex),
`sequence` (integer 1 through 9 007 199 254 740 991), `mutation_id` (32 lowercase
hex), `parent_root` (empty or 64 lowercase hex), and `root` (canonical standard
base64). Plaintext request limit: 704 KiB; decoded root limit: 512 KiB, checked
after decoding as well as before allocation. Vault/device identities are
bounded at 128 UTF-8 bytes on this new path.

The request digest is BLAKE3 over `obsetync.root-commit.v1\0`, an eight-byte
little-endian sequence, then each field as eight-byte little-endian byte length
followed by its bytes: vault, authenticated device, original incarnation,
mutation ID, parent, exact decoded root. String fields use UTF-8. JSON key order
and transport nonce/sequence are not part of this binding. Tree version is
already included in the exact serialized root.

`POST /api/v1/root-outcome/{vault}` accepts `protocol_version: 1` alone for
stream discovery, or the complete group `sequence`, `mutation_id`,
`request_hash` for an exact query. Limit: 4 KiB. The authenticated device owns
the stream; changing the transport session does not change that owner.

Accepted responses contain `protocol_version`, current `server_incarnation`,
`status: "accepted"`, receipt sequence/ID/digest and `result` with the original
legacy accepted/merged payload, including complete conflict details. After a
restart the outer incarnation changes; the stored result does not. Stream,
expired and unknown responses contain status, current incarnation,
`last_sequence` and `current_root_hash`, but no invented acceptance result.
Malformed requests return 400, wire/decoded/retention size refusals 413, and
identity mismatch, expired/gapped submissions or stale fresh incarnations 409.
IO/corrupt-head errors return 5xx, never an empty-stream answer.

The mutation critical section stays under the per-vault lock through actual
native completion. Current/history writes remain synchronous in that section;
they are not detached blocking tasks whose lock could disappear on request
cancellation. This assumes one server process owns the data directory; it does
not introduce a cross-process writer lock. Their Tokio scheduling cost and full
metadata memory accounting remain performance work. Standalone replay/history
durability barriers are not yet separately included in aggregate fsync metrics.

## Restore and incarnation

A random process incarnation changes on every server startup, conservatively
including ordinary restart and backup restore. Fresh submissions must name the
current incarnation. An exact stored receipt can still answer an old intent;
an absent receipt under a changed incarnation is unknown and cannot authorize
blind resubmission. A new request must follow reconciliation and get a new
durable identity. This is not a claim that an arbitrary live filesystem restore
can be detected, or that past acceptance proves objects still exist now.

Outcomes report acceptance at a historical point. They do not imply that the
returned root remains current, that a later rollback did not change it, or that
the current device's local source bytes still match the submitted generation.
Object presence and local applicability retain their independent checks.

## Negotiation, compatibility and rollout

New sealed HTTP routes and a separate `root-outcome-v1` capability are additive.
The existing root request bytes/response and old clients continue to work with
this server. WebSocket routing must not invent support before identical outcome
semantics are implemented there. Unsupported servers use the explicit legacy
client path, not fabricated receipts.

Old server binaries cannot interpret the new authoritative head. A client
feature flag does not make downgrading the server safe. Before release, provide
and test an explicit quiesced export/rollback path and backup procedure; do not
deploy this format to production without that gate and authorization. Do not
automatically flatten a head and discard its sequence/receipt history.

## Implementation and qualification boundaries

### Terminal cancellation extension (local implementation)

The additive `root-cancel-v1` capability and sealed
`POST /api/v1/root-cancel/{vault}` consume a pending application sequence without
publishing its candidate. Strict JSON contains `protocol_version: 1`, the current
`server_incarnation`, and the original intent's `sequence`, `mutation_id` and
`request_hash`. This is a conditional terminal operation, not transport abort.

Under the same vault lock, an exact existing receipt wins: accepted stays
accepted, cancelled stays cancelled. Mismatched identity, expired sequence and
gaps fail closed. A fresh cancellation requires current incarnation and the
next sequence; its durable receipt is `{ "cancelled": true }`. It preserves the
current root and all other streams, emits no root-change notification, and does
not consume a deletion bypass. Query/replay report `status: "cancelled"` for that
receipt; a subsequently arriving exact commit can never turn it into acceptance.
Cancellation after acceptance returns the accepted result, never a rollback.

The rich head must also represent `root_hash: null` when cancelling the first
operation of an otherwise empty vault. Null is an explicit checksummed state
with retained stream history, not a missing/corrupt file. Legacy empty heads are
still absent; receipt-free root writes remain 64-hex. Existing non-null rich
heads are unchanged, and no already-deployed format is migrated in this local
unreleased extension. Clients must negotiate cancellation support before relying
on terminal cancellation. Their base/journal ACK is allowed only for accepted
receipts, never for cancelled or unknown outcomes.

All three new routes require the authenticated semantic method to be POST,
not only the outer HTTP carrier method. A sealed GET/PUT/DELETE receives 405
before decoding or mutation; middleware dispatch cannot turn it into a POST.

### Client persistence work

The local `root-intent.store-v1` is a separate authoritative segmented store,
not an entry in the evictable prepared-manifest cache. It owns one pending
intent and a retained stream sequence for a fixed identity scope. Its strict
schema-1 rows consist of a header, bounded base64 root pieces, ordered base
entries, ordered exact journal cuts, and a closing seal. One segmented head
publishes that complete intent before any request is allowed. The header binds
the original wire request/digest, vault/device, candidate base publication and
validated journal epoch. Recovery verifies the exact wire digest again through
the portable BLAKE3 provider; it does not infer it from the semantic root hash.
The caller must validate that the serialized tree belongs to the candidate
before constructing an intent; wire binding is not tree validation.

Terminal receipts and retirement are later small WAL operations. An unknown or
expired query never becomes a terminal record. Equal identity with a different
intent or terminal payload fails closed; a changed outer server incarnation is
not a changed historical receipt. Retirement requires a saved terminal result;
for acceptance the coordinator must first complete base publication, verified
conflict preservation and exact epoch-bound journal ACK. Cancelling never ACKs.
Retirement retains the sequence, and neither load nor scope changes invent a
fresh stream or adopt a remotely discovered sequence. Lost local stream state
needs explicit reconciliation, not automatic reuse of a server watermark.

Initial local ceilings are 256 base entries and journal cuts, 512 KiB decoded
root, 1.25 MiB serialized intent, four queued operations and 8 MiB of estimated
queued metadata. Root pieces are at most 16 KiB ASCII each. These are portable
controlled-data limits, not measured JS heap/RSS or native fsync guarantees.
Invalid caller input does not poison a loaded store; ambiguous adapter writes
do, until a complete validated reload. Closure validation precedes promotion.
An advanced initialization fence forbids silently recreating an empty store
after loss of its selected heads. Close stops admission and joins dispatched
writes; it is not a timeout or permission to start another owner early.

The local base primitive publishes entries, candidate root, timestamp and a
SHA-256 full-payload applied marker under one segmented head. A schema-2 WAL
operation and all subsequent schema-2 snapshots fence old readers. Four queued
publications retain at most 1 MiB canonical payload; pre-existing ordinary
setter backlog remains separately unbounded. Synchronous submission barriers
order earlier saves and later setters; a same-visible-root setter cannot be
discarded while an earlier candidate is pending. Duplicate settlement does
not replay entries over a later pull/metadata update. Failed writes make the
applied marker unavailable until validated reload.

`RootSettlementCoordinator` currently handles only local terminal bookkeeping.
For acceptance it checks the journal epoch, applies the atomic base publication,
joins the optional conflict preserver, performs the exact epoch/issued-generation-bound
ACK and then retires the intent. Concurrent
callers share one actual operation. Cancellation never mutates base or ACKs
and can retire even while a separate journal needs recovery;
unknown/expired outcomes are not accepted. A receipt with unrecorded conflicts stays
`conflicts-pending` without writes if no preservation provider is available.
Recorded historical copies need no provider or repeat IO. With a provider,
failure after base publication leaves a replayable pending intent and no ACK.
The candidate root is returned separately from the observed server
root, including a conflict-free merge. The engine invokes this coordinator and
intent store through its exclusively owned `RootSyncRuntime`.

The server receipt is a prerequisite, not the complete client TransferPlan.

### Conflict preservation extension (engine integrated)

An accepted merged receipt can carry per-path losing content. Each must match
an upsert in the detached publication with the same exact hash; its required
size comes from that publication, never from the current local path. The
downloader verifies every object, contiguous manifest ranges and the complete
fresh concatenation. It emits bounded borrowed chunks to a staging writer;
the full proof exists only after all writes and the final hash succeed. A
size-only resumed `.part` is not a verified prefix. Staging retries use fresh
internal names and do not overwrite old ambiguous partials. Four retained
attempts per conflict are allowed before explicit recovery/cleanup is required;
this inventory check happens before another download or staging write. Only
the newly owned staging file may be removed after its copy receipt is saved.
Unknown/old partials are not silently garbage-collected. Private staging assumes
one plugin writer; random fresh names are not an atomic exclusive-create API
or protection against external changes in that private namespace.

Content below 1 MiB uses one owned blob; content at or above that threshold
uses a strict, bounded manifest and one owned chunk per request. At most
16,384 contiguous chunks, 4 MiB per chunk and 2 MiB of raw manifest are admitted.
Every chunk is verified before append, and a fresh whole-file hash is required
before publication. Feeds yield at most every 64 KiB. The writer borrows each
download's child work quota through actual IO completion, not another global
reservation. Large staging requires a real native append capability; it never
falls back to rereading/concatenating the growing prefix. Native receive
allocation, fixed WASM heap and bounded parsed metadata are not claimed as
fully measured transient-memory or RSS coverage.

Visible destinations derive deterministically from the complete operation
identity, original path and losing hash. Publication uses the public
DataAdapter `copy` contract (fail if the destination already exists), not
`exists` followed by overwrite/rename. Native copy completion is joined even
on cancellation. It is not advertised as atomic visibility, native bounded
RSS, fsync or protection from external writes during an in-progress copy.
The contract is documented in the [public Obsidian SDK](https://github.com/obsidianmd/obsidian-api/blob/master/obsidian.d.ts)
and the locally installed `DataAdapter.copy` declaration. Those stronger
guarantees still need native host qualification. Fresh verified
staging plus completed copy proves the selected SDK-level persistence action;
it does not claim an additional readback verification.

On restart, a deterministic destination without a saved completion receipt
needs a full content hash before reuse. Stat/size alone never acknowledge it.
Desktop may use its bounded native reader with regular-file/path identity and
independent-inode checks; symlink/hardlink aliases do not qualify. Mobile may use an admitted small
whole-file read or an independently qualified bounded reader. A large existing
destination without that capability remains pending, without allocating the
whole file or overwriting/deleting the destination. An edited destination also
remains pending until an explicit conflict-resolution policy is supplied.

After completed copy or verified existing content, a new strict `conflict-copy`
row in `root-intent.store-v1` records `{path, copyPath, hash, size}` under that
pending accepted identity. It must match both the exact server conflict and
the original publication entry. At most 256 detached records are retained;
snapshot rows are path ordered. Unknown-row readers fail closed before
promotion, including after compaction. Equal repeated receipts are no-ops;
changed receipts and duplicate WAL rows fail closed. Acceptance with conflicts
cannot retire before every required copy is recorded. These are historical
completion receipts: later deliberate edits/removal of an already recorded
copy do not trigger duplicate recreation during journal settlement. No journal
generation is inferred from a conflict copy, and cancellation cannot save one.

Closing the preserver stops new admission and aborts further verification,
download and staging work at cooperative boundaries. It still joins the actual
native work already dispatched. Once visible copy starts, completion or full
verification of an ambiguous error and the receipt write remain owned even
after cancellation. A replacement owner must wait for that drain.

### Root recovery coordinator (engine integrated)

`RootRecoveryCoordinator` owns one immutable vault/device/stream scope and one
actual operation, without a queued promise backlog. The runtime must supply an
API credential-scope assertion before each new network dispatch. Only a newly
prepared intent inside `prepareAndSend` can authorize the initial send;
an existing prepared intent is not proof that it was never sent. Exact payload
is captured by the store before awaiting, then read back from validated storage.

Restart/lost-response recovery queries the original sequence, mutation and
request hash after freshly negotiating both capabilities. A terminal receipt
is persisted before local settlement. If the outcome is unknown and the stream
is exactly one sequence behind, conditional cancellation names the query's
current incarnation and the original identity. It can return an already
accepted winner, never roll back that acceptance. Gaps, expired history,
unavailable capabilities or insufficient negotiated limits leave the original
intent pending. No remote sequence floor is adopted and no application-level
blind resend occurs. The exact original request is never rebased or rewritten.

Close stops subsequent negotiate/query/commit/cancel dispatches, including after
a scope callback closes reentrantly. Already dispatched native work must really
finish. Received terminal receipts and their local persistence/settlement tail
are not dropped by close. The stable drain precedes store/credential replacement.
This coordinator does not load/reset stores, construct candidate trees, select
journal cuts, or repair in-memory engine caches.

### Engine activation and short transactions

The runtime captures endpoint, pinned long-term server key, vault and device
alongside the immutable API owner. Ignores, tree version, display name,
incarnation and rotating transport metadata do not partition this stream.
Bearer replacement preserves the digest but fences the old credential owner.
Main captures initialization policy before asynchronous WASM/cache work and
rejects scope changes before engine construction/activation. Ordinary engine
operations, not only new root submissions, check the current scope. Received
acceptance still completes its already admitted local settlement tail.

Local capture/journal replay precede intent-store load and network recovery.
Startup and every ordinary pull, push, full scan, metadata scan and repair
entrypoint wait for a pending outcome and derived-tree repair. Fresh publication
requires both capabilities and an exact local/server sequence-floor match.
Only explicit unsupported capability selection with no pending intent permits
the existing legacy path; an error or a lost response never selects fallback.
Scope change/lost-stream reconciliation and safe legacy downgrade are not
implemented by this refusal.

The engine saves `candidateRoot` independently from
`acceptedRoot`: a server merge does not make local candidate entries match the
merged tree. Its replayable order is durable intent → request → durable receipt
→ one local base publication (entries, candidate base root and applied-intent
marker) → idempotent conflict copies → exact journal ACK → intent retirement.
The intent captures path watermarks from its detached queue cut and a validated
journal-store identity; it must not reconstruct ACK generations from newer work.
The durable branch replaces the legacy split base saves and full-queue ACK;
they are not called in addition to settlement. Candidate commit/cache failure
does not restore a generation already known to have been acknowledged.

The complete coalesced materialized snapshot passes the existing bulk/deletion
review before selection. A root contains at most 256 paths/cuts and a bounded
encoded metadata estimate, with exact store/request ceilings checked later.
Current explicit rename links and shared journal generations form indivisible
components. Missing, uncertain, stale or oversized components stay pending;
independent components may progress. Late source deferral first holds the
existing global delete set, then its complete selected dependency components,
before any candidate mutation. Only actually published entries supply ACK cuts.

Explicit excluded paths and confirmed old untracked-directory hints can retire
through a separate bounded local `acknowledgeOwned` cut after full successful
materialization/review. They cannot touch a dependency endpoint or shared-ID
group, and cannot infer an omission from IO failure. Dirty-hint retirement uses
original take/restore provenance and a captured owner-local token, not merely
an inherited watermark. A newer no-ID hint survives even if its old WAL cut is
ACKed. Mixed excluded rename components and unproved volatile omissions remain
pending; no synthetic server deletion is invented to make the queue disappear.

Recovery reloads validated stores under exclusive engine mutation ownership.
It rebuilds the complete tree graph from a captured CURRENT base index, not
historical intent entries or root bytes without children. The saved honest
parent is kept separately: a partial pull can make current base entries differ
from that parent without granting authority to substitute the historical merged
root. Rebuild preparation yields every 256 rows, limits 65,536 entries/8 MiB
JSON, and reserves `6 × serializedBytes + 128 KiB` for controlled input copies
after releasing the first-pass scratch lease. The final native replacement is
still one synchronous call; native parsed entries/graph/WASM retained heap are
not fully accounted. Oversize/invalid/drifted repair keeps ordinary work closed.

Exact-generation dirty retirement also covers same-renderer IO errors after
retirement reached disk. A bounded attempted journal cut survives handoff;
after validated reload only absent paths in that same epoch, with matching
owned hint provenance, may be removed. Newer pending/no-ID hints survive.
Metadata auditing now holds the same mutation exclusion as pull/push, so its
refreshes cannot overlap recovery reload or root publication.

One tracked iterative push drains successful short transactions through real
host task boundaries, including manual sync with auto-sync disabled. No new
three-second debounce is inserted between ready durable batches. No-progress,
unresolved outcome, review, close or failed eligibility stops this cycle; close
joins actual native request/base/ACK tails before store/tree replacement.
Mobile heavy-work visibility is checked before planning and between batches.
Metadata materialization yields every 256 paths, including excluded paths.
Full-queue re-stat/sort/planner work is still repeated across root transactions;
backlog indexes, sorting and dependency construction are not yet a bounded,
incrementally scheduled pipeline or a qualified UI/RSS/throughput guarantee.

Path-free status distinguishes pending intent/repair, last bounded selection
and held component reasons. Equal old roots do not mark queued or unresolved
work complete. Durable scan/object progress, staging leases/maintenance,
complete memory accounting, adaptive root transport and native tests remain.

An unknown outcome does not permit replacing a persisted sequence with a new
payload, even if stream lookup currently reports the preceding number: the old
request could still arrive. Stop/new edits retain the pending intent while
capture continues. A safe terminal rejection/cancellation consuming a sequence
was not supplied by the initial accepted-receipt protocol; the terminal
cancellation extension above provides that prerequisite. The activated
coordinator negotiates it and persists its actual outcome before abandoning
work. An automatic continuation is a new settled sequence, not a blind retry
of the previous root request.

Tests must cover same/different request replay, lost response, concurrent
devices, later roots, legacy/admin changes, restart/restore incarnation,
expired/gapped sequences, scoped authorization, byte/count limits, malformed
heads and every filesystem publication boundary. A successful in-process
filesystem test does not by itself prove whole-machine power-loss recovery.

The native process tests now run twenty-four separate test executables:
six replacement and six first-publication cuts each for acceptance and
cancellation. Each must reach an exact
checkpoint and exit with code 73 without stack destructors before the parent
opens a new store. Before rename, the old selected head (if any) wins; after
rename, the selected root/receipt pair stays whole. An orphan first temp never
becomes acceptance. These runs retain the OS page cache and do not emulate
power loss, storage-controller failure or production backup restoration.
