# Single-writer live document v1 durability ADR

Status: foundation implemented; wire/editor activation disabled.

## Decision

The Markdown file and its accepted root revision remain authoritative. A live
session is a low-latency, single-writer preparation path for that file, not a
second multi-writer authority and not CRDT activation. It is intended for the
common case where one person edits a note from one device. A competing writer
must be rejected and preserve its text as a conflict/fork; it is never merged
silently.

Registering a document creates one stable `docId`, one random epoch, an exact
path binding and the committed base content hash. Concurrent registration of
the same base joins that identity; a different base fails closed. Registration
alone does not fence ordinary file mode. A path hash is only a storage lookup
key and is never document identity.

Every operation crosses an exact session boundary:

`(vaultId, docId, epoch, path, leaseId, clientId, clientSequence)`.

Before appending, a client acquires the only writer lease at an exact file
revision. Reconnect by the same client returns the durable lease token. Another
client receives a conflict/fork result. The lease does not expire implicitly:
losing a token cannot silently hand unmaterialized edits to another writer.

One client has at most one unacknowledged operation per document. The next
sequence must be contiguous. Replaying its last sequence with identical bytes
returns the original watermark; changing the bytes fails. This bounded cursor
model avoids an unbounded in-memory or persisted op-ID set.

The server acknowledges only after an immutable checksummed operation record
and the new head have been flushed and their directory publications synced.
The head is the ACK boundary. A record left before head publication is an
unacknowledged orphan: an identical retry may finish publication, while a
different record fails as corruption.

The live suffix is bounded by operation count and bytes. Bootstrap reads are
paged by both dimensions. Snapshot compaction is a generation CAS over a
covered durable watermark; it publishes the new snapshot/head before removing
covered records and therefore never discards a concurrent suffix. Corrupt or
missing authoritative records stop recovery instead of being skipped.

Materialization may be a deterministic patch/text codec in the client or
server; Yrs is not required for this single-writer protocol. It prepares one
monotonic `(docId, epoch, leaseId, revision, watermark, contentHash,
transactionId)` file candidate. The exact witness is the only write the file
fence admits. After the existing root sequencer has a durable accepted outcome,
confirmation advances the authoritative file revision. A lost root ACK can
replay the same prepared witness. The lease can be released only after every
durable micro-batch is covered and no file candidate remains in flight.

While a lease, unmaterialized suffix or prepared file revision exists, an
ordinary file write is fenced. Thus an old client, external writer or normal
whole-file push cannot silently replace in-flight work. A dormant registered
document with no lease and no pending work is ordinary file mode. Until the
root push guard consumes this fence, live sessions stay off.

## Limits

- path: 4 KiB UTF-8;
- update: 4 MiB;
- live suffix: 4,096 records and 64 MiB;
- clients retained per document: 32;
- update page: 256 records and 4 MiB;
- snapshot: 16 MiB.

All lengths and counters are checked before allocation or mutation. Hot client
sessions will be separately limited by the renderer resource governor; this
server foundation does not imply one materialized document per vault file.

## Compatibility and rollout

The existing path-based opaque CRDT endpoints and `ops` WebSocket frame are not
this protocol and grant no writer lease or file revision. They remain dormant;
no wire capability is advertised by this change. Rollout requires, in order:

1. a versioned deterministic micro-batch codec and real Obsidian input tests;
2. sealed HTTP lease/append/bootstrap/ACK mapped to this store;
3. local durable micro-batch queue and one-document CM6 binding;
4. deterministic file materialization through the root sequencer;
5. server root guard enforcement of the writer fence;
6. only then a capability and WS latency path with HTTP-equivalent ACKs.

If multi-writer co-editing becomes justified later, the compatibility spike is
the stable Yjs 13 and `y-codemirror.next` lines with Yrs using `small-client`;
offsets must use UTF-16. Exact versions belong to that corpus/lockfile change,
not this dormant foundation. Those dependencies are explicitly deferred.

Rename and delete are rejected while a writer lease is active until epoch-bound
mapping/tombstone transactions exist. External file import is also rejected in
v1; a later three-way import may replace that policy without weakening the
fence. Downgrade cannot deactivate an epoch or reinterpret it as file mode.

## Recovery and rollback

Head and immutable records are self-validating. Unknown schema, contradictory
identity, checksum failure, missing suffix data or stale generation requires
operator-visible recovery; none becomes an empty document. Unreachable old
snapshots, covered operations and activation-orphan document directories are
safe maintenance candidates only after the current binding/head proves they
are unreachable.

The feature can be rolled back while no writer lease exists. A durable pending
lease requires a compatible reader plus an explicit replay, file commit and
release transaction; installing an older binary is not a valid rollback
procedure.
