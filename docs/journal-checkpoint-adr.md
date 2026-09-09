# Local journal checkpoint hardening (PR07, partial)

Status: historical checkpoint slice, now retained as a legacy import format.
Current runtime semantics are documented in [segmented journal](segmented-journal-adr.md).
In particular, current compaction preserves rename groups and independent ACKs;
the flattening limitation below describes the previous implementation, not the
new segmented writer. Neither format is a TransferPlan.

The journal remains the authoritative queue of pending local path generations.
Regular NDJSON mutation rows and per-path ACK watermarks keep their existing
meaning. One linked rename is appended as a single existing `renamed` row;
both recovered path hints share its ID. A failed append is ambiguous and
poisons further writes until reload validates/repairs the journal.

Compacted snapshots have a first `journal-meta` row with schema 1, a monotonic
checkpoint generation and the next mutation ID. A `journal-seal` footer records
the snapshot row count, UTF-8 byte length and CRC32 of every preceding byte.
The checksum detects accidental damage; it is not authentication. Ordinary
mutation/ACK rows may follow the sealed checkpoint. IDs never reset merely
because no dirty paths remain. Explicit reset also retains that high watermark.

Checkpoint publication is serialized with appends: write `.next`, read and
validate it, move the old main to `.bak`, move `.next` to main, and validate the
replacement. The previous backup is retained until a later validated successor
needs its slot. Recovery compares validated checkpoint generations, not filename
precedence. Contradictory copies of one checkpoint fail closed. A failed or
ambiguous replacement never licenses writing to a guessed main file.

Only confirmed absence is an empty store. IO failures, unknown schema/operation,
invalid complete rows and middle corruption stop authoritative recovery. An
incomplete EOF append can preserve its valid prefix; it must be repaired to a
new validated checkpoint before appending again. An incomplete staged snapshot
does not supersede a verified authoritative copy. No arbitrary damaged middle
row is silently skipped.

An ordinary sealed checkpoint may have appended mutation rows in its main WAL.
A `.next` stage never receives such appends: a complete seal followed by a torn
tail there is therefore an invalid recovery candidate, not an automatic repair.
Unknown or corrupt backup copies also fail closed instead of guessing which
writer understood their state.

The adapter completion/rename contract provides recoverable renderer-crash
boundaries to be tested on actual Obsidian platforms. It does not promise native
fsync or power-loss durability. Existing files are read through the portable
whole-file text adapter; avoiding `split` and spread removes extra growth but
does not make legacy import memory-bounded.

Historical binaries do not enforce a persisted compatibility fence. They may
ignore or remove the new checkpoint metadata and reset IDs on later compaction.
Downgrading such a store to an old binary is unsupported; a compatible reader
or an explicit export/recovery procedure is required. Keeping a legacy-looking
copy is not a downgrade guarantee.

Next integration slice: reuse this strict row/checkpoint validation behind
bounded immutable WAL segments and paged snapshots, persist a store epoch and
commit cut, then integrate versioned TransferPlan preparation/commit-intent
records. Keep committed sync-base separate from prepared/uploaded work. Root
outcome recovery and crash-atomic plan/base/journal checkpoints are separate
work; this change does not claim those PR08–11 semantics.

Current compaction expands a rename into durable final states for both paths.
This preserves the old-path deletion even after a newer destination edit, but
does not preserve an explicit transaction group once those generations diverge.
The present global hold on deletions when any source is deferred remains
required. PR10 short root transactions must persist group identity/membership
across compaction before removing that conservative safety rule.
