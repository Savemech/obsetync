# Durable object-presence confirmations

Status: accepted for the bounded file-sync path.

## Decision

Positive `content`, `content-chunk`, and Merkle `index-chunk` presence results may survive an Obsidian
renderer restart in a separate segmented store. A record is usable only for the
exact prepared-transfer scope and the server incarnation observed by the same
durable root preflight. Missing results are never persisted.

The current server has no online object GC. It validates every referenced
content object again before accepting a root. Its process incarnation is
therefore a conservative storage generation: a server restart invalidates all
local confirmations even when storage survived. If online GC or an independent
object-store reset is introduced, the server must advertise a dedicated
monotonic storage epoch before this optimization can span that event.

## Persistence and bounds

- Directory: `.obsidian/plugins/obsetync/object-confirmation.store-v1`.
- Schema 1 stores one scope hash, one 256-bit server generation, and canonical
  `(kind, BLAKE3 hash)` rows under verified segmented heads.
- A generation change atomically replaces the prior snapshot; generations are
  never combined.
- Retention is capped at 65,536 records and 8 MiB estimated owned metadata.
  One publication accepts at most 512 records and queued persistence is capped.
- A full cache or failed cache write only causes later network checks. It never
  acknowledges journal work, advances sync-base, or publishes a root.

## Failure and retry semantics

The client still revalidates the current local source before using a prepared
manifest. A positive confirmation skips only the remote presence check; it does
not skip source hashing or candidate-root validation. Successful check and
upload ACKs are persisted before the batch is treated as prepared.

Any failed or indeterminate root publication atomically invalidates the exact
confirmation generation on a best-effort basis. Thus an unexpected missing
server object can cause at most another failed attempt before the retry performs
fresh checks. Ambiguous persistence poisons the local writer until validated
reload; poisoned state supplies no positive hints.

## Compatibility and rollback

The store is additive and is not interpreted by older plugin builds. Disabling
or removing it returns to authoritative checks/uploads without changing the
journal, roots, sync-base, prepared manifests, or wire protocol. Unknown schema
or corrupt closure fails local preparation rather than being reinterpreted as a
fresh empty confirmation set; deleting the isolated store is a safe explicit
optimization reset after preserving it for diagnosis.
