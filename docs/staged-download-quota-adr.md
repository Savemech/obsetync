# Staged download quota and maintenance boundary

Status: implemented for resumable large downloads (P11).

## Decision

The final vault path remains the authority. Downloaded bytes are temporary until
the complete file hash is verified and the internal `.part` file is promoted by
`replaceFile`. A resumable transfer is identified by its content-addressed
internal pair and its versioned checkpoint, including vault, root, file
generation, target path and target state. A pair is resumable only when all of
those fields and the part length agree. A completed checkpoint also carries a
completion marker written only after all chunks were verified and, on a fresh
download, the independently assembled whole-file hash matched. A kill after
the final append but before that marker cannot promote unverified bytes.

Before creating or appending a staged download, the client reserves the pair's
complete eventual size. Admission is serialized in-process and includes active
reservations, so the current resumable pair is represented exactly once by its
full planned size rather than counted again at its partial on-disk size. The
lease is held through verification, promotion and checkpoint retirement, and is
released on success, abort or any thrown error.

Active reservations from the same JavaScript runtime are counted globally, but
path suppression and same-target exclusion are scoped to the owning
`PlatformIO`; identical relative paths in distinct vault namespaces cannot hide
one another's evidence. Inactive disk evidence is necessarily enumerated per
vault transfer directory because `PlatformIO` has no process-wide vault list.

## Bounds

- Staged bytes: 4 GiB, including a 16 KiB allowance for every admitted
  checkpoint.
- Direct entries in the internal transfer directory: 128; subdirectories are
  rejected.
- Concurrent active reservations and queued admission attempts: 16 each.
- Checkpoint read/write size: 16 KiB.

All sizes and totals must be non-negative safe integers. Admission enumerates
only `.obsidian/plugins/obsetync/transfers`, rejects duplicate or non-canonical
listing paths, and re-enumerates and re-stats the bounded evidence before
granting the lease.

## Fail-closed cases

The transfer is deferred before content download when the directory cannot be
listed, its shape or size cannot be proven, arithmetic overflows, an entry
escapes the internal directory, evidence changes during validation, the exact
current pair is malformed, cleanup fails, admission is busy, or either quota is
full. Unknown, malformed and unrelated internal files count toward quota but
are not read as checkpoints and are not deleted.

## Garbage-collection boundary

Maintenance may remove only a complete `.part` plus `.checkpoint.json` pair
whose canonical key, checkpoint fields and observed part length validate, whose
target path equals the requested target, and whose key differs from the current
transfer. Both files must be confirmed absent after deletion. It never deletes
the target path, another target's pair, an orphan, an opaque internal file, or
anything outside the transfer directory. The current resumable pair is always
preserved.

A valid exact-target checkpoint with an uncheckpointed part tail is the normal
crash boundary between append and checkpoint publication. It is retired and
restarted instead of becoming a permanent blocker. Cleanup deletes checkpoint
authority before its part; if part removal then fails, the remaining orphan is
quota-counted but cannot be resumed or promoted. A malformed checkpoint,
wrong-target checkpoint, or part without checkpoint proof remains fail-closed.

## Not covered

This policy is not a filesystem transaction or a free-space guarantee. The
adapter exposes no directory lease, atomic enumerate-and-reserve operation,
fsync contract, or protection from another process changing an entry after the
second evidence check (including same-size replacement). Process death loses
in-memory reservations; restart safety comes from bounded disk re-enumeration
and checkpoint validation. There is no age-based or vault-wide garbage
collection, no cleanup of opaque/unrelated entries, and no deletion to make an
unprovable state fit. Platform disk quotas and the 4 GiB product cap require
separate policy and telemetry.
