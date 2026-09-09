# Safe legacy downgrade boundary

Status: implemented, including command-host and UI integration.

Release gate: the command host stops admission synchronously, drains settings,
diagnostic, conflict-modal and sync owners, freezes the engine, detaches vault
listeners, constructs the exclusive authority lease, and keeps it held through
`activateLegacyDowngrade` or `resumeLegacyDowngrade`. Calling the core with a
ceremonial/no-op lease is not a supported production workflow.

The `active` marker is a durable conversion fact, not permission for current
readers. The host must validate the retained root sequence and call
`authorizeLegacyDowngradeCurrentImport`, load both current stores, then call
`completeLegacyDowngradeCurrentImport`. The durable completed state stays
distinct from pristine `none`, so a repeated downgrade cannot appear safe while
the original ACTIVE chain remains. Core APIs alone must not be exposed as an
end-user feature without that coordinator.

ObsetyNC must not let a released legacy client silently flatten newer durable
authority. A downgrade is therefore an explicit, exclusive transition, not a
copy of files and not a schema-parser fallback.

The supported target is the released 1.11.3 journal and sync-base format. The
transition is allowed only while the engine/listeners are stopped, tree format
is v1, the sync base has verified ancestry, and no root intent is pending or
uncertain. The caller holds that exclusion lease until activation completes.

The converter writes bounded (8 MiB/file, 65,536 entries) projections and
verifies their SHA-256 identities before publishing a prepared marker. It then
renames the two segmented stores, their migration markers, and any old legacy
copies into a content-addressed archive. Archives are never deleted. Immutable
`prepared`, `archived`, and `active` phase markers make restart recovery
idempotent. Current readers refuse to open an incomplete transition; the
explicit resume entrypoint validates the same projection digests and continues.
The revocable lease is rechecked immediately before every phase write or source
rename and before activation, so an old/ABA lease cannot cross the next
irreversible boundary. Every archived target and pre-activation source absence
is verified rather than inferred from the archived marker alone.

Pending journal generations and endpoint identity are preserved. Complete renames remain rename rows;
independently acknowledged rename endpoints become the exact pending delete or
modified row understood by 1.11.3. No entry is acknowledged by conversion. The
legacy sync base contains only fields understood by 1.11.3. The root-intent
store is not archived: pending intent is forbidden, while its `lastSequence`
must survive because the legacy endpoint does not consume that sequence.
Root-intent startup enforces the marker's sequence as a floor; a missing,
corrupt, or lower store is recovery-required rather than a new sequence-1
stream.

Already-ACKed journal IDs are not authority once no root intent is pending.
The 1.11.3 compactor can discard their watermark, so downgrade does not claim
global ID monotonicity across that interval. Re-upgrade creates a fresh journal
epoch; later root intents bind ACK cuts to that new epoch, making ID reuse safe
without inventing an ACK against a user path.

Once active, re-upgrade selects the first shallow-valid legacy sync-base crash
copy in the released order (`.next`, main, `.bak`) and applies every independently valid WAL row,
including around a torn row. Generic migration remains stricter; this exception
is available only with a complete, matching active downgrade marker.

Writes made by 1.11.3 remain in the legacy files. On re-upgrade, the current
segmented-store migration imports those writes and preserves the root sequence
store. Marker cleanup and repeated downgrade generations are deliberately not
automatic. The plugin exposes separate prepare, resume and import commands for
exactly 1.11.3. Its confirmation modal names that target, requires two explicit
acknowledgements, and offers no reset/delete shortcut. Startup inspects the
marker before current stores, WASM, network or settings migration; interrupted
states stay fail-closed. Successful import reloads both current stores, writes
the completed marker, retires the legacy owners, and only then creates one fresh
current engine. A failed or stale continuation re-inspects durable state and
cannot activate either host by assumption.

SHA-256 here binds the marker to the exact locally validated projections and
detects torn or substituted archive data. It is not a signature against an
attacker who can rewrite the entire plugin data directory.
