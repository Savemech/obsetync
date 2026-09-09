# Engine replacement, capture handoff and actual retirement

Status: lifecycle implementation slice of the responsive-sync roadmap, not a
native-device/reload qualification or a complete cross-store transaction plan.

## Failure being removed

`stop()` previously closed scheduling/listeners without joining a started push.
An HTTP root request could already be accepted while its response was delayed.
A replacement engine using the same base/journal could publish a newer root;
the old response then overwrote local base with the older version and ACKed its
journal. Startup generation checks did not cover this accepted transaction tail.

The regression control reproduces exactly that sequence with actual engines,
segmented sync-base and journal, plus deterministic transport/tree ports. The
server ends on B while a fresh local store load returns A and an empty journal.
The fix must finish the accepted old transaction before starting replacement
work, not suppress its acknowledgement or trust an idle status flag.

## Completion boundaries

`EngineWorkTracker` registers an owner synchronously before work starts. Its
idempotent release belongs to the actual final completion; closing admission
does not release existing owners. Drain uses no timers or promise races.

Operations include startup/local replay, explicit persistence reload, pull,
push, scans, force-sync, repair and startup health requests. The lifetime spans
checkpoint begin/complete, root outcome, base/cache save, journal ACK and hint
cleanup. Bounded parallel scan groups join every sibling before propagating a
failure. Local vault callbacks have a separate tracker spanning append,
optional echo classification/ACK and restoration of session hints.

| Operation | New sync work | Local capture | Completion |
| --- | --- | --- | --- |
| `quiesceAndDrain()` | Closed | Remains live | All admitted operation owners |
| `handoffCaptureTo(next)` | Old closed, next startup-gated | Synchronous ownership handoff | Finite old callback cut |
| `stopAndDrain()` | Closed | Closed | Operations and admitted callbacks |

Repeated drains share their completion boundary. A stopped continuation may
finish accepted work but cannot start a new transport preflight after a delayed
checkpoint begin. A queued mobile visibility wait accepts an abort signal so
retirement need not wait for foreground; only this unstarted wait is cancelled,
not an admitted native IO operation.

## Capture without a replacement gap

The old engine keeps capturing edits while its accepted request and replacement
preparation are pending. Once operations drain, the new engine receives the
same `DirtyPathSet`, `DeferredChangeTracker` and `LocalEventGuards` by reference.
These describe local bytes, not the old server's root or negotiated policy.
No vault-sized copy, flattening of rename links or guessed journal ID is needed.

This matters when an append failed: the current-session hint and uncertain
rename dependency may be the only surviving protection. Replay of durable
journal rows alone cannot replace them. Shared current tokens/reference counts
also prevent a finishing old echo callback from ignoring a newer callback.
Failed optional echo verification remains dirty, including abort during the
mobile large-file yield; it cannot silently discard a failed-append hint.

Listener installation and old admission closure/detachment happen in one
synchronous turn. An incoming owner on the new engine spans the old callback
cut, so A → B → C replacement or unload cannot bypass unfinished A callbacks.
Partial listener-registration failure keeps old capture active and retains the
partially entered new callbacks through an old bridge owner until they finish.

Only after that finite cut may the new engine reload base/journal. Reload is
itself an owned operation; a third replacement joins actual store recovery.
Failure keeps activation gated until an explicit successful reload. New capture
remains active throughout reload and local replay. A stale startup continuation
quiesces instead of terminal-stopping listeners needed by its replacement.

## Native workers and tree ownership

The existing worker `close()` remains a bounded caller wait. The separate
`closeAndDrainNative()` waits for actual exit or fulfilled native termination
for every owned worker, including retired/replaced slots and in-progress factory
calls. Timeout, thrown/rejected termination and a caller-facing job failure are
not native completion. If no authoritative completion arrives, replacement
remains waiting; diagnostics expose the retained owner count.

A failed constructor may return no pool. Its actual cleanup promise is handed
to the plugin and retained across initialization generations. A successful new
pool or renderer fallback cannot erase that native lifetime.

After operation and worker drains, the plugin explicitly frees the old packaged
WASM tree once. Capture callbacks use separate Hasher instances, journal and
base, not this tree. Main clears its matching tree reference; drained engine
debug getters no longer call the freed wrapper. A throwing destructor poisons
replacement instead of attempting a second native free. This releases tree
allocations for reuse; it does not promise shrinking WASM linear memory or RSS.

## Plugin reload and settings writes

Synchronous `onunload` closes admission and registers retirement before returning.
Scheduler, preparation store and memory services remain available until accepted
owners finish. In the same renderer, a versioned `Symbol.for` WeakMap keyed by
the host vault object lets another bundle wait for the previous retirement
before opening storage/installing loggers. The captured prior cut avoids a
self-wait cycle if the new instance is unloaded during its waiting onload.
Failures remain fail-closed; this is not cross-process persistent coordination.

Settings writes use one active and one coalesced latest pending JSON snapshot.
Native writes are serial, snapshots are detached at submission, and pending
callers share the latest publication outcome. An old delayed `saveData` cannot
finish after a newer write. Unload drains both writes, including native errors;
drain means completion, while each save promise reports success/failure. The
bound covers retained snapshot count, not all settings bytes or encoding CPU.

## Remaining qualification

Actual plugin lifecycle call-order tests use explicit host/engine/storage/worker
ports; actual engine tests use isolated adapter/transport/tree ports. Neither
is proof of Obsidian native adapter durability, actual UI responsiveness,
power-loss recovery or mobile memory plateau. Same-renderer reload coordination
depends on a stable host vault identity and does not survive process termination.
Edits while the plugin is disabled are offline edits, not captured events.
Session-only hints after failed append survive same-instance engine replacement;
this slice does not make them durable across process loss or a new plugin instance.

The excluded crash logger's final asynchronous flush is not part of the
authoritative base/journal cut. Exceptional host listener teardown can still
retain closures despite closed admission. Full metadata/host/worker heap
admission, safe recovery/rollback UI, durable commit intents/outcomes, short root
transactions, mobile readers and the real-device matrix remain separate work.
