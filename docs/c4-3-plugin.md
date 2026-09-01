# C4 Level 3 — Plugin Components

This diagram zooms inside the **ObsetyNC Plugin** container from Level 2 and shows every significant TypeScript module, what each one does, and how they call each other. External containers (sync-core WASM, Sync Server) are shown at the boundary so data-flow arrows are complete.

---

```mermaid
C4Component
    title Component Diagram — ObsetyNC Plugin

    Person(user, "Obsidian User", "Configures plugin. Triggers manual sync. Resolves conflicts.")

    System_Ext(obsidian_app, "Obsidian App", "Loads and unloads the plugin. Fires vault file-change events.")

    Container_Boundary(b_plugin, "ObsetyNC Plugin · TypeScript · main.js") {
        Component(sync_plugin, "ObsetyncPlugin", "main.ts", "Obsidian Plugin entry point. Initialises WASM, wires all components together, registers commands (sync-now, full-rescan, show-conflicts), manages status bar.")
        Component(sync_engine, "ObsetyncSyncEngine", "sync.ts", "Core orchestrator. Attaches listeners before the first network await, coalesces one metadata-only dirty record per path, protects edits arriving during pull, audits all metadata drift and offline deletions, and keeps an honest verified tree base.")
        Component(push_engine, "PushEngine", "push.ts", "Bounded 5-phase push: batch hash/check, ordered byte/count-bounded object packs, streaming FastCDC planning, one tree.update_batch, then putRoot. Chunk records always precede their manifest.")
        Component(pull_engine, "PullEngine", "pull.ts", "Consumes a fixed historical diff snapshot one bounded OBD1 page at a time, then applies cache/local-hash/bulk-download tiers. Each applied page and its next cursor are WAL-checkpointed before progress; unknown local bytes remain visible as a conflict copy.")
        Component(sync_api, "ObsetyncApi", "api.ts · diff-page-codec.ts", "AEAD HTTP client with authenticated capability negotiation. Strict binary codecs bound bulk objects and paged diffs; mobile clamps OBD1 plaintext to 512 KiB and desktop to the authenticated 2 MiB server limit. Stable endpoints remain as fallbacks.")
        Component(ws_data_lane, "WS Data Lane", "ws-data.ts · ws-data-codec.ts", "Optional socket independent of realtime. Multiplexes bulk RPCs by request id under negotiated request/byte credits and bufferedAmount hysteresis; reconnect/backoff falls through to bulk HTTP.")
        Component(secure_channel, "ObsetyncSecureChannel", "secure.ts", "Wire-v2 channel: pinned-static + rotating-server double DH, per-message HKDF/AES-GCM keys, encrypted bearer/sequence/status, and request-nonce response binding. One client ephemeral keypair per channel.")
        Component(sync_base, "ObsetyncSyncBase", "sync-base.ts", "Last-synced path metadata plus verified treeBaseRoot and the fixed-root OBD1 continuation. Atomic rotated JSON snapshot + append-only idempotent WAL checkpoints let a large pull resume without skipping or retaining completed pages.")
        Component(journal, "ObsetyncJournal", "journal.ts", "Serialized append-only event/ack WAL. Exact per-path id watermarks cannot erase a newer edit while an older push is in flight; torn tails are recovered and records compact periodically.")
        Component(platform_io, "PlatformIO", "platform.ts", "Adapter I/O, binary append, recoverable staging replacement, cached bulk stat, and desktop absolute-path streaming. Mobile whole-file source reads are capped; restore writes are chunked.")
        Component(operation_checkpoint, "OperationCheckpoint", "operation-checkpoint.ts", "Durable active-phase/progress breadcrumb for pull, push, reconcile, and full scan. An orphaned marker on next launch identifies where an iOS renderer/Jetsam termination occurred.")
        Component(conflict_modal, "ObsetyncConflictModal", "conflict-ui.ts", "Modal dialog that lists three-way merge conflicts found in the vault (files renamed to *.conflict by the server's merge engine). Lets the user inspect and resolve each conflict.")
        Component(settings_tab, "ObsetyncSettingTab", "settings.ts", "Obsidian settings tab. Server URL, vault ID, device name, bearer token, sync interval, reconcile toggle, .obsidian sync option. Also surfaces debug log and enrolled-device list.")
        Component(debug_panel, "ObsetyncDebugLog + ObsetyncDebugModal", "debug-log.ts · debug-modal.ts", "Installs a console.log interceptor at startup to capture every [obsetync] log line into a fixed-size ring buffer. A modal command surfaces them — essential on iOS where there is no developer console.")
    }

    Container_Ext(wasm_core, "sync-core WASM", "Rust → wasm32 · inlined in main.js", "Blake3 hashing, FastCDC chunking, Merkle tree build / update / query.")
    Container_Ext(sync_server, "Sync Server", "Rust · axum · port 27182", "Sync API: diff, getRoot, getContent, putContent, putRoot, etc.")

    Rel(obsidian_app, sync_plugin, "Loads plugin · fires vault events", "Obsidian Plugin API")
    Rel(user, settings_tab, "Configures server URL, vault ID, bearer token")
    Rel(user, conflict_modal, "Reviews and resolves conflicts")

    Rel(sync_plugin, sync_engine, "Creates and starts on load · stops on unload")
    Rel(sync_plugin, conflict_modal, "Opens on 'show-conflicts' command", "findConflicts(syncBase)")
    Rel(sync_plugin, debug_panel, "Installs log interceptor at startup", "debugLog.install()")

    Rel(sync_engine, push_engine, "Calls push() on pending changes", "api · io · syncBase · wasm · tree")
    Rel(sync_engine, pull_engine, "Calls pull() on startup and 30s timer", "api · io · syncBase · localRootHash · wasm")
    Rel(sync_engine, sync_api, "reconcileContent: checkChunks · checkContent · checkManifests · putChunk · putContent")
    Rel(sync_engine, sync_base, "allPaths() · getEntry() — bootstrap tree for reconcile")
    Rel(sync_engine, journal, "Appends vault events · replays on crash recovery")
    Rel(sync_engine, operation_checkpoint, "Begins, progresses, and completes durable phase markers")
    Rel(sync_engine, wasm_core, "reconcile: wasm_tree_chunk_hashes · wasm_tree_get_chunk · tree.build_from_entries")

    Rel(push_engine, sync_api, "checkContent · checkChunks · putContent · putContentChunk · putRoot")
    Rel(push_engine, platform_io, "Reads file bytes in batches (READ_CONCURRENCY=4)")
    Rel(push_engine, sync_base, "Reads hash/mtime/size · saves after successful push")
    Rel(push_engine, wasm_core, "wasm_hash_batch (small files) · WasmChunker (large) · tree.update_batch · tree.root_bytes")

    Rel(pull_engine, sync_api, "getDiffPage/getRootAt · getContent · getContentChunk · getManifest; legacy getDiff fallback")
    Rel(pull_engine, platform_io, "writeFile · renameFile · deleteFile · stat · mkdir")
    Rel(pull_engine, sync_base, "Apply path mutations, then append exact next OBD1 cursor in the same ordered WAL; adopt tree base only after final parity")
    Rel(pull_engine, wasm_core, "Tier-2: hashFileStreaming (64 KB slices) · wasm_root_hash_from_bytes")

    Rel(sync_api, secure_channel, "Encrypts every request body · decrypts every response")
    Rel(sync_api, sync_server, "AEAD-encrypted HTTP POST", "plain HTTP · port 27182")
    Rel(sync_api, ws_data_lane, "Tries negotiated CHECK/PUT/GET fast path; owns HTTP fallback")
    Rel(ws_data_lane, secure_channel, "Fresh ticket/session · ws-data-v1 AAD")
    Rel(ws_data_lane, sync_server, "Sealed binary bulk RPCs", "separate WS · port 27182")

    UpdateLayoutConfig($c4ShapeInRow="4", $c4BoundaryInRow="1")
```

---

## Components

| Component | Source file | Role |
|-----------|-------------|------|
| **ObsetyncPlugin** | `main.ts` | Obsidian `Plugin` subclass. `onload()` initialises WASM synchronously (base64 Uint8Array → `initWasm()`), creates all component instances, starts `ObsetyncSyncEngine`, registers three commands, adds the status-bar element. `onunload()` stops the engine and removes listeners. |
| **ObsetyncSyncEngine** | `sync.ts` | The orchestrator. Separates last-observed server root from the verified tree merge base, coalesces dirty paths in `DirtyPathSet`, and serializes mutation of the WASM tree. Startup sequence: `attachVaultListeners → pullRemote → recoverFromJournal → metadataScan`; `forceSync` runs `pull → reconcileContent → pushPending`. The scan compares every current stat with sync-base instead of trusting wall-clock direction, so copied-in files with old mtimes, backwards mtimes, and offline deletions are found. |
| **PushEngine** | `push.ts` | Processes byte-bounded small-file batches and each large file alone. Missing small blobs, chunks, manifests, and index nodes are submitted as ordered records; the API splits them by the negotiated byte/count caps while preserving chunk-before-manifest dependencies. Progress and tree state advance only after every record receives a stored/already-present ACK, and one `putRoot` commits the candidate. |
| **PullEngine** | `pull.ts` | When `paged-diff-v1` is authenticated, consumes a server-frozen `(fromRoot,toRoot)` snapshot as bounded `OBD1` pages instead of retaining a whole JSON delta. Each page is validated, applied, rebased into the volatile WASM tree, and persisted to sync-base before its exact continuation cursor is durable. A restart resumes that target without re-fetching earlier pages; a completed cursor can reconstruct the volatile tree after a kill before base adoption. Deferred paths deliberately keep the cursor before their page so no change is skipped. Legacy servers still use `getDiff`. Content application then uses the existing cache/local-hash tiers, bounded bulk pages, per-object hashes, and conflict-copy protection. |
| **ObsetyncApi** | `api.ts`, `diff-page-codec.ts` | Every protected call reserves a durable sequence, tunnels its semantic method through wire POST, and decrypts the semantic response. Authenticated negotiation enables `bulk-http-v1`, `ws-data-v1`, and `paged-diff-v1` independently. Diff requests use `OBQ1`; strict `OBD1` decoding rejects byte/record/path-limit violations, truncation, trailing data, non-canonical varints, unsafe paths/u64 values, duplicate or unordered keys, cursor mismatch, and root substitution. Mobile clamps a page to 512 KiB and desktop to the authenticated 2 MiB server cap. Historical-root reads bind final parity to the frozen target. Missing capabilities retain stable fallbacks. |
| **WS Data Lane** | `ws-data.ts`, `ws-data-codec.ts` | Lazily mints an independent single-use ticket, performs strict `OBW1` HELLO negotiation, atomically reserves request/plaintext-byte credits, serializes AEAD sequence use, and correlates out-of-order responses by request id. It pauses above the socket high watermark, bounds waiting callers, closes on crypto/structural failure, and makes every unknown result retry through bulk HTTP/content-address checks. |
| **ObsetyncSecureChannel** | `secure.ts` | Wire-v2 client. Combines DH against the pinned static server key and rotating memory-only server key, derives per-message AES-GCM keys with HKDF-SHA256, binds method/path and request nonce in AAD, carries durable sequences, and decrypts the encrypted semantic status. Uses `@noble/curves` for X25519 and SubtleCrypto for HKDF/AES-GCM; see `transport.md`. |
| **ObsetyncSyncBase** | `sync-base.ts` | In-memory `path → {hash, local mtime, size, optional server tree mtime}` plus timestamp, verified `treeBaseRoot`, and a versioned fixed-root diff checkpoint. Batch mutations and the cursor that follows them append to `sync-base.wal.ndjson` in that order; a torn tail therefore replays work but cannot persist progress ahead of local state. `save()` rotates `.next → main` with `.bak` recovery and only then clears the idempotent WAL. |
| **ObsetyncJournal** | `journal.ts` | Append-only NDJSON event/ack WAL with monotonic ids. A successful push appends exact per-path acknowledgement watermarks, so a later edit on the same path survives. Parseable records before a torn final append recover on startup; periodic compaction keeps only pending final states. |
| **PlatformIO** | `platform.ts` | Uniform adapter I/O including `appendFile` and recoverable `replaceFile`. Desktop can expose an absolute path for true 64 KiB streaming hashes. Mobile has no ranged DataAdapter read, so source files are whole-buffer and capped at 128 MiB; chunked restore remains bounded. |
| **OperationCheckpoint** | `operation-checkpoint.ts` | Persists the current phase and rate-limited progress. Normal completion removes the active marker; startup promotes an orphan to `last-interruption.json`, making otherwise silent iOS renderer kills diagnosable. |
| **ObsetyncConflictModal** | `conflict-ui.ts` | `class ObsetyncConflictModal extends Modal`. `findConflicts(syncBase)` scans for `*.conflict` files (created by the server's merge engine when both sides changed the same file). The modal renders a diff and offers "Keep mine", "Keep server", or "Keep both" per conflict. |
| **ObsetyncSettingTab** | `settings.ts` | `class ObsetyncSettingTab extends PluginSettingTab`. Four tabs: Connection (server URL, vault ID, device name, bearer token), Sync (interval, priority, `.obsidian/` toggle), Reconcile (manual trigger + status), Debug (last errors, debug log viewer). Calls `ObsetyncSyncEngine.reconcileContent()` when the user hits the reconcile button. |
| **ObsetyncDebugLog + ObsetyncDebugModal** | `debug-log.ts` · `debug-modal.ts` | `debugLog.install()` monkey-patches `console.log` and `console.warn` to also push matching lines into a fixed-size ring buffer. `ObsetyncDebugModal` renders the buffer in a scrollable modal. Critical on iOS where there is no accessible developer console and log lines would otherwise be invisible. |

---

## Key Data Flows

### Startup (every time Obsidian opens)
```
ObsetyncPlugin.onload
  → initWasm(wasmBytes)               # WASM boot
  → ObsetyncSyncBase.load() + ObsetyncJournal.load()  # read disk state
  → ObsetyncSyncEngine.start()
      → attachVaultListeners()        # before the first network await
      → pullRemote()                  # pull.ts → ObsetyncApi → ObsetyncSecureChannel → server
      → recoverFromJournal()          # replay journal → push.ts
      → metadataScan()                 # all metadata drift + offline deletes → push.ts
      → setInterval(pullRemote, 30s)  # periodic pull
```

### Sync Now (forceSync)
```
ObsetyncSyncEngine.forceSync
  → pullRemote()            # pull latest deltas (pull.ts)
  → reconcileContent()      # verify server has everything sync-base claims
      → wasm_tree_chunk_hashes  → ObsetyncApi.checkChunks
      → ObsetyncApi.checkContent    (caller batches; transport caps each pack at 256)
      → ObsetyncApi.checkManifests  (same bounded binary check path when negotiated)
      → ObsetyncApi.putObjects      (ordered byte/count-bounded packs; stable fallback)
  → pushPending()           # push.ts for any dirty local changes
```

### Pull (snapshot pages, then apply tiers)
```
PullEngine:
  negotiate paged-diff-v1
    → OBQ1(fromRoot, zero target, no cursor) → first OBD1 freezes toRoot
    → for each bounded page:
        validate roots/order/caps/cursor before applying any record
        guard journal/in-flight edits; preserve unknown bytes as conflict copy
        tier-1 sync-base match → zero content work
        tier-2 local hash match → repair sync-base
        tier-3 bounded object/chunk pages → hash verify → write/replace
        sync-base path mutations → WAL, then exact next cursor → WAL
        rebase only this page into the volatile WASM tree
    → fetch immutable root bytes for toRoot and verify semantic tree parity
    → treeBaseRoot adoption + cursor clear in one sync-base save

  no paged capability:
    → legacy whole getDiff path with the same apply tiers and safety guards
```

---

## What is out of scope at this level

- The internals of sync-core WASM (TreeBuilder, DiffEngine, FastCDCChunker, etc.) — see [c4-3-wasm.md](c4-3-wasm.md)
- The sync server's component breakdown — see [c4-3-server.md](c4-3-server.md)
- The wire-level cryptographic protocol — see [transport.md](transport.md)
