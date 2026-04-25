# Conflict Resolution — How Sync Works in ObsetyNC

This document is the canonical explanation of how ObsetyNC reconciles divergent
edits when two or more devices push to the same vault. It is written to be
dropped into the README as-is once the moving parts settle.

The goal is not "conflict avoidance" — Obsidian users routinely edit the same
file on phone and desktop within minutes of each other. The goal is
**correctness under any concurrent edit pattern, with no silent data loss**.
Every scenario below has a deterministic answer that the server reaches purely
from the three Merkle roots (base, side A, side B) it sees on a divergent
push. No per-file timestamps, no last-writer-wins, no central clock.

Every behaviour in this document is locked in by a test under
[`crates/e2e-tests/tests/`](../crates/e2e-tests/tests). When you change
behaviour, change the corresponding test.

---

## 1. The mental model

A vault is a **Merkle tree of file metadata**, plus a **content-addressed
blob store** for file bytes:

```
RootNode                     <- single hash that names the vault state
├── notes/   → InternalNode  <- per-prefix subtree (or LeafChunk if small)
│   ├── jan.md  → FileEntry { hash, mtime, size }
│   └── feb.md  → FileEntry { hash, mtime, size }
└── photos/ → LeafChunk
    └── header.png → FileEntry { hash, mtime, size }
```

Every node is identified by Blake3 of its serialized form. Every file's `hash`
is Blake3 of the file's bytes. The bytes themselves live in a separate
content store, keyed by hash. Identical content across files (or across
devices) is stored once.

Sync is moving these hashes around. Bytes only move when a hash refers to
content the receiver doesn't already have.

### What "the current state" means

Each vault on the server has a pointer file `vaults/<vault>/current` whose
contents are the hex of the **current root hash**. That's the entire
authoritative state of a vault — one hash. Every prior root the server has
ever seen lives in `vaults/<vault>/roots/<hash>.bin` so any push can be
diffed or merged against any historical state.

### What a client sends

A push is one HTTP request: `PUT /api/v1/root/{vault_id}` with a body that
starts with **64 bytes of ASCII hex** — the **parent root** the device last
saw — followed by the new root's serialized bytes. The parent hash sits
inside the AEAD-encrypted body, not in an HTTP header, so a man-in-the-middle
cannot strip or swap it.

Before pushing the root, the client uploads any chunks and content blobs the
server doesn't already have. `POST /api/v1/chunks/check` and
`/api/v1/content/check` filter a list of hashes down to the ones the server
is missing — the wire only carries the delta.

---

## 2. The two paths a push can take

When a push arrives, the server reads `current` and compares it against the
device's claimed `parent`:

```
              push(parent, new_root)
                        │
                        ▼
              ┌─────────────────────┐
   current?   │ get_current_root(v) │
              └────┬────────────┬───┘
                   │            │
              None │            │ Some(current)
                   ▼            ▼
        ┌──────────────┐    parent == current ?
        │ first push   │       │           │
        │   accept     │   yes │           │ no
        └──────────────┘       ▼           ▼
                          fast-forward   server-side
                            accept       three-way merge
```

Three outcomes, each returns a JSON `PutRootResponse` to the client:

| outcome        | shape                                             | meaning                                   |
|----------------|---------------------------------------------------|-------------------------------------------|
| first push     | `{ accepted: true, root_hash: … }`                | vault didn't exist; nothing to merge     |
| fast-forward   | `{ accepted: true, root_hash: … }`                | parent matched current; client was up-to-date |
| merge          | `{ merged: true, root_hash: …, conflicts, auto_resolved }` | divergent; server merged the trees |

The "first push" and "fast-forward" cases are uninteresting — the client's
new root becomes the current root and that's it. The merge case is where the
work happens.

---

## 3. The server-side three-way merge

When the parent doesn't match the current root, the server has three roots
in hand:

- **base** — what both devices last agreed on (the parent the client sent)
- **side A** — the current server root (something else, by another device)
- **side B** — what this client just pushed

It runs `sync_core::merge::merge_trees(store, base, side_a, side_b)`, which
operates at **two levels**: directory prefix, then file entry inside a prefix.

### 3.1 Tree-level (per top-level prefix)

For every prefix that appears in any of the three roots:

| (base, A, B) state                | action                            | rationale                       |
|----------------------------------|-----------------------------------|---------------------------------|
| all three equal                   | keep base                         | nothing changed                 |
| A changed, B unchanged            | take A (auto-resolved)            | only one side changed           |
| B changed, A unchanged            | take B (auto-resolved)            | only one side changed           |
| both changed differently          | recurse into entries (see 3.2)    | needs file-by-file resolution   |
| prefix new in A only              | take A                            | added directory                 |
| prefix new in B only              | take B                            | added directory                 |
| prefix new in both                | recurse into entries with empty base | both added; per-file diff required |
| A deleted, B unchanged            | honour deletion                   | one-sided delete                |
| B deleted, A unchanged            | honour deletion                   | one-sided delete                |
| A deleted, B changed              | keep B (B wins over A's delete)   | **change beats delete**         |
| B deleted, A changed              | keep A                            | **change beats delete**         |
| both deleted                      | stay deleted                      | no-op                           |

This decision is made on **directory prefixes** (`notes/`, `photos/`, root-
level `""`). Two devices touching different top-level directories never
reach the file-entry path — that's a fast win for the common case
("desktop edited notes/, phone added photos/").

### 3.2 File-entry level (when a prefix changed on both sides)

Both sides' chunks for the prefix are loaded into entry lists, sorted by
path, then walked path-by-path:

| (base, A, B) state                                        | action                                           |
|-----------------------------------------------------------|--------------------------------------------------|
| all three present, A.hash == base.hash == B.hash          | keep base                                        |
| A == base, B different                                    | take B (auto)                                    |
| B == base, A different                                    | take A (auto)                                    |
| both differ from base, A.hash != B.hash                   | **conflict recorded**, keep A in tree            |
| only A has it                                             | take A                                           |
| only B has it                                             | take B                                           |
| A and B both have it (no base entry), same hash           | take it (no conflict)                            |
| A and B both have it (no base entry), different hashes    | **conflict recorded** with `base_hash = 0…0`, keep A |
| A missing, B == base                                      | drop (A deleted, B didn't change → honour delete) |
| A missing, B != base                                      | take B (B's change beats A's delete)             |
| B missing, A == base                                      | drop (mirror)                                    |
| B missing, A != base                                      | take A                                           |
| only base has it (both deleted)                           | drop                                             |

The merged entries are written into a fresh tree, the new root is stored,
and `vaults/<vault>/current` advances atomically. The pushing client gets
back the merged hash and a list of conflicts to resolve client-side.

### 3.3 Why "change beats delete"

If a single device deletes a file you also edited, the edit must survive.
Otherwise an offline phone deleting a stale draft would silently delete the
desktop's hour of work the next time they sync. The rule is: a delete only
sticks if **the other side did not change the file**.

The flip side — a delete that the other side already saw a change to — is a
*real* conflict, but it surfaces as: "the file is in the merged tree with
A's content; B's deletion was overruled." The user sees the file reappear,
which is the safe outcome.

### 3.4 Why "same content add ≠ conflict"

Two devices independently typing identical content (templates, tag lines,
CI configs that get regenerated) hash to the same bytes. The merge layer
checks `A.hash == B.hash` before flagging a conflict, so these collide
into a single entry and produce no false positives.

### 3.5 The "winner" in a real conflict

When the merge has to flag a conflict, **side A's version wins the tree
slot** — A is the server's existing current root, so the visible vault
keeps moving. **Side B's content blob is still on the server** (the client
uploaded it before pushing the root). The `conflicts[]` array hands back
the path plus all three hashes (`base_hash`, `side_a_hash`, `side_b_hash`),
which gives the client everything it needs to materialize a resolution.

---

## 4. Client-side resolution (per-file strategy)

A server-flagged conflict isn't the end of the story. The client receives
the `conflicts[]` list and for each entry decides what to write to the
vault on disk. That decision depends on the file extension via
`sync_core::sync_rules::SyncRules`:

| strategy        | applies to                              | behaviour on conflict                            |
|-----------------|-----------------------------------------|--------------------------------------------------|
| `ConflictCopy`  | unrecognised binary, default            | keep both versions, rename loser to `…-conflict-<hash6>.ext` |
| `LocalOnly`     | `*.sqlite`, `*.sqlite-wal`, `.DS_Store`, `thumbs.db` | never sync; each device has its own copy |
| `Immutable`     | `*.png`, `*.jpg`, `*.pdf`, `*.mp4`, `*.zip`, … | "write-once" semantics; loser auto-renamed     |
| text three-way  | `.md`, `.txt`, `.json`, `.canvas`, …    | attempt three-way line merge; fall back to `ConflictCopy` on overlap |

The text three-way merge (`sync_core::conflict::three_way_text_merge`)
diffs base→A and base→B independently. If their changed line ranges don't
overlap, both edits are applied and the file lands clean. If they do
overlap, the file falls back to `ConflictCopy` and both versions are
written to disk with the loser renamed.

### Why this split (server tree merge + client file resolve)?

The server is content-blind — it never reads `.md` to understand if it's
text. Storing every device's conflict-copy logic on the server would
require shipping the full strategy table to every server install and
keeping them in sync. Instead the server hands back enough information
for the client to do the resolution locally, where strategy can evolve
per-platform.

---

## 5. Transport guarantees

Conflict resolution is only safe if the inputs are authentic. Every
protected request rides through an AEAD envelope:

```
[ 1B version=0x01 ][ 12B nonce ][ 32B client ephemeral pub ][ ciphertext+tag ]

shared       = X25519(client_eph_priv, server_box_pub)
request_key  = HKDF-SHA256(salt = nonce, ikm = shared, info = "obsetync/v1/c2s")
ciphertext   = AES-256-GCM(request_key, nonce, plaintext, AAD)
AAD          = b"obsetync/v1 " + METHOD + b" " + PATH
plaintext    = bearer_token (64 ASCII hex) || inner_body
```

What this buys:

- **Authentication** — only a holder of the bearer token (issued at
  enrollment, registered server-side) can produce a plaintext that decrypts.
- **Authorization** — the server pulls the bearer out of the plaintext and
  looks up which device owns it. Revoked devices are rejected.
- **Replay binding** — AAD binds method+path. An envelope captured on
  `PUT /api/v1/chunk/aa…` cannot be replayed against `PUT /api/v1/chunk/bb…`
  or `GET /api/v1/chunk/aa…`. Verified by
  [`transport_security::cross_path_replay_is_rejected`](../crates/e2e-tests/tests/transport_security.rs).
- **Forward secrecy** — every request uses a fresh client ephemeral
  keypair. If `box.key` leaks tomorrow, captured ciphertexts from yesterday
  do not become readable.
- **Integrity** — bit-flips in the ciphertext (including the GCM tag) fail
  to decrypt; the server returns 401 plaintext, never reaches the handler.
- **Network-level fingerprint resistance** — bearer tokens never appear in
  HTTP headers or URLs. A traffic capture sees `POST /api/v1/...` with an
  opaque ciphertext body and no obvious device identifier.

The parent root prefix on `PUT /api/v1/root` and the device root prefix on
`POST /api/v1/diff` both ride **inside** the encrypted body for the same
reason — a header would be tamper-prone.

The single exception is `GET /health`. It returns plaintext `{"ok":true}`
so a brand-new client (one that doesn't yet have the server pubkey from
enrollment) can ping the server before enrollment completes.

---

## 6. Edge-case catalog

The matrix below is what the e2e suite verifies. Each row is one
deterministic answer the server gives, locked in by the named test.

### Sync correctness

| scenario                                                                 | server behaviour                          | test                                                                     |
|--------------------------------------------------------------------------|-------------------------------------------|---------------------------------------------------------------------------|
| First push (vault didn't exist)                                          | accept directly                           | `single_device::first_push_creates_vault_and_round_trips`                |
| Repeat push, parent matches current                                      | fast-forward                              | `single_device::fast_forward_push_with_correct_parent_is_accepted`        |
| `device_root = 0…0` on diff                                              | every file returned as `Added`            | `single_device::diff_against_zero_root_returns_full_inventory`            |
| `device_root == current` on diff                                         | in-sync (empty deltas)                    | `single_device::diff_at_current_root_returns_in_sync`                     |
| Hash mismatch on `PUT /chunk`                                            | 4xx, body not stored                      | `single_device::put_chunk_rejects_hash_mismatch`                          |
| `chunks/check` with mix of present/missing                               | only missing ones returned                | `single_device::chunks_check_filters_to_only_missing`                     |
| Two vaults same device                                                   | independent histories, no bleed           | `two_device_sync::vaults_are_isolated_between_ids`                        |

### Multi-device cooperative

| scenario                                                                 | server behaviour                          | test                                                                     |
|--------------------------------------------------------------------------|-------------------------------------------|---------------------------------------------------------------------------|
| A pushes, B pulls                                                        | B sees A's snapshot exactly               | `two_device_sync::b_pulls_state_a_pushed`                                 |
| A pushes, B pulls, B edits, B pushes (parent=A's root)                   | fast-forward                              | `two_device_sync::b_fast_forward_push_after_pulling_a`                    |
| A pushes twice, B (still on v1) pushes a different file                  | server merges; no conflicts               | `two_device_sync::b_stale_parent_triggers_server_merge`                   |

### Real divergence

| scenario                                                                 | server behaviour                          | test                                                                     |
|--------------------------------------------------------------------------|-------------------------------------------|---------------------------------------------------------------------------|
| A and B edit different files                                             | merged, no conflicts, auto_resolved ≥ 2   | `conflicts::modify_different_files_auto_resolves_no_conflict`             |
| A and B edit same file with different content                            | merged, conflict reported with all 3 hashes; A wins tree slot, B's blob preserved | `conflicts::modify_same_file_different_bytes_flags_conflict` |
| A and B add same path with same bytes                                    | merged, no conflict (hashes match)        | `conflicts::add_same_path_same_content_is_not_a_conflict`                 |
| A and B add same path with different bytes                               | merged, conflict reported with `base_hash = 0…0` | `conflicts::add_same_path_different_content_is_a_conflict`        |
| A modifies x.md, B deletes x.md                                          | merged, no conflict, A's modification kept | `conflicts::modify_beats_delete`                                          |
| A leaves x.md alone, B deletes x.md (with A having edited an unrelated file) | merged, deletion honoured              | `conflicts::delete_beats_unchanged`                                       |
| A adds notes/x, B adds photos/y                                          | merged, no conflict (different prefixes)  | `conflicts::changes_in_different_directories_no_conflict`                 |
| `GET /root` after a merge                                                | returns the merged root, hash matches put response | `conflicts::merged_root_is_retrievable_via_get_root`             |

### Auth + transport

| scenario                                                                 | server behaviour                          | test                                                                     |
|--------------------------------------------------------------------------|-------------------------------------------|---------------------------------------------------------------------------|
| Plaintext POST to a protected route                                      | 401/400 (envelope absent)                 | `transport_security::plaintext_request_to_protected_endpoint_is_rejected` |
| Envelope encrypted to a forged server pubkey                             | 401 (AEAD open fails)                     | `transport_security::envelope_encrypted_against_wrong_pubkey_is_unauthorized` |
| Envelope decrypts but bearer is unknown                                  | 401 (lookup miss)                         | `transport_security::unknown_bearer_token_is_unauthorized`                |
| Envelope signed for `/path1` replayed against `/path2`                   | 401 (AAD mismatch)                        | `transport_security::cross_path_replay_is_rejected`                       |
| Envelope signed for `PUT` replayed via `X-Obsetync-Method: GET`          | 401 (AAD mismatch)                        | `transport_security::cross_method_replay_is_rejected`                     |
| Single-byte tamper in ciphertext or tag                                  | 401, blob not stored                      | `transport_security::tampered_ciphertext_is_rejected`                     |
| Wire byte 0 ≠ 0x01                                                        | 401 (BadVersion)                          | `transport_security::bad_wire_version_byte_is_rejected`                   |

### Enrollment lifecycle

| scenario                                                                 | server behaviour                          | test                                                                     |
|--------------------------------------------------------------------------|-------------------------------------------|---------------------------------------------------------------------------|
| Two enrollments, two devices                                             | distinct device_id + bearer; same server pubkey | `enrollment::enroll_two_devices_yields_distinct_credentials`        |
| Bearer registered → first authenticated request                          | accepted                                  | `enrollment::enrolled_device_can_authenticate_against_sync_api`           |
| Claim a code that was never issued                                       | 400 with JSON `{ "error": ... }`          | `enrollment::unknown_enrollment_code_is_rejected`                         |
| Claim the same code twice                                                | second claim 400 (file deleted on first claim) | `enrollment::claimed_code_cannot_be_reclaimed`                       |
| Revoke device, then make a request with that bearer                      | 403 plaintext "device revoked"            | `enrollment::revoked_device_is_forbidden`                                 |
| Enrollment older than 10 minutes                                         | 400 "enrollment code expired", file purged | unit-tested in `crates/sync-server/src/enrollment.rs::tests`             |

---

## 7. Behaviour we deliberately do *not* support

These are the conscious limitations. They're not bugs:

- **No three-way base lookup beyond the parent.** When B pushes parent=X
  but `current = Y`, the server merges with `base = X`. It does not walk
  parent chains to find the lowest common ancestor of X and Y. If a client
  pushes a parent that isn't in the server's history at all, the server
  responds 400 "parent root not found in history — full rescan needed".
  In practice this happens only if a vault was wiped or the device's
  local cache lost the root chain.
- **No clock-based last-writer-wins.** Two devices that edit the same
  file with different bytes ALWAYS produce a conflict, regardless of mtime.
  Mtimes ride in the FileEntry but are advisory; the merge logic ignores
  them.
- **No rename detection.** A delete of `old.md` plus an add of `new.md`
  with identical content looks like two unrelated operations to the merge
  layer (both succeed). The hook for rename inference (`detect_renames`
  in `sync_core::diff`) is currently a passthrough.
- **No partial-file merging beyond text three-way.** If two devices both
  rotate a `.png`, you get two PNGs and a rename (`Immutable` strategy).
  The system does not attempt image diffing.
- **No server-side authentication of revoked tokens at the AEAD layer.**
  Once a device is revoked, the server still decrypts its envelopes
  (the bearer is in the plaintext, decryption succeeds), but the
  middleware refuses with 403 right after token lookup. The 403 is
  plaintext, not encrypted — clients learn revocation without needing a
  fresh handshake.

---

## 8. Failure modes and recovery

| symptom                                                                  | likely cause                                                                  | what to do                                                  |
|--------------------------------------------------------------------------|-------------------------------------------------------------------------------|-------------------------------------------------------------|
| Client gets `400 parent root not found in history`                       | Client's last-known root was never sent to the server, or the volume was wiped | Client sets `device_root = 0…0` on next diff, full pull     |
| Client gets `merged: true, conflicts: [...]` with same path repeatedly   | A different device keeps editing that file faster than this one syncs         | Resolve the conflict locally; next push is a fast-forward   |
| `GET /root/<vault>` returns 404                                          | Vault never received a push                                                   | Push something, or treat the vault as empty                 |
| `POST /diff` returns deltas referencing hashes the client doesn't have   | Normal — client now uploads/downloads via `/content/<hash>`                   | (Expected behaviour, not an error)                          |
| `PUT /chunk/<hash>` returns 4xx                                          | Hash in URL doesn't match Blake3 of body                                      | Recompute hash, retry                                       |
| `401 unauthorized` on every request                                      | Stale `box_pub` (server keypair rotated) **or** bearer revoked                | Re-enroll                                                   |

---

## 9. Running the verification suite

```sh
just build-image     # one-time: build obsetync/server:local
just e2e             # bring up isolated stack, run all e2e tests, tear down
```

Or hold the stack open while iterating:

```sh
just e2e-up                                  # stack stays up
cargo test -p e2e-tests --features e2e \
    -- --test-threads=1 --nocapture          # iterate freely
just e2e-logs                                # tail server logs
just e2e-down                                # done
```

The unit-test suite (`cargo test --workspace`) covers the sync-core merge
algorithm, transport primitives, storage layout, devices, enrollment, error
mapping, and admin helpers in isolation. The e2e suite proves the whole
stack — Docker container + HTTP + real binary on a real volume — composes
the way the unit tests imply.

---

## 10. Where to look in the code

| concern                          | file                                                            |
|----------------------------------|-----------------------------------------------------------------|
| Merkle tree shape + hashing      | `crates/sync-core/src/chunk.rs`, `crates/sync-core/src/tree.rs` |
| Tree-level + entry-level merge   | `crates/sync-core/src/merge.rs`                                 |
| File-level resolution strategies | `crates/sync-core/src/sync_rules.rs`, `crates/sync-core/src/conflict.rs` |
| Diff (root → root)               | `crates/sync-core/src/diff.rs`                                  |
| AEAD wire format                 | `crates/sync-server/src/secure.rs`, `docs/transport.md`         |
| Server-side merge orchestration  | `crates/sync-server/src/api.rs::put_root`                        |
| Enrollment + bearer lookup       | `crates/sync-server/src/enrollment.rs`, `…/devices.rs`          |
| Storage layout on disk           | `crates/sync-server/src/storage.rs`                             |
| E2E truth-table tests            | `crates/e2e-tests/tests/`                                       |
