# Transport security (wire version 0x02)

This document specifies the obsetync sync-API transport protocol at wire
version `0x02`. It replaces `docs/transport.md` from v1 entirely.
Servers running this version do not accept v1 clients; existing devices
must re-enroll on upgrade.

Building blocks are unchanged from v1:

```
X25519 ECDH  +  HKDF-SHA256  +  AES-256-GCM
```

…all from audited, widely-deployed libraries (`x25519-dalek` on the
server, `@noble/curves` + `SubtleCrypto` in the browser).

Reference implementations (post-upgrade):

- Server: [`crates/sync-server/src/secure.rs`](../crates/sync-server/src/secure.rs)
- Server ephemeral rotation: [`crates/sync-server/src/eph_rotation.rs`](../crates/sync-server/src/eph_rotation.rs)
- Server replay tracker: [`crates/sync-server/src/seq_tracker.rs`](../crates/sync-server/src/seq_tracker.rs)
- Server middleware: [`crates/sync-server/src/api.rs`](../crates/sync-server/src/api.rs) (`secure_envelope`)
- Client: [`plugin/src/secure.ts`](../plugin/src/secure.ts) (`SecureChannel`)
- Client API funnel: [`plugin/src/api.ts`](../plugin/src/api.ts)

---

## 0. What changed from v1

Three properties v1 explicitly accepted as limitations are now addressed:

1. **Forward secrecy against `box.key` compromise.** v1's table claimed
   "ephemerals gone → past traffic safe." That holds against client
   compromise but not against server-static compromise: an attacker with
   `box.key` and the captured wire reconstructs `shared` directly,
   because the server side of the ECDH only needed its static private
   key. v2 introduces a medium-term server ephemeral
   (`Es_priv` / `Es_pub`) rotated on a 24-hour timer, mixed into a
   double-DH key derivation. Destroying `Es_priv` after rotation grace
   makes past traffic genuinely unrecoverable to a future `box.key`
   thief.
2. **Replay of a captured envelope to the same endpoint.** v1's AAD
   binds method+path so cross-endpoint replay fails, but a verbatim
   replay of the exact same envelope to the exact same endpoint
   succeeds. v2 puts a monotonic per-device sequence number inside the
   encrypted plaintext; the server tracks `last_seen_seq` per device and
   rejects non-advancing sequences.
3. **Errors leaked information on the wire.** v1 returned plaintext
   `(401, "unauthorized")`, `(401, "unknown bearer token")`,
   `(403, "device revoked")` — a network attacker could distinguish
   "envelope decrypts but bearer is wrong" from "envelope itself is
   malformed." v2 enveloped every response that follows successful
   decryption, and returns a constant-shape 256-byte zero-padded body
   for every decrypt failure regardless of cause.

Three less-visible tightenings:

4. **`X-Obsetync-Method` header is now strictly required.** v1 fell back
   to the wire method (always `POST`) on missing header; the AAD
   mismatch with the client's intended verb meant the request would
   fail-closed anyway, but the spec was silent on this. v2 mandates the
   header explicitly: missing → 400 (decoy) without any decrypt
   attempt.
5. **204 / 304 promotion is now part of the spec, not just an
   implementation quirk.** Handlers returning 204 or 304 are rewritten
   to 200 OK with an empty inner body before envelope wrapping. HTTP/1.1
   forbids bodies on these statuses and would otherwise drop the
   encrypted envelope.
6. **Ephemeral private key zeroization is mandated.** The session
   precomputes the two DH outputs once and zeros `ec_priv` immediately
   after. v1 did this in practice; v2 codifies it.

The migration is a **clean break**. v1 servers and v2 servers do not
interoperate. See §15.

---

## 1. How we got here

The v1 design space, recapped in 30 seconds: TLS with client certs got
killed by iOS WKWebView's refusal to attach certs to `fetch`; libsodium's
`crypto_box` got killed by load-time WASM cost on iOS and an opaque wire
format; v1 picked a minimal AEAD envelope from NIST/IRTF primitives.

That choice was correct. v1's wire format also turned out to be small
enough that extending it without rebuilding from scratch is feasible.
v2 keeps every primitive choice and the overall envelope shape. What
changes:

- One extra DH per session, against a rotating server ephemeral.
- An 8-byte fingerprint slot on the wire so the server knows which
  `Es_priv` (current or previous-during-grace) to use.
- An 8-byte sequence number inside the encrypted plaintext.
- A 2-byte semantic-status prefix inside the encrypted response
  plaintext.
- One new endpoint, `POST /api/v1/server-eph`, which uses a single-DH
  bootstrap key schedule (not the default double-DH) so it's reachable
  before the client has cached `Es_pub`.

That's the entire wire delta. The rest of this document specifies it.

---

## 2. Threat model

The envelope still defends against a network attacker who can observe,
modify, replay, inject, or drop any HTTP request between client and
server. v2 adds:

| Property                            | v1 | v2 |
|-------------------------------------|----|----|
| Confidentiality of bodies           | ✓  | ✓  |
| Integrity of bodies                 | ✓  | ✓  |
| Authenticity of server              | ✓  | ✓  |
| Authenticity of device              | ✓  | ✓  |
| Endpoint binding (method+path)      | ✓  | ✓  |
| Forward secrecy vs client compromise | ✓ | ✓  |
| **Forward secrecy vs `box.key` compromise** | ✗ | ✓ (window-bounded; see §14) |
| **Replay of captured envelope**     | ✗ (AAD prevents only cross-endpoint) | ✓ (monotonic seq) |
| **Error responses don't leak cause** | ✗ | ✓ |
| Traffic analysis resistance         | ✗  | ✗  |

What v2 still does not defend against:

- An attacker who has both the server's current `box.key` AND every
  historical `Es_priv` that was ever live during the captured-traffic
  window. (Same property as TLS 1.3 if you keep all historical session
  keys around.) Operator practice — overwrite + fsync stale `Es_priv`
  files at rotation grace expiry — closes this.
- An attacker who has compromised the device. They are that device.
- A malicious server operator. They see plaintext content of every
  sync. Client-side per-vault encryption is a separable feature.
- Side channels (timing, CPU, memory).

---

## 3. Wire format

All multi-byte integers are network byte order (big-endian). Byte
offsets are 0-indexed and inclusive-exclusive.

### 3.1 Request

```
byte offset   field                              size     notes
─────────────────────────────────────────────────────────────────────────
  0           version                             1 B     always 0x02
  1  .. 13    nonce                              12 B     random per request
 13  .. 45    client ephemeral X25519 public key 32 B     fresh per session
 45  .. 53    Es_pub fingerprint                  8 B     SHA-256(Es_pub)[0:8]
                                                          OR 0x00·8 for bootstrap
 53  .. N-16  ciphertext                         var      AES-256-GCM
 N-16 .. N    GCM tag                            16 B
```

Header size grew from 45 → 53 bytes (added 8-byte fingerprint).

The plaintext that the ciphertext decrypts to is:

```
byte offset   field              size     notes
─────────────────────────────────────────────────────
  0  .. 64    bearer token       64 B     ASCII, 64 hex chars
 64  .. 72    sequence number     8 B     uint64 BE, monotonic per device
 72  .. M     inner body         var      the actual request body
```

Plaintext prefix size grew from 64 → 72 bytes (added 8-byte seq).

Minimum request size: `53 + 64 + 8 + 16` = **141 bytes**.

### 3.2 Response

```
byte offset   field              size     notes
──────────────────────────────────────────────────
  0           version             1 B     always 0x02
  1  .. 13    nonce              12 B     fresh per response
 13  .. N-16  ciphertext         var      AES-256-GCM
 N-16 .. N    GCM tag            16 B
```

Response wire size unchanged.

The plaintext that decrypts is:

```
byte offset   field              size     notes
─────────────────────────────────────────────────────
  0  .. 2     semantic status     2 B     u16 BE (e.g. 200, 401, 409)
  2  .. M     inner body         var      handler output OR error JSON
```

Wire HTTP status is **always 200 OK** when the envelope decrypted
successfully. The semantic status (which the client surfaces to its
caller) lives in the encrypted plaintext.

Minimum response size: `13 + 2 + 16` = **31 bytes** (empty body, just
header + status + tag).

### 3.3 Decrypt-failure response

When the server cannot decrypt — bad envelope, wrong server key, version
mismatch, missing `X-Obsetync-Method` header, AAD mismatch, replay
detected before envelope opens, or any other pre-handler failure — it
returns a constant-shape decoy:

```
HTTP 400 BAD REQUEST
Content-Type: application/octet-stream
Body: 256 bytes, all 0x00
```

Same shape, same length, same status, regardless of cause. The cause is
logged on the server but never appears on the wire. Clients recognise
the decoy by its exact size (256 zero bytes) and follow this recovery
sequence:

1. Re-fetch `Es_pub` via the single-DH bootstrap call to
   `/api/v1/server-eph` (§7.4) and replace the cached value.
2. Recompute the session keys with the fresh `Es_pub` and retry the
   original request **once**.
3. If the retry also lands a decoy, surface "re-enroll" to the user.

The most common cause of an unexpected decoy is a stale `Es_pub` cache
that outlived the rotation grace window (§7.2); steps 1–2 recover from
that without operator action. Genuine re-enrollment is only needed for
bearer revocation, wire-version mismatch, or a server reinstall — and
those will continue to decoy after step 2 anyway.

### 3.4 HTTP wrapping

- Wire HTTP method is always `POST`. Obsidian's `requestUrl` on iOS
  drops bodies on GET; we tunnel everything through POST.
- The semantic method goes in the `X-Obsetync-Method` header. **REQUIRED
  on every sealed request.** Missing or invalid → decrypt-failure
  decoy response (no decrypt attempt).
- `Content-Type` is `application/octet-stream` both directions.
- `/health` remains the only public plaintext endpoint, intended for
  pre-enrollment reachability checks.

---

## 4. Cryptographic primitives

Same library choices as v1. The KDF info labels change to
`obsetync/v2/...` to namespace away from v1.

| Primitive       | Purpose                      | Standard   | Where on the client                | Where on the server |
|-----------------|------------------------------|------------|------------------------------------|---------------------|
| **X25519**      | Two ECDH operations per session | RFC 7748   | `@noble/curves`                    | `x25519-dalek`      |
| **HKDF-SHA256** | Per-message AES key derivation | RFC 5869  | `SubtleCrypto.deriveBits`          | `hkdf` crate        |
| **AES-256-GCM** | Authenticated encryption     | SP 800-38D | `SubtleCrypto.encrypt` / `.decrypt`| `aes-gcm` crate     |
| **SHA-256**     | `Es_pub` fingerprint (§3.1)  | FIPS 180-4 | `SubtleCrypto.digest`              | `sha2` crate        |

`sha2` is already a transitive dependency through `hkdf` on the server,
so adding fingerprint computation pulls in no new crate. The plugin
already has `SubtleCrypto` available for HKDF and AES-GCM.

HKDF info labels in v2:

- `"obsetync/v2/c2s"` — client → server, double-DH (default)
- `"obsetync/v2/s2c"` — server → client, double-DH (default)
- `"obsetync/v2/c2s-boot"` — client → server, single-DH bootstrap
- `"obsetync/v2/s2c-boot"` — server → client, single-DH bootstrap

A different label produces a different key, so a key from one direction
or one mode cannot decrypt the other.

---

## 5. Key schedule

v2 has two key-schedule modes. Default is **double-DH**, used for every
endpoint except `/api/v1/server-eph`. Bootstrap is **single-DH**, used
only for `/api/v1/server-eph` when the client has no cached `Es_pub` (or
its cache is stale).

### 5.1 Double-DH (default)

```
                client                                     server
               ────────                                   ────────

(enrollment:  S_pub, Es_pub_initial ──────────────────►  learned once)

per session:  ec_priv, ec_pub = X25519.gen()             (S_priv long-term)
              dh_s = X25519(ec_priv, S_pub)              (Es_priv current)
              dh_e = X25519(ec_priv, Es_pub_current)
              ikm  = dh_s || dh_e                         (64 bytes)
              zero(ec_priv)
              fp   = SHA-256(Es_pub_current)[0:8]

per request:  nonce_req = random(12)
              seq       = next monotonic uint64
              key_req   = HKDF-SHA256(
                              ikm  = ikm,
                              salt = nonce_req,
                              info = "obsetync/v2/c2s",
                              len  = 32)
              aad_req   = "obsetync/v2 <METHOD> <PATH>"
              pt        = bearer_token(64) || seq_be64 || inner_body
              ct        = AES-256-GCM_encrypt(key_req, nonce_req, pt, aad_req)
              wire_req  = 0x02 || nonce_req || ec_pub || fp || ct
                                                       │
                                                       ▼
                                              ──── POST body ────►

                                                                  Lookup Es_priv by fp:
                                                                    fp == SHA-256(Es_pub_curr)[0:8]?
                                                                                    use Es_priv_curr
                                                                    fp == SHA-256(Es_pub_prev)[0:8]?
                                                                                    use Es_priv_prev
                                                                    else: decrypt-failure decoy
                                                                  dh_s = X25519(S_priv, ec_pub)
                                                                  dh_e = X25519(Es_priv_chosen, ec_pub)
                                                                  ikm  = dh_s || dh_e
                                                                  key_req = HKDF-SHA256(
                                                                                ikm  = ikm,
                                                                                salt = nonce_req,
                                                                                info = "obsetync/v2/c2s")
                                                                  aad_req = "obsetync/v2 <METHOD> <PATH>"
                                                                  pt = AES-256-GCM_decrypt(...)
                                                                  bearer = pt[..64]
                                                                  seq    = pt[64..72]
                                                                  inner  = pt[72..]
                                                                  → device = lookup(bearer)
                                                                  → if seq <= last_seen_seq[device]:
                                                                      enveloped 401 "replay"
                                                                  → run handler

per response:                                                     nonce_resp = random(12)
                                                                  key_resp   = HKDF-SHA256(
                                                                                  ikm  = ikm,
                                                                                  salt = nonce_resp,
                                                                                  info = "obsetync/v2/s2c")
                                                                  aad_resp = "obsetync/v2 <METHOD> <PATH>"
                                                                  pt = status_be16 || handler_body
                                                                  ct = AES-256-GCM_encrypt(
                                                                          key_resp, nonce_resp, pt, aad_resp)
                                                                  wire_resp = 0x02 || nonce_resp || ct
                                                                      │
                                                  ◄──── response body ────

              key_resp = HKDF-SHA256(ikm cached,
                                     salt = nonce_resp,
                                     info = "obsetync/v2/s2c")
              pt = AES-256-GCM_decrypt(...)
              status = pt[0..2]
              body   = pt[2..]
```

The 64-byte `ikm` is cached for the session. `ec_priv` is zeroed
immediately after computing `dh_s` and `dh_e`. Per-request cost is
HKDF + AES-GCM — microseconds.

If `Es_pub_current` rotates mid-session, the client refreshes via
`/api/v1/server-eph` and recomputes only `dh_e` (it kept `ec_priv`?
NO — `ec_priv` was zeroed). The client must regenerate `ec_priv` and
recompute both DHs, i.e. start a new session. Acceptable; rotations
happen daily.

### 5.2 Single-DH bootstrap (only `/api/v1/server-eph`)

When the client doesn't have a current `Es_pub` (just enrolled past
`Es_pub_valid_until`, or its cache expired), it cannot do double-DH. The
bootstrap path uses single-DH against `S_pub` only:

```
client:
  ec_priv, ec_pub = X25519.gen()
  dh_s = X25519(ec_priv, S_pub)
  zero(ec_priv)
  fp = 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00       (sentinel)
  ikm = dh_s                                          (32 bytes, single DH)

  key_boot = HKDF-SHA256(
               ikm  = ikm,
               salt = nonce_req,
               info = "obsetync/v2/c2s-boot",
               len  = 32)
  aad_boot = "obsetync/v2 POST /api/v1/server-eph"
  pt = bearer_token(64) || seq_be64 || empty_body
  ct = AES-256-GCM_encrypt(key_boot, nonce_req, pt, aad_boot)
  wire = 0x02 || nonce_req || ec_pub || 0x00·8 || ct

server:
  see fp == 0x00·8 and path == "/api/v1/server-eph":
    → use single-DH bootstrap mode
    dh_s = X25519(S_priv, ec_pub)
    ikm  = dh_s
    key_boot = HKDF-SHA256(... info = "obsetync/v2/c2s-boot")
    decrypt; lookup device; check seq.
    encrypt response with HKDF info = "obsetync/v2/s2c-boot"

  see fp == 0x00·8 and path != "/api/v1/server-eph":
    → decrypt-failure decoy. Bootstrap mode is ONLY for /server-eph.
```

The single-DH bootstrap inherits v1's FS limitation (an attacker with
`S_priv` plus the captured `/server-eph` traffic can derive
`shared_boot`). This is acceptable because the only payload of a
bootstrap response is the current `Es_pub` — public information.
Knowing past `Es_pub` values doesn't help an attacker decrypt past
non-bootstrap traffic, because the corresponding `Es_priv` was destroyed
at rotation grace.

### 5.3 Why HMAC of `Es_pub` is not used

The natural instinct is to MAC `Es_pub` so clients can verify it came
from the `box.key` holder. With pure HMAC + HKDF, this is impossible
asymmetrically: the MAC key would have to be derivable from `S_priv`
(server-only) AND from something the client has (`S_pub`, public; or
`bearer`, per-device). Neither path works:

- HKDF(`S_priv`) is unverifiable client-side.
- HKDF(`S_pub`) is forgeable by anyone who has `S_pub`.
- HKDF(`bearer`) requires per-device MACs, and `/server-eph` would have
  to be a per-device endpoint to know which MAC to publish — leaking
  device identity on the wire.

The single-DH bootstrap channel achieves the same property
(only the `box.key` holder can publish a valid `Es_pub`) using the same
primitives already in use, with no new key files.

---

## 6. AAD and endpoint binding

```
"obsetync/v2 <METHOD> <PATH>"
```

`<METHOD>` is the **semantic** HTTP method (`GET`/`PUT`/`POST`/`DELETE`).
`<PATH>` is the URI path.

Consequences:

- A captured envelope cannot be replayed to a different path. AAD
  mismatch → AEAD failure.
- A captured envelope cannot be reflected as a response (different HKDF
  info labels for c2s vs s2c).
- A captured request envelope cannot be replayed to the same path
  either, because the embedded sequence number must strictly advance
  (see §8).
- Truncation and extension fail (GCM tag covers entire ciphertext).

### 6.1 X-Obsetync-Method strict requirement

The header is REQUIRED on every sealed request. No fallback to wire
method. Three failure cases, all indistinguishable on the wire:

| Failure                                              | Server response          |
|------------------------------------------------------|--------------------------|
| Header missing                                       | Decrypt-failure decoy    |
| Header value not a valid HTTP method                 | Decrypt-failure decoy    |
| Header value differs from what the client signed AAD with | AEAD tag fails → decrypt-failure decoy |

The server reads the header BEFORE attempting decrypt. If absent or
malformed, it returns the decoy without touching the body.

---

## 7. Server ephemeral rotation

### 7.1 Key files on the server

```
data/server/
├── box.key             0600  long-term static identity (S_priv)
├── box.pub             0644  long-term static identity (S_pub, base64)
├── box-eph.key         0600  current medium-term ephemeral (Es_priv_curr)
├── box-eph.pub         0644  current medium-term ephemeral (Es_pub_curr, base64)
├── box-eph-prev.key    0600  previous (in grace window; absent if past grace)
├── box-eph-prev.pub    0644  previous (in grace window)
└── box-eph.meta        0644  JSON: rotation timestamps + valid_until
```

`box-eph.meta` schema:

```json
{
  "current": {
    "rotated_at": 1735689600,
    "valid_until": 1735776000,
    "fingerprint": "<hex of SHA-256(Es_pub_curr)[0:8]>"
  },
  "previous": {
    "rotated_at": 1735603200,
    "valid_until": 1735776000,
    "fingerprint": "<hex of SHA-256(Es_pub_prev)[0:8]>"
  }
}
```

### 7.2 Rotation parameters

| Parameter        | Value          | Rationale                                      |
|------------------|----------------|------------------------------------------------|
| Rotation period  | 24 hours       | Bounds the FS exposure window per `Es_priv`    |
| Grace window     | 48 hours total | 2× rotation; clients with stale cache still work for one full period |
| Refresh margin   | 1 hour         | Clients refresh `Es_pub` 1h before `valid_until` |

### 7.3 In-process rotation timer

The server runs an async task at startup:

```
loop {
  sleep until next rotation boundary
  repeat:
    generate Es_priv_new, Es_pub_new
    fp_new = SHA-256(Es_pub_new)[0:8]
  until fp_new != 0x00·8           ; reject bootstrap-sentinel collision
  atomically:
    if box-eph-prev.{key,pub} exists:
      securely_overwrite_and_unlink(box-eph-prev.key)
      unlink(box-eph-prev.pub)
    rename(box-eph.key, box-eph-prev.key)
    rename(box-eph.pub, box-eph-prev.pub)
    write(box-eph.key, Es_priv_new, mode 0600, fsync)
    write(box-eph.pub, Es_pub_new, mode 0644, fsync)
    update_meta(box-eph.meta)
  in-memory: swap state.eph_curr, state.eph_prev
  log("rotated to fingerprint=<hex>")
}
```

`securely_overwrite_and_unlink` writes 32 random bytes over the file,
fsyncs, then unlinks. On the same filesystem this is best-effort
(SSDs may retain old data); operators paranoid about that should
filesystem-encrypt `data/server/`.

The same sentinel-collision guard applies at first server start
(`sync-server init` / `init_eph_keys` per §16): regenerate until the
new `Es_pub`'s fingerprint differs from `0x00·8`. The probability of a
hit is 2⁻⁶⁴ — operationally never — but unchecked it would silently
route every regular request to the bootstrap path and trip §9.2's
"fingerprint sentinel with non-bootstrap path → decoy" rule.

### 7.4 Distribution endpoint

```
POST /api/v1/server-eph
```

Sealed v0x02 envelope, **single-DH bootstrap mode** (`fp = 0x00·8`).
This is the ONLY endpoint allowed to use bootstrap mode.

Request body (after decrypt): empty.

Response body (after decrypt), JSON:

```json
{
  "Es_pub":            "<base64 of current Es_pub>",
  "Es_pub_prev":       "<base64 of previous Es_pub or null>",
  "rotation_timestamp": 1735776000,
  "valid_until":        1735862400,
  "rotation_period_seconds": 86400,
  "grace_seconds":          172800
}
```

The client caches `Es_pub`, `Es_pub_prev`, and `valid_until`. It
schedules a refresh at `valid_until - 3600`. If the cache is stale at
request time, it does a bootstrap fetch synchronously before the next
sealed request to a regular endpoint.

`Es_pub_prev` is included so a client whose own clock is skewed against
the server can still pick the right fingerprint for in-flight requests
during the grace window.

### 7.5 First-rotation handling

On first server start (`sync-server init`), both `box.key` and
`box-eph.{key,pub}` are generated. There is no `box-eph-prev` yet. The
server returns `"Es_pub_prev": null` for the first 24 hours.

---

## 8. Replay defense

Every sealed request carries a monotonic uint64 sequence number inside
the encrypted plaintext:

```
plaintext = bearer(64) || seq_be64 || inner_body
```

### 8.1 Client side

Each device persists `last_outgoing_seq` in plugin storage, initialized
to `0` at enrollment. Before every sealed request:

```
seq = last_outgoing_seq + 1
fsync_persist(last_outgoing_seq = seq)
embed seq in plaintext
send
```

The persist + fsync MUST happen before the request is sent on the wire,
so a crash between send and persist does not cause sequence reuse on
restart.

### 8.2 Server side

The server stores `last_seen_seq` per device. Storage schema:

```
data/devices/<device_id>/seq    8 bytes, uint64 BE
```

Or the equivalent column in SQLite if the deployment migrates to a
database. Update is atomic with respect to the request handler.

On every successful decrypt (including bootstrap mode):

```
seq_in = pt[64..72] as u64
seq_last = read(devices/<device_id>/seq)
if seq_in <= seq_last:
    encrypt and return semantic 401:
      { "error": "replay", "last_seen_seq": seq_last }
else:
    proceed to handler
    on handler success: write(devices/<device_id>/seq, seq_in, fsync)
    on handler failure (5xx): do NOT advance seq (so client may safely retry)
```

The advance-on-success rule means a transient handler error doesn't
poison the sequence counter. A retry with the SAME `seq` value is
permitted, but only until ANY successful request with a higher `seq`
lands.

### 8.3 Sequence recovery after client crash

If a client crashes hard enough to lose `last_outgoing_seq` from its
plugin settings, no dedicated recovery endpoint is required — the
replay error in §8.2 already carries the authoritative
`last_seen_seq`. Recovery flow:

1. Client picks any `seq` (e.g. `1`) and sends its next sealed
   request normally (persist-before-send still applies).
2. If `last_outgoing_seq` happened to still be ahead of the server's
   `last_seen_seq` (rare — implies the loss was very recent), the
   request succeeds and the client persists the new value as usual.
3. Otherwise the server returns the §8.2 replay error containing
   `last_seen_seq`. The client sets
   `last_outgoing_seq = last_seen_seq + 1` and retries the original
   request.

One extra round-trip in the worst case; no endpoint to maintain or
secure separately. A first-time device after enrollment has
`last_outgoing_seq = 0` on the client and `last_seen_seq = 0` on the
server; its first request uses `seq = 1` and never needs recovery.

### 8.4 Why monotonic counter, not seen-nonce LRU

- O(1) server state per device (8 bytes), not LRU growing with traffic.
- One fsync per request to guarantee crash-safety; trivially atomic.
- Tracks ordering, not just uniqueness — useful for application-layer
  observability (gaps may indicate dropped requests).
- No window-vs-memory tradeoff. Strict, simple.

---

## 9. Enveloped errors

### 9.1 After successful decrypt

If the server decrypts the envelope (regardless of what the handler
later decides), it encrypts the response. The response plaintext begins
with a 2-byte semantic HTTP status, followed by the inner body.

Wire HTTP status is **always 200 OK** in this case. The semantic status
the application layer cares about lives in the encrypted body.

Examples:

| Inner failure                | Wire HTTP | Inner status (be16) | Inner body                                         |
|------------------------------|-----------|---------------------|----------------------------------------------------|
| Bearer unknown               | 200       | 0x0191 (401)        | `{"error":"unknown_bearer"}`                       |
| Device revoked               | 200       | 0x0193 (403)        | `{"error":"revoked"}`                              |
| Replay (seq not advancing)   | 200       | 0x0191 (401)        | `{"error":"replay","last_seen_seq":12345}`         |
| Body too large               | 200       | 0x0199 (413)        | `{"error":"body_too_large","limit":1073741824}`    |
| Handler 5xx                  | 200       | 0x01F4 (500)        | `{"error":"internal"}`                             |
| Handler success              | 200       | 0x00C8 (200)        | …handler-defined body…                             |

Non-2xx semantic statuses NEVER appear as wire HTTP status. This
prevents a network attacker from distinguishing failure modes by HTTP
status code alone.

### 9.2 Decrypt failure — constant-shape decoy

Any failure that prevents decryption returns:

```
HTTP 400 Bad Request
Content-Type: application/octet-stream
Content-Length: 256
Body: 0x00 × 256
```

256 bytes of zeros, exactly. Causes that produce this response:

- Body shorter than `MIN_REQUEST_LEN` (141 bytes).
- Version byte ≠ 0x02.
- Fingerprint matches no known `Es_pub` (current or previous-in-grace),
  AND fingerprint ≠ 0x00·8.
- Fingerprint = 0x00·8 but path ≠ `/api/v1/server-eph`.
- AEAD decrypt fails (tampered, wrong key, AAD mismatch).
- `X-Obsetync-Method` header missing or invalid.
- Body larger than configured cap (default 1 GiB).

Causes that produce ENVELOPED errors instead (because decrypt succeeded):

- Bearer not in devices index → 200 wire, semantic 401 inside.
- Device revoked → 200 wire, semantic 403 inside.
- Sequence not advancing → 200 wire, semantic 401 inside.
- Handler error → 200 wire, semantic ≥400 inside.

### 9.3 Why both shapes

A consistent "everything is 200" wire would require the server to
synthesize a fake successful envelope when decrypt fails. That requires
inventing a `shared` from nothing, which is meaningless and only adds
attack surface. The chosen split — "decrypt failed → fixed-shape decoy;
decrypt succeeded → enveloped semantic status" — keeps the boundary
honest. A network attacker observing only sizes sees:

- A 256-byte response = "your envelope wasn't openable."
- A response of any other size = "your envelope opened; it might have
  been a success or any kind of semantic failure; you have to decrypt
  to find out."

That's the limit of metadata they can glean. No oracle on cause.

---

## 10. 204 / 304 status promotion

HTTP/1.1 (RFC 9112) forbids a body on responses with status `1xx`,
`204`, and `304`. Hyper enforces this at serialization and silently
drops the body. Our body would be the encrypted envelope — dropping it
leaves the client with zero bytes where it expects an AEAD-sealed blob.

Therefore the middleware drops the inner body for 204 / 304 but keeps
the original status in the encrypted semantic prefix (§3.2):

```
# wire HTTP is unconditionally 200 OK on envelope success (§3.2);
# only the inner body is shortened for 204 / 304.
if handler_status in {204, 304}:
    inner_body = b""
else:
    inner_body = handler_body
encrypt_response(handler_status, inner_body)   # status goes into the
                                               # 2-byte semantic prefix
                                               # verbatim — 204 / 304
                                               # survive the round-trip
```

The HTTP/1.1 "no body on 204/304" rule applies only to the **wire**
status, which v2 already forces to 200 OK on successful decrypt. The
semantic intent — "success, no content" or "not modified" — is
preserved in the encrypted 2-byte status prefix, so callers that
already treat 304 as a sentinel (e.g. the plugin's `getDiff`
returning `null` on "in sync") keep working unchanged after they
switch to reading the post-decrypt `status` field instead of the wire
HTTP status.

---

## 11. Session lifecycle (client)

A session = lifetime of a `SecureChannel` instance. Created once per
plugin load.

```
SecureChannel.create(server_url, S_pub_pinned, bearer, Es_pub_cached, valid_until):
  if Es_pub_cached is null OR now > valid_until - margin:
    → bootstrap-fetch /api/v1/server-eph
    → cache returned Es_pub, Es_pub_prev, valid_until

  ec_priv, ec_pub = x25519_generate()
  dh_s = X25519(ec_priv, S_pub_pinned)
  dh_e = X25519(ec_priv, Es_pub_current)
  ikm  = dh_s || dh_e          (64 bytes, cached for session)
  zero(ec_priv)
  fp   = SHA-256(Es_pub_current)[0:8]

  store: ec_pub, ikm, fp, S_pub_pinned, bearer, last_outgoing_seq

SecureChannel.encryptRequest(method, path, body):
  if now > valid_until - margin: refresh Es_pub, recreate session
  seq = persist_and_increment(last_outgoing_seq)
  nonce = random(12)
  key = HKDF(ikm, nonce, "obsetync/v2/c2s")
  pt  = bearer || seq_be64 || body
  ct  = aes_gcm_encrypt(key, nonce, pt, aad="obsetync/v2 " || method || " " || path)
  return 0x02 || nonce || ec_pub || fp || ct

SecureChannel.decryptResponse(method, path, wire):
  parse wire: version, nonce, ct
  if version != 0x02: throw envelope-error
  key = HKDF(ikm, nonce, "obsetync/v2/s2c")
  pt  = aes_gcm_decrypt(key, nonce, ct, aad="obsetync/v2 " || method || " " || path)
  status = pt[0..2]
  body   = pt[2..]
  return { status, body }
```

Plugin unload (Obsidian restart, plugin disable) drops the
`SecureChannel`. The cached `ikm` is zeroed (best-effort given JS GC).
Next load generates a fresh `ec_priv` → a new session, a new `ikm`, a
new "session" for FS purposes.

`last_outgoing_seq` and the `Es_pub` cache survive plugin reloads
(persisted to plugin settings).

---

## 12. Enrollment

Admin UI (`http://<server>:27183/admin`) generates a random base32
enrollment code with a 10-minute TTL. Operator copies it to the client.
Then:

1. Client → `GET /admin/enrollment/<code>` on the **admin port**
   (plaintext, no envelope — client has nothing to encrypt with yet).
2. Admin responds with the bundle:

   ```json
   {
     "device_name":          "iPhone",
     "device_id":            "473769b6c05db8cf265a0a63e647c550",
     "bearer_token":         "<64 hex chars>",
     "server_box_pub":       "<base64 of S_pub>",
     "wire_version":         "0x02",
     "eph_endpoint":         "/api/v1/server-eph",
     "Es_pub_initial":       "<base64 of current Es_pub at enrollment>",
     "Es_pub_valid_until":   1735862400
   }
   ```

3. Client persists `S_pub`, `bearer`, `Es_pub_initial`,
   `Es_pub_valid_until` in plugin settings. Initializes
   `last_outgoing_seq = 0`. Enrollment code burns.
4. Subsequent sync-API requests use double-DH against
   `S_pub` and the cached `Es_pub` — no bootstrap call needed for the
   first 24 hours.
5. Background timer schedules an `Es_pub` refresh at
   `Es_pub_valid_until - 3600`.

The trust boundary at enrollment is unchanged from v1: the admin-port
plaintext fetch is the moment the operator has to vouch for the network
path. Practical mitigations (in order of strength):

1. Type the enrollment bundle manually.
2. Enroll over `localhost` via SSH tunnel.
3. Enroll over Tailscale or another mesh VPN.
4. Enroll over a trusted LAN.

Once the bundle is in place, all subsequent traffic — including the
`Es_pub` rotation refreshes — is sealed end-to-end.

---

## 13. Forward-secrecy properties (full table)

| Compromise scenario                                      | Past traffic safe? | Future traffic safe? |
|----------------------------------------------------------|--------------------|----------------------|
| Ciphertext only                                          | Yes                | Yes                  |
| Bearer token                                             | Yes                | No (impersonates that device) |
| `S_pub`                                                  | Yes                | Yes                  |
| `S_priv` (`box.key`) only, all `Es_priv` destroyed      | **Yes** ✓ (this is the v2 upgrade) | No (impersonates server) |
| Current `Es_priv` only                                   | Only current rotation window's traffic at risk | No until next rotation |
| `S_priv` + current `Es_priv`                             | Current rotation window only | No |
| `S_priv` + EVERY historical `Es_priv` ever generated     | All past traffic exposed | No |
| One session's `ec_priv` leaked (post-zeroize, attacker had a window before zeroize) | That session: yes (already used). Other sessions: yes | Yes (`ec_priv` is session-scoped) |
| Full client memory, live                                 | Current session: no. Past sessions: yes | Until next plugin reload: no |

**Critical operator practice.** Destroy `Es_priv` files at rotation
grace expiry. The in-process timer does this automatically; if the
operator backs up `data/server/` they should exclude or
pre-encrypt `box-eph*.key` files older than 48 hours.

---

## 14. Migration from v1 (clean break)

Servers running v2 reject v1 clients on the wire (version byte 0x01 →
decrypt-failure decoy). Existing devices must re-enroll.

### 14.1 Server-side procedure

1. Stop the v1 server.
2. Back up `data/`.
3. Upgrade binary to v2.
4. On first start, v2 runs `box-eph` initialization: generates initial
   `Es_priv`, `Es_pub`, writes `box-eph.{key,pub,meta}`. Logs
   `eph: initialized fingerprint=<hex>`.
5. Operator regenerates enrollment codes for each device:

   ```
   sync-server admin enrollment-codes generate --name "iPhone"
   sync-server admin enrollment-codes generate --name "Desktop"
   ```

   (or via the admin UI).
6. Old `devices/` rows are wiped (their bearers are now invalid against
   v2 because there's no v1 → v2 bearer migration, and re-enrollment
   issues fresh ones).

   Alternatively: keep the rows, mark them all `revoked`, let
   re-enrollment overwrite. Either is fine; clean is simpler.

### 14.2 Client-side procedure

For each device:

1. Plugin shows error "server speaks unsupported protocol" on first
   sync attempt after server upgrade.
2. User opens the operator's admin URL (via SSH/VPN/LAN as before),
   gets a fresh enrollment code.
3. User pastes it into the plugin's "re-enroll" prompt.
4. Plugin fetches new bundle, persists, resumes sync.

`last_outgoing_seq` resets to 0 on re-enrollment.

### 14.3 Why clean break

- Wire format changed (header grew 8 bytes, plaintext prefix grew 8
  bytes, response plaintext gained 2-byte status prefix).
- Key schedule changed (single-DH → double-DH).
- Error model changed (plaintext `(401, ...)` → constant-shape decoy or
  enveloped semantic status).
- A dual-stack server would need two complete code paths and a way to
  decide which to use per request. Personal-scale deployments don't
  need that; operators re-enroll a handful of devices and move on.

---

## 15. Non-goals (still)

Same as v1:

- **Post-quantum security.** v2 reserves byte 0x03 for a future hybrid
  KEM (X-Wing or ML-KEM + X25519). When 0x03 ships, repeat this same
  clean-break migration pattern.
- **Metadata privacy.** Server still sees method, path, timing, and
  device identity (after decrypt). Use Tailscale/WireGuard for
  network-level metadata hiding.
- **Resistance to a compromised server operator.** They see plaintext
  content of every sync. Client-side per-vault encryption is a separate
  feature.
- **Defense against bugs in primitives.** A CVE in `x25519-dalek`,
  `hkdf`, `aes-gcm`, `@noble/curves`, or SubtleCrypto can break the
  envelope.

---

## 16. Code map (deltas from v1)

### Server (`crates/sync-server/src/`)

NEW files:

- `eph_rotation.rs` — Tokio interval task that rotates `Es_priv` /
  `Es_pub` every 24h. Manages `box-eph-prev` grace window. Securely
  overwrites stale private files at expiry.
- `seq_tracker.rs` — Per-device sequence counter persistence at
  `data/devices/<id>/seq`. Atomic read-modify-write under a per-device
  file lock (or transaction if SQLite).

CHANGED:

- `secure.rs`:
  - `WIRE_VERSION = 0x02`
  - `REQUEST_HEADER_LEN = 53` (was 45)
  - `MIN_REQUEST_LEN = 141` (was 125)
  - `decrypt_request` reads 8-byte fingerprint, looks up `Es_priv`,
    does double-DH (or single-DH if `fp = 0x00·8` AND path is
    `/server-eph`), returns `{bearer, seq, inner_body}`.
  - `encrypt_response` takes a `(status_u16, body)` tuple, prefixes
    status into plaintext.
  - New helper `decoy_response()` returns the canonical 256-byte zero
    body with HTTP 400 for all decrypt failures.
- `api.rs`:
  - `secure_envelope` middleware:
    - Reads `X-Obsetync-Method`, returns decoy if missing.
    - Calls `decrypt_request`; returns decoy on failure.
    - Looks up device by bearer; on failure encrypts semantic 401.
    - Checks revocation; encrypts semantic 403 if revoked.
    - Calls `seq_tracker.check_and_advance(device, seq)`; encrypts
      semantic 401 with `{error: replay}` if seq doesn't advance.
    - Dispatches handler.
    - Promotes 204/304 → 200.
    - Encrypts response with semantic status.
  - New route `POST /api/v1/server-eph` handled by `eph_handler` —
    expects single-DH bootstrap.
- `state.rs`:
  - Adds `eph_curr: Arc<RwLock<EphKeyMaterial>>`,
    `eph_prev: Arc<RwLock<Option<EphKeyMaterial>>>`,
    `rotation_state: Arc<RwLock<RotationMeta>>`.
- `enrollment.rs`:
  - Bundle JSON gains `wire_version`, `eph_endpoint`,
    `Es_pub_initial`, `Es_pub_valid_until`.
- `box_key.rs`:
  - Gains `init_eph_keys(data_dir)` for first-time `box-eph.*` setup.

### Plugin (`plugin/src/`)

CHANGED:

- `secure.ts`:
  - `WIRE_VERSION = 0x02`
  - `SecureChannel.create` now takes `Es_pub` and `valid_until`,
    handles bootstrap path.
  - `encryptRequest` embeds 8-byte fingerprint and 8-byte sequence.
  - `decryptResponse` returns `{status, body}` instead of just `body`.
- `api.ts`:
  - `SyncApi.sealed(method, path, body)` returns `{status, body}` —
    callers handle semantic status.
  - New helper `bootstrap_eph()` calls `/api/v1/server-eph` in
    bootstrap mode; called automatically when cache is missing or
    stale.
  - Sequence counter persisted in plugin settings, fsync-style flush
    (await `saveSettings()`) before each request.
- `settings.ts`:
  - New fields (camelCase, matching the existing convention —
    `serverBoxPub`, `bearerToken`, `syncIntervalMs`, etc.):
    `wireVersion`, `esPub`, `esPubPrev`, `esPubValidUntil`,
    `lastOutgoingSeq`. These are local plugin state. The
    snake_case field names used elsewhere in this spec
    (`Es_pub_initial`, `Es_pub_valid_until`, `last_seen_seq`, …)
    refer to wire JSON received from / sent to the server — they
    keep the server-side serde convention and match the existing
    `EnrollmentBundle` interface in `plugin/src/api.ts`.

### Tests

Add to `secure.rs` test module:

- Round-trip request + response with double-DH.
- Round-trip request + response with single-DH bootstrap.
- Bootstrap fingerprint accepted only on `/server-eph`, rejected
  elsewhere.
- Sequence not advancing → semantic 401 with replay error inside
  envelope.
- Bearer unknown → semantic 401 inside envelope (wire status 200).
- Revoked device → semantic 403 inside envelope.
- Decrypt failures (each cause from §9.2) all return
  exactly-256-byte zero body with HTTP 400.
- Wrong fingerprint → decoy.
- `Es_pub_prev` accepted within grace, rejected after grace expiry.
- Wire version 0x01 → decoy.

Property tests (over arbitrary bytes / paths / methods):

- Round-trip preserves `(bearer, seq, body)`.
- Any single-byte mutation of the wire fails decrypt.
- Different sessions produce different ciphertexts for the same body.
- Sequence rejection is deterministic in `(server_state, seq_in)`.

---

## 17. Wire-byte example

A `GET /api/v1/root/example-vault` round-trip with `seq = 42`, empty inner
body, double-DH (sample bytes for illustration):

### Request

```
Wire byte    Field                Hex (sample)
─────────────────────────────────────────────────────────────────
  0          version              02
  1  .. 13   nonce                8a 2f 1c 77 09 4b 63 d1 5e ff a0 22
 13  .. 45   ec_pub               3f a9 c0 ed fb 24 e0 43 4d 8e 48 2e
                                  12 b1 44 25 21 47 bd b7 86 0f 98 5c
                                  e8 81 aa 2e ba a9 7d e6
 45  .. 53   Es_pub fingerprint   91 4c 22 7d 5b a0 8e f3
 53  .. 125  ciphertext           (72 bytes: 64 bearer + 8 seq + 0 body
                                              encrypted with AES-GCM)
125  .. 141  GCM tag              16 bytes
```

Total request: 141 bytes.

Plaintext that decrypts:

```
0x00 .. 0x40   "a2df3c…" 64-byte ASCII bearer
0x40 .. 0x48   00 00 00 00 00 00 00 2A      ← seq = 42
0x48 .. M       (empty)
```

AAD committed to:

```
"obsetync/v2 GET /api/v1/root/example-vault"
```

### Response (server has the vault, 4400-byte root)

```
Wire byte      Field         Hex (sample)
──────────────────────────────────────────────────
  0            version       02
  1  .. 13     nonce         74 0b c0 33 9d 5a 1a 8c 2e f0 11 94
 13  .. N-16   ciphertext    (4402 bytes: 2-byte status + 4400-byte body
                              encrypted with AES-GCM)
 N-16 .. N     GCM tag       16 bytes
```

Total response: 4431 bytes (was 4429 in v1; +2 for status).

Plaintext after decrypt:

```
0x00 .. 0x02   00 C8                         ← semantic status = 200
0x02 .. M      (4400 bytes flatbuffers RootNode)
```

AAD committed to:

```
"obsetync/v2 GET /api/v1/root/example-vault"
```

### Decrypt-failure example

Client sends a malformed envelope (wrong server key, say). Server
returns:

```
HTTP/1.1 400 Bad Request
Content-Type: application/octet-stream
Content-Length: 256

00 00 00 00 00 00 00 00 ... (256 bytes of 0x00)
```

Same response for: wrong key, tampered ciphertext, missing
`X-Obsetync-Method`, version mismatch, fingerprint mismatch, etc.

---

## 18. Versioning policy

The `0x02` byte at the front of every envelope is the only coordination
point for future change. Reserved values:

| Byte  | Status   | Description                                        |
|-------|----------|----------------------------------------------------|
| 0x00  | Reserved | Never used                                         |
| 0x01  | Retired  | v1 wire format (`docs/transport-v1-archive.md`)    |
| 0x02  | Current  | This document                                      |
| 0x03  | Reserved | Hybrid PQC KEM (X-Wing or ML-KEM + X25519) — TBD   |
| 0x04+ | Reserved | Future                                             |

When 0x03 ships, follow the same clean-break migration pattern as
v1 → v2.

Until 0x03 is specified, this document is the complete specification of
what's on the wire.

---

## 19. Pre-release checklist

- [ ] All cryptographic primitives have KAT tests against published
      vectors (X25519, HKDF-SHA256, AES-256-GCM).
- [ ] `Es_priv` files have mode 0600, owned by app user.
- [ ] `box-eph-prev.key` securely overwritten on grace expiry (verify
      via integration test).
- [ ] Sequence counter is persisted with fsync before request send
      (verify via crash-injection test).
- [ ] Decrypt-failure response is exactly 256 bytes for every cause
      (verify via test sweep).
- [ ] Logs do not contain ciphertext, plaintext bodies, or bearer
      tokens (even truncated).
- [ ] Bundle includes `wire_version: "0x02"`, `Es_pub_initial`,
      `Es_pub_valid_until`.
- [ ] Forward secrecy verified by a soak test: capture 24h of traffic,
      reveal the relevant `box.key`, confirm captured traffic remains
      undecryptable.
- [ ] Replay test: capture a sealed request, replay it verbatim,
      confirm semantic 401 with replay error.
- [ ] All existing v1 device records cleaned or migrated as per §14.1.
- [ ] User-facing migration docs updated.
