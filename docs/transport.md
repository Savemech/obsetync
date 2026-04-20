# Transport security

Every sync-API request is wrapped in an **authenticated-encryption
envelope** that the server's middleware opens before routing and that
the middleware reseals on the way back out. The wire is plain HTTP;
everything inside the POST body is ciphertext.

Building blocks:

```
X25519 ECDH   +   HKDF-SHA256   +   AES-256-GCM
```

…all from audited, widely-deployed libraries (`x25519-dalek` on the
server, `@noble/curves` + `SubtleCrypto` in the browser). This document
specifies the protocol completely: the choice space that led here, the
wire format, the key schedule, the threat model, and a worked example.

Reference implementations:

- Server: [`crates/sync-server/src/secure.rs`](../crates/sync-server/src/secure.rs) (decrypt + encrypt + round-trip tests)
- Client: [`plugin/src/secure.ts`](../plugin/src/secure.ts) (`SecureChannel`)
- Middleware wiring the envelope to axum routes: [`crates/sync-server/src/api.rs`](../crates/sync-server/src/api.rs) (`secure_envelope`)

---

## 1. How we got here

Self-hosted sync needs a secure channel between the plugin (desktop
Obsidian + iOS Obsidian) and the server. Three realistic paths:

### Path A — standard HTTPS / TLS with client certs

The textbook answer. The server runs TLS, clients present a client
certificate at enrollment, and mTLS authenticates each direction. This
is what ObsetyNC used up to 1.0.x.

It broke on two hard edges:

1. **iOS WKWebView will not attach a client certificate** to `fetch` or
   Obsidian's `requestUrl`. Mobile enrollment needed a *second* auth
   system (a bearer token) running next to mTLS, effectively doubling
   the attack surface and the operator's cognitive load.
2. **Users had to install a private CA certificate** on every device.
   That's a significant security footgun (a rogue CA cert can sign
   anything for any host the device reaches) and a significant UX
   footgun (iOS's trust-profile installation is a maze and can be
   undone silently by an iOS update that distrusts unknown roots).

### Path B — libsodium's `crypto_box` / sealed boxes

libsodium ships a batteries-included primitive
([`crypto_box`](https://doc.libsodium.org/public-key_cryptography/authenticated_encryption))
that bundles X25519 + XSalsa20 + Poly1305 and handles nonce generation,
key derivation, and AEAD for you. It's correct, fast, and well-reviewed.

It has two costs for this specific product:

1. **No universal browser build.** libsodium.js exists, but it ships
   ~300 KB of emscripten-compiled WASM whose loading path is the exact
   thing iOS WKWebView is strictest about (CSP + eval). We already have
   one WASM module to load (for hashing + Merkle-tree operations);
   bolting another one on for just ECDH+AEAD doubles the load-time
   failure surface.
2. **Wire format is opaque.** `crypto_box` picks a nonce layout, a
   key-derivation step, and a padding scheme that are baked into the
   library. If it ever needs to change — say, to add a bearer-token
   slot inside the ciphertext, or to bind the request method into the
   authenticated data — you're either forking libsodium or layering
   another framing on top.

### Path C — a minimal AEAD envelope from well-reviewed primitives

The path this project took. Instead of depending on a single
batteries-included library, we compose three NIST/IRTF-standardized
primitives that every cryptographic library and every modern browser
already ships:

| Primitive        | Purpose                      | Standard   | Where on the client                | Where on the server |
|------------------|------------------------------|------------|------------------------------------|---------------------|
| **X25519**       | Ephemeral ECDH key agreement | RFC 7748   | `@noble/curves`                    | `x25519-dalek`      |
| **HKDF-SHA256**  | Derive per-message AES keys  | RFC 5869   | `SubtleCrypto.deriveBits`          | `hkdf` crate        |
| **AES-256-GCM**  | Authenticated encryption     | SP 800-38D | `SubtleCrypto.encrypt` / `.decrypt`| `aes-gcm` crate     |

The protocol is **stateless on the wire** — one HTTP request, one
envelope — so no handshake-message ordering, no replay windows, no
session-resumption ticket format. A single 45-byte header carries
everything the server needs to decrypt the body. The client's identity
is a 64-char bearer token *inside* the encrypted plaintext, so packet
captures can't even tell which device is talking.

This is what the rest of this document describes in detail.

---

## 2. Threat model

The envelope is designed against a **network attacker** who can observe,
modify, replay, inject, or drop any HTTP request between client and
server. It is explicitly **not** designed against:

- An attacker with the server's private key (`data/server/box.key`). They
  can impersonate the server going forward — treat this file like a TLS
  server key: mode 0600, backed up encrypted, rotated on compromise.
- An attacker with a device's bearer token + the server pubkey. They
  *are* that device, as far as the server can tell. Revoke the device in
  the admin UI.
- A malicious server operator. They see plaintext of every sync — same
  property mTLS would have given. Client-side per-vault encryption is a
  separable, much more complex feature, not yet implemented.
- Side channels (timing, CPU, memory).

What the envelope guarantees against a network attacker:

| Property               | Mechanism                                                                     |
|------------------------|-------------------------------------------------------------------------------|
| Confidentiality        | AES-256-GCM on every body (request and response)                              |
| Integrity              | GCM tag, verified before the plaintext is touched                             |
| Authenticity of server | Client pins the server's X25519 pubkey at enrollment                          |
| Authenticity of device | Bearer token (64 hex chars) buried inside the encrypted plaintext             |
| Endpoint binding       | AAD includes `"obsetync/v1 <METHOD> <PATH>"` — envelope can't be replayed cross-endpoint |
| Forward secrecy        | Per-session ephemeral client keypair; past sessions stay private even if the client is compromised |
| Traffic analysis       | Nothing. Sizes, timing, and device identity (to the server) are visible      |

---

## 3. Wire format

All multi-byte integers are network byte order (big-endian). Byte offsets
are 0-indexed and inclusive-exclusive.

### 3.1 Request

```
byte offset   field                              size     notes
─────────────────────────────────────────────────────────────────────────
  0           version                             1 B     always 0x01
  1  .. 13    nonce                              12 B     random per request
 13  .. 45    client ephemeral X25519 public key 32 B     fresh per session
 45  .. N-16  ciphertext                         var      AES-256-GCM
 N-16 .. N    tag                                16 B     GCM authentication tag
```

Minimum request size: `45 + 64 (bearer) + 16 (tag)` = **125 bytes**.

The plaintext that this ciphertext decrypts to is:

```
byte offset   field              size     notes
─────────────────────────────────────────────────────────────────────
  0 .. 64     bearer token       64 B     ASCII, 64 hex chars
 64 .. M      inner body         var      the actual request body
```

### 3.2 Response

Responses omit the ephemeral pubkey — the server reuses the shared secret
from the request, so there's no second handshake to carry.

```
byte offset   field              size     notes
─────────────────────────────────────────────────────────────────────
  0           version             1 B     always 0x01
  1  .. 13    nonce              12 B     fresh per response
 13  .. N-16  ciphertext         var      AES-256-GCM
 N-16 .. N    tag                16 B
```

Minimum response size: `13 + 16` = **29 bytes** (empty plaintext, just
header + tag).

### 3.3 HTTP wrapping

- Wire method is always `POST` (Obsidian's `requestUrl` on iOS drops the
  body on GET). The server routes per-path; the semantic method lives in
  the `X-Obsetync-Method` header and is bound into the AAD — see §5.
- Content-Type is `application/octet-stream` both directions.
- `/health` is a public plaintext endpoint — it's the only request the
  client can make before enrollment (to verify reachability).

---

## 4. Cryptographic primitives

### 4.1 X25519 — elliptic-curve Diffie-Hellman

Performed on the Montgomery form of Curve25519. 32-byte keys, 32-byte
shared secrets, constant-time, no parameter choice. Standard RFC 7748.

- **Server key** (`StaticSecret`): long-term, generated once by
  `sync-server init`, stored at `data/server/box.key` (mode 0600). The
  public half at `data/server/box.pub` (base64) is what clients pin at
  enrollment.
- **Client key** (ephemeral `StaticSecret`): fresh 32 random bytes per
  plugin-load session. Private half held in JS memory, zeroed after the
  single ECDH. Public half sent on the wire in every request.

**Why noble/curves in the plugin instead of WebCrypto.** The browser
`SubtleCrypto.generateKey({ name: "X25519" }, ...)` API only shipped in
Chromium 133 (Feb 2025) / Safari 17 / iOS 17. Older Obsidian-mobile
WebKit (anything pre-iOS-17) and older Electron (Obsidian desktop ships
N months behind upstream Chromium) simply don't have it. Rather than
bifurcate the code path, the plugin uses
[`@noble/curves`](https://github.com/paulmillr/noble-curves) for X25519
only. Noble is pure-JS, audited, zero-dep, and byte-compatible with
SubtleCrypto's eventual implementation. HKDF and AES-GCM stay on
SubtleCrypto (universally supported since 2014).

### 4.2 HKDF-SHA256 — key derivation

RFC 5869. Takes the 32-byte ECDH shared secret, a random salt (= the
per-request nonce), and an info-label; outputs 32 bytes of AES key.

One shared secret per session is expanded into **many** AES keys (one
per request) via different salt + info triples. The salt is the same
random nonce we put on the wire; the info labels are:

- `"obsetync/v1/c2s"` — client → server direction (request key)
- `"obsetync/v1/s2c"` — server → client direction (response key)

Different label ⇒ different key, so a response-key leak wouldn't help an
attacker decrypt requests.

### 4.3 AES-256-GCM — authenticated encryption

NIST SP 800-38D. 256-bit key, 96-bit nonce, 128-bit tag. Standard GCM
security proof: ≈2^32 random nonces per key before collision probability
becomes a concern. Because we derive a **fresh key per request** (via
HKDF with the random nonce as salt), collision would require identical
nonce AND identical shared secret AND identical info label — which is
just a repeat of the whole request. Not a concern in practice.

The **AAD** (additional authenticated data, integrity-protected but not
encrypted) for every message is:

```
"obsetync/v1 <METHOD> <PATH>"
```

Where `<METHOD>` is the **semantic** HTTP method (GET/PUT/POST/DELETE —
whatever the handler expects) and `<PATH>` is the URI path. See §5.

---

## 5. Key schedule

For one request + response round-trip:

```
                    client                                     server
                   ────────                                   ────────

(enrollment:    server_pub ────────────────────────────────►  learned once)

per session:    eph_priv, eph_pub = x25519_generate()        (long-term priv)

per request:    nonce_req = random(12)
                shared = x25519(eph_priv, server_pub)
                key_req = HKDF-SHA256(
                              ikm  = shared,
                              salt = nonce_req,
                              info = "obsetync/v1/c2s",
                              len  = 32)
                aad_req = "obsetync/v1 <METHOD> <PATH>"
                pt  = bearer_token(64) || inner_body
                ct  = AES-256-GCM_encrypt(key_req, nonce_req, pt, aad_req)

                wire_req = 0x01 || nonce_req || eph_pub || ct
                                                    │
                                                    ▼
                                             ──── POST body ────►

                                                                shared = x25519(server_priv, eph_pub)
                                                                key_req = HKDF-SHA256(
                                                                              ikm  = shared,
                                                                              salt = nonce_req,
                                                                              info = "obsetync/v1/c2s",
                                                                              len  = 32)
                                                                aad_req = "obsetync/v1 <METHOD> <PATH>"
                                                                pt  = AES-256-GCM_decrypt(...)
                                                                bearer = pt[..64]
                                                                inner = pt[64..]

                                                                → lookup device by bearer
                                                                → run handler on inner body
                                                                → response_body = handler output

per response:                                                   nonce_resp = random(12)
                                                                key_resp = HKDF-SHA256(
                                                                              ikm  = shared,
                                                                              salt = nonce_resp,
                                                                              info = "obsetync/v1/s2c",
                                                                              len  = 32)
                                                                aad_resp = "obsetync/v1 <METHOD> <PATH>"
                                                                ct  = AES-256-GCM_encrypt(
                                                                           key_resp, nonce_resp,
                                                                           response_body, aad_resp)
                                                                wire_resp = 0x01 || nonce_resp || ct
                                                                    │
                                                 ◄──── response body ────

                key_resp = HKDF-SHA256(
                              ikm  = shared (cached from request),
                              salt = nonce_resp,
                              info = "obsetync/v1/s2c")
                pt  = AES-256-GCM_decrypt(key_resp, nonce_resp, ct, aad_resp)
                (pt is the handler's response body)
```

Subsequent requests in the same session reuse the cached `shared`. That
means the per-request cost is just HKDF (≈microseconds) + AES-GCM.

---

## 6. AAD and endpoint binding

AAD is authenticated but not encrypted — the GCM tag commits to it. A
request's AAD is:

```
obsetync/v1 PUT /api/v1/chunk/3fa9c0…
```

Consequences:

- A captured envelope can't be **replayed** to a different path. If a MITM
  tries to send a stolen `PUT /api/v1/chunk/X` envelope at `DELETE
  /api/v1/device/Y`, the server computes AAD for the new path, the GCM
  tag fails, the request 401s.
- A captured envelope can't be **truncated** or **extended** — GCM tag
  covers the whole ciphertext.
- **Direction can't be reflected.** A request key (`c2s`) cannot decrypt
  a response (`s2c`) because the HKDF info differs.

**Why the semantic method matters.** The plugin tunnels every HTTP verb
through wire POST because iOS' `requestUrl` drops bodies on GET. The
*intended* method goes in the `X-Obsetync-Method` header. The middleware
reads that header and feeds it into AAD. Both client and server must
agree on the semantic method, otherwise decrypt fails — which means a
MITM can't lie about what a request was supposed to do, and the client
can't be tricked into accepting a response meant for a different verb.

---

## 7. Session lifecycle

- **Plugin load** → `SecureChannel.create(server_pub, bearer)` generates
  a fresh ephemeral keypair, performs ECDH, imports `shared` as HKDF key
  material, zeroes the ephemeral private key bytes.
- **Each request** → random 12-byte nonce; `deriveBits` new AES key;
  encrypt; send.
- **Each response** → derive response AES key from cached `shared` + the
  response nonce; decrypt.
- **Plugin unload** (Obsidian restart, plugin disable) → the whole
  `SecureChannel` goes out of scope; the shared secret is dropped. Next
  load generates a new ephemeral keypair → a new session, a new shared
  secret.

This gives **forward secrecy per session**. An attacker who compromises
a device and extracts memory today cannot decrypt captured traffic from
previous plugin sessions — those ephemeral keys are gone. (Same property
as TLS 1.3 with session tickets.)

---

## 8. Enrollment — how pinning happens

The server admin UI (`http://<server>:27183/admin`) generates a random
enrollment code (base32, 10-minute TTL). The operator copies it to the
client. Then:

1. Client hits `GET /admin/enrollment/<code>` on the **admin port**
   (plaintext HTTP, no envelope — it has nothing to encrypt yet, doesn't
   know the server key).
2. Admin responds with a JSON bundle:

   ```json
   {
     "device_name":    "iPhone",
     "device_id":      "473769b6c05db8cf265a0a63e647c550",
     "bearer_token":   "<64 hex chars>",
     "server_box_pub": "<base64 of the server's X25519 public key>"
   }
   ```

3. Client persists `server_box_pub` + `bearer_token` in plugin settings.
   Enrollment code burns (one-shot).
4. Every subsequent sync-API request uses `server_box_pub` as the
   ECDH target and embeds `bearer_token` in the encrypted plaintext.

**What the operator must trust at enrollment time.** The admin-port
connection for step 1 is plaintext, so the network path between client
and admin UI during enrollment is a trust boundary. Practical mitigations
(in order of strength):

1. Type the enrollment bundle in manually — don't use the URL flow.
2. Enroll over `localhost` via SSH tunnel or Tailscale.
3. Enroll over a LAN you control.
4. Put the admin port behind a reverse proxy with its own TLS.

Once the bundle is in place, all subsequent traffic is sealed end-to-end
regardless of the sync-port network path — the pinned `server_box_pub`
prevents impersonation.

---

## 9. Forward-secrecy properties in detail

| Compromise                                              | Past traffic safe? | Future traffic safe? |
|---------------------------------------------------------|--------------------|----------------------|
| Attacker records ciphertext, has nothing else           | Yes                | Yes                  |
| Attacker obtains the bearer token                       | Yes                | No (can impersonate) |
| Attacker obtains `server_box_pub`                       | Yes                | Yes                  |
| Attacker obtains `server/box.key` (server private)      | Yes (ephemerals gone) | No (can impersonate server) |
| Attacker obtains a session's ephemeral client private   | That session only: yes. Other sessions: yes | Yes (ephemeral is session-scoped) |
| Attacker gets full-device memory access on a live client | Current session: no. Past sessions: yes | Until the next plugin reload: no |

The practical upshot: **rotating `box.key` on the server breaks future
sessions for every enrolled device**. They'd need to re-enroll to learn
the new pubkey. This is intentional — it's the only channel to revoke
the server's identity. Clients don't try to be clever about rotation.

To revoke a **device**, just flip `devices/<id>/revoked` on the server.
The next request from that device will 403 at the middleware (bearer
token looked up, device marked revoked → returned).

---

## 10. Non-goals

- **Post-quantum security.** X25519 and AES-256-GCM are both classical.
  Migration path when PQC KEMs mature: the wire version byte (0x01)
  reserves room for a v2 with a hybrid X-Wing or ML-KEM + X25519 KEM.
- **Metadata privacy.** The server sees method, path, and timing of
  every request — identical to mTLS with SNI + HTTP path-in-plaintext.
  Use Tailscale / WireGuard underneath if you need network-level
  metadata hiding.
- **Resistance to a compromised server.** A server operator can read
  every file (they hold the blobs). This is sync-tool-standard behavior;
  client-side encryption with per-user keys is a separable, much more
  complex feature not yet implemented.
- **Defense against bugs in the underlying primitives.** We wrap
  [`x25519-dalek 2`](https://docs.rs/x25519-dalek/latest/x25519_dalek/)
  (server), [`@noble/curves`](https://github.com/paulmillr/noble-curves)
  (client ECDH), [`hkdf 0.12`](https://docs.rs/hkdf/),
  [`aes-gcm 0.10`](https://docs.rs/aes-gcm/), and SubtleCrypto for HKDF +
  AES on the client. A CVE in any of those can break the envelope.

---

## 11. Code map

Follow the bytes through the implementation:

**Server encryption boundary.** Every protected route is wrapped by
`secure_envelope` middleware
([`api.rs`](../crates/sync-server/src/api.rs)):

```
wire POST body      ─► axum::body::to_bytes        (middleware)
─► secure::decrypt_request(bytes, server_priv, method, path)
─► DecryptedRequest { bearer, inner_body, shared_secret }
─► devices::lookup_token(bearer)    → device_id
─► devices::is_revoked(device_id)?  → 403 if yes
─► route handler (sees the decrypted inner_body)
─► handler response body            (inner plaintext out)
─► secure::encrypt_response(bytes, shared_secret, method, path)
─► wire POST response body
```

Primitives live in [`crates/sync-server/src/secure.rs`](../crates/sync-server/src/secure.rs):

- `WIRE_VERSION`, `NONCE_LEN`, `PUBKEY_LEN`, `KEY_LEN`, `TAG_LEN`,
  `BEARER_LEN`, `MIN_REQUEST_LEN` — wire-format constants
- `decrypt_request` — §3.1 → §5 → return bearer + inner
- `encrypt_response` — §3.2 → §5 with `c2s` swapped for `s2c`
- `hkdf_key` — §4.2
- `build_aad` — §4.3

Unit tests in the same file cover:

- Round-trip request + response
- Tampered ciphertext rejected
- Wrong server key rejected
- AAD mismatch rejected
- Version byte mismatch rejected
- Too-short body rejected
- Two independent sessions derive independent keys

**Client encryption boundary.**
[`plugin/src/secure.ts`](../plugin/src/secure.ts):

- `SecureChannel.create(server_pub_b64, bearer_hex)` → §5 setup
- `encryptRequest(method, path, body)` → §3.1 wire request bytes
- `decryptResponse(method, path, wire)` → opens §3.2

[`plugin/src/api.ts`](../plugin/src/api.ts)'s `SyncApi.sealed(method,
path, body)` is the single funnel every sync call goes through:

```
SyncApi.sealed:
  channel = await getChannel()                     ← first call only
  wireBody = channel.encryptRequest(method, path, body)
  res = requestUrl({
    url: `${serverUrl}${path}`,
    method: "POST",                                ← always POST on wire
    headers: { "X-Obsetync-Method": method, ... }, ← semantic verb here
    body: wireBody,
  })
  if 2xx:
    plaintext = channel.decryptResponse(method, path, res.arrayBuffer)
    return plaintext parsed as JSON / raw bytes
  else:
    return status + plaintext body (non-2xx are plaintext errors
                                     from the middleware — 401/403/500,
                                     never the AEAD envelope)
```

---

## 12. Wire-byte example

A `GET /api/v1/root/example-vault` round-trip, inner body empty (just the
64-byte bearer), using made-up bytes for illustration:

### Request

```
Wire byte      |  Field          | Hex (sample)
─────────────────────────────────────────────────────────────
  0            | version         | 01
  1  .. 13     | nonce           | 8a 2f 1c 77 09 4b 63 d1 5e ff a0 22
 13  .. 45     | eph pubkey      | 3f a9 c0 ed fb 24 e0 43 4d 8e 48 2e
                                   12 b1 44 25 21 47 bd b7 86 0f 98 5c
                                   e8 81 aa 2e ba a9 7d e6
 45  .. 141    | ciphertext      | (aes-gcm ct of: bearer(64) || "")
 141 .. 157    | tag             | 16-byte GCM tag
```

Plaintext that decrypts from bytes 45..141 (96 bytes → 80 ct + 16 tag =
64 pt bearer + empty body):

```
00:  61 32 64 66 33 63 …       ← "a2df3c…" bearer in ASCII hex
 …
63:  … e1
```

AAD committed to by the GCM tag:

```
"obsetync/v1 GET /api/v1/root/example-vault"
```

### Response (server has the vault, 4400-byte root)

```
Wire byte      |  Field          | Hex (sample)
─────────────────────────────────────────────────────────────
  0            | version         | 01
  1  .. 13     | nonce           | 74 0b c0 33 9d 5a 1a 8c 2e f0 11 94
 13  .. N-16   | ciphertext      | (4400 bytes of flatbuffers-encoded
                                    RootNode, AES-GCM-encrypted)
 N-16 .. N     | tag             | 16-byte GCM tag
```

N = `13 + 4400 + 16` = 4429 bytes of HTTP response body. Client verifies
AAD `"obsetync/v1 GET /api/v1/root/example-vault"`, decrypts, hands the 4400
bytes to `wasm_root_hash_from_bytes(...)` to learn the root hash.

---

## 13. Protocol versioning

The `0x01` byte at the front of every envelope is the only coordination
point for future change. A v2 with hybrid post-quantum KEM, or any other
breaking wire change, bumps that byte. Servers can accept both versions
during a migration; clients learn the supported version range at
enrollment time if we need to extend the bundle.

Until `0x02` ships, this document is the complete specification of
what's on the wire.
