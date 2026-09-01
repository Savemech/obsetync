# Transport security (HTTP wire 0x02)

This is the normative specification for the sync API. Wire v2 was introduced
in 1.10.1 and is intentionally incompatible with wire v1; devices enrolled by
a v1 server must re-enroll. The capability-negotiated bulk HTTP extension is
additive in 1.11.1 and does not change enrollment identity or the outer wire.

The sync port uses plain HTTP as a carrier. Request and response bodies are
protected with X25519, HKDF-SHA256, and AES-256-GCM. The public `/health`
route and the separate admin/enrollment port are outside this envelope.
Enrollment therefore has to happen over a trusted local network, VPN, or SSH
tunnel.

Implementation entry points:

- server crypto: `crates/sync-server/src/secure.rs`
- rotating server key: `crates/sync-server/src/eph_rotation.rs`
- anti-replay state: `crates/sync-server/src/seq_tracker.rs`
- server middleware: `crates/sync-server/src/api.rs`
- plugin crypto: `plugin/src/secure.ts`
- plugin request funnel: `plugin/src/api.ts`
- client sequence allocator: `plugin/src/transport-sequence.ts`

## Keys and default key schedule

The server has two X25519 key pairs:

- `S_priv/S_pub`: long-term identity key. `S_pub` is pinned at enrollment.
- `Es_priv/Es_pub`: rotating transport key. It exists only in server memory.
  A key is current for 24 hours, then remains as the previous slot for up to
  one additional 24-hour period. A server restart immediately generates a
  fresh key.

The client generates `Ec_priv/Ec_pub` when it creates a secure channel. For
ordinary authenticated requests both sides derive:

```text
dh_static = X25519(Ec_priv, S_pub)
dh_rotate = X25519(Ec_priv, Es_pub)
ikm       = dh_static || dh_rotate

request_key  = HKDF-SHA256(ikm, salt=request_nonce,
                           info="obsetync/v2/c2s", len=32)
response_key = HKDF-SHA256(ikm, salt=response_nonce,
                           info="obsetync/v2/s2c", len=32)
```

The client zeroes `Ec_priv` and the raw DH results after importing the combined
material into WebCrypto. The server zeroizes retired rotating keys and
per-request combined material on drop. This is best-effort process-memory
hygiene, not a guarantee against a live process compromise or memory dump.

## Request wire format

All integers are unsigned big-endian.

```text
offset       size   field
0            1      version = 0x02
1            12     request nonce
13           32     Ec_pub
45           8      SHA-256(Es_pub)[0..8]
53           N      AES-GCM ciphertext and 16-byte tag
```

AAD is the exact byte string:

```text
obsetync/v2 <SEMANTIC_METHOD> <URI_PATH>
```

The wire HTTP method is always `POST`. The required
`X-Obsetync-Method` header carries the semantic method used in AAD and for
inner routing.

Default plaintext is:

```text
offset       size   field
0            64     bearer token as ASCII hex
64           8      per-device sequence number
72           N      semantic request body
```

Minimum authenticated request size is 141 bytes.

## Response wire format

```text
offset       size   field
0            1      version = 0x02
1            12     response nonce
13           N      AES-GCM ciphertext and 16-byte tag
```

Response AAD is request AAD followed by the raw 12-byte request nonce. This
binds a response to one exact request, including repeated calls to the same
endpoint.

Response plaintext is:

```text
offset       size   field
0            2      semantic HTTP status
2            N      semantic response body
```

After a request has opened successfully, the outer HTTP status is always 200.
Statuses such as 204, 304, 400, 401, 403, 409, and 500 remain intact inside
the ciphertext. Bodies for semantic 204 and 304 are empty so HTTP libraries
cannot strip the outer encrypted envelope.

## Rotating-key bootstrap

`POST /api/v1/server-eph` uses fingerprint `00 00 00 00 00 00 00 00` and a
single-DH schedule:

```text
ikm = X25519(Ec_priv, S_pub)
request info  = "obsetync/v2/c2s-boot"
response info = "obsetync/v2/s2c-boot"
```

Bootstrap request plaintext is exactly empty. It carries no bearer and no
sequence number. The endpoint only returns public rotating-key metadata, so
client authentication and replay state are unnecessary. Keeping credentials
out of this single-DH mode is important: otherwise a future theft of
`S_priv` could recover a bearer from captured bootstrap traffic.

The encrypted response authenticates the new `Es_pub` to the already pinned
`S_pub`. The response JSON contains:

```json
{
  "Es_pub": "base64-encoded 32-byte public key",
  "rotation_timestamp": 0,
  "valid_until": 0,
  "rotation_period_seconds": 86400,
  "grace_seconds": 172800,
  "capabilities": ["bulk-http-v1"],
  "limits": {
    "bulk_request_bytes": 8388608,
    "bulk_objects": 256,
    "ws_frame_bytes": 4194304,
    "diff_page_bytes": 2097152
  }
}
```

`rotation_timestamp` and `valid_until` are Unix seconds. The plugin refreshes
one hour before `valid_until`. If the server restarts or a cached key falls
outside the previous-key window, the ordinary request receives the constant
decoy described below; the plugin performs one bootstrap refresh and retries
once.

The capability/limit fields are authenticated by the same pinned bootstrap
response. A client that already has a valid cached ephemeral key discovers an
in-place server upgrade once per plugin session through sealed
`POST /api/v1/capabilities`; no enrollment identity changes. Only implemented
capabilities are advertised. Unknown/missing capabilities retain the stable
single-object HTTP endpoints.

## Bulk HTTP v1

`bulk-http-v1` keeps the wire-v2 envelope unchanged and replaces many inner
object transactions with three sealed binary endpoints:

- `POST /api/v1/bulk/check`: `OBC1`, one object kind, a u32 count, then ordered
  raw 32-byte hashes. `OBA1` returns the same count and an LSB-first needed
  bitmap.
- `POST /api/v1/bulk/put`: `OBP1`, zero v1 flags, a u32 record count, then
  `(kind, flags, hash, plain_len, stored_len, bytes)` records. Compression is
  disabled, so both lengths must agree. `OBK1` returns one ordered status byte
  per record: stored, already present, bad hash, rejected limit, or retryable
  storage error.
- `POST /api/v1/bulk/get`: `OBG1`, kind/count/cursor/response budget and ordered
  hashes. `OBD1` returns the next cursor, an LSB-first bitmap for the uninspected
  tail, and a nested `OBP1`. A completed cursor with no matching record is a
  missing-object error, never an implicit success.

All integer fields are little-endian. Requests and responses reject unknown
magic/kinds or pack flags, count or arithmetic overflow, truncation, trailing
bytes, and non-zero bitmap padding. An unsupported per-record flag or length is
reported at that record's `rejected_limit` ACK position so the rest of a valid
pack can still complete. Desktop plaintext is capped at 8 MiB; mobile chooses
2 MiB; both cap a request at 256 objects and a bulk record below 1 MiB. Larger
FastCDC chunks use the unchanged single-object route. The receiver revalidates
every content address (and full manifest semantics) before storage or
application. Packs are idempotent, mixed ACKs are legal, and progress moves only
after stored/already-present ACKs.

The server feeds both bulk uploads and stable single-object uploads into one
bounded dedicated storage writer. A whole accepted group is framed into the
active append-only segment and crosses exactly one `fdatasync` before any
success ACK can be returned. Each record carries its kind, raw content address,
lengths, CRC32C trailer, and zero padding; BLAKE3 remains the semantic address.
Sealed segments publish a sorted checksummed sidecar atomically, so startup
loads their metadata without rereading payloads. Only the bounded active tail
is scanned; an incomplete last record is truncated, while a fully framed bad
record is omitted from visibility. A durable record lost between `fdatasync`
and index publication is reconstructed on restart.

Checks use the verified index. GET performs an exact record read and rechecks
header/index agreement, CRC32C, and BLAKE3 before returning bytes; failure hides
that location so a normal check/re-upload repairs it. A periodic scrub applies
the same rule. During rolling migration reads are pack-first with verified
loose fallback. The background importer is byte/count bounded and deletes a
loose object only after a successful pack ACK, a verified pack-only reread, and
a directory sync. The previous `OWG1` group journal is migrated synchronously
before the API opens because it may contain acknowledged bytes without a loose
mirror. Queue exhaustion maps to the retryable storage status (or HTTP 503 on a
legacy endpoint), preserving backpressure without weakening idempotent retry
semantics.

## Replay protection and concurrency

Ordinary requests carry a positive per-device sequence. A sequence is
consumed before the application handler runs, including when that handler
later fails. Every retry uses a new value.

The plugin reserves sequences in blocks of 4096. It durably saves the block's
high-water mark before returning its first value. A client crash can therefore
leave a gap but cannot reuse a number.

The server keeps a 1024-bit sliding replay window in memory:

- a value above the greatest observed value advances the window;
- an unseen value within the window is accepted, allowing network reordering
  of parallel uploads;
- a duplicate or a value older than the window is rejected.

Server crash durability is also reserved in 4096-value blocks. Before the
server accepts the first value in a new block, it persists and fsyncs the
block ceiling. Most requests then update only the in-memory bitmap. After a
restart, every value through the persisted ceiling is conservatively treated
as consumed. This amortizes durable I/O to roughly one fsync per 4096 requests
without creating a post-crash replay window.

A replay response is an encrypted semantic 401:

```json
{"error":"replay","last_seen_seq":4096}
```

Despite the historical field name, `last_seen_seq` is safe to treat as the
server's recovery ceiling. The client jumps past the maximum of this value,
its durable high-water mark, and its current in-memory reservation, persists
a new block, and retries once.

## Decrypt-failure decoy

Failures that prevent the server from opening and authenticating an envelope
all have one wire shape:

```text
HTTP 200
Content-Type: application/octet-stream
body: exactly 256 zero bytes
```

This includes malformed lengths, wrong wire versions, unknown rotating-key
fingerprints, invalid bootstrap use, missing/invalid semantic method, AAD
mismatch, and GCM failure. The server logs the local reason but does not put it
on the wire.

Unknown bearer, revoked device, replay, handler errors, and successful results
occur after decryption and are encrypted semantic responses. Ciphertext length
and timing are not padded, so this is not traffic-analysis resistance.

## Security properties and limits

Against a network attacker, ordinary wire-v2 traffic provides body
confidentiality and integrity, server authentication through pinned `S_pub`,
device authentication through the encrypted bearer, method/path binding,
request/response binding, and same-endpoint replay rejection.

Forward secrecy against a later theft of `S_priv` is bounded by the lifetime
of the relevant in-memory `Es_priv`. Once that rotating key and its previous
slot have been dropped—or the server has restarted—captured ordinary traffic
cannot be opened with `S_priv` alone. This claim does not hold if an attacker
also captured the applicable rotating private key, compromised the live
server process, obtained a memory/core dump containing it, or compromised the
client while its channel material was live.

The protocol does not hide sizes, timing, IP addresses, or request frequency.
It does not protect content from the server operator, a compromised device,
or an attacker holding a valid bearer. It does not make the plaintext admin
enrollment port safe on an untrusted network. Availability and volumetric DoS
are outside the envelope's guarantees.

## Required regression coverage

The repository tests both independent implementations and the full middleware:

- Rust double-DH and bootstrap round trips;
- TypeScript-to-Rust-compatible byte layout, KDF labels, sequence encoding,
  encrypted semantic status, and credential-free bootstrap;
- exact same-envelope replay and out-of-order unique requests;
- replay state across restart and reservation gaps;
- wrong key, bad fingerprint, AAD mismatch, tamper, and bad-version decoys;
- encrypted 204/304 and handler-error delivery;
- v2 E2E client compilation and full-stack security cases.
