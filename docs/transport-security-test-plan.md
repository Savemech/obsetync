# Transport security release gates

This tracked plan is the release authority for ObsetyNC transport tests. Private
working notes outside `docs/` are not release gates. The protocol definition is
[`transport.md`](transport.md); tests must follow that document and the current
wire constants, never weaken production behavior to satisfy an obsolete test.

## Current contract

- The only ordinary HTTP wire is `0x02`. Retired `0x01` is not a fallback.
- Ordinary requests bind semantic method and path in AEAD AAD and contain an
  encrypted bearer plus a positive durable per-device sequence.
- The server owns a durable 1024-sequence sliding anti-replay window with
  reserved sequence blocks. Exact wire replay is rejected after the envelope
  opens, including across restart.
- Bootstrap is an endpoint-scoped, credential-free exception with sequence
  zero. It cannot be used on ordinary endpoints.
- A response is direction-separated and bound to the request nonce. It cannot
  be reflected or transplanted to another request.
- Pre-open failures (bad length/version/fingerprint/method, AAD mismatch, or
  failed AEAD) have one outer shape: HTTP 200 and exactly 256 zero bytes.
- Post-open outcomes (unknown/revoked bearer, replay, semantic errors, and
  success) are encrypted semantic responses.
- An outer HTTP request is capped at 64 MiB. Receive/decrypt admission must
  bound aggregate concurrent ownership before crypto or semantic dispatch.

Raw transport replay and application retry are different tests. An exact
captured envelope must fail. A replay-safe semantic operation may be retried
with a fresh nonce and transport sequence; root mutations additionally retain
their stable application mutation identity and durable outcome protocol.

## Mandatory matrix

| ID | Case | Required result |
|---|---|---|
| T01 | TypeScript/Rust golden request and response vectors | Byte layout, KDF labels, sequence endianness, AAD, and status decode agree |
| T02 | Exact envelope replay at the same method/path | Encrypted semantic 401 with `error: replay`; handler effect occurs once |
| T03 | Unique sequences arrive out of order inside the window | Each is accepted once; a duplicate and a sequence below the window are rejected |
| T04 | Restart after sequence reservation/persistence boundaries | Reserved tail is conservatively consumed; captured pre-restart bytes cannot execute again |
| T05 | Cross-path, cross-method, and cross-protocol replay | Constant pre-open decoy; no credential, handler, or storage oracle |
| T06 | Bit flip, truncation, trailing bytes, wrong ephemeral key, nonce, or fingerprint | Constant pre-open decoy; no partial plaintext reaches a handler |
| T07 | Wire `0x00`, retired `0x01`, `0x03`, and `0xFF` | Constant pre-open decoy; no downgrade, allocation spike, or panic |
| T08 | Unknown bearer and a concurrently revoked device | Encrypted semantic authorization failure; the request either precedes revocation or is rejected, never bypasses it |
| T09 | Response reflection and response replay onto another request | Client rejects both before exposing semantic bytes |
| T10 | Body shorter than minimum, above 64 MiB, slow oversized stream, and many concurrent near-limit bodies | Bounded receive/decrypt ownership, deterministic overload response, no semantic dispatch, and no retained admission after disconnect/error |
| T11 | Lost response after an accepted content/root mutation | Retry uses fresh transport material; content is idempotent and durable root outcome returns the original application result |
| T12 | Capability or endpoint downgrade attempt | Unsupported capability stays disabled or fails clearly; it never selects wire `0x01` or an unauthenticated equivalent |
| T13 | Trace disabled versus enabled on the same corpus | Transport sampling adds at most 3% CPU/wall time and does not grow queues |

For every rejection, assert the exact outer-versus-semantic layer, response
shape, handler call count, durable sequence state, storage effect, and release
of request-memory admission. A generic “some 4xx” assertion is insufficient.

## Execution gates

The fast pull-request gate runs the focused TypeScript secure/router/API tests
and the sync-server unit tests. The pre-release gate additionally runs the real
cross-language transport suite:

```sh
cd plugin && npm test
cargo test -p sync-server
cargo test -p e2e-tests --features e2e --test transport_security
```

The e2e gate uses isolated temporary data directories and an ephemeral local
server. Production data, real vaults, and deployed services are never test
fixtures. Any skipped platform/network case remains explicitly unverified in
the release record.
