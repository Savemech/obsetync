# Context — v2 transport spec audit

## What we're working on

- `obsetync-transport-v2.md` — a 1148-line spec for wire version `0x02`, sitting
  in the repo root, untracked. Replaces the v1 spec at `docs/transport.md`.
- The spec is **forward-looking**. The actual code is still v1
  (`crates/sync-server/src/secure.rs`, `plugin/src/secure.ts`).
- This task fixes problems **in the spec document** before we implement v2.

## Authoritative sources

- v1 spec: `docs/transport.md`
- v1 server: `crates/sync-server/src/secure.rs`, `api.rs`, `box_key.rs`,
  `state.rs`, `enrollment.rs`, `admin.rs`, `devices.rs`, `main.rs`
- v1 client: `plugin/src/secure.ts`, `api.ts`, `settings.ts`
- v2 spec under review: `obsetync-transport-v2.md`

## What the audit found

16 problems, three buckets:

1. **Hard errors that would break implementation** (P1–P5).
2. **Internal contradictions / gaps** (P6–P12).
3. **Inherited / unaddressed security & UX issues** (P13–P16).

Cross-checks against current code that came out clean (so the v2 "what changed
from v1" claims are factually accurate):

- v1 error strings in `api.rs:96,109,119` match §0.3's quotes.
- Wire-method fallback in `api.rs:52-54` matches §0.4.
- Client zeroization in `secure.ts:148` matches §0.6.
- 204/304 promotion in `api.rs:147-150` matches §0.5.
- Header / minimum-length constants in `secure.rs:60-62` match §16's "was 45 /
  was 125" parentheticals.

## The 16 problems (one-line each, anchored to spec line numbers)

| ID  | Spec ref | One-line summary                                                                 |
|-----|----------|----------------------------------------------------------------------------------|
| P1  | §17 L1031–1045 | Wire-byte request example arithmetic off by 24 bytes (96 vs 72, 165 vs 141). |
| P2  | §10 L721–728   | Pseudocode rewrites semantic 204/304 → 200, but v2's encrypted prefix can carry them. |
| P3  | §11 vs §16     | `SecureChannel.create` shape contradicts itself (HTTP-doing vs pure crypto). |
| P4  | §8.3 L615–624  | Dedicated seq-recovery endpoint is redundant with §8.2's replay error.        |
| P5  | §7.3 L491–506  | `Es_pub` generation doesn't reject `fp == 0x00·8` (collides with bootstrap sentinel). |
| P6  | §11 L754 vs §16 L992 | `last_outgoing_seq` ownership split between SecureChannel and plugin settings. |
| P7  | §5.2 vs §16    | Bootstrap-mode selection ownership unclear (middleware vs handler).            |
| P8  | §7.3 L491–506  | Rotation "atomic" block isn't atomic; crash-recovery rule missing.             |
| P9  | §7.4 L530, L545–547 | `Es_pub_prev` returned to client but no defined client-side consumer.    |
| P10 | §4 L237–241    | SHA-256 used by the protocol (fingerprint) but absent from primitives table.   |
| P11 | §16 L994       | New plugin settings use snake_case; current `settings.ts:14-26` is camelCase.  |
| P12 | §3.3 L217      | "Decoy → re-enroll" contradicts §7.4's whole purpose; missing bootstrap-retry. |
| P13 | §5.1, §6       | In-session response replay still works (no `nonce_req` binding in `aad_resp`). |
| P14 | §8             | Strict-advance forecloses any in-flight parallelism per device.                |
| P15 | §3.3, §9.2     | Decoy at HTTP 400 specifically is a fingerprint and conflicts with WAFs.       |
| P16 | §7.4 L532–534  | `valid_until` and `next_rotation_at` are the same number; one is redundant.    |

## Exit criterion for this phase

`plan.md` exists with each P-ID assigned to a slice, sequenced, with a
recommended resolution for the design-decision items (P3, P6, P7, P9, P13–P15).
