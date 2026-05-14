# Goal

Make `obsetync-transport-v2.md` internally consistent, factually correct, and
unambiguous enough that someone — us, in a later phase — can implement it
without re-deriving the design from the audit transcript.

This task is **spec-only**. No v1 code changes here. A few problems (most
notably P13) have v1 implementation follow-ups — those are filed as Out of
Scope below and tracked separately.

# Acceptance Criteria

- [ ] All 16 P-IDs from `context.md` either edited into the spec, marked
      RESOLVED with a one-line rationale, or explicitly deferred with a TODO
      block in the spec that names the open question.
- [ ] No two sections of the spec contradict each other on the same fact.
- [ ] Every arithmetic example checks out by hand.
- [ ] §16 (deltas from v1) is realistic against the actual code shapes in
      `crates/sync-server/src/` and `plugin/src/`.
- [ ] The spec lists every cryptographic primitive it actually uses.
- [ ] One commit per fix; commit messages describe the doc change, not the AI
      that wrote it.

# Context Summary

- 1148-line spec, untracked at repo root, forward-looking.
- Current code is v1 across both server and plugin — verified.
- The "what changed from v1" claims at the top of the spec are factually
  correct (we cross-checked file:line refs).
- The defects are: (a) one arithmetic error, (b) several places where parts of
  the spec disagree with other parts of the same spec, (c) a few unresolved
  design questions left as undeclared "TBD."

# Plan

We execute in three **rounds**. One commit per problem. Each round ends with a
checkpoint where I stop and we agree before moving on.

The ordering inside each round is "least dependencies first" so cheap edits
land before architectural cascades.

---

## Round 1 — Cheap factual / contradiction fixes (no design decisions needed)

These nine are straight edits. I can land them sequentially without further
discussion if you give a single "go." Each is its own commit.

| # | P-ID | Edit                                                                                  | Risk |
|---|------|---------------------------------------------------------------------------------------|------|
| 1 | P1   | Fix §17 arithmetic: 96 → 72, 149 → 125, 165 → 141. Prose stays as "0 body".          | none |
| 2 | P10  | Add SHA-256 row to §4 primitives table; cite `crypto.subtle.digest` / `sha2 crate`.  | none |
| 3 | P11  | In §16 add a one-line note: "plugin field names follow existing camelCase in `settings.ts`; snake_case in this spec is wire JSON." | none |
| 4 | P5   | In §7.3 add `if SHA-256(Es_pub_new)[0:8] == 0x00·8: regenerate` to the rotation loop. | none |
| 5 | P16  | In §7.4 drop `next_rotation_at`; keep `valid_until` (clients use it for refresh).    | none |
| 6 | P12  | In §3.3 replace "disconnect / re-enroll" with the bootstrap-retry-then-re-enroll sequence. | low  |
| 7 | P2   | In §10 stop rewriting semantic status; only the wire HTTP status promotes to 200. Encrypted prefix keeps 204/304. | low  |
| 8 | P4   | Drop §8.3 entirely. Add a sentence to §8.2 stating: "on `replay` error, client sets `last_outgoing_seq = error.last_seen_seq` and retries; no separate endpoint is needed." | low  |
| 9 | P8   | In §7.3 spell out the rename order (tmp → fsync(dir) → swap) and add a startup recovery rule: if `box-eph.key` is missing but `box-eph-prev.key` exists, fail loud — do **not** silently regenerate. | low  |

**Round 1 exit checkpoint:** spec re-read end-to-end; no new contradictions
introduced; we confirm before moving on.

---

## Round 2 — Architecture decisions (need your call before I edit)

Four problems cluster around the same architectural question: what does
`SecureChannel` know about, and what does `SyncApi` keep?

Current code (`plugin/src/secure.ts:122-162`) keeps `SecureChannel` HTTP-free.
`SyncApi` does the HTTP. That separation is deliberate and serves us well.

My recommendation: **keep that separation in v2.** Concrete consequences:

- **P3** — `SecureChannel.create(serverPubBase64, bearerHex, esPubBase64, esPubValidUntilMs)`.
  No `server_url`. No bootstrap inside `create`. If the cache is stale,
  `SyncApi` bootstraps first and *then* calls `SecureChannel.create` with the
  fresh `Es_pub`. Edit §11 to match §16.
- **P6** — `last_outgoing_seq` lives in `settings.ts` (single source of truth).
  Per request: `SyncApi.sealed` reads it, `await saveSettings()`, then calls
  `channel.encryptRequest(method, path, body, seq)`. `SecureChannel` is
  stateless w.r.t. the counter.
- **P7** — `secure_envelope` middleware reads the 8-byte fingerprint, picks
  mode + HKDF info label, decrypts, and *passes mode* into the handler context
  (or stores it on the request extensions). `encrypt_response` gains a `mode`
  parameter so it picks `s2c` vs `s2c-boot`. The `eph_handler` itself stays
  mode-agnostic — it just emits the JSON payload.
- **P9** — drop `Es_pub_prev` from the bootstrap response payload. We cannot
  name a real client-side consumer; the server's prev slot handles
  rotation-grace decryption without any client knowing about it. If you'd
  rather keep it for operator visibility / debugging, we relabel it as
  informational in the prose and document that clients ignore it.

**Round 2 commit list** (after we agree):

| #  | P-ID | Edit                                                                            |
|----|------|---------------------------------------------------------------------------------|
| 10 | P3   | Rewrite §11's `SecureChannel.create` pseudocode to match §16; remove bootstrap-inside-create. |
| 11 | P6   | Add explicit "seq lives in settings; passed as parameter" paragraph to §8 and §11. |
| 12 | P7   | Add a "Server-side mode dispatch" subsection to §5; thread `mode` through `decrypt_request` / `encrypt_response` signatures in §16. |
| 13 | P9   | Drop `Es_pub_prev` from §7.4 response JSON + remove the misleading justification paragraph. |

---

## Round 3 — Security / design (need your call before I edit)

These three are independent.

- **P13 — In-session response replay.** Recommended: bind the request nonce
  into the response AAD:

  ```
  aad_resp = "obsetync/v2 <METHOD> <PATH>" || nonce_req
  ```

  12 extra bytes in the AAD, zero wire-format change, both sides already have
  `nonce_req` at response time. Tag fails cleanly on cross-request swap.

  **Note: this is a v1 bug too.** Once we land the spec edit, P13 grows a
  follow-up code task to apply the same binding to v1 (`secure.rs::encrypt_response`,
  `secure.ts::decryptResponse`) — the wire stays compatible because AAD isn't
  on the wire. We'd treat it as a security patch to v1.

- **P14 — Strict-advance vs replay window.** Recommended: **keep strict for
  now.** The plugin doesn't pipeline today and our threat surface stays
  minimal. Add a note to §8.4 that names the window option as a future
  evolution (64-bit bitmap, IPsec-style) gated on "first time someone wants
  parallel uploads." We don't pay the complexity until we need it.

- **P15 — Decoy shape.** Recommended: **HTTP 200 + 256 zero bytes** instead of
  400 + 256 zero bytes. Reasoning:
  - The 256-byte length is already the observable that says "decrypt failed."
  - HTTP 400 specifically conflicts with reverse-proxy / WAF 400s, muddying
    real ops debugging.
  - §9.3's argument against "everything is 200" doesn't apply: we're not
    synthesizing a valid envelope, we're returning 256 raw zeros that any
    client decode path will reject on version byte + tag.

  If you'd rather keep 400 because "400 means something failed and operators
  expect that," I'll go with 400 + 256 zeros and just document it as a
  protocol fingerprint in §2.

**Round 3 commit list** (after we agree):

| #  | P-ID | Edit                                                                       |
|----|------|----------------------------------------------------------------------------|
| 14 | P13  | Edit §5.1 / §6 to specify `aad_resp = … \|\| nonce_req`; file v1 follow-up. |
| 15 | P14  | Edit §8.4 to add a "Future evolution: window-based replay" note.           |
| 16 | P15  | Edit §3.3 and §9.2 to specify HTTP 200 + 256 zeros (or keep 400 with a fingerprint disclosure — your call). |

---

## Working Notes

- Round 1 has 9 commits → roughly half a day of careful editing, all
  reversible. Round 2 has 4 commits but each touches multiple sections.
  Round 3 has 3 commits and at least one (P13) generates a v1 follow-up.
- We're editing a single file (`obsetync-transport-v2.md`). No rebases, no
  cross-cutting refactors.
- Commit message style under undercover mode: imperative, describe the doc
  change, no AI attribution, no `Co-Authored-By`.

## Verification per slice

For a spec-only change, "verification" is:

1. Read the section before and after the edit; check it still flows.
2. Re-check any cross-references the edit touches (links, table rows, line
   refs in other sections).
3. For arithmetic edits, recompute by hand.
4. After every Round, re-read the table of contents and make sure nothing
   that used to reference (e.g.) "§8.3 sequence recovery" is now dangling.

No tests fire on this file. We rely on careful reading and the user's review.

## Out of Scope

- **Implementing v2 in code.** That's the next task. This one is spec-only.
- **Migrating v1 device records to v2.** Spec already says clean break (§14);
  no tooling to write.
- **The v1 in-session response replay code fix.** Tracked as a follow-up of
  P13; lands as its own task once the spec edit defines the canonical
  AAD shape.
- **Documentation outside this file.** `docs/transport.md` becomes
  `docs/transport-v1-archive.md` at the v2 implementation step, not now.
- **Tests for v1 code.** Not touching v1 behavior here.

## How we proceed

You read this plan. We discuss anything that looks wrong. Then:

- "go round 1" → I land commits 1–9, pausing at the round-1 checkpoint.
- For rounds 2 and 3, we settle each P-ID's decision before I edit.

If at any point a fix reveals something the plan didn't anticipate, I stop and
re-plan rather than improvise.
