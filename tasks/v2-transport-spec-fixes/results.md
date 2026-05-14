# Results — v2 transport spec fixes

## Outcome

All 16 problems from `context.md` resolved in `obsetync-transport-v2.md`,
each as a single atomic commit on the `docs/v2-spec-fixes` branch.

| Round | P-IDs | Commits |
|-------|-------|---------|
| Baseline | — | `f3781af` |
| 1 — cheap factual / contradiction fixes | P1, P10, P11, P5, P16, P12, P2, P4, P8 | 9 commits |
| 2 — architectural cluster | P3, P6, P7, P9 | 4 commits |
| 3 — security / design | P13, P14, P15 | 3 commits |
| **Total** | **16** | **17 commits** |

## Commit log

```
1aba603 docs(transport-v2): decoy returns HTTP 200, not 400 (P15)
79ce8c6 docs(transport-v2): document strict-advance constraint and window upgrade path
37cac4f docs(transport-v2): bind nonce_req into response AAD (P13)
bfc2708 docs(transport-v2): drop Es_pub_prev from the wire — server-side slot only
e40dbcd docs(transport-v2): formalize server-side bootstrap-vs-default mode dispatch
ac851f2 docs(transport-v2): seq lives in plugin settings, passed into encryptRequest
0e0e0fb docs(transport-v2): keep SecureChannel HTTP-free in §11
e822a61 docs(transport-v2): make §7.3 rotation crash-safe, add §7.6 recovery rules
5ddb37d docs(transport-v2): recover via §8.2 replay error, not a dedicated endpoint
32a7263 docs(transport-v2): preserve semantic 204/304 across the envelope
ba4a43f docs(transport-v2): specify decoy-recovery sequence in §3.3
5606473 docs(transport-v2): drop redundant next_rotation_at from §7.4 response
689b801 docs(transport-v2): reject bootstrap-sentinel fingerprint in §7.3 rotation
edaa372 docs(transport-v2): rename §16 settings fields to camelCase
0d78f57 docs(transport-v2): add SHA-256 row to §4 primitives table
08c2b2e docs(transport-v2): fix wire-byte example arithmetic in §17
f3781af docs: add v2 transport spec draft and fix plan
```

## Verification story

Spec-only task — no test suite to run. End-to-end re-read scan after
each round confirmed:

- **No orphan references** to dropped concepts (`next_rotation_at`,
  `/api/v1/devices/me/seq`, `S_pub_pinned`, `persist_and_increment`,
  `disconnect / re-enroll`, `MUST rewrite handler outputs`,
  `esPubPrev`).
- **Arithmetic consistent.** §3.1 says minimum request = 141 bytes;
  §17 wire-byte example matches at 141 bytes (was 165 before P1).
- **`Es_pub_prev` references** only remain in three places, all
  server-side concept (the eph_prev slot dispatch in §5.1 server
  pseudocode, the `box-eph.meta` JSON schema in §7.1, and the
  explanatory prose in §7.4). No client-facing wire reference.
- **`HTTP 400` reference** survives only in the rhetorical "Why not
  match-status as well" question in §9.3 that names the rejected
  alternative — intentional.
- **TOC clean.** New subsections §5.4 (mode dispatch) and §7.6
  (crash recovery) integrate cleanly. §8.3 retained with renamed
  heading. No numbering gaps.
- **Commit messages** are undercover-clean (no AI attribution, no
  Co-Authored-By trailers) and follow project Conventional Commits
  style.

## Architectural decisions locked in (from planning round)

1. **SecureChannel stays pure crypto** — `SyncApi` keeps HTTP +
   settings I/O. P3 / P6 / P7 / P9 all consistent with this.
2. **`nonce_req` binds into response AAD** — closes in-session
   response replay. P13.
3. **Decrypt-failure decoy returns HTTP 200 + 256 zero bytes** —
   not 400. P15.

## Out of scope (filed as follow-up)

- **v1 in-session response-replay patch.** P13's spec edit closes
  the gap in v2; the same flaw exists in shipping v1 (`secure.rs`
  / `secure.ts`). Wire format is unchanged because AAD isn't on
  the wire, so v1 can be patched compatibly. Tracked as a separate
  security-patch task; not part of this doc-only round.
- **Implementing v2 in code.** Next task.
- **Migration tooling for v1 → v2 device records.** Spec already
  mandates a clean-break re-enrollment (§14); no tooling to write.
- **`docs/transport.md` → `docs/transport-v1-archive.md`** rename:
  happens with the v2 implementation, not now.

## Working notes consumed

- `plan.md` was an accurate roadmap; no replan was required.
- One mild scope adjustment per slice: in P3 I deferred the
  `last_outgoing_seq` removal from SecureChannel's store list to
  P6 to keep P3 internally self-consistent. Documented inline in
  the P3 commit message.
- Two slices grew slightly beyond their original one-liner plan:
  P11 (renamed all the §16 plugin field names to camelCase, not
  just the convention note) and P8 (added §7.6 as its own
  subsection rather than a paragraph inside §7.3). Both judged
  better information architecture, both still within the P-ID's
  scope.

## Lessons (none new)

No corrections from user during execution; nothing for
`tasks/lessons.md` this round.
