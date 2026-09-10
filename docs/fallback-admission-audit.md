# Capability fallback and admission audit

Updated: 2026-09-10. Scope: production client file-sync paths in
`plugin/src`; server storage and dormant live-document code are outside this
pass.

## Result

The audit found 14 fallback-boundary families. All 14 production paths now
preserve the original bounded ownership model or fail closed. The Windows
manifest validator, desktop reconcile fallback, legacy V1 conflict
preservation, mobile capability retry, and old Tree/WASM ABI defects were fixed
during this audit.

The failure pattern under review is:

1. a preferred capability is unavailable or fails after admission;
2. control enters a different implementation with a larger allocation or
   synchronous-work model;
3. the original reservation, cleanup, freshness, or retry assumptions no
   longer describe the fallback;
4. the item either crashes the renderer, repeats forever, or stalls the UI.

## Inventory

| ID | Boundary | Degraded route | Admission and liveness result | Priority | Status |
| --- | --- | --- | --- | --- | --- |
| FA-01 | `push`: desktop worker absent at startup | Native ranged renderer FastCDC + ranged upload | Feed and range queue remain bounded; no adapter whole-file read | closed | Fixed in 1.12.2; transaction regression present |
| FA-02 | `push`: desktop worker fails after dispatch | Drain native worker, then native ranged renderer FastCDC | Old worker lifetime settles before reuse; fallback remains bounded | closed | Fixed in 1.12.2; cleanup/handoff regression present |
| FA-03 | `resolvePreparedDesktopManifest`: worker/renderer result handoff | Validate and retain prepared manifest | Windows `dev`/`ino` may be integer-valued numbers above `MAX_SAFE_INTEGER`; the stricter prepared validators rejected a range-qualified source after a complete pass | P0 | Fixed in 1.12.3; direct validation and durable restart tests added |
| FA-04 | `push`: desktop worker fails for a small source | Adapter whole-file read | Worker cleanup is joined and a larger whole-source plan is re-admitted before reading; oversized sources defer | closed | Existing transaction coverage |
| FA-05 | `hashStableFile`: desktop worker unavailable/fails | Native `createReadStream`, otherwise admitted adapter read | Native stream is feed-bounded. Adapter fallback reserves the complete source first and rejects growth | closed | Existing hash-source and scan coverage; native-device qualification still required |
| FA-06 | Browser hash worker unavailable/fails | Renderer hashes already admitted bytes, or performs a fresh admitted reread after transferred-buffer loss | Generation and cleanup fences prevent reuse of detached bytes | closed | Existing browser-worker fallback tests |
| FA-07 | `repairLargeContent`: desktop worker unavailable/fails | Native ranged renderer FastCDC + ranged upload | Range qualification is independent of worker-pool availability. Confirmed runtime worker cleanup is followed by a newly admitted bounded renderer pass; no adapter whole-file read occurs | closed | Fixed in 1.12.4; constructor/unavailable and runtime-failure regressions present |
| FA-08 | `readAdmittedPushSource`: no qualified no-follow reader | Adapter whole-file read | The original whole-source reservation remains live; unsafe/drifting native observations reject rather than fall back | closed | Existing verified-read and push tests |
| FA-09 | Mobile large-file range capability unavailable | Defer the path | A stable `range-unavailable` reason remains held for the same source and capability key; capability/source changes and manual retry wake it | closed | Fixed in 1.12.5; no periodic unsupported-work churn |
| FA-10 | WS data lane unavailable/retryable failure | HTTP bulk | Pending WS crypto/send tasks drain before HTTP reuses child quota; operation must be content-addressed replay-safe | closed | Router, WS lifetime, and API tests present |
| FA-11 | Bulk endpoint unavailable or rejects effective pack size | Legacy object endpoint or recursively split pack | Encoded pack quota is released before fallback; single-object transport remains under the caller's source scope | closed | Existing API/bulk tests |
| FA-12 | Legacy V1 conflict preservation cannot fetch a small content object | Reconstruct the immutable losing object into exclusive staging | The losing bytes come only from verified server content or manifest chunks; large copies require native append and never reread the current local path | closed | Fixed in 1.12.5; large immutable reconstruction and newer-local-generation regression added |
| FA-13 | Existing conflict-copy verification lacks qualified native range IO | Refuse preservation | Files above 1 MiB return `reader-unavailable`; no unbounded portable read occurs | closed | Existing conflict verification tests |
| FA-14 | Incremental WASM tree-job ABI missing | Synchronous compatibility helpers | Engine-owned candidate open/mutation, chunk-page collection, and root export require the incremental ABI and fail before invoking a legacy getter/mutator | closed | Fixed in 1.12.5; direct structural compatibility remains explicit and outside production engine paths |

Scalar WASM selection is tracked as a performance profile change rather than
an admission fallback: it keeps the same source ownership and scheduler
boundaries. Platform atomic-write/copy capability checks are also excluded
from the count because they do not select a larger source materialization;
their durability behavior is covered by the storage and conflict ADRs.

## Resolution order

1. FA-03 shipped the Windows prepared-manifest identity correction.
2. FA-07 kept reconcile fallback on fingerprint-qualified ranged reads.
3. FA-12 replaced current-path conflict fallback with immutable verified
   server-object reconstruction.
4. FA-09 introduced stable capability-keyed suppression for unsupported mobile
   range reads.
5. FA-14 made the incremental Tree/WASM ABI mandatory for engine-owned work;
   missing or partial capability is refused before synchronous compatibility
   code executes.

## Required regression shape

Every new fallback test must prove all of the following, not only successful
output:

- the preferred capability fails at the actual handoff boundary;
- no whole-file adapter read occurs for a ranged source;
- the old native task settles before its reservation is reused;
- peak feed/range buffers stay within the admitted ceiling;
- source identity and journal generation are rechecked after every async
  boundary;
- failure leaves the root unpublished and the journal item retryable;
- retry resumes from server-confirmed objects instead of retransmitting the
  complete file.

## Search coverage

This pass inspected production catches and capability branches in `push.ts`,
`sync.ts`, `reconcile-upload.ts`, `desktop-ranged-upload.ts`,
`desktop-verified-read.ts`, `conflict-file-verification.ts`,
`root-conflicts.ts`, `api.ts`, `ws-data.ts`, `transport-router.ts`, `pull.ts`,
`index-upload.ts`, and the browser/desktop worker pools. It also compared every
direct production `PlatformIO.readFile` call with its owning admission scope.

This is a static contract audit plus focused regression evidence. It is not a
Windows/macOS/iOS/Android RSS or fault-injection qualification.
