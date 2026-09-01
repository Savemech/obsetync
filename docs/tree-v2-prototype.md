# Tree v2 range-Merkle format

Tree v2 began as the isolated Slice 11 prototype documented in this file. Slice
12 completed the mixed-version migration: the plugin and server can now build,
validate, persist, diff, merge, inspect, and roll back v1 and v2 roots. Tree v2
is deliberately not enabled by version upgrade alone. A vault stays on Tree v1
until every enrolled device reports support over its authenticated session and
an operator explicitly projects the current semantic state into v2.

## Why Tree v2 uses a range tree

Tree v1 places at most 1,000 entries in each fixed-position leaf under a
top-level path prefix. A content edit changes one leaf, but inserting or deleting
near the beginning shifts every later fixed-position boundary. A wide prefix can
therefore force the client to rebuild and the server to diff almost its entire
entry stream.

Tree v2 sorts the same `FileEntry` semantic state by normalized vault-relative
path and groups records at deterministic, path-derived boundaries. An existing
path's content hash, mtime, and size do not participate in segmentation. This
has two useful consequences:

- changing file metadata cannot move a boundary;
- inserting or deleting a path perturbs records only until a later natural
  boundary reappears.

The implementation is in `sync_core::tree_v2`. It intentionally has distinct
node magics and a distinct semantic root domain so that v2 bytes cannot be
mistaken for Tree v1 FlatBuffers.

## Canonical layout

The root is:

```text
TreeV2Root {
  version: 2,
  total_files,
  child: optional RangeRef
}
```

Every immutable child is addressed by BLAKE3 over its exact canonical bytes. A
`RangeRef` commits to:

```text
min_path, max_path, child_hash, file_count, serialized_bytes, height
```

Leaves use the `OVL2` domain and store complete ordered `FileEntry` records.
Internal nodes use the `OVI2` domain and store ordered, non-overlapping range
descriptors of one common child height. The semantic root hash uses the `OVR2`
domain and commits to the version, total file count, and complete top descriptor.

The current format parameters are:

| Property | Limit |
|---|---:|
| Natural leaf eligibility | at least 128 entries and 32 KiB serialized |
| Hard leaf entry cap | 1,024 |
| Hard leaf/internal byte cap | 256 KiB |
| Internal fan-out | 32–256 where natural boundaries permit |
| Path length | 4,096 UTF-8 bytes |
| Tree depth | 16 |
| Nodes per traversal | 1,000,000 |
| Entries per traversal | 10,000,000 |

The lower bounds guide natural cuts; a final tail may be smaller. Hard byte and
count limits are always enforced.

## Deterministic path CDC

The boundary predicate is a Gear-style 64-bit fingerprint over path bytes only.
Each byte selects a stable SplitMix-derived table word; the accumulator rotates
and adds that word. A cut becomes eligible only after the minimum record/byte
conditions. A hard record or serialized-byte cap forces a cut.

The implementation pins a known fingerprint fixture in tests. Changing the Gear
table, update rule, masks, limits, record encoding, or path normalization moves
canonical boundaries and therefore requires an explicit tree-format migration.
The algorithm does not depend on native endianness, pointer width, random state,
filesystem enumeration order, or content metadata, so native and WASM builds
produce identical projections.

Internal descriptors are segmented by the same principle using their maximum
path. Stable internal boundaries prevent a localized leaf change from rebuilding
an unbounded suffix of parent nodes.

## Local update

An update receives final upserts and deletions and performs these steps:

1. Validate the root, operation paths, and traversal limits.
2. Descend internal nodes to collect leaf descriptors without loading leaf
   payloads.
3. Locate the first and last leaf ranges touched by the operation paths.
4. Load that span and apply the same stable, duplicate-aware linear merge used
   by Tree v1.
5. Re-segment the span. If its final canonical boundary equals the old range end,
   the new stream has resynchronized; otherwise load one more old leaf and retry.
6. Reuse all untouched leaf hashes and rebuild deterministic internal descriptor
   levels to the root.

Insertion before the first path and after the last path are handled by including
the adjacent edge leaf. If the altered stream never finds an old boundary, it is
bounded by the end of the tree. The update result exposes diagnostic counters for
loaded leaves/entries, scanned resynchronization leaves, replacement leaves, and
new internal nodes.

Leaf payload I/O is proportional to the resynchronization window, not the vault.
The implementation still enumerates leaf descriptors and reconstructs the small
descriptor levels, so its current CPU term is `O(number of range descriptors +
affected leaf records)`, rather than the eventual `O(log N + affected leaf
bytes)` target. A future format-preserving optimization can retain the traversed
parent spine and resynchronize each internal level locally; immutable nodes make
that refinement retry-safe without changing the leaf format.

## Recursive range diff

Diff first compares semantic root hashes. For unequal roots it aligns ordered
range windows by path:

- identical path bounds and child hash skip the whole subtree;
- unequal internal ranges expand one level;
- boundary drift combines adjacent descriptors until both sides reach a common
  maximum path;
- only an irreducibly unequal leaf window materializes `FileEntry` records.

Leaf records use the production two-pointer metadata comparison. Raw deltas are
sorted canonically before the production rename detector runs, so Tree v1 and v2
emit byte-for-byte equivalent ordered `FileDelta` values. `range_page` separately
proves exact, non-overlapping cursor traversal without flattening the tree.

## Validation and failure model

Every loaded node is checked against both its content address and its parent
descriptor. Decoders reject unknown magic, truncation at every byte boundary,
trailing bytes, invalid reserved fields, count/length overflow, unsafe paths,
duplicate or unordered entries, overlapping ranges, inconsistent heights,
declared-size mismatch, file-count mismatch, and excessive traversal depth or
cardinality.

Updates write immutable candidate nodes behind a separate candidate root. The
plugin promotes that root only after server acceptance; commit and abort both
mark the selected graph and sweep unreachable candidate nodes. On the server,
root history and the current pointer are published only after validation,
content checks, and the per-vault write transaction complete. A failed operation
therefore cannot partially replace the published root.

## Property and benchmark evidence

Tests compare 250 deterministic randomized batches of add, modify, delete, and
duplicate final-state operations against both a flat `BTreeMap` oracle and Tree
v1. Every incremental v2 root must equal a fresh v2 rebuild, and every ordered
v2 delta must equal Tree v1. Separate tests cover metadata-independent
boundaries, anchor deletion and resynchronization, bounded single-edit churn,
cursor paging, platform-stable Gear output, malformed codecs, and descriptor
tampering.

The reproducible W5 benchmark builds independent v1 and v2 stores outside timed
regions and alternates execution order over three runs. It measures a content
edit, insertion near the beginning, and deletion of a natural boundary. Before
accepting a scenario it requires:

- exact flat semantic state on v1 and v2;
- exact v1/v2 delta equality;
- incremental v2 root equality with a fresh canonical rebuild;
- bounded v2 entry loading and reachable-node churn.

The Linux/x86_64 100,000-path result is stored in
`benchmarks/after-1.11.1/W5-tree-v2-slice11.json`. Median update speedups are
40.62–95.34x and median diff speedups are 43.87–154.18x. Tree v2 materializes at
most 3,457 diff records instead of approximately 200,000 and loads at most 1,729
entries during update. Insertion and anchor deletion create at most 3 and 4 new
reachable v2 nodes, versus 102 and 100 Tree v1 nodes respectively.

These results selected the path-based range tree over the radix alternative for
the migration slice. They are algorithmic evidence on one host, not a substitute
for the A17, M1, Snapdragon, and x86 hardware gate.

## Production migration and rollout

Slice 12 added:

- a persisted `OVR2` root envelope and strict `VersionedRoot` dual decoder;
- v1↔v2 semantic projection plus version-aware diff and three-way merge;
- transactional candidate upload, reachability, history, rollback, and garbage
  collection;
- authenticated device capability tracking and an all-device fleet gate;
- a semantic audit during explicit activation and a clear HTTP 426 response for
  old plugin sessions without changing their enrollment identity;
- downgrade through a verified server-generated v1 projection or an explicit v1
  rollback, never an implicit incompatible publication.

Tree v1 remains the default and the compatibility fallback. An active v2 vault
rejects roots of the other format and old sessions receive an upgrade-required
response containing explicit guidance to update or reload, not re-enroll. The
server's bounded OBD1 transport pages are production-ready for both formats;
feeding those pages directly from `range_page` without first materializing the
complete recursive v2 delta remains a future memory optimization.
