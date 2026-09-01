# Obsetync 1.11.1 performance evidence

This directory contains deterministic, synthetic-only evidence produced while
implementing the 1.11.1 performance slices. Reports are additive: each slice
keeps the workload identity, platform, exact operation count, semantic oracle
result, and resource invariants needed to judge that slice.

`W6-slice1.json` is the full-scale transactional-tree soak, not a scaled smoke
run. It applies all 10,000 edits to the 10,000-file large-prefix workload. Each
edit uses begin/apply/commit plus reachability GC, and the final root is checked
against an independent flat rebuild. The mandatory Slice 1 invariant is
`unreachable_tree_chunks == 0`.

`W6-smoke.json` repeats the exact 1% dataset used by the 1.10.2 baseline. Its
semantic final root is identical while the final in-memory store drops from
101 chunks (one live + 100 historical) to exactly one reachable chunk.

This Linux/x86_64 run is implementation evidence, not the final cross-hardware
release report. A17, M1, Snapdragon, cold/warm cache, power, and network fields
are added by the final hardware gate.
