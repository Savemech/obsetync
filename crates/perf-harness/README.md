# Obsetync deterministic performance harness

`obsetync-perf` materializes the W1–W8 datasets from
`tasks/performance-1.11.1.md` without using real vault data. Every byte, path,
operation, and interruption interval is derived from `(workload, seed, scale)`.
The generated `manifest.json` commits to the plan and full dataset digest;
`operations.ndjson` stays outside `vault/`, so the sync workload is not polluted
by harness metadata.

## Commands

Inspect cost before writing anything:

```bash
cargo run -p perf-harness --release -- describe w2
```

Create a dataset in a new directory (existing destinations are refused):

```bash
cargo run -p perf-harness --release -- generate w1 --output /bench/W1
```

W3 and W4 exceed the default 5 GiB accident guard. Their full versions require
an explicit `--allow-large`. For CI/smoke tests, scale count and bytes together:

```bash
cargo run -p perf-harness --release -- generate w8 \
  --scale 0.001 --output /tmp/obsetync-W8-smoke
```

Validate counts/sizes quickly, or reread and hash every byte with `--deep`:

```bash
cargo run -p perf-harness --release -- verify \
  --input /bench/W1 --deep
```

Apply one deterministic scenario step after the previous one has synchronized:

```bash
cargo run -p perf-harness --release -- apply \
  --input /bench/W6 --operation 0
```

Run the native scan/hash/tree baseline. W6/W7 can be bounded for smoke runs;
full release reports omit the bound:

```bash
cargo run -p perf-harness --release -- bench \
  --input /bench/W6 --iterations 5 --output reports/W6.json
```

## Workload semantics

- W1: 100,000 Markdown files, each 1–16 KiB.
- W2: exactly 25,000 mixed Markdown/JPEG/PDF paths and exactly 4.5 GiB.
- W3: 10,000 high-entropy JPEG/PDF-shaped assets, 100 KiB–20 MiB, skewed
  toward smaller objects.
- W4: one deterministic 20 GiB stream; generation and verification are
  bounded-memory.
- W5: the W1 base plus one deterministic same-path modification.
- W6: 10,000 files under one large tree-v1 prefix plus 10,000 sequential
  modifications, exposing prefix rebuild and unreachable-chunk growth.
- W7: 50,000 source paths followed by 25,000 renames and 25,000 deletions.
- W8: 68,000 first-pull files plus a deterministic 16-step interruption
  schedule, each interval between 5 and 20 seconds.

The JPEG/PDF payloads intentionally model sync/storage entropy and extension
mix; they are not intended as visual document fixtures. Benchmark runs must
record filesystem, thermal state, power mode, cache state, server storage, and
network profile alongside the JSON report.
