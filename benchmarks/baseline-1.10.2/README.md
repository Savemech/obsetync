# Obsetync 1.10.2 performance baseline

This directory stores public, synthetic-only baseline evidence for the 1.11.1
performance program. No real vault paths or content are used.

Two evidence levels are intentionally kept distinct:

- `*-smoke.json`: a 1% deterministic dataset used to prove generator,
  verifier, tree benchmark, and scenario execution on every development run;
- full release-gate results: generated on each named target class (A17, M1,
  Snapdragon, x86) with scale `1`, warm/cold cache state, thermal/power state,
  server storage, and network profile recorded alongside the report.

Scaled results are not accepted as throughput claims. They catch regressions
and make the benchmark mechanics reproducible while avoiding an accidental
~80 GiB local materialization of W1–W8.

Default seed: `819217251961995265` (`0x0b5e71c011110001`).

## Full plan identities

| Workload | Files | Bytes | Operations | Plan digest |
|---|---:|---:|---:|---|
| W1 | 100,000 | 868,632,587 | 0 | `dee62a4233c54fbdbb7c346ef453b92000202f0108b4aef879626c2e17d093b4` |
| W2 | 25,000 | 4,831,838,208 | 0 | `cc78e13fd1e7c14b8eaa041ad589e7fab85650a118ecb15215fcc57011817dc7` |
| W3 | 10,000 | 53,771,008,729 | 0 | `164470d1065e0ba31c8f5d51c110585d017656fe21256f9ddf59435b6fb126ca` |
| W4 | 1 | 21,474,836,480 | 0 | `14a002710883e9b28fb6d64faccdc9b1ae42cf650a5c22dcc05f1852903a9cc8` |
| W5 | 100,000 | 868,632,587 | 1 | `5b72941e86a98171e30ebc00899995a1b386750ced79f83617a2a9e5846e887d` |
| W6 | 10,000 | 86,984,465 | 10,000 | `c8d099683f33cd4b4bd44806e5b915f70d109847428c195fcde97377e581c3e1` |
| W7 | 50,000 | 115,246,707 | 50,000 | `60071ecf12d219e61e4c8a6e7577d2764656c63d1a0983aa591c7a612f616d7d` |
| W8 | 68,000 | 2,258,862,019 | 16 kill intervals | `dd2d900a89090cbf5714abbf8962b64154ea661cf406916b2078cbedd5d19a40` |

## Reproduction

See `crates/perf-harness/README.md`. A smoke report is produced from a fresh,
nonexistent output directory with:

```bash
cargo run -p perf-harness --release -- generate w1 \
  --scale 0.01 --output /tmp/obsetync-W1-smoke
cargo run -p perf-harness --release -- verify \
  --input /tmp/obsetync-W1-smoke --deep
cargo run -p perf-harness --release -- bench \
  --input /tmp/obsetync-W1-smoke --iterations 3 \
  --output benchmarks/baseline-1.10.2/W1-smoke.json
```

For W6/W7 smoke reports, `--max-operations` bounds only scenario execution;
it does not alter the dataset manifest. Full W6/W7 gates omit that flag.

## Instrumentation overhead gate

`client-instrumentation-overhead.json` is produced by `npm run bench:perf` in
`plugin/`. Each raw run hashes 512 MiB as 8,192 independent 64 KiB files, with
instrumented and control work interleaved file-by-file. One reported sample
combines two opposite A/B order patterns (1 GiB per arm) before calculating
the ratio, cancelling the repeatable cache/order bias instead of selecting a
favourable whole-run ordering. The median of seven balanced samples is the
release gate; every balanced and raw run delta is retained in the report.

High-cardinality read/hash/FastCDC phases use deterministic weighted sampling:
the first 32 observations are exact, then one of every 16 represents the tail.
Weights cover the complete population exactly; low-cardinality network,
envelope, tree, root, checkpoint, and durability phases remain exact.
