# Slice 14 hardware qualification matrix

Status: **partial**. The reproducible desktop controller gate is implemented and
has passed on real x86_64 hardware. Physical A17, M1, Windows ARM64, and slow-disk
Obsidian runs remain release evidence requirements; this file deliberately does
not substitute simulated profile tests for those runs.

## Recorded devices

| Family | Device/runtime | Evidence | Median adaptive | Median conservative | Ratio | Worst event-loop p95 | Result |
|---|---|---|---:|---:|---:|---:|---|
| x86_64 | AMD Ryzen 9 5950X, 32 logical cores, Linux x64 under WSL2, Node 22.23.2 | `x86-resource-governor-slice14.json` | 3,341.74 MiB/s | 1,652.08 MiB/s | 2.02x | 1.08 ms | controller gate passed |
| Apple M1 | Physical macOS/Obsidian run required | pending | — | — | — | — | pending |
| Snapdragon | Physical Windows ARM64/Obsidian run required | pending | — | — | — | — | pending |
| A17-class | Physical iPhone/iPad WebKit/Obsidian W8 run required | pending | — | — | — | — | pending |
| slow x86 storage | Physical SSD/HDD contention run required | pending | — | — | — | — | pending |

The recorded x86 run used the production minified SIMD worker bundle, 32 real
sparse files of 8 MiB each (256 MiB per run), a warm filesystem cache, and three
counterbalanced runs per profile. Conservative used two workers with 256 KiB
feeds; the selected balanced profile used four workers with 512 KiB feeds. The
gate requires adaptive throughput to remain within 5% of conservative and every
run's event-loop p95 to remain at or below 16 ms. Both conditions passed.

RSS is recorded per run as both absolute peak and peak delta. It is diagnostic,
not a cross-profile allocation oracle: worker retirement, allocator reuse, and
garbage collection can move the process baseline between counterbalanced runs.
The exact samples and limits are in the JSON evidence.

## Reproducing desktop evidence

From `plugin/` on the physical target:

```sh
npm ci
npm run bench:governor -- --record
```

The runner builds the production SIMD worker and controller, refuses unsupported
architectures, fails closed if either release gate fails, and records one of:

- `x86-resource-governor-slice14.json`;
- `m1-resource-governor-slice14.json`;
- `snapdragon-resource-governor-slice14.json`.

The M1 and Snapdragon artifacts must come from the named physical architecture;
unit tests that instantiate those profile ladders are correctness coverage, not
hardware evidence.

## Physical Obsidian protocol

Each pending row must use the same 1.11.1 plugin/server candidate and record:

1. exact device, OS, Obsidian, WebView/Electron, plugin commit, and server commit;
2. power mode, thermal state if exposed, cold/warm cache, network, and server
   filesystem;
3. selected family/profile, actual hash/read/network/apply limits, SIMD mode,
   transient budget, and recovery penalty from debug info;
4. at least three W1 runs and three W8 runs, with the first run marked cold;
5. throughput, wall time, event-loop p95, peak renderer memory where available,
   retries/backpressure, interruption/resume, and final full tree/content audit;
6. one contention run while Obsidian indexes the vault and one hidden/foreground
   transition on mobile.

Acceptance requires no content/root mismatch, no re-enrollment, no unbounded
memory growth, no editor starvation above the 16 ms p95 target, successful W8
resume after interruption, and adaptive median throughput no worse than 95% of
the conservative profile. Any failed row stays pending and blocks claims about
that hardware family.
