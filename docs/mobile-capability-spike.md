# Browser/mobile capability spike (PR04, partial)

Status: opt-in synthetic diagnostic implemented; native-device results pending.
No production browser worker, ranged mobile reader, or automatic resource-limit
increase is enabled by this diagnostic.

## Run safely

Use a separately approved test build in a test vault. Keep Obsidian visible and
wait for the current sync operation to finish. In plugin settings choose
**Run synthetic test**, or run the command **Test browser worker capabilities**.
The result opens in a copyable debug modal and appears in subsequent debug info
for the current plugin session. It is not persisted or sent anywhere automatically.

The probe itself reads/writes no vault files, makes no network request, and does
not use resource URLs. It constructs one classic Blob worker, sends one synthetic
64 KiB fixture and private copies of the embedded WASM modules, then terminates
the worker and revokes the Blob URL. Normal sync may start after the initial idle
check; this is a capability test, not an isolated throughput benchmark.

There is at most one concurrent probe, no automatic retry, and a five-second
deadline including memory-admission wait. Visibility loss, pagehide and unload
request cancellation. An uncertain dispatched job or unconfirmed shutdown fences
another attempt until plugin reload. This fence limits repeated allocations;
reload is not claimed to prove native heap collection.

## What is measured

- Worker construction/startup under the actual host CSP, independently of main
  renderer WASM availability.
- Actual scalar initialization and a fixed expected hash; independent SIMD
  validation, initialization and the same hash if SIMD is supported.
- Transfer ownership/detachment of all three buffers in both directions, with
  fixture integrity and original main-renderer module buffers left intact.
- Fixed result codes, duration, reported linear-memory sizes, cooperative result,
  shutdown request and URL-revocation evidence. No file paths or raw host errors.

`PASSED` may mean scalar-only when SIMD validation reports unsupported. It does
not enable a sync execution path. Reported WASM linear memory is neither total
worker memory nor process RSS. The temporary scheduling reservation estimates
JS/module/Blob bridge buffers; retained/native heaps are not fully admitted.

The browser API provides a shutdown request, not a native exit/heap-release
acknowledgement. A timeout ends the diagnostic reservation with residual lifetime
explicitly unconfirmed; it must not be used as proof of full-stage budget coverage.
Likewise transferable buffers establish ownership semantics, not a universal
physical zero-copy guarantee. See the [HTML worker lifecycle](https://html.spec.whatwg.org/multipage/workers.html#dom-worker)
and [transferable-object specification](https://html.spec.whatwg.org/multipage/structured-data.html#transferable-objects).

## Host and reader decisions

`Platform.isMobile/isDesktop` describe UI mode, while `isMobileApp/isDesktopApp`
identify the app host. Production IO/worker policy now uses app-host flags; unknown
or contradictory flags receive the conservative mobile policy. A desktop UI on
a tablet is not evidence of Node access. See the [official Obsidian API declarations](https://github.com/obsidianmd/obsidian-api/blob/master/obsidian.d.ts).

The installed declarations expose whole-file `readBinary`, not a cancellable
ranged reader. `getResourcePath` promises a browser/embed URI, not Fetch, Range
or bounded native allocation. `appendBinary` is runtime-capability checked; its
presence in newer declarations does not make it available on every supported app.
Therefore this spike deliberately leaves resource-URL reads disabled.

A future reader experiment needs explicit fixture-write consent: creating a vault
fixture is observable by other plugins/watchers even if this plugin ignores it.
Its HTTP Range test must require 206, exact Content-Range, expected bytes and a
bounded stream; reject 200 before consuming the body, never call whole-body
`arrayBuffer()` as fallback. Even a correct response cannot prove the native host
avoided reading the full source. This contract follows [HTTP Range semantics](https://www.rfc-editor.org/rfc/rfc9110.html#name-range)
and still needs native memory measurements.

## Verification and remaining gate

Pure fake-host regressions cover lifecycle, protocol, corruption, transfer failure,
constructor/CSP-like denial, cancellation and admission timeout. A separate Node
worker bridge executes the real browser-targeted bundle and generated scalar/SIMD
glue in development and minified forms. It rejects network/imported assets and
checks expected hashes plus buffer ownership. This is packaging/protocol evidence,
not an actual browser CSP or Obsidian integration test.

CI has a separate fresh-WASM → parity → browser-probe → production-bundle job;
release preflight runs the same browser-bundle test. Pure plugin tests remain
independent of generated WASM. The new remote workflow has not yet run.

| Native host | Synthetic probe | Sustained memory/UI and background transitions |
| --- | --- | --- |
| iPhone 16 Pro Max / iOS | Not run | Not run |
| iPad / iPadOS | Not run | Not run |
| Android Obsidian | Not run | Not run |
| macOS Obsidian | Not run | Not run |
| Windows Obsidian | Not run | Not run |

Record app/plugin/OS versions, report, foreground/hidden transition and system
memory/termination evidence for each device. Passing this small probe only opens
the next experiment: a bounded worker pool with cooperative source feeds, known
heap ownership, cancellation and sustained input-to-paint testing. It does not
close PR04/05 or establish an absence of Jetsam/reloads.
