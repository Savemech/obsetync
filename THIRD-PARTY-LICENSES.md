# Third-party licenses

ObsetyNC's own source code is licensed under the PolyForm Noncommercial License
1.0.0 (see `LICENSE`). The project additionally bundles third-party open-source
components, each distributed under its own **permissive** license. Those
components remain governed by their original licenses, reproduced or referenced
below. None of the bundled runtime components are copyleft (no GPL/LGPL/AGPL).

The authoritative, exhaustive dependency lists are the lockfiles committed to
this repository:

- JavaScript/TypeScript: `plugin/package-lock.json`
- Rust: `Cargo.lock`

## Shipped in the Obsidian plugin (`main.js`)

The plugin bundle inlines the following runtime dependencies:

| Component        | License | Copyright / Project |
| ---------------- | ------- | ------------------- |
| @noble/curves    | MIT     | © Paul Miller (paulmillr.com) |
| @noble/hashes    | MIT     | © Paul Miller (paulmillr.com) |

The Obsidian API (`obsidian`), Electron, and CodeMirror packages are marked
`external` and are **not** bundled — they are provided by the Obsidian runtime.

## Shipped in the WebAssembly core (`sync_core_bg.wasm`, inlined into `main.js`)

Compiled from `crates/sync-core` and its dependencies:

| Component                  | License |
| -------------------------- | ------- |
| blake3                     | CC0-1.0 OR Apache-2.0 OR Apache-2.0-with-LLVM-exception |
| serde, serde_json          | MIT OR Apache-2.0 |
| thiserror                  | MIT OR Apache-2.0 |
| hex                        | MIT OR Apache-2.0 |
| flatbuffers                | Apache-2.0 |
| similar                    | Apache-2.0 |
| fastcdc                    | MIT |
| async-trait                | MIT OR Apache-2.0 |
| wasm-bindgen, js-sys       | MIT OR Apache-2.0 |
| serde-wasm-bindgen         | MIT OR Apache-2.0 |
| console_error_panic_hook   | MIT OR Apache-2.0 |

## Shipped in the self-hosted server binary (`sync-server`)

The server binary (attached to GitHub releases; not distributed through the
Obsidian store) links additional permissive-licensed Rust crates — notably
`axum`, `tokio`, `hyper`, `rustls`, and `aws-lc-rs`/`aws-lc-sys`
(MIT / Apache-2.0 / ISC / OpenSSL-style). See `Cargo.lock` for the complete set.

## Apache-2.0 NOTICE

Some components above are under the Apache License 2.0. Redistributions must
retain the applicable copyright, patent, trademark, and attribution notices from
those components' source, per Section 4 of the Apache-2.0 license. No component
ships a separate `NOTICE` file that must be reproduced; where one is added
upstream, it will be mirrored here.
