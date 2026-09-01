#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f Cargo.toml || ! -d crates/sync-core ]]; then
    echo "build-wasm.sh must run from the repository root" >&2
    exit 2
fi

wasm_output="${1:-plugin/wasm}"
if [[ "$wasm_output" != /* ]]; then
    wasm_output="$PWD/$wasm_output"
fi
mkdir -p "$wasm_output"

# Universal fallback. Its wasm-opt profile deliberately does not enable SIMD,
# so accidentally introducing a v128 instruction fails the build.
wasm-pack build crates/sync-core \
    --target web \
    --release \
    --out-dir "$wasm_output" \
    --out-name sync_core \
    -- \
    --features wasm \
    --no-default-features

# Fast path. wasm-pack optimization is skipped here because the universal
# package profile rejects SIMD by design; optimize explicitly with SIMD enabled.
simd_rustflags="${RUSTFLAGS:+${RUSTFLAGS} }-C target-feature=+simd128"
env RUSTFLAGS="$simd_rustflags" wasm-pack build crates/sync-core \
    --target web \
    --release \
    --no-opt \
    --out-dir "$wasm_output" \
    --out-name sync_core_simd \
    -- \
    --features wasm-simd \
    --no-default-features

wasm-opt -O \
    --enable-bulk-memory \
    --enable-nontrapping-float-to-int \
    --enable-sign-ext \
    --enable-mutable-globals \
    --enable-reference-types \
    --enable-simd \
    "$wasm_output/sync_core_simd_bg.wasm" \
    -o "$wasm_output/sync_core_simd_bg.wasm.opt"
mv "$wasm_output/sync_core_simd_bg.wasm.opt" "$wasm_output/sync_core_simd_bg.wasm"
