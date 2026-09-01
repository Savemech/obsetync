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

# Cargo can otherwise reuse a dependency artifact compiled with +simd128 when
# the two feature sets are built back-to-back. wasm-pack also optimizes every
# generated module in its output directory, so a pre-existing SIMD module makes
# the scalar validator fail. Keep both Cargo graphs and package outputs apart.
wasm_target_root="${CARGO_TARGET_DIR:-$PWD/target}/wasm-pack"
scalar_output="$wasm_target_root/pkg-scalar"
simd_output="$wasm_target_root/pkg-simd"
mkdir -p "$scalar_output" "$simd_output"

# Universal fallback. Its wasm-opt profile deliberately does not enable SIMD,
# so accidentally introducing a v128 instruction fails the build.
wasm-pack build \
    --target web \
    --release \
    --out-dir "$scalar_output" \
    --out-name sync_core \
    crates/sync-core \
    -- \
    --target-dir "$wasm_target_root/scalar" \
    --features wasm \
    --no-default-features

# Fast path. wasm-pack optimization is skipped here because the universal
# package profile rejects SIMD by design; optimize explicitly with SIMD enabled.
simd_rustflags="${RUSTFLAGS:+${RUSTFLAGS} }-C target-feature=+simd128"
env RUSTFLAGS="$simd_rustflags" wasm-pack build \
    --target web \
    --release \
    --no-opt \
    --out-dir "$simd_output" \
    --out-name sync_core_simd \
    crates/sync-core \
    -- \
    --target-dir "$wasm_target_root/simd" \
    --features wasm-simd \
    --no-default-features

wasm-opt -O \
    --enable-bulk-memory \
    --enable-nontrapping-float-to-int \
    --enable-sign-ext \
    --enable-mutable-globals \
    --enable-reference-types \
    --enable-simd \
    "$simd_output/sync_core_simd_bg.wasm" \
    -o "$simd_output/sync_core_simd_bg.wasm.opt"
mv "$simd_output/sync_core_simd_bg.wasm.opt" "$simd_output/sync_core_simd_bg.wasm"

for artifact in sync_core.js sync_core.d.ts sync_core_bg.wasm sync_core_bg.wasm.d.ts; do
    install -m 0644 "$scalar_output/$artifact" "$wasm_output/$artifact"
done
for artifact in sync_core_simd.js sync_core_simd.d.ts sync_core_simd_bg.wasm sync_core_simd_bg.wasm.d.ts; do
    install -m 0644 "$simd_output/$artifact" "$wasm_output/$artifact"
done
