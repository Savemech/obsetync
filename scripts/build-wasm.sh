#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f Cargo.toml || ! -d crates/sync-core ]]; then
    echo "build-wasm.sh must run from the repository root" >&2
    exit 2
fi

wasm_output="${1:-plugin/wasm}"
wasm_opt="${WASM_OPT:-wasm-opt}"
if ! wasm_opt_path="$(command -v "$wasm_opt")"; then
    echo "wasm optimizer not found: $wasm_opt" >&2
    exit 2
fi
if [[ "$(basename "$wasm_opt_path")" != "wasm-opt" ]]; then
    echo "WASM_OPT must resolve to an executable named wasm-opt: $wasm_opt_path" >&2
    exit 2
fi
wasm_opt_version="$($wasm_opt_path --version 2>/dev/null || true)"
if [[ ! "$wasm_opt_version" =~ ^wasm-opt\ version\ ([0-9]+) ]] || (( BASH_REMATCH[1] < 117 )); then
    echo "wasm optimizer version 117 or newer required; found: ${wasm_opt_version:-unknown}" >&2
    exit 2
fi
wasm_opt="$wasm_opt_path"
wasm_tool_path="$(dirname "$wasm_opt"):$PATH"
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

# Universal fallback. Both packages use the same wasm-opt selected above; an
# explicit SIMD-disabled validation below prevents accidental v128 publication.
env PATH="$wasm_tool_path" wasm-pack build \
    --target web \
    --release \
    --out-dir "$scalar_output" \
    --out-name sync_core \
    crates/sync-core \
    -- \
    --target-dir "$wasm_target_root/scalar" \
    --features wasm \
    --no-default-features \
    --locked

# Fast path. The package metadata enables the SIMD parser, and wasm-pack uses
# the same pinned optimizer as the scalar build.
simd_rustflags="${RUSTFLAGS:+${RUSTFLAGS} }-C target-feature=+simd128"
env PATH="$wasm_tool_path" RUSTFLAGS="$simd_rustflags" wasm-pack build \
    --target web \
    --release \
    --out-dir "$simd_output" \
    --out-name sync_core_simd \
    crates/sync-core \
    -- \
    --target-dir "$wasm_target_root/simd" \
    --features wasm-simd \
    --no-default-features \
    --locked

"$wasm_opt" \
    --enable-bulk-memory \
    --enable-nontrapping-float-to-int \
    --enable-sign-ext \
    --enable-mutable-globals \
    --enable-reference-types \
    --disable-simd \
    "$scalar_output/sync_core_bg.wasm" \
    -o /dev/null

for artifact in sync_core.js sync_core.d.ts sync_core_bg.wasm sync_core_bg.wasm.d.ts; do
    install -m 0644 "$scalar_output/$artifact" "$wasm_output/$artifact"
done
for artifact in sync_core_simd.js sync_core_simd.d.ts sync_core_simd_bg.wasm sync_core_simd_bg.wasm.d.ts; do
    install -m 0644 "$simd_output/$artifact" "$wasm_output/$artifact"
done
