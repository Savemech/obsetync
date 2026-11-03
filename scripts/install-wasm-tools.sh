#!/usr/bin/env bash
set -euo pipefail

# Linux tools shared by Docker, the dev image and GitHub Actions.
tool_arch="${1:-$(uname -m)}"
tool_dest="${2:-/usr/local/bin}"
binaryen_version=117
wasm_pack_version=0.13.1
case "$tool_arch" in
    amd64|x86_64)
        tool_arch=x86_64
        binaryen_sha=3dc677006555b355ea2da5e82602065a161d5e83eaefd3f759afa00b96e83212
        wasm_pack_sha=c539d91ccab2591a7e975bcf82c82e1911b03335c80aa83d67ad25ed2ad06539
        ;;
    arm64|aarch64)
        tool_arch=aarch64
        binaryen_sha=ad560204426015a815faa45693c83bef7d58677d38a39422c272a30ba4b6da2a
        wasm_pack_sha=2e65038769f8bbaa5fc237ad4bb523e692df99458cbd3e3d92525b89d8762379
        ;;
    *) echo "unsupported WASM tool architecture: $tool_arch" >&2; exit 2 ;;
esac

tool_stage=$(mktemp -d)
trap 'rm -rf -- "$tool_stage"' EXIT

curl -sSfL "https://github.com/WebAssembly/binaryen/releases/download/version_${binaryen_version}/binaryen-version_${binaryen_version}-${tool_arch}-linux.tar.gz" \
    -o "$tool_stage/binaryen.tar.gz"
echo "$binaryen_sha  $tool_stage/binaryen.tar.gz" | sha256sum -c -
curl -sSfL "https://github.com/rustwasm/wasm-pack/releases/download/v${wasm_pack_version}/wasm-pack-v${wasm_pack_version}-${tool_arch}-unknown-linux-musl.tar.gz" \
    -o "$tool_stage/wasm-pack.tar.gz"
echo "$wasm_pack_sha  $tool_stage/wasm-pack.tar.gz" | sha256sum -c -

# Verify both archives before extracting or replacing either installed tool.
tar -xz -C "$tool_stage" --strip-components=2 -f "$tool_stage/binaryen.tar.gz" \
    "binaryen-version_${binaryen_version}/bin/wasm-opt"
tar -xz -C "$tool_stage" --strip-components=1 -f "$tool_stage/wasm-pack.tar.gz" \
    "wasm-pack-v${wasm_pack_version}-${tool_arch}-unknown-linux-musl/wasm-pack"
install -d "$tool_dest"
install -m 0755 "$tool_stage/wasm-opt" "$tool_stage/wasm-pack" "$tool_dest/"
