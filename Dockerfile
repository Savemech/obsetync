# syntax=docker/dockerfile:1.7
# ==============================================================================
# ObsetyNC — production multi-stage build.
#
# Build the server runtime image:
#   docker build --target server -t obsetync/server:local .
#
# Extract plugin artifacts (main.js + manifest.json + wasm/) to ./dist/plugin:
#   docker build --target plugin-dist -o type=local,dest=./dist/plugin .
#
# Or use the justfile wrappers: `just build-server`, `just build-plugin`.
# ==============================================================================


# ------------------------------------------------------------------------------
# Stage: rust-builder
# Compiles `sync-server` (native) and `sync-core` (wasm32-unknown-unknown).
# Uses BuildKit cache mounts so repeated builds don't refetch crates or redo
# unchanged dependency compilation.
# ------------------------------------------------------------------------------
ARG OBSETYNC_BUILD_GIT_COMMIT=unknown
ARG OBSETYNC_BUILD_SOURCE_STATE=local-unknown
ARG OBSETYNC_BUILD_EXPECTED_COMMIT=
ARG OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT=0
ARG OBSETYNC_BUILD_REQUIRE_CLEAN=0
ARG OBSETYNC_BUILD_EXPECTED_VERSION=
ARG TARGETARCH

FROM rust:1.95-bookworm AS rust-builder

ARG OBSETYNC_BUILD_GIT_COMMIT
ARG OBSETYNC_BUILD_SOURCE_STATE
ARG OBSETYNC_BUILD_EXPECTED_COMMIT
ARG OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT
ARG OBSETYNC_BUILD_REQUIRE_CLEAN
ARG OBSETYNC_BUILD_EXPECTED_VERSION
ARG TARGETARCH

ENV CARGO_TERM_COLOR=never \
    CARGO_TERM_PROGRESS_WHEN=never \
    CARGO_NET_RETRY=5 \
    RUSTFLAGS="--remap-path-prefix=/build=. --remap-path-prefix=/usr/local/cargo=/cargo" \
    SOURCE_DATE_EPOCH=1

# System deps:
#   clang     — aws-lc-sys (rustls backend) needs a modern C compiler
#   cmake     — aws-lc-sys build
#   perl      — aws-lc-sys build (OpenSSL-style scripts)
#   git       — cargo fetches some deps via git
# wasm-pack + Binaryen — pinned pre-built release tools for reproducible WASM
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        clang \
        cmake \
        perl \
        git \
        ca-certificates \
        curl \
        && rm -rf /var/lib/apt/lists/*

# Pin wasm-pack and its compatible optimizer for reproducibility.
ENV WASM_PACK_VERSION=0.13.1 \
    BINARYEN_VERSION=117
# BuildKit supplies TARGETARCH. If a classic/default builder leaves it empty,
# select tools for the architecture that is actually executing this stage.
# A non-empty override still goes through the strict allowlist below.
RUN tool_arch="${TARGETARCH}"; \
    if [ -z "${tool_arch}" ]; then \
        tool_arch="$(dpkg --print-architecture)"; \
    fi; \
    case "${tool_arch}" in \
        amd64) \
            binaryen_arch=x86_64; \
            binaryen_sha=3dc677006555b355ea2da5e82602065a161d5e83eaefd3f759afa00b96e83212; \
            wasm_pack_arch=x86_64; \
            wasm_pack_sha=c539d91ccab2591a7e975bcf82c82e1911b03335c80aa83d67ad25ed2ad06539 ;; \
        arm64) \
            binaryen_arch=aarch64; \
            binaryen_sha=ad560204426015a815faa45693c83bef7d58677d38a39422c272a30ba4b6da2a; \
            wasm_pack_arch=aarch64; \
            wasm_pack_sha=2e65038769f8bbaa5fc237ad4bb523e692df99458cbd3e3d92525b89d8762379 ;; \
        *) echo "unsupported Docker build TARGETARCH/host architecture: ${tool_arch:-<empty>}" >&2; exit 2 ;; \
    esac && \
    curl -sSfL "https://github.com/WebAssembly/binaryen/releases/download/version_${BINARYEN_VERSION}/binaryen-version_${BINARYEN_VERSION}-${binaryen_arch}-linux.tar.gz" \
        -o /tmp/binaryen.tar.gz && \
    echo "${binaryen_sha}  /tmp/binaryen.tar.gz" | sha256sum -c - && \
    tar -xz -C /usr/local/bin --strip-components=2 -f /tmp/binaryen.tar.gz \
        "binaryen-version_${BINARYEN_VERSION}/bin/wasm-opt" && \
    curl -sSfL "https://github.com/rustwasm/wasm-pack/releases/download/v${WASM_PACK_VERSION}/wasm-pack-v${WASM_PACK_VERSION}-${wasm_pack_arch}-unknown-linux-musl.tar.gz" \
        -o /tmp/wasm-pack.tar.gz && \
    echo "${wasm_pack_sha}  /tmp/wasm-pack.tar.gz" | sha256sum -c - && \
    tar -xz -C /usr/local/bin --strip-components=1 -f /tmp/wasm-pack.tar.gz \
        "wasm-pack-v${WASM_PACK_VERSION}-${wasm_pack_arch}-unknown-linux-musl/wasm-pack" && \
    rm -f /tmp/binaryen.tar.gz /tmp/wasm-pack.tar.gz

# Add WASM target (also declared in rust-toolchain.toml so this is idempotent).
RUN rustup target add wasm32-unknown-unknown

WORKDIR /build

# ------------------------------------------------------------------------------
# Dependency pre-fetch pass:
# Copy only manifests so BuildKit can cache this layer across source changes.
# After this step, `cargo fetch` has populated the registry; rebuilds after a
# source edit skip the download entirely.
# ------------------------------------------------------------------------------
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/sync-core/Cargo.toml   crates/sync-core/Cargo.toml
COPY crates/sync-schema/Cargo.toml crates/sync-schema/Cargo.toml
COPY crates/sync-server/Cargo.toml crates/sync-server/Cargo.toml
# perf-harness is another workspace member. It is not built into the runtime,
# but Cargo still needs its manifest and a target during the dependency-only
# resolver pass.
COPY crates/perf-harness/Cargo.toml crates/perf-harness/Cargo.toml
# e2e-tests is a workspace member but unused at runtime; cargo's manifest
# resolver still demands the Cargo.toml exist during the pre-fetch pass.
COPY crates/e2e-tests/Cargo.toml   crates/e2e-tests/Cargo.toml

# Stub out the actual crate sources so `cargo fetch` can parse everything
# without needing the real code. We throw these stubs away before the real build.
RUN mkdir -p crates/sync-core/src crates/sync-schema/src crates/sync-server/src crates/perf-harness/src crates/e2e-tests/src && \
    echo "fn main() {}" > crates/sync-server/src/main.rs && \
    echo ""            > crates/sync-core/src/lib.rs && \
    echo ""            > crates/sync-schema/src/lib.rs && \
    echo "fn main() {}" > crates/perf-harness/src/main.rs && \
    echo ""            > crates/e2e-tests/src/lib.rs

RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    cargo fetch --locked

# ------------------------------------------------------------------------------
# Real source + real build:
# Bring in the actual crate sources and compile.
# ------------------------------------------------------------------------------
COPY crates ./crates
COPY scripts ./scripts

# Overwrite the stubs so cargo recompiles with the real code.
RUN find crates -name '*.rs' -exec touch {} +

# Server: release build, symbols stripped (via profile.release in Cargo.toml).
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=server-target,target=/build/target,sharing=locked \
    CC=clang \
    OBSETYNC_BUILD_GIT_COMMIT="${OBSETYNC_BUILD_GIT_COMMIT}" \
    OBSETYNC_BUILD_SOURCE_STATE="${OBSETYNC_BUILD_SOURCE_STATE}" \
    OBSETYNC_BUILD_EXPECTED_COMMIT="${OBSETYNC_BUILD_EXPECTED_COMMIT}" \
    OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT="${OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT}" \
    OBSETYNC_BUILD_REQUIRE_CLEAN="${OBSETYNC_BUILD_REQUIRE_CLEAN}" \
    OBSETYNC_BUILD_EXPECTED_VERSION="${OBSETYNC_BUILD_EXPECTED_VERSION}" \
    cargo build --release --locked -p sync-server && \
    mkdir -p /out && \
    cp /build/target/release/sync-server /out/sync-server && \
    chmod 0755 /out/sync-server

# Universal scalar + SIMD WASM modules for the Obsidian plugin. The plugin
# validates SIMD at runtime and falls back to scalar on older WebViews.
# Output goes to /build/plugin/wasm which becomes part of the image layer.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=wasm-target,target=/build/target,sharing=locked \
    bash scripts/build-wasm.sh /build/plugin/wasm


# ------------------------------------------------------------------------------
# Stage: plugin-builder
# Bundles the TypeScript plugin with esbuild.
# ------------------------------------------------------------------------------
FROM node:20-bookworm-slim AS plugin-builder

ARG OBSETYNC_BUILD_GIT_COMMIT
ARG OBSETYNC_BUILD_SOURCE_STATE
ARG OBSETYNC_BUILD_EXPECTED_COMMIT
ARG OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT
ARG OBSETYNC_BUILD_REQUIRE_CLEAN
ARG OBSETYNC_BUILD_EXPECTED_VERSION

# Note: we deliberately don't set NODE_ENV=production here — esbuild + TS are
# in devDependencies and npm ci with NODE_ENV=production would skip them.
# The `production` mode of our build is driven by the argv to esbuild.config.mjs.
ENV NPM_CONFIG_UPDATE_NOTIFIER=false \
    NPM_CONFIG_FUND=false \
    NPM_CONFIG_AUDIT=false

WORKDIR /build/plugin

# Install dependencies from lockfile for reproducibility.
COPY plugin/package.json plugin/package-lock.json ./
RUN --mount=type=cache,id=npm-cache,target=/root/.npm \
    npm ci --no-audit --no-fund

# Bring in sources + the WASM produced by the rust-builder stage.
COPY plugin/tsconfig.json plugin/esbuild.config.mjs plugin/manifest.json plugin/styles.css ./
COPY plugin/scripts/build-identity-config.mjs ./scripts/build-identity-config.mjs
COPY plugin/src ./src
COPY --from=rust-builder /build/plugin/wasm ./wasm

# The helper narrows this exception to commit=unknown + source_state=local-unknown.
# Exact release SHA/version/clean requirements are still passed through unchanged.
RUN OBSETYNC_BUILD_GIT_COMMIT="${OBSETYNC_BUILD_GIT_COMMIT}" \
    OBSETYNC_BUILD_SOURCE_STATE="${OBSETYNC_BUILD_SOURCE_STATE}" \
    OBSETYNC_BUILD_EXPECTED_COMMIT="${OBSETYNC_BUILD_EXPECTED_COMMIT}" \
    OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT="${OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT}" \
    OBSETYNC_BUILD_REQUIRE_CLEAN="${OBSETYNC_BUILD_REQUIRE_CLEAN}" \
    OBSETYNC_BUILD_EXPECTED_VERSION="${OBSETYNC_BUILD_EXPECTED_VERSION}" \
    OBSETYNC_BUILD_ALLOW_LOCAL_UNKNOWN=1 \
    node esbuild.config.mjs production


# ------------------------------------------------------------------------------
# Stage: server (runtime image — this is what you ship / run)
# Tiny Debian base with just the server binary and CA certs.
# Runs as a non-root user. Data directory must be mounted at /data.
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim AS server

ARG OBSETYNC_BUILD_GIT_COMMIT
ARG OBSETYNC_BUILD_SOURCE_STATE
ARG OBSETYNC_BUILD_EXPECTED_VERSION

LABEL org.opencontainers.image.title="obsetync-server" \
      org.opencontainers.image.description="Self-hosted Obsidian vault sync server" \
      org.opencontainers.image.version="${OBSETYNC_BUILD_EXPECTED_VERSION}" \
      org.opencontainers.image.revision="${OBSETYNC_BUILD_GIT_COMMIT}" \
      org.opencontainers.image.source-state="${OBSETYNC_BUILD_SOURCE_STATE}" \
      org.opencontainers.image.obsetync-protocol="api-v1;transport-v2;tree-v1-v2;ws-data-v1-v2;root-outcome-v1"

# Minimal runtime deps.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=rust-builder /out/sync-server /usr/local/bin/sync-server

# Data directory — bind mount from the host via docker-compose. Writable by
# whatever UID runs the container (no USER directive = root inside). Container
# isolation comes from cap_drop: ALL + read_only + no-new-privileges in compose,
# not from Linux user separation, so host-side file ownership matches your user.
RUN mkdir -p /data
WORKDIR /data

# 27182 sync API (mTLS + bearer token)
# 27183 admin UI (plain HTTP — put behind reverse proxy / VPN)
EXPOSE 27182 27183

ENTRYPOINT ["/usr/local/bin/sync-server"]
CMD ["run", "--data-dir", "/data"]


# ------------------------------------------------------------------------------
# Stage: plugin-dist (scratch, artifact-only)
# Extract plugin files to the host via `docker build -o`:
#   docker build --target plugin-dist -o type=local,dest=./dist/plugin .
# produces:
#   ./dist/plugin/main.js
#   ./dist/plugin/manifest.json
#   ./dist/plugin/sync_core.js
#   ./dist/plugin/sync_core_bg.wasm
#   ./dist/plugin/sync_core_simd.js
#   ./dist/plugin/sync_core_simd_bg.wasm
# All flat — drop the whole folder into a vault's .obsidian/plugins/obsetync/.
# ------------------------------------------------------------------------------
FROM scratch AS plugin-dist

COPY --from=plugin-builder /build/plugin/main.js                  /main.js
COPY --from=plugin-builder /build/plugin/manifest.json            /manifest.json
COPY --from=plugin-builder /build/plugin/styles.css               /styles.css
COPY --from=plugin-builder /build/plugin/wasm/sync_core.js        /sync_core.js
COPY --from=plugin-builder /build/plugin/wasm/sync_core_bg.wasm   /sync_core_bg.wasm
COPY --from=plugin-builder /build/plugin/wasm/sync_core_simd.js   /sync_core_simd.js
COPY --from=plugin-builder /build/plugin/wasm/sync_core_simd_bg.wasm /sync_core_simd_bg.wasm


# ------------------------------------------------------------------------------
# Stage: binary-dist (scratch, artifact-only)
# Extract just the server binary to the host for bare-metal deployment:
#   docker build --target binary-dist -o type=local,dest=./dist/bin .
# produces:
#   ./dist/bin/sync-server
# ------------------------------------------------------------------------------
FROM scratch AS binary-dist

COPY --from=rust-builder /out/sync-server /sync-server
