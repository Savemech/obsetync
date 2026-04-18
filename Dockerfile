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
FROM rust:1.95-bookworm AS rust-builder

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
# wasm-pack — fetches pre-built binary from rustwasm releases
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        clang \
        cmake \
        perl \
        git \
        ca-certificates \
        curl \
        && rm -rf /var/lib/apt/lists/*

# Pin wasm-pack for reproducibility.
ENV WASM_PACK_VERSION=0.13.1
RUN curl -sSfL https://github.com/rustwasm/wasm-pack/releases/download/v${WASM_PACK_VERSION}/wasm-pack-v${WASM_PACK_VERSION}-x86_64-unknown-linux-musl.tar.gz \
    | tar -xz -C /usr/local/bin --strip-components=1 \
        wasm-pack-v${WASM_PACK_VERSION}-x86_64-unknown-linux-musl/wasm-pack

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

# Stub out the actual crate sources so `cargo fetch` can parse everything
# without needing the real code. We throw these stubs away before the real build.
RUN mkdir -p crates/sync-core/src crates/sync-schema/src crates/sync-server/src && \
    echo "fn main() {}" > crates/sync-server/src/main.rs && \
    echo ""            > crates/sync-core/src/lib.rs && \
    echo ""            > crates/sync-schema/src/lib.rs

RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    cargo fetch --locked

# ------------------------------------------------------------------------------
# Real source + real build:
# Bring in the actual crate sources and compile.
# ------------------------------------------------------------------------------
COPY crates ./crates

# Overwrite the stubs so cargo recompiles with the real code.
RUN find crates -name '*.rs' -exec touch {} +

# Server: release build, symbols stripped (via profile.release in Cargo.toml).
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=server-target,target=/build/target,sharing=locked \
    CC=clang cargo build --release --locked -p sync-server && \
    mkdir -p /out && \
    cp /build/target/release/sync-server /out/sync-server && \
    chmod 0755 /out/sync-server

# WASM module for the Obsidian plugin.
# Output goes to /build/plugin/wasm which becomes part of the image layer.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=wasm-target,target=/build/target,sharing=locked \
    wasm-pack build crates/sync-core \
        --target web \
        --out-dir /build/plugin/wasm \
        --release \
        -- --features wasm --no-default-features


# ------------------------------------------------------------------------------
# Stage: plugin-builder
# Bundles the TypeScript plugin with esbuild.
# ------------------------------------------------------------------------------
FROM node:20-bookworm-slim AS plugin-builder

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
COPY plugin/tsconfig.json plugin/esbuild.config.mjs plugin/manifest.json ./
COPY plugin/src ./src
COPY --from=rust-builder /build/plugin/wasm ./wasm

RUN node esbuild.config.mjs production


# ------------------------------------------------------------------------------
# Stage: server (runtime image — this is what you ship / run)
# Tiny Debian base with just the server binary and CA certs.
# Runs as a non-root user. Data directory must be mounted at /data.
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim AS server

LABEL org.opencontainers.image.title="obsetync-server" \
      org.opencontainers.image.description="Self-hosted Obsidian vault sync server"

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
# All flat — drop the whole folder into a vault's .obsidian/plugins/obsetync/.
# ------------------------------------------------------------------------------
FROM scratch AS plugin-dist

COPY --from=plugin-builder /build/plugin/main.js                  /main.js
COPY --from=plugin-builder /build/plugin/manifest.json            /manifest.json
COPY --from=plugin-builder /build/plugin/wasm/sync_core.js        /sync_core.js
COPY --from=plugin-builder /build/plugin/wasm/sync_core_bg.wasm   /sync_core_bg.wasm


# ------------------------------------------------------------------------------
# Stage: binary-dist (scratch, artifact-only)
# Extract just the server binary to the host for bare-metal deployment:
#   docker build --target binary-dist -o type=local,dest=./dist/bin .
# produces:
#   ./dist/bin/sync-server
# ------------------------------------------------------------------------------
FROM scratch AS binary-dist

COPY --from=rust-builder /out/sync-server /sync-server
