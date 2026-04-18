# ObsetyNC

[![CI](https://github.com/Savemech/obsetync/actions/workflows/ci.yml/badge.svg)](https://github.com/Savemech/obsetync/actions/workflows/ci.yml)

Self-hosted, end-to-end sync for [Obsidian](https://obsidian.md) vaults.

Your notes stay on your infrastructure. Desktop and iOS sync through your own server — no third-party cloud, no vendor lock-in.

## Features

- **Self-hosted** — the only server involved is yours
- **Cross-platform** — desktop (Windows, macOS, Linux) and iOS
- **Content-addressed storage** — blake3-hashed blobs, automatic deduplication
- **Incremental sync** — FastCDC chunking means a 1-byte edit to a 200 MB PDF uploads ~64 KB, not the whole file
- **Merkle tree index** — O(log n) diff, constant-time cached roots on reconnect
- **mTLS + bearer-token auth** — TLS client certs on desktop, bearer token on iOS (WKWebView can't present client certs from JavaScript)
- **Three-way merge** — server reconciles concurrent edits; conflicts preserved as copies instead of clobbered

## Architecture

| Piece              | Language      | Role                                                            |
|--------------------|---------------|-----------------------------------------------------------------|
| `sync-server`      | Rust (axum)   | HTTPS endpoint, content store, Merkle tree merge/diff           |
| `sync-core`        | Rust + WASM   | Hashing, FastCDC chunker, tree operations, wire (flatbuffers)   |
| `plugin/`          | TypeScript    | Obsidian plugin — orchestrates scan/hash/push/pull through WASM |
| `sync-schema`      | flatbuffers   | On-wire format for tree nodes                                   |

The plugin runs WASM in the Obsidian renderer. Blake3 hashing, FastCDC chunking, and tree updates happen in WASM; the TypeScript side handles I/O, HTTP, and the Obsidian API. Peak memory during a 10k-file scan stays bounded because files are streamed in 64 KB slices — the WASM heap never grows past one slice.

## Prerequisites

Just **Docker** with the Compose plugin. No Rust, no Node.js, no `wasm-pack` on your host — every build runs inside a container.

Optionally install [`just`](https://github.com/casey/just) for shorter commands (see [Using `just`](#using-just) below). The justfile is a thin wrapper over the same compose calls; both work.

## Build

Two output modes, pick whichever fits your deployment:

### Option 1 — clean artifacts (bare-metal / manual install)

Extract the server binary + plugin files to `./dist/`:

```sh
docker compose run --rm binary    # → ./dist/bin/sync-server
docker compose run --rm plugin    # → ./dist/plugin/{main.js,manifest.json,wasm/}

# or in one step:
just build-artifacts
```

Result:

```
dist/
├── bin/
│   └── sync-server           # stripped, LTO'd release binary
└── plugin/
    ├── main.js
    ├── manifest.json
    └── wasm/
```

Use this when you want to drop the binary onto a VPS with systemd, or copy the plugin files into an Obsidian vault manually.

### Option 2 — ready-to-run container image

```sh
docker compose build server       # → image obsetync/server:local
# or:
just build-image
```

Use this when you want to run the server via `docker compose up`. See [Run the server](#run-the-server).

Pre-built images are published on GitHub Container Registry on every tagged release:

```sh
docker pull ghcr.io/savemech/obsetync:latest         # Docker-built (standard)
docker pull ghcr.io/savemech/obsetync-nix:latest     # Nix-built (hermetic)
```

### Everything at once

```sh
docker compose --profile tools build    # all images + toolchain
just build                              # image AND artifacts
```

### Option 3 — hermetic build with Nix

For bit-for-bit reproducible builds (same inputs → identical outputs on any machine, now or in five years), there's a Nix flake alongside the Docker setup. Use it if you have [Nix installed](https://nixos.org/download) with flakes enabled.

```sh
# Everything pinned via flake.lock — Rust toolchain, nixpkgs, build inputs.
nix build .#server           # -> ./result/bin/sync-server
nix build .#wasm             # -> ./result/  (wasm-bindgen output)
nix build .#plugin           # -> ./result/  (main.js + manifest.json + wasm/)

nix flake check              # runs the full cargo test suite
nix develop                  # shell with the exact toolchain
nix run .#server -- run --data-dir ./data    # run the server directly
```

The `npmDepsHash` in `flake.nix` is pinned. If you ever bump `plugin/package-lock.json`, Nix will fail with the new correct hash — paste that into the `npmDepsHash = ...` line and rerun.

### Option 4 — Nix-built Docker image (hermetic + ready-to-run)

The best of both: Nix compiles the binary deterministically, then *builds the OCI image itself* (no Dockerfile). Same `flake.lock` → byte-identical image hash, loadable into any Docker or Podman.

```sh
just build-nix-image         # runs both steps below:
# nix build .#dockerImage    # -> ./result (OCI tarball)
# docker load < result       # -> image obsetync-server:nix

docker run --rm -v obsetync-data:/data -p 27182:27182 -p 127.0.0.1:27183:27183 \
    obsetync-server:nix

# Or swap it into compose by setting image: obsetync-server:nix on the server service.
```

This is the strongest guarantee: the *image itself* is reproducible. Two people running `nix build .#dockerImage` with the same `flake.lock` will get the same image digest.

Docker, Nix, and Nix-built Docker images are independent routes to the same functional artifacts. Pick the one that fits your environment.

## Run the server

State lives on the host at `./data/server/` — a plain directory you can back up, inspect, or `tar` around. No Docker named volumes anywhere.

```sh
# First-time: create CA, server cert, directory layout in ./data/server.
docker compose run --rm server init --data-dir /data

# Start / stop / logs / restart.
docker compose up -d
docker compose down
docker compose logs -f
docker compose restart

# Shell inside the running container (diagnostics).
docker compose exec server sh
```

`just` shortcuts: `just init`, `just up`, `just down`, `just logs`, `just restart`, `just shell`.

### Host directory layout

Everything is under `./data/` (gitignored):

```
./data/server/            persistent server state: ca/, devices/, vaults/, index/, content/
./data/cache/cargo-*/     dev shell: cargo dep cache (optional, speeds up rebuilds)
./data/cache/target/      dev shell: incremental compile cache (optional)
./dist/bin/sync-server    extracted binary (from `docker compose run --rm binary`)
./dist/plugin/            extracted plugin files (from `docker compose run --rm plugin`)
```

Wipe any of these freely. The server's `init` subcommand recreates `./data/server/` from scratch.

The server exposes two ports on the host:

- **27182** — sync API. Accepts mTLS client certs from desktop clients, bearer tokens from mobile. Expose this publicly (or via VPN) so your devices can reach it.
- **27183** — admin web UI. Bound to `127.0.0.1` only by default. Access it at `http://localhost:27183/admin`, or put it behind a reverse proxy / SSH tunnel / Tailscale for remote access.

Data is stored in the `obsetync-data` named volume.

## Installing the plugin in Obsidian

Three paths, pick whichever suits your platform:

### A. BRAT (recommended — works on desktop and iOS)

[BRAT](https://github.com/TfTHacker/obsidian42-brat) ("Beta Reviewers Auto-update Tester") is a community plugin that installs and auto-updates plugins directly from GitHub releases.

1. In Obsidian, open **Settings → Community plugins** and disable Restricted Mode if it's on.
2. **Browse**, search for *BRAT*, install it, and enable it.
3. Open **BRAT** settings → **Add Beta plugin** → paste `Savemech/obsetync` and confirm.
4. BRAT downloads `main.js`, `manifest.json`, and the two WASM files from our latest GitHub release into your vault.
5. Back in **Community plugins**, enable *ObsetyNC*.

Subsequent releases auto-update via BRAT — no further action needed.

### B. Manual install (no BRAT, fully offline after first download)

1. Go to the [Releases page](https://github.com/Savemech/obsetync/releases/latest) and download `obsetync-<version>.zip`.
2. Unzip it — you'll see an `obsetync/` folder containing `main.js`, `manifest.json`, `sync_core.js`, `sync_core_bg.wasm`.
3. Drop that `obsetync/` folder into `<your-vault>/.obsidian/plugins/`.
4. In Obsidian, **Settings → Community plugins** → disable Restricted Mode → refresh the list → enable *ObsetyNC*.

On **iOS**, the same flow works via the Files app:

1. Open **Safari** on your iPhone, download `obsetync-<version>.zip` from the Releases page.
2. Tap the downloaded file to open it in **Files**. iOS unzips it in place, giving you an `obsetync/` folder.
3. In Files, browse to **iCloud Drive → Obsidian → `<your vault>` → `.obsidian` → `plugins`** (create `plugins` if missing). If your vault is stored on-device instead of iCloud, navigate via **On My iPhone → Obsidian → `<your vault>`**.
4. Long-press the `obsetync/` folder you unzipped → **Move** → place it under `plugins/`.
5. Open Obsidian on iPhone, **Settings → Community plugins**, turn on Community plugins, enable *ObsetyNC*.

### After install (any path): enroll the device

1. Open the server admin UI (`http://<server>:27183/admin`), click **Add device**, name it, copy the enrollment code.
2. In Obsidian → **Settings → ObsetyNC**, fill in:
   - **Server URL** — e.g. `https://your-server:27182`
   - **Vault ID** — any name you like; use the same ID on every device that syncs the same vault
   - **Enrollment code** — paste it from the admin UI
3. Hit **Enroll**. First device pushes; later devices pull.

Repeat on each desktop + phone + tablet you want in the sync.

## Authentication

On enrollment the server issues three credentials:

- A client certificate + key (desktop uses these for mTLS at the TLS layer)
- A 256-bit random bearer token (sent as `Authorization: Bearer <token>` on every request)

The server authorizes based on the bearer token. Desktop additionally presents its client cert for defense in depth, but the token is the canonical check. On iOS the token is the sole auth because WKWebView can't attach client certs to JavaScript `fetch` or `requestUrl`.

## Optional: sync your `.obsidian/` folder

The plugin settings include a toggle to sync your `.obsidian/` directory alongside notes — themes, hotkeys, plugin settings, snippets. Off by default because plugin caches (e.g. the Omnisearch full-text index, which can be hundreds of MB) are included too and regenerate on every device anyway. Turn it on if you want a truly identical Obsidian experience across devices.

## Server data layout

```
<data volume>/
├── ca/              CA cert + key (trust anchor for client certs)
├── server/          server cert
├── devices/         enrolled devices + bearer-token index
├── enrollments/     pending enrollment codes (10-minute TTL)
├── vaults/          per-vault root pointers and root history
├── index/           Merkle tree chunks (leaf + internal nodes)
└── content/         content-addressed file storage
    ├── manifests/   per-file chunk manifests (for large files)
    └── chunks/      FastCDC chunks (for large files)
```

Small files (< 1 MB) go to `content/<hash>` whole. Large files are chunked via FastCDC; the manifest records chunk hashes and offsets. All storage is content-addressed — identical files across multiple paths use one physical blob.

## Development

```sh
# Interactive dev shell — source mounted at /build, full toolchain available.
docker compose run --rm dev

# Run the Rust test suite across the workspace.
docker compose run --rm test
```

`just` shortcuts: `just dev`, `just test`.

The dev container has Rust, Node, wasm-pack, cargo-watch, and `just` pre-installed. A cargo registry + target cache is stored in named volumes so incremental builds are fast.

## Maintenance

```sh
just clean-server   # wipe synced content (preserves certs + enrolled devices)
just clean-cache    # drop Docker BuildKit caches
just nuke           # remove all ObsetyNC images + volumes (fresh start)
```

## Using `just`

[`just`](https://github.com/casey/just) is an optional command runner — it reads the `justfile` at the repo root and exposes each recipe as a short command. Every recipe here is a one-liner over `docker compose`, so running the underlying compose command directly is equivalent.

**Install:**

```sh
brew install just                  # macOS
cargo install just --locked        # Linux / Windows / anywhere with Rust
# Ubuntu 24.04+:  apt install just
# Arch:           pacman -S just
```

**Core flags:**

```sh
just                       # same as `just --list` — show every recipe
just --list                # list recipes with their doc comments
just --show <recipe>       # print the body of a recipe without running it
just --dry-run <recipe>    # print the commands a recipe would run, don't run them
just --summary             # one-line list of recipe names
just --evaluate            # print all justfile variables and their values
just --choose              # interactive picker (requires fzf)
```

**Recipe map** — every recipe and its compose equivalent:

| `just` recipe        | `docker compose` equivalent                                |
|----------------------|------------------------------------------------------------|
| `just build`         | `docker compose build server` + `run --rm binary` + `run --rm plugin` |
| `just build-image`   | `docker compose build server`                              |
| `just build-artifacts`| `docker compose run --rm binary` + `docker compose run --rm plugin` |
| `just build-binary`  | `docker compose run --rm binary`                           |
| `just build-plugin`  | `docker compose run --rm plugin`                           |
| `just build-dev`     | `docker compose --profile tools build dev`                 |
| `just init`          | `docker compose run --rm server init --data-dir /data`     |
| `just up`            | `docker compose up -d server`                              |
| `just down`          | `docker compose down`                                      |
| `just logs`          | `docker compose logs -f server`                            |
| `just restart`       | `docker compose restart server`                            |
| `just shell`         | `docker compose exec server sh`                            |
| `just dev`           | `docker compose run --rm dev`                              |
| `just test`          | `docker compose run --rm test`                             |
| `just ship`          | *(personal: `build-artifacts` + rsync binary + copy plugin)* |
| `just clean-server`  | `docker compose exec server sh -c '…wipe vaults+index+content…'` + `restart` |
| `just clean-cache`   | `docker builder prune -af`                                 |
| `just nuke`          | `docker compose --profile tools down -v` + `docker rmi …` + `docker builder prune -af` |

**Environment overrides.** The justfile loads `.env` if present (`set dotenv-load := true`). Copy `.env.example` to `.env` — used only by `just ship` for rsync target and local vault path.

### `just ship` — personal deploy helper

If you want to build via Docker and ship the binary to a bare-metal server (no Docker on the target), set the three vars in `.env`:

```
OBSETYNC_SERVER=user@host        # ssh target
OBSETYNC_DEST=/opt/obsetync      # where the binary goes
OBSETYNC_VAULT=/path/to/vault/.obsidian/plugins/obsetync   # optional local copy
```

Then `just ship` does:

1. Build binary + plugin artifacts via Docker
2. rsync `dist/bin/sync-server` → `$OBSETYNC_SERVER:$OBSETYNC_DEST/`
3. `systemctl restart obsetync` on the remote (no-op if the unit isn't installed)
4. If `OBSETYNC_VAULT` is set, copy plugin files into it locally

Public users: ignore `just ship` — `just build` + `just up` is the full flow.


