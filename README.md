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
- **End-to-end encrypted transport** — X25519 ECDH + HKDF-SHA256 + AES-256-GCM wrap every request body. No TLS, no CA, no cert install ceremony. The server's X25519 public key is pinned at enrollment; every request is sealed end-to-end with a fresh ephemeral client keypair and a bearer token buried inside the AEAD envelope. Full protocol at [`docs/transport.md`](docs/transport.md)
- **Three-way merge** — server reconciles concurrent edits; conflicts preserved as copies instead of clobbered

## Architecture

| Piece              | Language      | Role                                                            |
|--------------------|---------------|-----------------------------------------------------------------|
| `sync-server`      | Rust (axum)   | HTTP endpoint, content store, Merkle tree merge/diff            |
| `sync-core`        | Rust + WASM   | Hashing, FastCDC chunker, tree operations, wire (flatbuffers)   |
| `plugin/`          | TypeScript    | Obsidian plugin — orchestrates scan/hash/push/pull through WASM |
| `sync-schema`      | flatbuffers   | On-wire format for tree nodes                                   |

The plugin runs WASM in the Obsidian renderer. Blake3 hashing, FastCDC chunking, and tree operations happen in WASM; the TypeScript side handles I/O, HTTP, the Obsidian API, and X25519 ECDH via [`@noble/curves`](https://github.com/paulmillr/noble-curves) (so iOS and older Chromium, which don't ship X25519 in WebCrypto yet, still work). Peak memory during a 10k-file scan stays bounded because files are streamed in 64 KB slices — the WASM heap never grows past one slice.

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

docker run --rm -v ./data:/data -p 27182:27182 -p 27183:27183 \
    obsetync-server:nix

# Or swap it into compose by setting image: obsetync-server:nix on the server service.
```

This is the strongest guarantee: the *image itself* is reproducible. Two people running `nix build .#dockerImage` with the same `flake.lock` will get the same image digest.

Docker, Nix, and Nix-built Docker images are independent routes to the same functional artifacts. Pick the one that fits your environment.

## Run the server

State lives on the host at `./data/server/` — a plain directory you can back up, inspect, or `tar` around. No Docker named volumes anywhere.

```sh
# First-time: create the X25519 box keypair + directory layout in ./data/.
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

After `init`, the base64 of the server's public box key is printed to stdout and stored at `data/server/box.pub`. Clients learn it automatically during enrollment — no manual copy needed.

### Logging

Structured tracing via the standard `RUST_LOG` env var. The container default is `sync_server=debug,warn` so you get per-request detail out of the box. Quieter:

```sh
RUST_LOG=sync_server=info docker compose up -d
```

Keys in logs: `device`, `vault`, `method`, `path`, `in_body`, `out_body`, `elapsed_ms` for each request; `put_root: first push accepted / fast-forward accepted / merged divergent roots` for tree updates; enrollment / revocation events at info.

### Host directory layout

Everything is under `./data/` (gitignored):

```
./data/server/box.key     X25519 private key (mode 0600)
./data/server/box.pub     X25519 public key (base64, operator-inspectable)
./data/devices/           enrolled devices + bearer-token index
./data/enrollments/       pending enrollment codes (10-minute TTL)
./data/vaults/            per-vault root pointers and root history
./data/index/             Merkle tree chunks (leaf + internal nodes)
./data/content/           content-addressed file storage (blobs + manifests + chunks)
./data/cache/cargo-*/     dev shell: cargo dep cache (optional, speeds up rebuilds)
./data/cache/target/      dev shell: incremental compile cache (optional)
./dist/bin/sync-server    extracted binary (from `docker compose run --rm binary`)
./dist/plugin/            extracted plugin files (from `docker compose run --rm plugin`)
```

Wipe any of these freely. The server's `init` subcommand recreates the layout from scratch. *But* — see [Reconcile with server](#reconcile-with-server) below before wiping `content/` while devices still hold a local sync-base: it silently causes drift.

The server exposes two ports on the host:

- **27182** — sync API. Plain HTTP; the AEAD envelope is the trust boundary. Expose this publicly (or behind a VPN) so devices can reach it.
- **27183** — admin web UI. Dashboard with uptime, per-device online status, per-vault storage stats, device enrollment. No auth of its own — put it behind localhost / Tailscale / VPN / reverse proxy.

## Installing the plugin in Obsidian

Pick whichever suits your platform:

### A. Community plugins (once approved)

Once ObsetyNC is accepted into the Obsidian Community Plugins directory:

1. In Obsidian, open **Settings → Community plugins** and disable Restricted Mode if it's on.
2. **Browse**, search for *ObsetyNC*, click **Install**, then **Enable**.

Obsidian keeps it up to date automatically. Until the directory listing lands, use BRAT or a manual install below.

### B. BRAT (works today on desktop and iOS)

[BRAT](https://github.com/TfTHacker/obsidian42-brat) ("Beta Reviewers Auto-update Tester") is a community plugin that installs and auto-updates plugins directly from GitHub releases.

1. In Obsidian, open **Settings → Community plugins** and disable Restricted Mode if it's on.
2. **Browse**, search for *BRAT*, install it, and enable it.
3. Open **BRAT** settings → **Add Beta plugin** → paste `Savemech/obsetync` and confirm.
4. BRAT downloads `main.js`, `manifest.json`, and the two WASM files from our latest GitHub release into your vault.
5. Back in **Community plugins**, enable *ObsetyNC*.

Subsequent releases auto-update via BRAT — no further action needed.

### C. Manual install (no BRAT, fully offline after first download)

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
   - **Server URL** — `http://your-server:27182` (plain HTTP; the AEAD envelope encrypts the payload itself, so HTTPS is unnecessary and actively wrong)
   - **Vault ID** — any name you like; use the same ID on every device that syncs the same vault
   - **Enrollment code** — paste it from the admin UI
3. Hit **Enroll**. First device bulk-pushes its vault; every later device does a first-sync pull (downloads the vault from the server) — progress shown in the status bar + notices.

Repeat on each desktop + phone + tablet you want in the sync.

## Authentication & transport

Sync traffic is sealed inside an **AEAD envelope** carried in plain HTTP bodies: X25519 ECDH + HKDF-SHA256 + AES-256-GCM. No TLS, no CA, no client certs. Clients pin the server's long-term X25519 public key at enrollment (`data/server/box.pub`, base64). Each plugin session generates a fresh ephemeral X25519 keypair; each request carries its own 12-byte nonce + AAD-bound HTTP method and path.

The **bearer token** lives inside the encrypted plaintext (first 64 ASCII hex chars), not in a header — so packet captures can't even tell which device is talking. Revoking a device drops its bearer token from the server index; the next request 401s immediately.

**Full protocol specification**: [`docs/transport.md`](docs/transport.md) — byte-level wire format, key schedule, threat model, code map, and a worked example of a request/response round-trip.

## Reconcile with server

The client maintains a local `sync-base.json` that caches "what hashes the server has". If the server storage is wiped or restored from an older backup, that cache lies — sync would silently say "in sync" while the server is actually missing content. **Settings → ObsetyNC → Reconcile with server** runs a `checkContent` / `checkContentChunks` sweep across every entry in sync-base and re-uploads anything the server is missing. Cheap when the server is in parity (one batched request); corrective when it isn't. This also runs automatically on every **Sync Now**.

## Optional: sync your `.obsidian/` folder

The plugin settings include a toggle to sync your `.obsidian/` directory alongside notes — themes, hotkeys, plugin settings, snippets. Off by default because plugin caches (e.g. the Omnisearch full-text index, which can be hundreds of MB) are included too and regenerate on every device anyway. Turn it on if you want a truly identical Obsidian experience across devices.

## Server data layout

```
<data dir>/
├── server/
│   ├── box.key     X25519 private key (mode 0600) — the server's long-term identity
│   └── box.pub     X25519 public key (base64) — safe to display; clients pin it
├── devices/        enrolled devices + bearer-token index
├── enrollments/    pending enrollment codes (10-minute TTL)
├── vaults/         per-vault root pointers and root history
├── index/          Merkle tree chunks (leaf + internal nodes)
└── content/        content-addressed file storage
    ├── manifests/  per-file chunk manifests (large files ≥ 1 MB)
    └── chunks/     FastCDC chunks (large files)
```

Small files (< 1 MB) go to `content/<hash>` whole. Large files are chunked via FastCDC; the manifest records chunk hashes and offsets. All storage is content-addressed — identical files across multiple paths use one physical blob. Compromise of `box.key` lets an attacker impersonate the server going forward but does **not** reveal past session content (forward secrecy via per-session ephemeral client keys).

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
just clean-server   # wipe synced content (preserves server/box.key + enrolled devices)
just clean-cache    # drop Docker BuildKit caches
just nuke           # remove all ObsetyNC images + volumes (fresh start)
```

**After `clean-server` (or any time the server loses data while clients still hold a sync-base)**: every enrolled device should run **Settings → ObsetyNC → Reconcile with server** once. That uploads every file the server is missing. Without it, `Sync Now` will say "Already up to date" while the server silently holds only the Merkle tree, no content.

## Troubleshooting

**"In sync" but the server clearly has nothing** — the client's `sync-base.json` cache is lying. Use **Reconcile with server**.

**`ERR_SSL_PROTOCOL_ERROR` from the plugin** — you saved the Server URL as `https://…`. The sync port is plaintext HTTP; the plugin auto-migrates stored URLs on load, but if you typed a fresh one manually, use `http://`. The AEAD envelope is the trust boundary — TLS would be double-encryption of the same bytes.

**iPhone loops on `GET /api/v1/root/...` forever** — the plugin WASM module failed to load (check *Show debug info* → recent logs for `WASM load failed`). That drops the plugin to a stub that can't hash. Usually caused by an older iOS/WebKit missing a WASM feature; upgrade iOS first.

**`getContent ...: 400` on first sync** — was a real bug in ≤1.1.10 (delta hashes serialized as JSON number arrays instead of hex strings). Upgrade to 1.1.11+ on BOTH the server and every plugin.

**Admin dashboard says `(empty)` next to a vault that should have data** — the vault dir exists but no root was ever pushed successfully. Check `docker logs obsetync-server` during the client's next Sync Now for `put_root:` events. If none appear, the push itself is failing (decrypt error, bearer rejected, etc — the log will say).

**`Last error: [pull] getDiff failed: 404`** — the server has no root for this vault yet. This is harmless on a first-sync-to-an-empty-server (the client treats 404 as empty delta). If it persists across syncs, the server lost its `vaults/<id>/current_root` pointer — check file permissions under `data/vaults/`.

**Tailing live traffic with structured fields:**

```sh
ssh <server> 'docker logs -f obsetync-server' | grep -E 'put_root|post_diff|unauthorized'
```

**Finding a specific device's activity:**

```sh
ssh <server> 'docker logs obsetync-server' | grep 'device=<first 12 hex chars>'
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

Builds a hermetic Nix Docker image locally, transfers it to a remote server via `docker save | ssh | docker load`, copies the current `docker-compose.yml` to the remote, tags the loaded image as `obsetync/server:local`, and runs `docker compose up -d` + `/health` verification. Also copies fresh plugin files to a local Obsidian vault if configured.

Set the three vars in `.env` (gitignored):

```
OBSETYNC_SERVER=user@host        # ssh target
OBSETYNC_DEST=/opt/obsetync      # where docker-compose.yml + data/ live on the remote
OBSETYNC_VAULT=/path/to/vault/.obsidian/plugins/obsetync   # optional local copy
```

Then `just ship` does:

1. `nix build .#dockerImage` — hermetic OCI image
2. `scp result $OBSETYNC_SERVER:/tmp/obsetync-nix.tar.gz`
3. `scp docker-compose.yml $OBSETYNC_SERVER:$OBSETYNC_DEST/`
4. `ssh` — `docker load`, tag as both `obsetync/server:local` and `ghcr.io/savemech/obsetync-nix:<version>`, `docker compose up -d`
5. `curl /health` to verify the new image is serving
6. If `OBSETYNC_VAULT` is set: rebuild plugin artifacts and copy `main.js` + `manifest.json` + `sync_core_bg.wasm` into that vault

Public users: ignore `just ship` — `just build` + `just up` is the full flow.

## License

ObsetyNC's own source code is licensed under the **[PolyForm Noncommercial License 1.0.0](LICENSE)**.

- **Free for any noncommercial use** — personal vaults, hobby projects, study, and use by charitable, educational, or government organizations. You may run it, modify it, and redistribute it for noncommercial purposes.
- **Commercial use is reserved to the author.** Using ObsetyNC for a commercial purpose requires a separate commercial license. Contact **Anton Strukov (Savemech)** — savemech@gmail.com.

The plugin and server bundle third-party open-source components under their own permissive licenses (MIT / Apache-2.0 / ISC / BSD / 0BSD); see **[THIRD-PARTY-LICENSES.md](THIRD-PARTY-LICENSES.md)**. There are no ads and no in-app payments — the plugin is free to use noncommercially and requires only a server you host yourself.


