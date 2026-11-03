# ObsetyNC

[![CI](https://github.com/Savemech/obsetync/actions/workflows/ci.yml/badge.svg)](https://github.com/Savemech/obsetync/actions/workflows/ci.yml)

Self-hosted sync for [Obsidian](https://obsidian.md) vaults with an authenticated, encrypted client/server transport.

Your notes stay on your infrastructure. Desktop and iOS sync through your own server — no third-party cloud, no vendor lock-in.

## Features

- **Self-hosted** — the only server involved is yours
- **Cross-platform** — desktop (Windows, macOS, Linux) and iOS
- **Content-addressed storage** — blake3-hashed blobs, automatic deduplication
- **Incremental sync** — FastCDC chunking means a small edit to a 200 MB binary usually uploads about one content-defined chunk, not the whole file
- **Merkle tree index** — compare subtree hashes and reuse cached roots on reconnect
- **Application-layer encrypted transport** — X25519 ECDH + HKDF-SHA256 + AES-256-GCM wrap every request body. No TLS, no CA, no cert install ceremony. The server's long-term key is pinned at enrollment; each plugin channel combines it with a rotating memory-only server key, and the bearer stays inside the AEAD envelope. Full protocol at [`docs/transport.md`](docs/transport.md)
- **Three-way merge** — server reconciles concurrent edits; conflicts preserved as copies instead of clobbered

## Architecture

| Piece              | Language      | Role                                                            |
|--------------------|---------------|-----------------------------------------------------------------|
| `sync-server`      | Rust (axum)   | HTTP endpoint, content store, Merkle tree merge/diff            |
| `sync-core`        | Rust + WASM   | Hashing, FastCDC chunker, tree operations, wire (flatbuffers)   |
| `plugin/`          | TypeScript    | Obsidian plugin — orchestrates scan/hash/push/pull through WASM |
| `sync-schema`      | flatbuffers   | On-wire format for tree nodes                                   |

The plugin runs WASM in the Obsidian renderer. Blake3 hashing, FastCDC chunking, and tree operations happen in WASM; the TypeScript side handles I/O, HTTP, the Obsidian API, and X25519 ECDH via [`@noble/curves`](https://github.com/paulmillr/noble-curves). Desktop hashing uses a bounded pool of up to four workers, each opening its file directly and running an independent SIMD-WASM instance; only hash/manifest metadata crosses back to the renderer. Feeds adapt between 64 KiB and 1 MiB. Authenticated bulk HTTP collapses checks, uploads, and downloads into strict ordered binary packs capped at 256 objects and 8 MiB on desktop or 2 MiB on mobile; old peers and objects above the per-record cap keep using the stable single-object API. New peers can carry those same packs over a separately sealed, credit-bounded WebSocket data lane with multiplexed request IDs; realtime notifications stay on their own socket, and every interrupted RPC falls back to bulk HTTP/content-address checks without re-enrollment. A bounded dedicated server writer commits each accepted object group to an append-only segment with one `fdatasync`. Sealed segments use sorted checksummed sidecar indexes, while startup verifies only the bounded active tail. Reads recheck CRC32C and BLAKE3, corruption is hidden for repair, and a resumable background importer removes a loose copy only after a verified durable pack read. Obsidian's mobile adapter has no ranged reads, so large local uploads require one whole-file JS buffer (capped at 128 MiB to fail visibly instead of inviting an iOS Jetsam restart); large downloads/restores are staged and verified one chunk at a time.

## Prerequisites

Just **Docker** with the Compose plugin. No Rust, no Node.js, no `wasm-pack` on your host — every build runs inside a container.

Optionally install [`just`](https://github.com/casey/just) for shorter commands (see [Using `just`](#using-just) below). Basic builds wrap Compose; release deployment also uses Nix, identity checks and rollback.

## Build

Choose artifacts or a runnable image, built with Docker or Nix:

### Option 1 — clean artifacts (bare-metal / manual install)

Extract the server binary + plugin files to `./dist/`:

```sh
docker compose run --build --rm binary    # → ./dist/bin/sync-server
docker compose run --build --rm plugin    # → ./dist/plugin/ (seven flat files)

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
    ├── styles.css
    ├── sync_core.js
    ├── sync_core_bg.wasm
    ├── sync_core_simd.js
    └── sync_core_simd_bg.wasm
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

The Nix flake pins the toolchain and dependencies. Reproducing an artifact also requires the same source revision, build parameters and target platform. Use it if you have [Nix installed](https://nixos.org/download) with flakes enabled.

```sh
# Everything pinned via flake.lock — Rust toolchain, nixpkgs, build inputs.
nix build .#server           # -> ./result/bin/sync-server
nix build .#wasm             # -> ./result/  (scalar + SIMD wasm-bindgen output)
nix build .#plugin           # -> ./result/  (the same seven flat plugin files)

nix flake check              # runs the full cargo test suite
nix develop                  # shell with the exact toolchain
nix run .#server -- run --data-dir ./data    # run the server directly
```

The `npmDepsHash` in `flake.nix` is pinned. If you ever bump `plugin/package-lock.json`, Nix will fail with the new correct hash — paste that into the `npmDepsHash = ...` line and rerun.

### Option 4 — Nix-built Docker image (hermetic + ready-to-run)

Nix compiles the binary and builds the OCI image itself without a Dockerfile. The image carries the source commit and version, and can be loaded into Docker or Podman on the matching target architecture.

```sh
just build-nix-image         # runs both steps below:
# nix build .#dockerImage    # -> ./result (OCI tarball)
# docker load < result       # -> image obsetync-server:nix

docker run --rm -v ./data:/data -p 27182:27182 -p 27183:27183 \
    obsetync-server:nix

# Or swap it into compose by setting image: obsetync-server:nix on the server service.
```

The image fixes its creation timestamp; source, lockfile, build identity and target platform are all inputs to its digest.

Docker, Nix, and Nix-built Docker images are independent routes to the same functional artifacts. Pick the one that fits your environment.

## Run the server

State lives on the host at `./data/`; `./data/server/` contains the server identity and short-lived connection tickets. Stop the server before taking a filesystem backup of the complete data directory. The production Compose service uses a bind mount.

On Unix, `init` and server startup set `server/`, `server/ws-tickets/`, `devices/`, `devices/tokens/` and `enrollments/` to owner-only access (0700), including existing installations. Run the server as the owner of these directories and preserve that ownership when restoring a backup. Symlinked credential directories are rejected before startup; keys and registrations are preserved. Windows directory access remains controlled by host ACLs.

```sh
# First-time: create the X25519 box keypair + directory layout in ./data/.
docker compose run --rm server init --data-dir /data

# Start / stop / logs / restart.
docker compose up -d
docker compose down
docker compose logs -f
docker compose restart

# Shell in the Dockerfile image (the Nix image has no shell).
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
./data/objects-v1/        primary packed objects and checksummed segment indexes
./data/index/             legacy loose Merkle tree chunks (imported into packs)
./data/content/           legacy loose blobs, manifests and chunks (imported into packs)
./data/storage-writer/    legacy acknowledged groups and migration state
./data/crdt/              legacy live-edit logs, if present
./data/config.json        initialization metadata (legacy fields remain readable)
./data/cache/cargo-*/     dev shell: cargo dep cache (optional, speeds up rebuilds)
./data/cache/target/      dev shell: incremental compile cache (optional)
./dist/bin/sync-server    extracted binary (from `docker compose run --build --rm binary`)
./dist/plugin/            extracted plugin files (from `docker compose run --build --rm plugin`)
```

Build caches and extracted artifacts can be regenerated. Server keys, device registrations, roots and stored content require a backup; `init` cannot recover them. For an intentional vault reset that preserves identity and enrollment, see [Maintenance](#maintenance).

`--data-dir` selects the storage directory, and `--sync-port` / `--admin-port`
override the default ports. Old port and password fields in `config.json` are
ignored; the admin UI has no built-in password authentication.

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
4. BRAT installs the plugin from our latest GitHub release into your vault; the production bundle embeds its WASM modules.
5. Back in **Community plugins**, enable *ObsetyNC*.

Subsequent releases auto-update via BRAT — no further action needed.

### C. Manual install (no BRAT, fully offline after first download)

1. Go to the [Releases page](https://github.com/Savemech/obsetync/releases/latest) and download `obsetync-<version>.zip`.
2. Unzip it — you'll see an `obsetync/` folder containing `main.js`, `manifest.json`, `styles.css`, and scalar (`sync_core*`) plus SIMD (`sync_core_simd*`) WASM artifacts. ObsetyNC validates SIMD at runtime and automatically falls back to scalar on older WebViews.
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
   - **Server URL** — `http://your-server:27182` (the direct sync port speaks HTTP; the AEAD envelope encrypts its payload)
   - **Vault ID** — a simple ASCII ID such as `personal-notes`; use the same ID on every device that syncs the vault
   - **Enrollment code** — paste it from the admin UI
3. Hit **Enroll**. First device bulk-pushes its vault; every later device does a first-sync pull (downloads the vault from the server) — progress shown in the status bar + notices.

Repeat on each desktop + phone + tablet you want in the sync.

## Authentication & transport

Sync traffic is sealed inside an **AEAD envelope** carried in plain HTTP bodies: X25519 ECDH + HKDF-SHA256 + AES-256-GCM. No TLS, no CA, no client certs. Clients pin the server's long-term X25519 public key at enrollment (`data/server/box.pub`, base64). Each plugin session generates a fresh ephemeral X25519 keypair; each request carries its own 12-byte nonce + AAD-bound HTTP method and path.

The **bearer token** lives inside the encrypted plaintext, not in a header — so packet captures can't tell which device is talking from credentials. Revocation is checked immediately after decryption; the next opened request receives an encrypted semantic 403.

**Full protocol specification**: [`docs/transport.md`](docs/transport.md) — byte-level wire format, key schedule, threat model, code map, and a worked example of a request/response round-trip.

## Reconcile with server

The client keeps its last synchronized file states in `sync-base.store-v1/` under the plugin directory; older `sync-base.json` files are migration inputs. If server storage is wiped, partially restored, or corrupted, those local records no longer prove that the content still exists remotely. **Settings → ObsetyNC → Reconcile with server** checks Merkle index nodes, small blobs, and large-file manifests in bounded batches; malformed manifests or missing referenced chunks are treated as absent, then local bytes are re-chunked and only missing/corrupt content is uploaded. The parity path transfers metadata only. **Sync Now** also reconciles content when it can start a sync cycle.

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
├── objects-v1/
│   └── segments/   primary .pack objects and sealed .idx indexes
├── storage-writer/ legacy group journal and migration state
├── index/          legacy loose Merkle tree chunks
├── content/        legacy loose content-addressed storage
│   ├── manifests/  per-file chunk manifests
│   └── chunks/     FastCDC chunks
├── crdt/           legacy live-edit logs, if present
└── config.json     initialization metadata
```

The writer stores new blobs, manifests, content chunks and Merkle index chunks in packs. Small files are whole blobs; larger files use FastCDC chunks and a manifest of hashes and offsets. Loose objects and the old writer journal remain recovery sources until migrated. All storage is content-addressed — identical files across multiple paths share content. Ordinary wire-v2 traffic has window-bounded forward secrecy from the rotating in-memory server key: after the applicable current/previous slot is gone, theft of `box.key` alone cannot open that captured traffic. Bootstrap traffic and the other limits are spelled out in [`docs/transport.md`](docs/transport.md).

## Development

```sh
# Interactive dev shell — source mounted at /build, full toolchain available.
docker compose run --rm dev

# Run the Rust test suite across the workspace.
docker compose run --rm test
```

`just` shortcuts: `just dev`, `just test`.

The dev container has Rust, Node, wasm-pack, Binaryen, cargo-watch, and `just` pre-installed. Docker and CI share the checksum-pinned WASM tool installer. Cargo registry, Git and target caches are bind-mounted from `./data/cache/`.

## Maintenance

```sh
just clean-server   # stop, erase all vault storage, restart; preserves identity and devices
just clean-cache    # drop Docker BuildKit caches
just nuke           # remove Docker-built images and BuildKit caches; ./data survives
```

`clean-server` uses the server's `clear-vault-data` command, so it works with Docker and shell-free Nix images. It removes roots, packed and loose objects, legacy writer recovery state and CRDT logs. Keys, configuration, device registrations and replay windows remain. A cleanup failure leaves the server stopped. The CLI command must only run while the server is stopped.

**After `clean-server` or a server restore**: run **Settings → ObsetyNC → Reconcile with server** on devices that hold the missing files. **Sync Now** also performs this check during a new sync cycle. Recovery can upload content still available locally; it cannot recover files erased from every copy.

## Troubleshooting

**"In sync" but server content is missing** — matching root hashes do not prove every stored object survived. Use **Reconcile with server** to check and repair content from local files.

**`ERR_SSL_PROTOCOL_ERROR` from the plugin** — the direct sync port expects `http://…`, not `https://…`. Use the address shown by the server. Payload encryption is provided by the AEAD envelope.

**WASM initialization fails** — startup stops before syncing. The plugin tries bundled SIMD and scalar modules; it never uses incompatible hashing stubs on a real vault. Check **Show debug info** for the failing initialization step and build identity.

**Obsidian disappears or restarts during a long iPad sync** — reopen it, then use **Settings → ObsetyNC → Show debug info**. “Previous interruption” reports the last durable phase/file-count/byte checkpoint even when iOS Jetsam killed the renderer before JavaScript could log an exception. Large downloads resume from their verified staging checkpoint; repeated interruption at a local upload above 128 MiB means that file must be uploaded from desktop or excluded.

**`getContent ...: 400` on first sync** — was a real bug in ≤1.1.10 (delta hashes serialized as JSON number arrays instead of hex strings). Upgrade to 1.1.11+ on BOTH the server and every plugin.

**Admin dashboard says `(empty)` next to a vault that should have data** — the vault dir exists but no root was ever pushed successfully. Check `docker logs obsetync-server` during the client's next Sync Now for `put_root:` events. If none appear, the push itself is failing (decrypt error, bearer rejected, etc — the log will say).

**`Last error: [pull] getDiff failed: 404`** — the server has no root for this vault yet. This can occur before its first successful publication. If it persists for an existing vault, inspect server logs and permissions for `vaults/<id>/current`, the authoritative root and publication-receipt file.

**Tailing live traffic with structured fields:**

```sh
ssh <server> 'docker logs -f obsetync-server' | grep -E 'put_root|post_diff|unauthorized'
```

**Finding a specific device's activity:**

```sh
ssh <server> 'docker logs obsetync-server' | grep 'device=<first 12 hex chars>'
```

## Using `just`

[`just`](https://github.com/casey/just) reads the `justfile` at the repo root and exposes each workflow as a short command. Basic recipes wrap Compose; `ship` coordinates clean release builds, verification and rollback across Docker, Nix and SSH.

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

**Common recipes** and the commands or workflows they run:

| `just` recipe        | Command or workflow                                       |
|----------------------|------------------------------------------------------------|
| `just build`         | `docker compose build server` + `run --build --rm binary` + `run --build --rm plugin` |
| `just build-image`   | `docker compose build server`                              |
| `just build-artifacts`| `docker compose run --build --rm binary` + `docker compose run --build --rm plugin` |
| `just build-binary`  | `docker compose run --build --rm binary`                   |
| `just build-plugin`  | `docker compose run --build --rm plugin`                   |
| `just build-dev`     | `docker compose --profile tools build dev`                 |
| `just init`          | `docker compose run --rm server init --data-dir /data`     |
| `just up`            | `docker compose up -d server`                              |
| `just down`          | `docker compose down`                                      |
| `just logs`          | `docker compose logs -f server`                            |
| `just restart`       | `docker compose restart server`                            |
| `just shell`         | `docker compose exec server sh`                            |
| `just dev`           | `docker compose run --rm dev`                              |
| `just test`          | `docker compose run --rm test`                             |
| `just ship`          | *(personal: strict release build + rollback-safe server deploy + local plugin install)* |
| `just clean-server`  | `stop server` → one-shot `clear-vault-data` → `up -d server` |
| `just clean-cache`   | `docker builder prune -af`                                 |
| `just nuke`          | `docker compose --profile tools down -v` + `docker rmi …` + `docker builder prune -af` |

**Environment overrides.** The justfile loads `.env` if present (`set dotenv-load := true`). Copy `.env.example` to `.env`; `just ship` uses it for the SSH target, remote Compose directory, and local plugin directory.

### `just ship` — personal deploy helper

Requires a clean Git commit and builds both the plugin and hermetic Nix server image with its exact version and commit identity. It validates both destinations before changing either one, deploys and health-checks the server first, and installs the plugin last. The remote deploy keeps the previous Compose file and image in `/backup/obsetync-deploy/` and automatically rolls back if the new container does not become healthy or does not use the expected image.

Set the three vars in `.env` (gitignored):

```
OBSETYNC_SERVER=user@host        # ssh target
OBSETYNC_DEST=/opt/obsetync      # where docker-compose.yml + data/ live on the remote
OBSETYNC_VAULT=/path/to/vault/.obsidian/plugins/obsetync
```

Then `just ship` does:

1. Prove a clean release commit and consistent version declarations.
2. Build and verify the plugin and Nix OCI image before changing either destination.
3. Upload the image, staged Compose file, and remote deploy helper.
4. Save the previous server image and Compose file under `/backup/obsetync-deploy/`.
5. Start the commit-qualified image, wait for container health, and verify its image ID; roll back on failure.
6. Atomically replace individual plugin artifacts, with `main.js` installed last as the activation marker.

Public users: ignore `just ship` — `just build` + `just up` is the full flow.

## License

ObsetyNC's own source code is licensed under the **[PolyForm Noncommercial License 1.0.0](LICENSE)**.

- **Free for any noncommercial use** — personal vaults, hobby projects, study, and use by charitable, educational, or government organizations. You may run it, modify it, and redistribute it for noncommercial purposes.
- **Commercial use is reserved to the author.** Using ObsetyNC for a commercial purpose requires a separate commercial license. Contact **Anton Strukov (Savemech)** — savemech@gmail.com.

The plugin and server bundle third-party open-source components under their own permissive licenses (MIT / Apache-2.0 / ISC / BSD / 0BSD); see **[THIRD-PARTY-LICENSES.md](THIRD-PARTY-LICENSES.md)**. There are no ads and no in-app payments — the plugin is free to use noncommercially and requires only a server you host yourself.
