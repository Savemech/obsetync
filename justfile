# ObsetyNC — thin wrapper around `docker compose`.
# Every recipe below is equivalent to a compose command; use compose directly
# if you prefer.
set dotenv-load := true

# --- optional .env overrides for `just ship` (personal rsync deploy) ---
# Leave these blank to disable `ship`; set them in .env to use it.
server := env_var_or_default("OBSETYNC_SERVER", "")
dest   := env_var_or_default("OBSETYNC_DEST",   "/opt/obsetync")
vault  := env_var_or_default("OBSETYNC_VAULT",  "")

# Default: list recipes.
default:
    @just --list

# --- build ----------------------------------------------------------------

# Option 1 + 2 combined: build image AND extract artifacts.
build: build-image build-artifacts

# Option 2: build the runtime server image (obsetync/server:local).
#   Use when you'll run the server via `docker compose up`.
build-image:
    docker compose build server

# Option 1: extract binary + plugin files to ./dist/.
#   Use when you want clean artifacts for bare-metal deploy or manual install.
build-artifacts: build-binary build-plugin

# Extract just the server binary to ./dist/bin/sync-server.
build-binary:
    docker compose run --rm binary

# Extract just the plugin files to ./dist/plugin/.
build-plugin:
    docker compose run --rm plugin

# Build the dev image (carries Rust + Node + wasm-pack toolchain).
build-dev:
    docker compose --profile tools build dev

# Option 3: build a fully hermetic Docker image via Nix, load into Docker.
# Same flake.lock → byte-identical image hash on any machine.
# Requires Nix with flakes enabled.
build-nix-image:
    nix build .#dockerImage
    docker load < result
    @echo "Loaded as obsetync-server:nix"

# Build the server binary via Nix (hermetic). Output: ./result/bin/sync-server
build-nix-binary:
    nix build .#server
    @echo "Binary at ./result/bin/sync-server"

# --- run ------------------------------------------------------------------

# First-run: create CA, server cert, directory layout inside the data volume.
init:
    docker compose run --rm server init --data-dir /data

# Start the server in the background.
up:
    docker compose up -d server

# Stop the server.
down:
    docker compose down

# Tail server logs.
logs:
    docker compose logs -f server

# Restart the server.
restart:
    docker compose restart server

# Shell inside the running server container (diagnostics).
shell:
    docker compose exec server sh

# --- dev ------------------------------------------------------------------

# Interactive dev shell with source mounted + full toolchain.
dev:
    docker compose run --rm dev

# Run the Rust test suite across the workspace.
test:
    docker compose run --rm test

# --- personal deploy (optional) -------------------------------------------

# Build fresh plugin + Nix Docker image, ship to both the remote server (via
# docker save/load) and the local Obsidian vault. Replaces the old binary +
# systemd flow — post-1.1.0 the server runs exclusively via docker compose.
#
# Requires OBSETYNC_SERVER and OBSETYNC_VAULT in .env (OBSETYNC_DEST defaults
# to /opt/obsetync).
ship: ship-plugin ship-server
    @echo "Shipped."

# Build plugin files and copy them into the local Obsidian vault.
ship-plugin: build-plugin
    @test -n "{{vault}}" || (echo "OBSETYNC_VAULT not set in .env — skipping plugin copy" && exit 0)
    @echo "→ copying plugin files to {{vault}}/"
    mkdir -p "{{vault}}"
    cp dist/plugin/main.js                 "{{vault}}/"
    cp dist/plugin/manifest.json           "{{vault}}/"
    cp dist/plugin/sync_core.js            "{{vault}}/"
    cp dist/plugin/sync_core_bg.wasm       "{{vault}}/"

# Build the Nix docker image, transfer it to the remote host, load + restart.
# Does NOT re-run `init` — the server's existing data dir + box keypair stay
# intact across deploys. Only the compose restarts to pick up the new image.
ship-server: build-nix-image
    @test -n "{{server}}" || (echo "OBSETYNC_SERVER not set in .env — skipping server ship" && exit 0)
    @echo "→ shipping Nix image to {{server}}"
    scp -q result {{server}}:/tmp/obsetync-nix.tar.gz
    @echo "→ shipping docker-compose.yml to {{server}}:{{dest}}"
    scp -q docker-compose.yml {{server}}:{{dest}}/docker-compose.yml
    ssh {{server}} "docker load < /tmp/obsetync-nix.tar.gz && docker tag obsetync-server:nix obsetync/server:local && docker tag obsetync-server:nix ghcr.io/savemech/obsetync-nix:1.1.9 && rm /tmp/obsetync-nix.tar.gz && cd {{dest}} && docker compose up -d"
    @echo "→ verifying /health"
    ssh {{server}} "curl -fsS http://127.0.0.1:27182/health"
    @echo

# --- maintenance ----------------------------------------------------------

# Wipe synced vault content on the server. Preserves certs + enrolled devices.
clean-server:
    @echo "This will erase all synced vault data on the server. Ctrl-C to abort."
    @sleep 3
    docker compose exec server sh -c " \
        rm -rf /data/vaults /data/index /data/content && \
        mkdir -p /data/vaults /data/index /data/content/manifests /data/content/chunks"
    docker compose restart server
    @echo "Server state wiped."

# Drop Docker BuildKit caches.
clean-cache:
    docker builder prune -af

# Nuke everything — images, volumes, caches. Fresh start next `just build`.
nuke:
    -docker compose --profile tools down -v
    -docker rmi obsetync/server:local obsetync/plugin-builder:local obsetync/dev:local obsetync/rust-builder:local 2>/dev/null
    -docker builder prune -af
