# ObsetyNC — thin wrapper around `docker compose`.
# Every recipe below is equivalent to a compose command; use compose directly
# if you prefer.
set dotenv-load := true

# --- optional .env overrides for `just ship` (personal rsync deploy) ---
# Leave these blank to disable `ship`; set them in .env to use it.
server := env_var_or_default("OBSETYNC_SERVER", "")
dest   := env_var_or_default("OBSETYNC_DEST",   "/opt/obsetync")
vault  := env_var_or_default("OBSETYNC_VAULT",  "")

# Release identity is pinned once when just loads this file. Every release
# build and deploy recipe in the same invocation therefore uses one exact HEAD.
release_version := `node -p "require('./manifest.json').version"`
release_commit  := `git rev-parse --verify HEAD`

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
# --build: the binary is baked into the image at build time, so rebuild it from
# current source before running or `run` ships whatever the last image had.
build-binary:
    docker compose run --build --rm binary

# Extract just the plugin files to ./dist/plugin/.
# --build: main.js/manifest.json are compiled INTO the plugin-builder image, so
# without a rebuild `run` copies stale artifacts (e.g. an old version) to dist/.
build-plugin:
    docker compose run --build --rm plugin

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

# Build a release plugin + Nix Docker image from one clean commit, then ship to
# both the remote server and the local Obsidian vault. All target checks happen
# before either destination is changed.
#
# Requires OBSETYNC_SERVER and OBSETYNC_VAULT in .env (OBSETYNC_DEST defaults
# to /opt/obsetync).
# Build and verify both artifacts before changing either destination. Update the
# server first so an older client never has to serve a newer wire contract.
ship: ship-preflight build-release-plugin build-release-nix-image ship-server ship-plugin
    @echo "Shipped."

# Fail before building or copying when release identity cannot be proven.
release-preflight:
    @commit="{{release_commit}}"; \
     test "${#commit}" -eq 40 || { \
        echo "release HEAD is not a full lowercase Git commit" >&2; exit 2; \
     }; \
     case "$commit" in \
        *[!0-9a-f]*) echo "release HEAD is not a full lowercase Git commit" >&2; exit 2 ;; \
     esac
    @test "$(git rev-parse --verify HEAD)" = "{{release_commit}}" || \
        (echo "release HEAD changed after just started" >&2; exit 2)
    @test -z "$(git status --porcelain --untracked-files=no)" || \
        (echo "release checkout has tracked modifications" >&2; exit 2)
    @test -z "$(git ls-files --others --exclude-standard -- \
        Cargo.toml Cargo.lock Dockerfile docker-compose.yml flake.nix flake.lock \
        justfile manifest.json versions.json crates plugin scripts)" || \
        (echo "release checkout has untracked build inputs" >&2; exit 2)
    @cmp -s manifest.json plugin/manifest.json || \
        (echo "root and plugin manifests differ" >&2; exit 2)
    @OBSETYNC_RELEASE_EVENT_NAME=push \
     OBSETYNC_RELEASE_REF_NAME="{{release_version}}" \
     node scripts/check-release-version.mjs
    @echo "Release identity: {{release_version}} @ {{release_commit}} (clean)"

# Validate all destinations without changing them. Shared dependencies are run
# only once when `just ship` executes the complete graph.
ship-preflight: ship-plugin-preflight ship-server-preflight
    @echo "Ship preflight passed."

ship-plugin-preflight: release-preflight
    @test -n "{{vault}}" || (echo "OBSETYNC_VAULT is not set" >&2; exit 2)
    @test -d "{{vault}}" || (echo "plugin destination does not exist: {{vault}}" >&2; exit 2)
    @test -w "{{vault}}" || (echo "plugin destination is not writable: {{vault}}" >&2; exit 2)
    @docker compose config --quiet
    @docker info >/dev/null

ship-server-preflight: release-preflight
    @test -n "{{server}}" || (echo "OBSETYNC_SERVER is not set" >&2; exit 2)
    @case "{{dest}}" in \
        /*) ;; \
        *) echo "OBSETYNC_DEST must be an absolute remote path" >&2; exit 2 ;; \
     esac
    @command -v nix >/dev/null
    @command -v scp >/dev/null
    @command -v ssh >/dev/null
    @docker compose config --quiet
    @docker info >/dev/null
    @ssh -o BatchMode=yes "{{server}}" "test -d '{{dest}}' && test -w '{{dest}}' && test -f '{{dest}}/docker-compose.yml' && test -d /backup && test -w /backup && test -w /tmp && command -v docker >/dev/null && command -v curl >/dev/null && command -v flock >/dev/null && command -v install >/dev/null && docker info >/dev/null && docker compose version >/dev/null && docker inspect obsetync-server >/dev/null"

# Build plugin files with strict release inputs. Docker builds do not receive
# .git, so the injected full commit is also required as the expected commit.
build-release-plugin: release-preflight
    @OBSETYNC_BUILD_GIT_COMMIT="{{release_commit}}" \
     OBSETYNC_BUILD_SOURCE_STATE=clean \
     OBSETYNC_BUILD_EXPECTED_COMMIT="{{release_commit}}" \
     OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT=1 \
     OBSETYNC_BUILD_REQUIRE_CLEAN=1 \
     OBSETYNC_BUILD_EXPECTED_VERSION="{{release_version}}" \
     docker compose run --build --rm plugin
    @test "$(git rev-parse --verify HEAD)" = "{{release_commit}}" && \
     test -z "$(git status --porcelain --untracked-files=no)" || \
        (echo "release checkout changed during plugin build" >&2; exit 2)
    @RELEASE_VERSION="{{release_version}}" RELEASE_COMMIT="{{release_commit}}" node -e ' \
        const fs = require("node:fs"); \
        const manifest = JSON.parse(fs.readFileSync("dist/plugin/manifest.json", "utf8")); \
        if (manifest.version !== process.env.RELEASE_VERSION) throw new Error("built plugin manifest version mismatch"); \
        const bundle = fs.readFileSync("dist/plugin/main.js", "utf8"); \
        if (!bundle.includes(process.env.RELEASE_COMMIT)) throw new Error("built plugin does not contain the release commit");'

# Build and load the Nix image, then verify its immutable OCI identity labels.
build-release-nix-image: release-preflight
    nix build .#dockerImage
    docker load < result
    @identity_file="$(mktemp)"; \
     trap 'rm -f "$identity_file"' EXIT; \
     docker run --rm obsetync-server:nix build-identity > "$identity_file"; \
     node scripts/release-provenance.mjs verify-server-json "$identity_file" \
        "{{release_version}}" "{{release_commit}}" clean
    @export RELEASE_VERSION="{{release_version}}" RELEASE_COMMIT="{{release_commit}}"; \
     docker image inspect obsetync-server:nix | node -e ' \
        let input = ""; \
        process.stdin.setEncoding("utf8"); \
        process.stdin.on("data", chunk => input += chunk); \
        process.stdin.on("end", () => { \
            const labels = JSON.parse(input)[0]?.Config?.Labels ?? {}; \
            const expected = { \
                "org.opencontainers.image.version": process.env.RELEASE_VERSION, \
                "org.opencontainers.image.revision": process.env.RELEASE_COMMIT, \
                "org.opencontainers.image.source-state": "clean", \
            }; \
            for (const [name, value] of Object.entries(expected)) \
                if (labels[name] !== value) throw new Error(`image label ${name}: expected ${value}, got ${labels[name] ?? "missing"}`); \
        });'
    @test "$(git rev-parse --verify HEAD)" = "{{release_commit}}" && \
     test -z "$(git status --porcelain --untracked-files=no)" || \
        (echo "release checkout changed during Nix image build" >&2; exit 2)

# Copy the already verified release plugin into the local Obsidian vault.
ship-plugin: ship-plugin-preflight build-release-plugin
    bash scripts/deploy-plugin.sh dist/plugin "{{vault}}" "{{release_commit}}"

# Build the Nix docker image, transfer it to the remote host, load + restart.
# Does NOT re-run `init` — the server's existing data dir + box keypair stay
# intact across deploys. Only the compose restarts to pick up the new image.
ship-server: ship-server-preflight build-release-nix-image
    @set -eu; \
     image_tar="/tmp/obsetync-server-{{release_commit}}.tar"; \
     staged_compose="{{dest}}/.docker-compose-{{release_commit}}.yml"; \
     remote_script="/tmp/obsetync-deploy-server-{{release_commit}}.sh"; \
     remote_script_uploaded=0; \
     cleanup() { \
        if [ "$remote_script_uploaded" -eq 1 ]; then \
            ssh "{{server}}" rm -f "$remote_script" >/dev/null 2>&1 || true; \
        fi; \
     }; \
     trap cleanup EXIT; \
     scp result "{{server}}:$image_tar"; \
     scp docker-compose.yml "{{server}}:$staged_compose"; \
     scp scripts/deploy-server.sh "{{server}}:$remote_script"; \
     remote_script_uploaded=1; \
     ssh "{{server}}" bash "$remote_script" "{{dest}}" "$image_tar" \
        "$staged_compose" "{{release_version}}" "{{release_commit}}"

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

# --- end-to-end tests ----------------------------------------------------

# Spin up the isolated e2e stack, run the e2e test crate, then tear it down.
# Each invocation starts from a wiped volume so install + first-time init are
# part of the surface under test. Set OBSETYNC_E2E_KEEP=1 to leave the stack
# running after tests finish (handy for ad-hoc curl / inspection).
e2e: build-image e2e-up
    # Preserve the test result across cleanup; if tests passed, cleanup itself remains a gate.
    @test_status=0; teardown_status=0; \
     cargo test -p e2e-tests --features e2e -- --test-threads=1 --nocapture || test_status=$?; \
     if [ -z "${OBSETYNC_E2E_KEEP:-}" ]; then just e2e-down || teardown_status=$?; \
     else echo "stack left running (OBSETYNC_E2E_KEEP=1); use 'just e2e-down' to stop"; fi; \
     if [ "$test_status" -ne 0 ]; then exit "$test_status"; fi; \
     exit "$teardown_status"

# Bring up the e2e stack and block until /health responds.
e2e-up:
    docker compose -f docker-compose.e2e.yml up -d
    @echo "waiting for sync API health on http://127.0.0.1:27282/health ..."
    @for i in $(seq 1 60); do \
        if curl -fsS http://127.0.0.1:27282/health >/dev/null 2>&1; then \
            echo "ready"; exit 0; \
        fi; \
        sleep 1; \
     done; \
     echo "server failed to become healthy"; \
     docker compose -f docker-compose.e2e.yml logs --tail=80 e2e-server; \
     exit 1

# Tear down the e2e stack and wipe its volume so the next run is a fresh install.
e2e-down:
    docker compose -f docker-compose.e2e.yml down -v --remove-orphans

# Tail the e2e server logs (useful while iterating against a kept stack).
e2e-logs:
    docker compose -f docker-compose.e2e.yml logs -f e2e-server

# Drop Docker BuildKit caches.
clean-cache:
    docker builder prune -af

# Nuke everything — images, volumes, caches. Fresh start next `just build`.
nuke:
    -docker compose --profile tools down -v
    -docker rmi obsetync/server:local obsetync/plugin-builder:local obsetync/dev:local obsetync/rust-builder:local 2>/dev/null
    -docker builder prune -af
