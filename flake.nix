{
  description = "ObsetyNC — self-hosted Obsidian vault sync (hermetic Nix build)";

  # Every input is pinned in flake.lock by content hash.
  # Same inputs → byte-identical outputs on any machine.
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, flake-utils, fenix, crane }:
    flake-utils.lib.eachSystem [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ] (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        lib  = pkgs.lib;

        # Rust toolchain. `stable` is whatever fenix's stable channel resolved to
        # when `flake.lock` was last updated — so builds are hermetic (same lock
        # → same compiler), and `nix flake update` is the single knob that pulls
        # in a new Rust. Adding the wasm32 target pulls in rust-std for WASM.
        fenixPkgs = fenix.packages.${system};
        rustToolchain = fenixPkgs.combine [
          fenixPkgs.stable.rustc
          fenixPkgs.stable.cargo
          fenixPkgs.stable.clippy
          fenixPkgs.stable.rustfmt
          fenixPkgs.stable.rust-src
          fenixPkgs.targets.wasm32-unknown-unknown.stable.rust-std
        ];

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        # Source filtered to Cargo-relevant files — keeps build cache stable
        # across plugin edits.
        src = craneLib.cleanCargoSource ./.;

        commonArgs = {
          inherit src;
          strictDeps = true;
          pname = "obsetync";
          version = "1.0.1";

          # aws-lc-sys (rustls backend) needs clang + cmake + perl to build.
          nativeBuildInputs = with pkgs; [
            clang
            cmake
            perl
            pkg-config
          ];

          # Use clang as the C compiler everywhere.
          CC = "${pkgs.clang}/bin/clang";
        };

        # Pre-compile every Cargo dep once. Server and WASM both reuse this.
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # ------------------------------------------------------------------
        # Server binary
        # ------------------------------------------------------------------
        sync-server = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          pname = "sync-server";
          cargoExtraArgs = "--locked -p sync-server --release";
          doCheck = false;  # test runs under `nix flake check` instead
        });

        # ------------------------------------------------------------------
        # WASM module — compile to wasm32, then run wasm-bindgen to emit
        # the JS glue + optimized .wasm.
        # ------------------------------------------------------------------
        sync-core-wasm-raw = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          pname = "sync-core-wasm";
          cargoExtraArgs = "--locked -p sync-core --release --target wasm32-unknown-unknown --features wasm --no-default-features";
          doCheck = false;

          # cargo won't put the .wasm anywhere pretty — grab it by hand.
          installPhaseCommand = ''
            mkdir -p $out/lib
            cp target/wasm32-unknown-unknown/release/sync_core.wasm $out/lib/
          '';
        });

        sync-core-wasm = pkgs.stdenv.mkDerivation {
          pname   = "sync-core-wasm-bindings";
          version = "1.0.1";

          src = sync-core-wasm-raw;

          nativeBuildInputs = with pkgs; [
            wasm-bindgen-cli
            binaryen
          ];

          buildPhase = ''
            runHook preBuild

            wasm-bindgen \
              --target web \
              --out-dir bindings \
              lib/sync_core.wasm

            # Optimize with wasm-opt (same feature flags as the Docker path).
            wasm-opt -O \
              --enable-bulk-memory \
              --enable-nontrapping-float-to-int \
              --enable-sign-ext \
              --enable-mutable-globals \
              --enable-reference-types \
              bindings/sync_core_bg.wasm \
              -o bindings/sync_core_bg.wasm.opt
            mv bindings/sync_core_bg.wasm.opt bindings/sync_core_bg.wasm

            runHook postBuild
          '';

          installPhase = ''
            mkdir -p $out
            cp -r bindings/. $out/
          '';
        };

        # ------------------------------------------------------------------
        # Plugin bundle — esbuild produces main.js, we ship it alongside
        # manifest.json and the WASM bindings.
        # ------------------------------------------------------------------
        plugin = pkgs.buildNpmPackage {
          pname   = "obsetync-plugin";
          version = "1.0.1";
          src     = ./plugin;

          # Replace on first build (Nix will tell you the right hash).
          npmDepsHash = lib.fakeHash;

          # Inject the WASM bindings before esbuild runs.
          preBuild = ''
            mkdir -p wasm
            cp -r ${sync-core-wasm}/. wasm/
          '';

          buildPhase = ''
            runHook preBuild
            node esbuild.config.mjs production
            runHook postBuild
          '';

          installPhase = ''
            runHook preInstall
            mkdir -p $out
            cp main.js manifest.json $out/
            cp -r wasm $out/
            runHook postInstall
          '';

          # We don't need to install anything into node_modules at runtime.
          dontNpmInstall = true;
        };

        # ------------------------------------------------------------------
        # Docker image built entirely by Nix — no Dockerfile involved.
        # Reproducible: same flake.lock + same nixpkgs → byte-identical image.
        # Only Linux systems can produce Linux images directly; on macOS use
        # `nix build .#dockerImage --system x86_64-linux` with a remote builder.
        # ------------------------------------------------------------------
        dockerImage = pkgs.dockerTools.buildLayeredImage {
          name = "obsetync-server";
          tag  = "nix";

          # Fixed creation date so the image hash is stable across rebuilds.
          created = "1970-01-01T00:00:00Z";

          contents = [
            pkgs.dockerTools.caCertificates  # /etc/ssl/certs for rustls
            pkgs.dockerTools.fakeNss         # minimal /etc/passwd (for `User: nobody`)
            sync-server                       # the binary at /bin/sync-server
          ];

          config = {
            Entrypoint   = [ "${sync-server}/bin/sync-server" ];
            Cmd          = [ "run" "--data-dir" "/data" ];
            ExposedPorts = {
              "27182/tcp" = {};
              "27183/tcp" = {};
            };
            Volumes    = { "/data" = {}; };
            User       = "nobody";
            WorkingDir = "/";
            Labels     = {
              "org.opencontainers.image.title"       = "obsetync-server";
              "org.opencontainers.image.description" = "Self-hosted Obsidian vault sync server";
            };
          };
        };

      in {
        packages = {
          server      = sync-server;
          wasm        = sync-core-wasm;
          plugin      = plugin;
          dockerImage = dockerImage;
          default     = sync-server;
        };

        # `nix flake check` runs the test suite across the workspace.
        checks = {
          inherit sync-server plugin;

          sync-tests = craneLib.cargoTest (commonArgs // {
            inherit cargoArtifacts;
            pname = "sync-tests";
            cargoExtraArgs = "--locked --workspace";
          });
        };

        # `nix run .#server -- run --data-dir /data`
        apps.server = {
          type = "app";
          program = "${sync-server}/bin/sync-server";
        };

        # `nix develop` — shell with the full toolchain.
        devShells.default = craneLib.devShell {
          packages = with pkgs; [
            rustToolchain
            nodejs_20
            wasm-bindgen-cli
            binaryen
            just
            clang
            cmake
            pkg-config
          ];

          CC = "clang";
        };
      });
}
