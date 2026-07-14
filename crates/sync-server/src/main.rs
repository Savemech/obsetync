mod admin;
mod api;
mod box_key;
mod bridge;
mod config;
mod crdt;
mod devices;
mod enrollment;
mod error;
mod guard;
mod secure;
mod state;
mod storage;
mod ws;
mod ws_ticket;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "obsetync-server", about = "ObsetyNC sync server")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize a new server data directory. Creates the X25519 "box"
    /// keypair clients use for encrypted transport + the directory layout.
    Init {
        /// Path to the data directory.
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Print the server's X25519 public key (base64). Useful for ops /
    /// verifying what clients will see during enrollment.
    ShowBoxPub {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Run the sync server.
    Run {
        /// Path to the data directory.
        #[arg(long)]
        data_dir: PathBuf,
        /// Sync API port (default: 27182).
        #[arg(long, default_value = "27182")]
        sync_port: u16,
        /// Admin GUI port (default: 27183).
        #[arg(long, default_value = "27183")]
        admin_port: u16,
    },
    /// Probe /health of a running server and exit 0/1. Intended for
    /// container HEALTHCHECKs — the hardened runtime image has no curl.
    Healthcheck {
        /// URL to probe.
        #[arg(long, default_value = "http://127.0.0.1:27182/health")]
        url: String,
    },
}

#[tokio::main]
async fn main() {
    // Log level via RUST_LOG (standard tracing env). Defaults to `info` for
    // the server's own spans and `warn` for everything else, so the container
    // output is readable without being noisy. Examples:
    //   RUST_LOG=sync_server=debug      → per-request details
    //   RUST_LOG=sync_server=trace      → includes plaintext body sizes etc.
    //   RUST_LOG=warn                   → quiet; only problems
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("sync_server=info,warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();

    match cli.command {
        Command::Init { data_dir } => {
            if let Err(e) = cmd_init(&data_dir) {
                tracing::error!("init failed: {}", e);
                std::process::exit(1);
            }
        }
        Command::ShowBoxPub { data_dir } => {
            if let Err(e) = cmd_show_box_pub(&data_dir) {
                tracing::error!("show-box-pub failed: {}", e);
                std::process::exit(1);
            }
        }
        Command::Run {
            data_dir,
            sync_port,
            admin_port,
        } => {
            if let Err(e) = cmd_run(&data_dir, sync_port, admin_port).await {
                tracing::error!("server failed: {}", e);
                std::process::exit(1);
            }
        }
        Command::Healthcheck { url } => match cmd_healthcheck(&url) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("healthcheck failed: {}", e);
                std::process::exit(1);
            }
        },
    }
}

/// Minimal blocking HTTP/1.1 GET over std::net — no client-lib dependency,
/// works inside the read-only distroless-style runtime image.
fn cmd_healthcheck(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Read, Write};

    let rest = url
        .strip_prefix("http://")
        .ok_or("healthcheck expects an http:// URL")?;
    let (host_port, path) = match rest.split_once('/') {
        Some((hp, p)) => (hp, format!("/{}", p)),
        None => (rest, "/health".to_string()),
    };

    let mut stream = std::net::TcpStream::connect(host_port)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(5)))?;
    write!(
        stream,
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, host_port
    )?;

    let mut response = String::new();
    stream.take(4096).read_to_string(&mut response)?;
    let status_line = response.lines().next().unwrap_or("");
    if status_line.contains(" 200 ") {
        Ok(())
    } else {
        Err(format!("unexpected response: {}", status_line).into())
    }
}

fn cmd_init(data_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let layout = storage::StorageLayout::new(data_dir);
    layout.init_directories()?;

    let (_priv, pub_key) = box_key::init_box_keypair(&layout)?;

    let config = config::ServerConfig::new(data_dir.clone());
    config.save()?;

    use base64::prelude::*;
    let pub_b64 = BASE64_STANDARD.encode(pub_key.as_bytes());

    println!("Server initialized at {}", data_dir.display());
    println!();
    println!("  Box public key: {}", pub_b64);
    println!(
        "  (stored at {}/server/box.pub — copy into clients at enrollment)",
        data_dir.display()
    );
    println!();
    println!("Run with:");
    println!("  obsetync-server run --data-dir {}", data_dir.display());
    Ok(())
}

fn cmd_show_box_pub(data_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let layout = storage::StorageLayout::new(data_dir);
    let pub_b64 = box_key::load_box_pub_base64(&layout)?;
    println!("{}", pub_b64);
    Ok(())
}

async fn cmd_run(
    data_dir: &PathBuf,
    sync_port: u16,
    admin_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = config::ServerConfig::load(data_dir)?;
    config.sync_port = sync_port;
    config.admin_port = admin_port;

    let state = Arc::new(state::AppState::new(config));

    let sync_app = api::sync_router(state.clone());
    let admin_app = admin::admin_router(state.clone());

    let admin_addr = format!("0.0.0.0:{}", admin_port);
    let admin_listener = tokio::net::TcpListener::bind(&admin_addr).await?;

    let sync_addr = format!("0.0.0.0:{}", sync_port);
    let sync_listener = tokio::net::TcpListener::bind(&sync_addr).await?;

    println!("Sync API:  http://{} (AEAD-encrypted payloads)", sync_addr);
    println!("Admin GUI: http://{}", admin_addr);

    tracing::info!(
        sync = %sync_addr,
        admin = %admin_addr,
        data_dir = %data_dir.display(),
        "server listening (AEAD envelope over HTTP: X25519 + HKDF-SHA256 + AES-256-GCM)"
    );

    tokio::select! {
        r = axum::serve(sync_listener, sync_app) => {
            if let Err(e) = r { tracing::error!("sync server error: {}", e); }
        }
        r = axum::serve(admin_listener, admin_app) => {
            if let Err(e) = r { tracing::error!("admin server error: {}", e); }
        }
    }

    Ok(())
}
