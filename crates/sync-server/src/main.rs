mod admin;
mod api;
mod box_key;
mod bridge;
mod config;
mod devices;
mod enrollment;
mod error;
mod secure;
mod state;
mod storage;

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

    println!(
        "Sync API:  http://{} (option-B encrypted payloads)",
        sync_addr
    );
    println!("Admin GUI: http://{}", admin_addr);

    tracing::info!(
        sync = %sync_addr,
        admin = %admin_addr,
        data_dir = %data_dir.display(),
        "server listening (option-B transport: X25519 + AES-256-GCM over HTTP)"
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
