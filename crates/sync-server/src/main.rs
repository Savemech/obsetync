mod admin;
mod api;
mod box_key;
mod bridge;
mod ca;
mod config;
mod devices;
mod enrollment;
mod error;
mod state;
mod storage;
mod tls;

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
    /// Initialize a new server data directory (CA, certs, directories).
    Init {
        /// Path to the data directory.
        #[arg(long)]
        data_dir: PathBuf,
        /// Hostname(s) / IP(s) clients will use to reach this server.
        /// Repeat once per name. Include every DNS name and IP — iOS strictly
        /// validates the TLS cert's SAN against the URL. localhost, 127.0.0.1,
        /// and ::1 are always added regardless.
        #[arg(long = "hostname", value_name = "HOST")]
        hostnames: Vec<String>,
    },
    /// Regenerate the server TLS cert (leaving the CA + enrolled devices alone).
    /// Use when adding a new hostname to an existing install.
    RegenServerCert {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long = "hostname", value_name = "HOST")]
        hostnames: Vec<String>,
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
        /// Disable TLS (for development only).
        #[arg(long, default_value = "false")]
        no_tls: bool,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Command::Init {
            data_dir,
            hostnames,
        } => {
            if let Err(e) = cmd_init(&data_dir, &hostnames) {
                tracing::error!("init failed: {}", e);
                std::process::exit(1);
            }
        }
        Command::RegenServerCert {
            data_dir,
            hostnames,
        } => {
            if let Err(e) = cmd_regen_server_cert(&data_dir, &hostnames) {
                tracing::error!("regen-server-cert failed: {}", e);
                std::process::exit(1);
            }
        }
        Command::Run {
            data_dir,
            sync_port,
            admin_port,
            no_tls,
        } => {
            if let Err(e) = cmd_run(&data_dir, sync_port, admin_port, no_tls).await {
                tracing::error!("server failed: {}", e);
                std::process::exit(1);
            }
        }
    }
}

fn cmd_init(data_dir: &PathBuf, hostnames: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let layout = storage::StorageLayout::new(data_dir);
    layout.init_directories()?;

    // Generate CA and server cert.
    ca::init_ca(&layout)?;
    ca::init_server_cert(&layout, hostnames)?;

    // Save config.
    let config = config::ServerConfig::new(data_dir.clone());
    config.save()?;

    println!("Server initialized at {}", data_dir.display());
    println!();
    println!("  CA certificate: {}/ca/ca.crt", data_dir.display());
    println!("  Server cert:    {}/server/server.crt", data_dir.display());
    if !hostnames.is_empty() {
        println!("  Cert SAN:       localhost + {}", hostnames.join(", "));
    } else {
        println!("  Cert SAN:       localhost only");
        println!();
        println!("WARNING: no --hostname was given. Clients connecting via DNS names other");
        println!("than localhost (iOS especially) will reject the cert. Re-run with:");
        println!(
            "  obsetync-server regen-server-cert --data-dir {} \\",
            data_dir.display()
        );
        println!("    --hostname <your-host>");
    }
    println!();
    println!("Run with:");
    println!("  obsetync-server run --data-dir {}", data_dir.display());
    Ok(())
}

fn cmd_regen_server_cert(
    data_dir: &PathBuf,
    hostnames: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let layout = storage::StorageLayout::new(data_dir);
    ca::init_server_cert(&layout, hostnames)?;
    println!("Server cert regenerated.");
    if !hostnames.is_empty() {
        println!("  SAN: localhost + {}", hostnames.join(", "));
    }
    println!();
    println!("Restart the server (docker compose restart / systemctl restart) to pick up");
    println!("the new cert. Enrolled devices keep their credentials.");
    Ok(())
}

async fn cmd_run(
    data_dir: &PathBuf,
    sync_port: u16,
    admin_port: u16,
    no_tls: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = config::ServerConfig::load(data_dir)?;
    config.sync_port = sync_port;
    config.admin_port = admin_port;

    let state = Arc::new(state::AppState::new(config));

    let sync_app = api::sync_router(state.clone());
    let admin_app = admin::admin_router(state.clone());

    // Admin GUI is always plain HTTP — secure it at the firewall level.
    let admin_addr = format!("0.0.0.0:{}", admin_port);
    let admin_listener = tokio::net::TcpListener::bind(&admin_addr).await?;

    if no_tls {
        let sync_addr = format!("0.0.0.0:{}", sync_port);
        let sync_listener = tokio::net::TcpListener::bind(&sync_addr).await?;

        println!("Sync API:  http://{} (NO TLS - dev mode)", sync_addr);
        println!("Admin GUI: http://{}", admin_addr);

        tokio::select! {
            r = axum::serve(sync_listener, sync_app) => {
                if let Err(e) = r { tracing::error!("sync server error: {}", e); }
            }
            r = axum::serve(admin_listener, admin_app) => {
                if let Err(e) = r { tracing::error!("admin server error: {}", e); }
            }
        }
    } else {
        let tls_config = tls::build_tls_config(&state.layout)?;
        let sync_addr: std::net::SocketAddr = format!("0.0.0.0:{}", sync_port).parse()?;

        println!("Sync API:  https://{} (mTLS)", sync_addr);
        println!("Admin GUI: http://{}", admin_addr);

        let sync_tls = axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(tls_config));

        tokio::select! {
            r = axum_server::bind_rustls(sync_addr, sync_tls).serve(sync_app.into_make_service()) => {
                if let Err(e) = r { tracing::error!("sync server error: {}", e); }
            }
            r = axum::serve(admin_listener, admin_app) => {
                if let Err(e) = r { tracing::error!("admin server error: {}", e); }
            }
        }
    }

    Ok(())
}
