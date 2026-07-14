use crate::box_key;
use crate::config::ServerConfig;
use crate::secure::KEY_LEN;
use crate::storage::{StorageLayout, VaultStore};
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;
use tokio::sync::Mutex as AsyncMutex;

/// Shared application state, passed to all axum handlers.
pub struct AppState {
    #[allow(dead_code)]
    pub config: ServerConfig,
    pub layout: StorageLayout,
    pub vaults: VaultStore,
    /// Raw bytes of the server's X25519 private key. Kept as bytes (not
    /// `StaticSecret`) so we can construct a fresh secret per request without
    /// worrying about Clone or thread-safety. 32 bytes copy per request is
    /// microseconds.
    pub server_priv_bytes: [u8; KEY_LEN],
    /// Wall-clock monotonic start time — used by the admin dashboard to show
    /// uptime. Instant is Copy, so reading from Arc<AppState> needs no lock.
    pub started_at: Instant,
    /// Per-vault write locks serializing every current-root mutation
    /// (put_root's read-modify-write and admin rollback). Without this, two
    /// concurrent pushes both read the same `current`, both pass the
    /// fast-forward check, and the second `set_current_root` silently drops
    /// the first push from the vault's advertised history. The std mutex
    /// guards only the map lookup (never held across an await); the async
    /// mutex it hands out IS held across the whole critical section,
    /// including merge and guard-scan awaits.
    vault_locks: StdMutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

impl AppState {
    pub fn new(config: ServerConfig) -> Self {
        let layout = StorageLayout::new(&config.data_dir);
        let vaults = VaultStore::new(layout.clone());

        // Require the box keypair to exist at startup. `init` creates it; if
        // it's missing the server is misconfigured.
        let (priv_key, _pub_key) = box_key::load_box_keypair(&layout)
            .expect("box keypair missing — run `sync-server init --data-dir …` first");
        let mut priv_bytes = [0u8; KEY_LEN];
        priv_bytes.copy_from_slice(priv_key.as_bytes());

        Self {
            config,
            layout,
            vaults,
            server_priv_bytes: priv_bytes,
            started_at: Instant::now(),
            vault_locks: StdMutex::new(HashMap::new()),
        }
    }

    /// The write lock for one vault. One entry per vault ever touched —
    /// unbounded in theory, a handful in practice for a self-hosted server.
    pub fn vault_lock(&self, vault_id: &str) -> Arc<AsyncMutex<()>> {
        self.vault_locks
            .lock()
            .expect("vault_locks poisoned")
            .entry(vault_id.to_string())
            .or_default()
            .clone()
    }
}

pub type SharedState = Arc<AppState>;
