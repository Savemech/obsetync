use crate::box_key;
use crate::config::ServerConfig;
use crate::secure::KEY_LEN;
use crate::storage::{StorageLayout, VaultStore};
use std::sync::Arc;

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
        }
    }
}

pub type SharedState = Arc<AppState>;
