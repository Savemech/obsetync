use crate::config::ServerConfig;
use crate::storage::{StorageLayout, VaultStore};
use std::sync::Arc;

/// Shared application state, passed to all axum handlers.
pub struct AppState {
    #[allow(dead_code)]
    pub config: ServerConfig,
    pub layout: StorageLayout,
    pub vaults: VaultStore,
}

impl AppState {
    pub fn new(config: ServerConfig) -> Self {
        let layout = StorageLayout::new(&config.data_dir);
        let vaults = VaultStore::new(layout.clone());
        Self {
            config,
            layout,
            vaults,
        }
    }
}

pub type SharedState = Arc<AppState>;
