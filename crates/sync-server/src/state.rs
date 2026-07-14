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
    /// Per-vault "root changed" broadcast — the Ph2 notify channel's fan-out.
    /// Publishers fire-and-forget after every set_current_root; WebSocket
    /// sessions subscribe per vault. Send is sync and non-blocking; a
    /// receiver that lags past the 16-message buffer just misses frames and
    /// falls back to its regular poll (data never depends on this channel).
    notifiers: StdMutex<HashMap<String, tokio::sync::broadcast::Sender<String>>>,
    /// Ephemeral presence: vault → device → (name, file, state). Ph3. Never
    /// persisted; refreshed by clients, expired by TTL, cleared on close.
    presence: StdMutex<HashMap<String, HashMap<String, PresenceEntry>>>,
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
            notifiers: StdMutex::new(HashMap::new()),
            presence: StdMutex::new(HashMap::new()),
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

    /// Publish a ready-to-send inner frame (JSON string) to a vault's live
    /// WebSocket subscribers. No subscribers → no channel is even created;
    /// never blocks.
    pub fn publish_frame(&self, vault_id: &str, frame_json: String) {
        let sender = self
            .notifiers
            .lock()
            .expect("notifiers poisoned")
            .get(vault_id)
            .cloned();
        if let Some(tx) = sender {
            // Err just means nobody is listening right now — fine.
            let _ = tx.send(frame_json);
        }
    }

    /// Announce a new current root to any live WebSocket subscribers.
    pub fn notify_root_changed(&self, vault_id: &str, root_hex: &str) {
        self.publish_frame(
            vault_id,
            serde_json::json!({ "v": 1, "t": "root", "vault": vault_id, "root": root_hex })
                .to_string(),
        );
    }

    /// Subscribe to a vault's frames (creates the channel on first use).
    pub fn subscribe_roots(&self, vault_id: &str) -> tokio::sync::broadcast::Receiver<String> {
        self.notifiers
            .lock()
            .expect("notifiers poisoned")
            .entry(vault_id.to_string())
            .or_insert_with(|| tokio::sync::broadcast::channel(16).0)
            .subscribe()
    }

    // --- Presence (Ph3) ---------------------------------------------------
    //
    // Ephemeral only: who is looking at which file right now. Never persisted;
    // entries expire when not refreshed and are cleared on session close.

    /// Record a device's presence and broadcast it to the vault's subscribers.
    pub fn set_presence(
        &self,
        vault_id: &str,
        device_id: &str,
        device_name: &str,
        file: Option<String>,
        state: &str,
    ) {
        let now = now_ms();
        {
            let mut map = self.presence.lock().expect("presence poisoned");
            let vault = map.entry(vault_id.to_string()).or_default();
            vault.insert(
                device_id.to_string(),
                PresenceEntry {
                    device_name: device_name.to_string(),
                    file: file.clone(),
                    state: state.to_string(),
                    updated_ms: now,
                },
            );
            // Opportunistic sweep of stale entries for this vault.
            vault.retain(|_, e| now.saturating_sub(e.updated_ms) < PRESENCE_TTL_MS);
        }
        self.publish_frame(
            vault_id,
            presence_frame(vault_id, device_id, device_name, file.as_deref(), state),
        );
    }

    /// Remove a device's presence (session closed) and broadcast "offline".
    pub fn clear_presence(&self, vault_id: &str, device_id: &str) {
        let removed = {
            let mut map = self.presence.lock().expect("presence poisoned");
            map.get_mut(vault_id)
                .map(|vault| vault.remove(device_id))
                .flatten()
        };
        if let Some(entry) = removed {
            self.publish_frame(
                vault_id,
                presence_frame(vault_id, device_id, &entry.device_name, None, "offline"),
            );
        }
    }

    /// Current live presence for a vault as ready-to-send frames — served to
    /// fresh subscribers so they immediately see who is where.
    pub fn presence_snapshot(&self, vault_id: &str) -> Vec<String> {
        let now = now_ms();
        let mut map = self.presence.lock().expect("presence poisoned");
        let Some(vault) = map.get_mut(vault_id) else {
            return Vec::new();
        };
        vault.retain(|_, e| now.saturating_sub(e.updated_ms) < PRESENCE_TTL_MS);
        vault
            .iter()
            .map(|(device_id, e)| {
                presence_frame(
                    vault_id,
                    device_id,
                    &e.device_name,
                    e.file.as_deref(),
                    &e.state,
                )
            })
            .collect()
    }
}

/// How long a presence entry survives without a refresh. Clients re-send
/// every ~45s while a file is open.
const PRESENCE_TTL_MS: u64 = 90_000;

pub struct PresenceEntry {
    pub device_name: String,
    pub file: Option<String>,
    pub state: String,
    pub updated_ms: u64,
}

fn presence_frame(
    vault_id: &str,
    device_id: &str,
    device_name: &str,
    file: Option<&str>,
    state: &str,
) -> String {
    serde_json::json!({
        "v": 1,
        "t": "presence",
        "vault": vault_id,
        "device": &device_id[..device_id.len().min(12)],
        "name": device_name,
        "file": file,
        "state": state,
    })
    .to_string()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub type SharedState = Arc<AppState>;
