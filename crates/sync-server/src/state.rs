use crate::blocking_io::{self, BlockingPool};
use crate::box_key;
use crate::config::ServerConfig;
use crate::devices::DeviceRegistry;
use crate::eph_rotation::{self, EphState};
use crate::perf::ServerPerfCounters;
use crate::secure::KEY_LEN;
use crate::seq_tracker::SequenceTracker;
use crate::storage::{StorageLayout, VaultStore};
use crate::storage_writer::StorageWriter;
use crate::transport_memory::TransportMemoryBudget;
use axum::extract::ws::Utf8Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::Instant;
use tokio::sync::{broadcast, Mutex as AsyncMutex};

/// Shared application state, passed to all axum handlers.
pub struct AppState {
    pub layout: StorageLayout,
    pub vaults: VaultStore,
    /// Changes on every process startup, including a restored backup. This
    /// invalidates unaccepted old submissions, not durable receipt lookup.
    pub root_incarnation: String,
    /// Raw bytes of the server's X25519 private key. Kept as bytes (not
    /// `StaticSecret`) so we can construct a fresh secret per request without
    /// worrying about Clone or thread-safety. 32 bytes copy per request is
    /// microseconds.
    pub server_priv_bytes: [u8; KEY_LEN],
    /// Current and grace-period server DH keys for HTTP wire v2.
    pub eph: Arc<RwLock<EphState>>,
    /// Durable sliding anti-replay window per enrolled device.
    pub sequences: SequenceTracker,
    /// Aggregate-only process metrics exposed by the local admin endpoint.
    /// Inputs intentionally cannot carry vault paths, hashes, or identities.
    pub perf: Arc<ServerPerfCounters>,
    /// Memory-resident auth/device state loaded from the filesystem before
    /// listeners open. The filesystem remains authoritative across restart.
    pub devices: DeviceRegistry,
    /// Reserved blocking capacity for tickets, enrollment, and durable admin
    /// mutations. Kept separate from bulk storage reads to prevent starvation.
    pub control_io: BlockingPool,
    /// Dedicated bounded immutable-object writer; each packed group is durable
    /// before its objects become visible to readers.
    pub storage_writer: StorageWriter,
    /// One process-wide owner for all sealed HTTP and bulk WS byte buffers.
    /// Admission happens before body receive/decrypt or logical frame
    /// assembly; dropping the phase owner returns capacity on every exit.
    pub(crate) transport_memory: TransportMemoryBudget,
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
    notifiers: StdMutex<HashMap<String, broadcast::Sender<VaultFrame>>>,
    /// Ephemeral presence: vault → device → (name, file, state). Ph3. Never
    /// persisted; refreshed by clients, expired by TTL, cleared on close.
    presence: StdMutex<HashMap<String, HashMap<String, PresenceEntry>>>,
    presence_session_sequence: AtomicU64,
}

impl AppState {
    pub fn new(config: ServerConfig) -> Self {
        let layout = StorageLayout::new(&config.data_dir);
        let perf = Arc::new(ServerPerfCounters::default());
        let vaults = VaultStore::with_perf(layout.clone(), Arc::clone(&perf));

        // Require the box keypair to exist at startup. `init` creates it; if
        // it's missing the server is misconfigured.
        let (priv_key, _pub_key) = box_key::load_box_keypair(&layout)
            .expect("box keypair missing — run `sync-server init --data-dir …` first");
        let mut priv_bytes = [0u8; KEY_LEN];
        priv_bytes.copy_from_slice(priv_key.as_bytes());
        let eph = eph_rotation::init_eph_keys().expect("transport-v2 ephemeral key unavailable");
        let sequences = SequenceTracker::new(layout.clone())
            .expect("anti-replay reservations could not be loaded from disk");
        let devices = DeviceRegistry::load(layout.clone())
            .expect("device registry could not be loaded from disk");
        let control_io = blocking_io::control_pool();
        let storage_writer = StorageWriter::start(layout.clone(), Arc::clone(&perf))
            .expect("storage writer journal recovery failed");
        let transport_memory = TransportMemoryBudget::process_default();

        Self {
            layout,
            vaults,
            root_incarnation: hex::encode(rand::random::<[u8; 32]>()),
            server_priv_bytes: priv_bytes,
            eph: Arc::new(RwLock::new(eph)),
            sequences,
            perf,
            devices,
            control_io,
            storage_writer,
            transport_memory,
            started_at: Instant::now(),
            vault_locks: StdMutex::new(HashMap::new()),
            notifiers: StdMutex::new(HashMap::new()),
            presence: StdMutex::new(HashMap::new()),
            presence_session_sequence: AtomicU64::new(1),
        }
    }

    /// The entry lives while any operation owns or is waiting for this lock.
    pub fn vault_lock<'a>(&'a self, vault_id: &'a str) -> VaultLock<'a> {
        let mutex = self
            .vault_locks
            .lock()
            .expect("vault_locks poisoned")
            .entry(vault_id.to_string())
            .or_default()
            .clone();
        VaultLock {
            state: self,
            vault_id,
            mutex: Some(mutex),
        }
    }

    /// Publish a ready-to-send inner frame (JSON string) to a vault's live
    /// WebSocket subscribers. No subscribers → no channel is even created;
    /// never blocks.
    fn publish_frame(&self, vault_id: &str, frame: VaultFrame) {
        let sender = self
            .notifiers
            .lock()
            .expect("notifiers poisoned")
            .get(vault_id)
            .cloned();
        if let Some(tx) = sender {
            // Err just means nobody is listening right now — fine.
            let _ = tx.send(frame);
        }
    }

    pub async fn relay_crdt_update(
        self: &Arc<Self>,
        vault_id: &str,
        note: &str,
        update: Vec<u8>,
        frame: String,
    ) -> std::io::Result<()> {
        let state = Arc::clone(self);
        let (vault, note) = (vault_id.to_owned(), note.to_owned());
        self.storage_writer
            .run_blocking(move |_| {
                crate::crdt::append(&state.layout, &vault, &note, &update)?;
                state.publish_frame(&vault, VaultFrame::Ops(frame.into()));
                Ok(())
            })
            .await
            .map_err(std::io::Error::other)?
    }

    /// Announce a new current root to any live WebSocket subscribers.
    pub fn notify_root_changed(&self, vault_id: &str, root_hex: &str) {
        self.publish_frame(
            vault_id,
            VaultFrame::Root(
                serde_json::json!({ "v": 1, "t": "root", "vault": vault_id, "root": root_hex })
                    .to_string()
                    .into(),
            ),
        );
    }

    /// Subscribe to a vault's frames (creates the channel on first use).
    pub fn subscribe_roots(self: &Arc<Self>, vault_id: &str) -> VaultSubscription {
        let receiver = self
            .notifiers
            .lock()
            .expect("notifiers poisoned")
            .entry(vault_id.to_owned())
            .or_insert_with(|| broadcast::channel(16).0)
            .subscribe();
        VaultSubscription {
            receiver: Some(receiver),
            state: Arc::clone(self),
            vault_id: vault_id.to_owned(),
        }
    }

    // --- Presence (Ph3) ---------------------------------------------------
    //
    // Ephemeral only: who is looking at which file right now. Never persisted;
    // entries expire when not refreshed and are cleared on session close.

    /// Order overlapping sessions so delayed old frames cannot replace new presence.
    pub fn new_presence_session_id(&self) -> u64 {
        self.presence_session_sequence
            .fetch_add(1, Ordering::Relaxed)
    }

    /// Record a device's presence and broadcast it to the vault's subscribers.
    pub fn set_presence(
        &self,
        vault_id: &str,
        device_id: &str,
        session_id: u64,
        device_name: &str,
        file: Option<String>,
        state: &str,
    ) {
        let now = now_ms();
        let mut map = self.presence.lock().expect("presence poisoned");
        let vault = map.entry(vault_id.to_string()).or_default();
        // Expired sessions must not prevent a still-live session from publishing.
        vault.retain(|_, e| now.saturating_sub(e.updated_ms) < PRESENCE_TTL_MS);
        if vault
            .get(device_id)
            .is_some_and(|entry| entry.session_id > session_id)
        {
            return;
        }
        vault.insert(
            device_id.to_string(),
            PresenceEntry {
                session_id,
                device_name: device_name.to_string(),
                file: file.clone(),
                state: state.to_string(),
                updated_ms: now,
            },
        );
        // Publish under the same lock so concurrent updates cannot reverse frames.
        self.publish_frame(
            vault_id,
            VaultFrame::Presence {
                device: Arc::from(device_id),
                json: presence_frame(vault_id, device_id, device_name, file.as_deref(), state)
                    .into(),
            },
        );
    }

    /// A closing session can only remove the last presence it published.
    pub fn clear_presence(&self, vault_id: &str, device_id: &str, session_id: u64) {
        let mut map = self.presence.lock().expect("presence poisoned");
        let Some(vault) = map.get_mut(vault_id) else {
            return;
        };
        if vault
            .get(device_id)
            .is_none_or(|entry| entry.session_id != session_id)
        {
            return;
        }
        if let Some(entry) = vault.remove(device_id) {
            self.publish_frame(
                vault_id,
                VaultFrame::Presence {
                    device: Arc::from(device_id),
                    json: presence_frame(vault_id, device_id, &entry.device_name, None, "offline")
                        .into(),
                },
            );
        }
        if vault.is_empty() {
            map.remove(vault_id);
        }
    }

    /// Current live presence for a vault as ready-to-send frames — served to
    /// fresh subscribers so they immediately see who is where.
    pub fn presence_snapshot(&self, vault_id: &str, recipient: &str) -> Vec<String> {
        let now = now_ms();
        let mut map = self.presence.lock().expect("presence poisoned");
        let Some(vault) = map.get_mut(vault_id) else {
            return Vec::new();
        };
        vault.retain(|_, e| now.saturating_sub(e.updated_ms) < PRESENCE_TTL_MS);
        if vault.is_empty() {
            map.remove(vault_id);
            return Vec::new();
        }
        vault
            .iter()
            .filter(|(device_id, _)| device_id.as_str() != recipient)
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

pub struct VaultLock<'a> {
    state: &'a AppState,
    vault_id: &'a str,
    mutex: Option<Arc<AsyncMutex<()>>>,
}

impl VaultLock<'_> {
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.mutex.as_ref().expect("live vault lock").lock().await
    }
}

impl Drop for VaultLock<'_> {
    fn drop(&mut self) {
        let mut locks = self.state.vault_locks.lock().expect("vault_locks poisoned");
        drop(self.mutex.take());
        // Lookup and the last-reference check use the same registry lock.
        if locks
            .get(self.vault_id)
            .is_some_and(|mutex| Arc::strong_count(mutex) == 1)
        {
            locks.remove(self.vault_id);
        }
    }
}

#[derive(Clone, Debug)]
pub enum VaultFrame {
    Root(Utf8Bytes),
    Presence { device: Arc<str>, json: Utf8Bytes },
    Ops(Utf8Bytes),
}

impl VaultFrame {
    pub fn for_device(self, recipient: &str) -> Option<Utf8Bytes> {
        match self {
            Self::Presence { device, .. } if device.as_ref() == recipient => None,
            Self::Presence { json, .. } | Self::Root(json) | Self::Ops(json) => Some(json),
        }
    }
}

pub struct VaultSubscription {
    receiver: Option<broadcast::Receiver<VaultFrame>>,
    state: SharedState,
    vault_id: String,
}

impl VaultSubscription {
    pub async fn recv(&mut self) -> Result<VaultFrame, broadcast::error::RecvError> {
        self.receiver
            .as_mut()
            .expect("live subscription")
            .recv()
            .await
    }

    #[cfg(test)]
    pub fn try_recv(&mut self) -> Result<Utf8Bytes, broadcast::error::TryRecvError> {
        self.receiver
            .as_mut()
            .expect("live subscription")
            .try_recv()
            .map(|frame| match frame {
                VaultFrame::Root(json)
                | VaultFrame::Ops(json)
                | VaultFrame::Presence { json, .. } => json,
            })
    }
}

impl Drop for VaultSubscription {
    fn drop(&mut self) {
        drop(self.receiver.take());
        let mut channels = self.state.notifiers.lock().expect("notifiers poisoned");
        // A reconnect may have subscribed while the old forwarder was closing.
        if channels
            .get(&self.vault_id)
            .is_some_and(|tx| tx.receiver_count() == 0)
        {
            channels.remove(&self.vault_id);
        }
    }
}

/// How long a presence entry survives without a refresh. Clients re-send
/// every ~45s while a file is open.
const PRESENCE_TTL_MS: u64 = 90_000;

struct PresenceEntry {
    session_id: u64,
    device_name: String,
    file: Option<String>,
    state: String,
    updated_ms: u64,
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
        "device": format!("{device_id:.12}"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn vault_lock_cleanup_preserves_waiters_and_independent_vaults() {
        let dir = tempfile::tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        box_key::init_box_keypair(&layout).unwrap();
        let state = AppState::new(ServerConfig::new(dir.path().to_path_buf()));
        use std::task::{Context, Waker};
        let mut cx = Context::from_waker(Waker::noop());
        let first = state.vault_lock("a");
        let held = first.lock().await;
        {
            let cancelled = state.vault_lock("a");
            let mut waiting = Box::pin(cancelled.lock());
            assert!(std::future::Future::poll(waiting.as_mut(), &mut cx).is_pending());
        }
        {
            let other = state.vault_lock("b");
            let _guard = other.lock().await;
        }
        assert_eq!(state.vault_locks.lock().unwrap().len(), 1);
        let second = state.vault_lock("a");
        let mut waiting = Box::pin(second.lock());
        assert!(std::future::Future::poll(waiting.as_mut(), &mut cx).is_pending());
        drop(held);
        drop(first);
        let next = waiting.await;
        {
            let third = state.vault_lock("a");
            let mut waiting = Box::pin(third.lock());
            assert!(std::future::Future::poll(waiting.as_mut(), &mut cx).is_pending());
        }
        drop(next);
        drop(second);
        assert!(state.vault_locks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn last_subscription_releases_channel_on_disconnect_or_task_abort() {
        let dir = tempfile::tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        box_key::init_box_keypair(&layout).unwrap();
        let state = Arc::new(AppState::new(ServerConfig::new(dir.path().to_path_buf())));
        let mut old = state.subscribe_roots("vault");
        let mut new = state.subscribe_roots("vault");
        state.publish_frame("vault", VaultFrame::Root("shared".into()));
        let first = old.try_recv().unwrap();
        let second = new.try_recv().unwrap();
        assert_eq!(first.as_ptr(), second.as_ptr());
        drop(old);
        state.publish_frame("vault", VaultFrame::Root("still live".into()));
        assert_eq!(new.try_recv().unwrap(), "still live");
        let forwarder = tokio::spawn(async move { new.recv().await });
        forwarder.abort();
        assert!(forwarder.await.unwrap_err().is_cancelled());
        assert!(state.notifiers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_crdt_append_does_not_publish_a_peer_update() {
        let dir = tempfile::tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        box_key::init_box_keypair(&layout).unwrap();
        let state = Arc::new(AppState::new(ServerConfig::new(dir.path().to_path_buf())));
        let mut events = state.subscribe_roots("vault");

        let parent = layout.crdt_log_path("vault", "unused");
        std::fs::create_dir_all(parent.parent().unwrap().parent().unwrap()).unwrap();
        std::fs::write(parent.parent().unwrap(), b"cannot create logs here").unwrap();
        assert!(state
            .relay_crdt_update("vault", "a.md", b"update".to_vec(), "frame".into())
            .await
            .is_err());
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn presence_routes_by_full_id_even_when_display_prefixes_match() {
        let dir = tempfile::tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        box_key::init_box_keypair(&layout).unwrap();
        let state = Arc::new(AppState::new(ServerConfig::new(dir.path().to_path_buf())));
        let mut events = state.subscribe_roots("vault");
        let sender = "123456789abc01";
        let peer = "123456789abc02";
        state.set_presence(
            "vault",
            sender,
            1,
            "Tablet",
            Some("note.md".into()),
            "active",
        );
        let event = events.recv().await.unwrap();
        assert!(event.clone().for_device(sender).is_none());
        let delivered = event.for_device(peer).unwrap();
        assert!(state.presence_snapshot("vault", sender).is_empty());
        assert_eq!(state.presence_snapshot("vault", peer), [delivered.as_str()]);
        let json: serde_json::Value = serde_json::from_str(&delivered).unwrap();
        assert_eq!(json["device"], "123456789abc");
    }

    #[tokio::test]
    async fn old_socket_cannot_replace_or_clear_a_reconnected_devices_presence() {
        let dir = tempfile::tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        box_key::init_box_keypair(&layout).unwrap();
        let state = Arc::new(AppState::new(ServerConfig::new(dir.path().to_path_buf())));
        let mut events = state.subscribe_roots("vault");
        let old_session = state.new_presence_session_id();
        let new_session = state.new_presence_session_id();
        state.set_presence(
            "vault",
            "device",
            old_session,
            "Laptop",
            Some("old.md".into()),
            "active",
        );
        state.set_presence(
            "vault",
            "device",
            new_session,
            "Laptop",
            Some("new.md".into()),
            "idle",
        );
        events.try_recv().unwrap();
        let latest = events.try_recv().unwrap();

        state.set_presence(
            "vault",
            "device",
            old_session,
            "Laptop",
            Some("late-old.md".into()),
            "active",
        );
        assert_eq!(state.presence_snapshot("vault", "peer"), [latest.as_str()]);
        assert!(
            events.try_recv().is_err(),
            "late old frames must not replace the new session"
        );
        state.clear_presence("vault", "device", old_session);
        assert_eq!(state.presence_snapshot("vault", "peer"), [latest.as_str()]);
        assert!(
            events.try_recv().is_err(),
            "old close must not broadcast offline"
        );

        state.clear_presence("vault", "device", new_session);
        assert!(state.presence.lock().unwrap().is_empty());
        assert!(state.presence_snapshot("vault", "peer").is_empty());
        let offline: serde_json::Value = serde_json::from_str(&events.try_recv().unwrap()).unwrap();
        assert_eq!(offline["state"], "offline");
        state.clear_presence("vault", "device", new_session);
        assert!(events.try_recv().is_err(), "cleanup is idempotent");

        state.set_presence("vault", "device", new_session, "Laptop", None, "idle");
        state
            .presence
            .lock()
            .unwrap()
            .get_mut("vault")
            .unwrap()
            .get_mut("device")
            .unwrap()
            .updated_ms = 0;
        assert!(state.presence_snapshot("vault", "peer").is_empty());
        assert!(state.presence.lock().unwrap().is_empty());
    }
}
