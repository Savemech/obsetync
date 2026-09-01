use crate::storage::StorageLayout;
use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio_util::sync::CancellationToken;

/// Enrolled device metadata. No client certificate is stored — device
/// identity is the random `device_id` from enrollment, and the bearer
/// token (carried inside the encrypted request body) is what actually
/// authenticates.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeviceInfo {
    pub name: String,
    /// Random 128-bit identifier assigned at enrollment (32 hex chars).
    pub device_id: String,
    pub enrolled_at: u64,
    pub last_seen: u64,
    #[serde(default)]
    pub vaults: Vec<String>,
    pub bearer_token: String,
}

/// Authentication result returned from the in-memory hot path.
#[derive(Debug, Clone)]
pub struct AuthenticatedDevice {
    pub device_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationError {
    Unknown,
    Revoked,
}

#[derive(Debug)]
struct RegistryEntry {
    name: String,
    device_id: String,
    enrolled_at: u64,
    vaults: Vec<String>,
    bearer_token: String,
    last_seen: AtomicU64,
    persisted_last_seen: AtomicU64,
    revoked: AtomicBool,
    revoked_at: AtomicU64,
    session_cancelled: CancellationToken,
}

impl RegistryEntry {
    fn from_disk(info: DeviceInfo, revoked_at: Option<u64>) -> Self {
        let last_seen = info.last_seen;
        Self {
            name: info.name,
            device_id: info.device_id,
            enrolled_at: info.enrolled_at,
            vaults: info.vaults,
            bearer_token: info.bearer_token,
            last_seen: AtomicU64::new(last_seen),
            persisted_last_seen: AtomicU64::new(last_seen),
            revoked: AtomicBool::new(revoked_at.is_some()),
            revoked_at: AtomicU64::new(revoked_at.unwrap_or(0)),
            session_cancelled: CancellationToken::new(),
        }
    }

    fn snapshot(&self) -> DeviceInfo {
        DeviceInfo {
            name: self.name.clone(),
            device_id: self.device_id.clone(),
            enrolled_at: self.enrolled_at,
            last_seen: self.last_seen.load(Ordering::Acquire),
            vaults: self.vaults.clone(),
            bearer_token: self.bearer_token.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct RegistryMaps {
    by_token: HashMap<String, Arc<RegistryEntry>>,
    by_id: HashMap<String, Arc<RegistryEntry>>,
}

/// Process-local device index. Device files remain authoritative across a
/// restart; while the server is running this registry is the only auth read
/// path and the only writer of device metadata.
#[derive(Debug, Clone)]
pub struct DeviceRegistry {
    layout: StorageLayout,
    maps: Arc<RwLock<RegistryMaps>>,
    /// Serializes disk mutations without blocking auth readers. Callers run
    /// mutation methods on the reserved control-I/O pool.
    mutations: Arc<Mutex<()>>,
}

impl DeviceRegistry {
    pub fn load(layout: StorageLayout) -> Result<Self, std::io::Error> {
        let mut maps = RegistryMaps::default();
        for info in load_devices_strict(&layout)? {
            if !valid_token(&info.bearer_token) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("device {} has an invalid bearer token", info.device_id),
                ));
            }
            if maps.by_id.contains_key(&info.device_id) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("duplicate device id {}", info.device_id),
                ));
            }
            if maps.by_token.contains_key(&info.bearer_token) {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "duplicate bearer token in device registry",
                ));
            }
            let revoked = load_revocation_strict(&layout, &info.device_id)?;
            let entry = Arc::new(RegistryEntry::from_disk(info, revoked));
            maps.by_token
                .insert(entry.bearer_token.clone(), Arc::clone(&entry));
            maps.by_id.insert(entry.device_id.clone(), entry);
        }
        Ok(Self {
            layout,
            maps: Arc::new(RwLock::new(maps)),
            mutations: Arc::new(Mutex::new(())),
        })
    }

    /// One memory lookup performs token resolution, revocation checking, and
    /// the coalesced last-seen touch.
    pub fn authenticate(&self, token: &str) -> Result<AuthenticatedDevice, AuthenticationError> {
        if !valid_token(token) {
            return Err(AuthenticationError::Unknown);
        }
        let entry = self
            .maps
            .read()
            .expect("device registry poisoned")
            .by_token
            .get(token)
            .cloned()
            .ok_or(AuthenticationError::Unknown)?;
        if entry.revoked.load(Ordering::Acquire) {
            return Err(AuthenticationError::Revoked);
        }
        entry.last_seen.fetch_max(now_ms(), Ordering::AcqRel);
        Ok(AuthenticatedDevice {
            device_id: entry.device_id.clone(),
        })
    }

    pub fn is_revoked(&self, device_id: &str) -> bool {
        self.entry(device_id)
            .is_some_and(|entry| entry.revoked.load(Ordering::Acquire))
    }

    pub fn revoked_at(&self, device_id: &str) -> Option<u64> {
        let entry = self.entry(device_id)?;
        entry
            .revoked
            .load(Ordering::Acquire)
            .then(|| entry.revoked_at.load(Ordering::Acquire))
    }

    pub fn get(&self, device_id: &str) -> Option<DeviceInfo> {
        self.entry(device_id).map(|entry| entry.snapshot())
    }

    pub fn list(&self) -> Vec<DeviceInfo> {
        let mut devices: Vec<_> = self
            .maps
            .read()
            .expect("device registry poisoned")
            .by_id
            .values()
            .map(|entry| entry.snapshot())
            .collect();
        devices.sort_by_key(|device| std::cmp::Reverse(device.last_seen));
        devices
    }

    /// Request hot path: atomics only after the id lookup. Persistence is
    /// coalesced by `flush_last_seen` instead of rewriting JSON per request.
    #[cfg(test)]
    fn touch_last_seen(&self, device_id: &str) {
        let Some(entry) = self.entry(device_id) else {
            return;
        };
        entry.last_seen.fetch_max(now_ms(), Ordering::AcqRel);
    }

    pub fn session_token(&self, device_id: &str) -> Option<CancellationToken> {
        let entry = self.entry(device_id)?;
        if entry.revoked.load(Ordering::Acquire) {
            None
        } else {
            Some(entry.session_cancelled.clone())
        }
    }

    /// Durable registration first; only then does the token become visible
    /// to auth. Serialized with revoke/purge/last_seen flush.
    pub fn register(
        &self,
        device_id: &str,
        name: &str,
        bearer_token: &str,
    ) -> Result<(), std::io::Error> {
        if !valid_token(bearer_token) {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid bearer token"));
        }
        let _mutation = self
            .mutations
            .lock()
            .expect("device mutation lock poisoned");
        {
            let maps = self.maps.read().expect("device registry poisoned");
            if let Some(existing) = maps.by_id.get(device_id) {
                if existing.name == name
                    && existing.bearer_token == bearer_token
                    && !existing.revoked.load(Ordering::Acquire)
                {
                    // A process may have died after publishing the canonical
                    // device record but before the claim response. Retrying
                    // that exact enrollment is idempotent.
                    atomic_write(&self.layout.token_path(bearer_token), device_id.as_bytes())?;
                    return Ok(());
                }
                return Err(Error::new(
                    ErrorKind::AlreadyExists,
                    "device id already exists with different metadata",
                ));
            }
            if maps.by_token.contains_key(bearer_token) {
                return Err(Error::new(
                    ErrorKind::AlreadyExists,
                    "bearer token already belongs to another device",
                ));
            }
        }
        register_device(&self.layout, device_id, name, bearer_token)?;
        let info = get_device(&self.layout, device_id)
            .ok_or_else(|| Error::other("registered device could not be reloaded"))?;
        let entry = Arc::new(RegistryEntry::from_disk(info, None));
        let mut maps = self.maps.write().expect("device registry poisoned");
        maps.by_token
            .insert(entry.bearer_token.clone(), Arc::clone(&entry));
        maps.by_id.insert(entry.device_id.clone(), entry);
        Ok(())
    }

    /// Persist the marker before flipping memory state. Once this method
    /// returns, new HTTP auth fails and all active WS session tokens fire.
    pub fn revoke(&self, device_id: &str) -> Result<(), std::io::Error> {
        let _mutation = self
            .mutations
            .lock()
            .expect("device mutation lock poisoned");
        let entry = self
            .entry(device_id)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "device not found"))?;
        let timestamp = persist_revocation(&self.layout, device_id, now_ms())?;
        entry.revoked_at.store(timestamp, Ordering::Release);
        entry.revoked.store(true, Ordering::Release);
        entry.session_cancelled.cancel();
        Ok(())
    }

    /// Coalesce hot-path timestamps into at most one durable JSON replace per
    /// device per throttle window. `force` is used by shutdown/tests.
    pub fn flush_last_seen(&self, force: bool) -> Result<usize, std::io::Error> {
        let _mutation = self
            .mutations
            .lock()
            .expect("device mutation lock poisoned");
        let entries: Vec<_> = self
            .maps
            .read()
            .expect("device registry poisoned")
            .by_id
            .values()
            .cloned()
            .collect();
        let mut flushed = 0;
        let flush_now = now_ms();
        for entry in entries {
            let observed = entry.last_seen.load(Ordering::Acquire);
            let persisted = entry.persisted_last_seen.load(Ordering::Acquire);
            if observed <= persisted {
                continue;
            }
            if !force && flush_now.saturating_sub(persisted) < TOUCH_THROTTLE_MS {
                continue;
            }
            let mut info = entry.snapshot();
            info.last_seen = observed;
            persist_device_info(&self.layout, &info)?;
            entry.persisted_last_seen.store(observed, Ordering::Release);
            flushed += 1;
        }
        Ok(flushed)
    }

    pub fn purge_expired_revoked(&self, ttl_secs: u64) -> Result<usize, std::io::Error> {
        let _mutation = self
            .mutations
            .lock()
            .expect("device mutation lock poisoned");
        let now = now_ms();
        let ttl_ms = ttl_secs.saturating_mul(1000);
        let entries: Vec<_> = self
            .maps
            .read()
            .expect("device registry poisoned")
            .by_id
            .values()
            .cloned()
            .collect();
        let mut remove_ids = Vec::new();
        for entry in entries {
            if !entry.revoked.load(Ordering::Acquire) {
                continue;
            }
            let timestamp = entry.revoked_at.load(Ordering::Acquire);
            if timestamp == 0 {
                let timestamp = persist_revocation(&self.layout, &entry.device_id, now)?;
                entry.revoked_at.store(timestamp, Ordering::Release);
            } else if now.saturating_sub(timestamp) >= ttl_ms {
                delete_device(&self.layout, &entry.device_id)?;
                remove_ids.push(entry.device_id.clone());
            }
        }
        let removed = remove_ids.len();
        if removed > 0 {
            let mut maps = self.maps.write().expect("device registry poisoned");
            for device_id in remove_ids {
                if let Some(entry) = maps.by_id.remove(&device_id) {
                    entry.session_cancelled.cancel();
                    maps.by_token.remove(&entry.bearer_token);
                }
            }
        }
        Ok(removed)
    }

    fn entry(&self, device_id: &str) -> Option<Arc<RegistryEntry>> {
        self.maps
            .read()
            .expect("device registry poisoned")
            .by_id
            .get(device_id)
            .cloned()
    }
}

/// Persist a newly-enrolled device + index its bearer token for O(1) lookup.
pub fn register_device(
    layout: &StorageLayout,
    device_id: &str,
    name: &str,
    bearer_token: &str,
) -> Result<(), std::io::Error> {
    let dir = layout.device_dir(device_id);
    fs::create_dir_all(&dir)?;
    let token_root = layout.token_path("");
    let tokens_dir = token_root.parent().unwrap();
    fs::create_dir_all(tokens_dir)?;

    let now = now_ms();
    let info = DeviceInfo {
        name: name.to_string(),
        device_id: device_id.to_string(),
        enrolled_at: now,
        last_seen: now,
        vaults: vec![],
        bearer_token: bearer_token.to_string(),
    };

    // Publish the compatibility token index first. It is not authoritative
    // for runtime auth; a crash here leaves at most an inert stale index. The
    // canonical device.json is published last and is what startup loads.
    atomic_write(&layout.token_path(bearer_token), device_id.as_bytes())?;
    sync_directory(tokens_dir)?;
    persist_device_info(layout, &info)?;

    Ok(())
}

/// Look up the device_id that owns a bearer token.
#[cfg(test)]
pub fn lookup_token(layout: &StorageLayout, token: &str) -> Option<String> {
    if !valid_token(token) {
        return None;
    }
    fs::read_to_string(layout.token_path(token))
        .ok()
        .map(|s| s.trim().to_owned())
}

const TOUCH_THROTTLE_MS: u64 = 30_000;

/// Update last_seen for a device. Throttled to ~once per 30s per device so
/// big pushes don't rewrite device.json hundreds of times.
#[cfg(test)]
pub fn touch_last_seen(layout: &StorageLayout, device_id: &str) -> Result<(), std::io::Error> {
    let path = layout.device_dir(device_id).join("device.json");
    let data = fs::read_to_string(&path)?;
    let mut info: DeviceInfo = serde_json::from_str(&data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let now = now_ms();
    if now.saturating_sub(info.last_seen) < TOUCH_THROTTLE_MS {
        return Ok(());
    }

    info.last_seen = now;
    persist_device_info(layout, &info)?;
    Ok(())
}

/// Mark a device revoked — subsequent requests with its bearer token are
/// rejected. The revocation timestamp is written so a background sweep can
/// rotate the device out completely after the TTL.
#[cfg(test)]
pub fn revoke_device(layout: &StorageLayout, device_id: &str) -> Result<(), std::io::Error> {
    persist_revocation(layout, device_id, now_ms())?;
    Ok(())
}

#[cfg(test)]
pub fn is_revoked(layout: &StorageLayout, device_id: &str) -> bool {
    layout.device_dir(device_id).join("revoked").exists()
}

/// Epoch-ms when a device was revoked, or None if it isn't revoked. An old
/// empty `revoked` marker (written before timestamps) reads as `Some(0)`.
#[cfg(test)]
pub fn revoked_at(layout: &StorageLayout, device_id: &str) -> Option<u64> {
    let p = layout.device_dir(device_id).join("revoked");
    let s = fs::read_to_string(&p).ok()?;
    Some(s.trim().parse::<u64>().unwrap_or(0))
}

/// Remove a device completely: its token mapping FIRST — with the directory
/// gone a lingering token would otherwise look up a device_id whose
/// (now-absent) `revoked` marker no longer trips `is_revoked`, silently
/// un-revoking it — then the directory.
pub fn delete_device(layout: &StorageLayout, device_id: &str) -> Result<(), std::io::Error> {
    if let Some(info) = get_device(layout, device_id) {
        let token_path = layout.token_path(&info.bearer_token);
        let _ = fs::remove_file(&token_path);
        if let Some(parent) = token_path.parent() {
            let _ = sync_directory(parent);
        }
    }
    let dir = layout.device_dir(device_id);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
        if let Some(parent) = dir.parent() {
            sync_directory(parent)?;
        }
    }
    Ok(())
}

/// Delete revoked devices whose revocation is older than `ttl_secs`. Old empty
/// markers (pre-timestamp) get their clock started now, so they age out over
/// the next window instead of vanishing at once. Returns the count removed.
#[cfg(test)]
pub fn purge_expired_revoked(layout: &StorageLayout, ttl_secs: u64) -> usize {
    let now = now_ms();
    let ttl_ms = ttl_secs.saturating_mul(1000);
    let mut removed = 0;
    let Ok(entries) = fs::read_dir(layout.base.join("devices")) else {
        return 0;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        if entry.file_name() == "tokens" {
            continue;
        }
        let device_id = entry.file_name().to_string_lossy().to_string();
        match revoked_at(layout, &device_id) {
            None => {}
            Some(0) => {
                let _ = fs::write(entry.path().join("revoked"), now.to_string());
            }
            Some(ts) if now.saturating_sub(ts) >= ttl_ms => {
                removed += delete_device(layout, &device_id).is_ok() as usize;
            }
            Some(_) => {}
        }
    }
    removed
}

/// Grant a ONE-TIME guard bypass for this device's next push — lets an operator
/// approve an intentional bulk change (e.g. deleting a 40k-file build tree) that
/// the blast-radius guard would otherwise reject. Stored with an expiry so an
/// unused grant can't linger and weaken the guard indefinitely.
pub fn grant_deletion_bypass(
    layout: &StorageLayout,
    device_id: &str,
    ttl_secs: u64,
) -> Result<(), std::io::Error> {
    let expiry = now_ms().saturating_add(ttl_secs.saturating_mul(1000));
    atomic_write(
        &layout.device_dir(device_id).join("guard-bypass"),
        expiry.to_string().as_bytes(),
    )
}

/// Consume the one-time guard bypass: removes the marker (so it's used at most
/// once, even if it turns out to be expired) and returns whether it was valid.
pub fn consume_deletion_bypass(layout: &StorageLayout, device_id: &str) -> bool {
    let p = layout.device_dir(device_id).join("guard-bypass");
    let Ok(s) = fs::read_to_string(&p) else {
        return false;
    };
    let _ = fs::remove_file(&p);
    s.trim()
        .parse::<u64>()
        .map(|exp| now_ms() < exp)
        .unwrap_or(false)
}

/// Remaining ms on a pending (unexpired) bypass — for the admin UI; does NOT
/// consume it. `None` when there's no valid grant.
pub fn deletion_bypass_remaining_ms(layout: &StorageLayout, device_id: &str) -> Option<u64> {
    let s = fs::read_to_string(layout.device_dir(device_id).join("guard-bypass")).ok()?;
    let exp = s.trim().parse::<u64>().ok()?;
    exp.checked_sub(now_ms()).filter(|&r| r > 0)
}

#[cfg(test)]
pub fn list_devices(layout: &StorageLayout) -> Result<Vec<DeviceInfo>, std::io::Error> {
    let devices_dir = layout.base.join("devices");
    let mut devices = Vec::new();

    if !devices_dir.exists() {
        return Ok(devices);
    }

    for entry in fs::read_dir(&devices_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if entry.file_name() == "tokens" {
            continue;
        }
        let json_path = entry.path().join("device.json");
        if let Ok(data) = fs::read_to_string(&json_path) {
            if let Ok(info) = serde_json::from_str::<DeviceInfo>(&data) {
                devices.push(info);
            }
        }
    }

    devices.sort_by_key(|device| std::cmp::Reverse(device.last_seen));
    Ok(devices)
}

pub fn get_device(layout: &StorageLayout, device_id: &str) -> Option<DeviceInfo> {
    let path = layout.device_dir(device_id).join("device.json");
    let data = fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

fn load_devices_strict(layout: &StorageLayout) -> Result<Vec<DeviceInfo>, std::io::Error> {
    let devices_dir = layout.base.join("devices");
    if !devices_dir.exists() {
        return Ok(Vec::new());
    }
    let mut entries = fs::read_dir(&devices_dir)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_unstable_by_key(std::fs::DirEntry::file_name);
    let mut devices = Vec::new();
    for entry in entries {
        if !entry.file_type()?.is_dir() || entry.file_name() == "tokens" {
            continue;
        }
        let directory_id = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path().join("device.json");
        let data = match fs::read_to_string(&path) {
            Ok(data) => data,
            // A crash before registration publishes device.json can leave an
            // empty directory. With no canonical record there is no device;
            // ignore it rather than making the whole server unbootable.
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(Error::new(
                    error.kind(),
                    format!("read device metadata {}: {error}", path.display()),
                ))
            }
        };
        let info: DeviceInfo = serde_json::from_str(&data).map_err(|error| {
            Error::new(
                ErrorKind::InvalidData,
                format!("parse device metadata {}: {error}", path.display()),
            )
        })?;
        if info.device_id != directory_id {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "device metadata id {} does not match directory {}",
                    info.device_id, directory_id
                ),
            ));
        }
        devices.push(info);
    }
    Ok(devices)
}

fn persist_device_info(layout: &StorageLayout, info: &DeviceInfo) -> Result<(), std::io::Error> {
    let encoded = serde_json::to_vec_pretty(info).map_err(Error::other)?;
    atomic_write(
        &layout.device_dir(&info.device_id).join("device.json"),
        &encoded,
    )
}

fn persist_revocation(
    layout: &StorageLayout,
    device_id: &str,
    timestamp: u64,
) -> Result<u64, std::io::Error> {
    let path = layout.device_dir(device_id).join("revoked");
    if !path.parent().is_some_and(Path::exists) {
        return Err(Error::new(ErrorKind::NotFound, "device not found"));
    }
    atomic_write(&path, timestamp.to_string().as_bytes())?;
    Ok(timestamp)
}

fn load_revocation_strict(
    layout: &StorageLayout,
    device_id: &str,
) -> Result<Option<u64>, std::io::Error> {
    let path = layout.device_dir(device_id).join("revoked");
    let value = match fs::read_to_string(&path) {
        Ok(value) => value,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Error::new(
                error.kind(),
                format!("read revocation marker {}: {error}", path.display()),
            ))
        }
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(Some(0));
    }
    let timestamp = trimmed.parse::<u64>().map_err(|error| {
        Error::new(
            ErrorKind::InvalidData,
            format!("parse revocation marker {}: {error}", path.display()),
        )
    })?;
    Ok(Some(timestamp))
}

fn valid_token(token: &str) -> bool {
    token.len() == 64 && token.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Replace one small metadata file durably. The temp file is synced before
/// publication and the directory is synced after rename, so a successful
/// admin mutation survives a power loss before memory state is changed.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "metadata path has no parent"))?;
    fs::create_dir_all(parent)?;
    let nonce = rand::random::<u64>();
    let temp = path.with_extension(format!("tmp-{}-{nonce:016x}", std::process::id()));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        replace_file(&temp, path)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

#[cfg(not(windows))]
fn replace_file(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
    fs::rename(source, destination)
}

#[cfg(windows)]
fn replace_file(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
    // Rust's Windows rename cannot replace an existing file. This fallback
    // preserves write-before-publish ordering, though NTFS replacement is not
    // atomic across the short remove/rename gap.
    if destination.exists() {
        fs::remove_file(destination)?;
    }
    fs::rename(source, destination)
}

fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        fs::File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fresh_layout() -> (tempfile::TempDir, StorageLayout) {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        layout.init_directories().unwrap();
        (dir, layout)
    }

    fn token_64() -> String {
        "a".repeat(64)
    }

    #[test]
    fn register_creates_device_json_and_token_index() {
        let (_d, layout) = fresh_layout();
        register_device(&layout, "dev-id-1", "Desktop", &token_64()).unwrap();

        let info = get_device(&layout, "dev-id-1").expect("device should exist");
        assert_eq!(info.device_id, "dev-id-1");
        assert_eq!(info.name, "Desktop");
        assert_eq!(info.bearer_token, token_64());
        assert!(info.vaults.is_empty());

        // Token index must point back to the device id.
        assert_eq!(
            lookup_token(&layout, &token_64()),
            Some("dev-id-1".to_string())
        );
    }

    #[test]
    fn lookup_token_rejects_wrong_length() {
        let (_d, layout) = fresh_layout();
        register_device(&layout, "dev", "x", &token_64()).unwrap();
        // Short token — never even checks the filesystem.
        assert!(lookup_token(&layout, "short").is_none());
        // 65 chars — also rejected.
        let long = "a".repeat(65);
        assert!(lookup_token(&layout, &long).is_none());
    }

    #[test]
    fn lookup_token_rejects_non_hex() {
        let (_d, layout) = fresh_layout();
        // 64 chars but contains 'z'.
        let bad = "z".repeat(64);
        assert!(lookup_token(&layout, &bad).is_none());
    }

    #[test]
    fn lookup_token_returns_none_for_unknown_token() {
        let (_d, layout) = fresh_layout();
        let other = "b".repeat(64);
        assert!(lookup_token(&layout, &other).is_none());
    }

    #[test]
    fn revoke_and_is_revoked() {
        let (_d, layout) = fresh_layout();
        register_device(&layout, "dev", "x", &token_64()).unwrap();
        assert!(!is_revoked(&layout, "dev"));
        revoke_device(&layout, "dev").unwrap();
        assert!(is_revoked(&layout, "dev"));
        // A timestamp is recorded (not the old empty marker).
        assert!(revoked_at(&layout, "dev").unwrap() > 0);
    }

    #[test]
    fn delete_device_removes_dir_and_token() {
        let (_d, layout) = fresh_layout();
        register_device(&layout, "dev", "x", &token_64()).unwrap();
        assert!(lookup_token(&layout, &token_64()).is_some());
        delete_device(&layout, "dev").unwrap();
        // Gone entirely: no device.json, no token mapping (so the token can't
        // be looked up and wrongly slip past is_revoked).
        assert!(get_device(&layout, "dev").is_none());
        assert!(lookup_token(&layout, &token_64()).is_none());
    }

    #[test]
    fn purge_keeps_recent_revocations_removes_expired() {
        let (_d, layout) = fresh_layout();
        register_device(&layout, "fresh", "x", &"a".repeat(64)).unwrap();
        register_device(&layout, "stale", "y", &"b".repeat(64)).unwrap();
        revoke_device(&layout, "fresh").unwrap(); // revoked "now"
                                                  // Backdate "stale"'s revocation well past a 30-day TTL.
        let long_ago = now_ms().saturating_sub(31 * 86_400 * 1000);
        fs::write(
            layout.device_dir("stale").join("revoked"),
            long_ago.to_string(),
        )
        .unwrap();

        let removed = purge_expired_revoked(&layout, 30 * 86_400);
        assert_eq!(removed, 1);
        assert!(get_device(&layout, "fresh").is_some()); // within TTL — kept
        assert!(get_device(&layout, "stale").is_none()); // expired — gone
    }

    #[test]
    fn purge_starts_the_clock_on_old_empty_markers() {
        let (_d, layout) = fresh_layout();
        register_device(&layout, "old", "x", &token_64()).unwrap();
        // Simulate a pre-timestamp revoked marker (empty file).
        fs::write(layout.device_dir("old").join("revoked"), "").unwrap();
        assert_eq!(revoked_at(&layout, "old"), Some(0));

        let removed = purge_expired_revoked(&layout, 30 * 86_400);
        assert_eq!(removed, 0); // not deleted — clock only just started
        assert!(revoked_at(&layout, "old").unwrap() > 0); // now stamped
    }

    #[test]
    fn deletion_bypass_grant_then_consume_once() {
        let (_d, layout) = fresh_layout();
        register_device(&layout, "dev", "x", &token_64()).unwrap();
        assert!(deletion_bypass_remaining_ms(&layout, "dev").is_none());
        grant_deletion_bypass(&layout, "dev", 900).unwrap();
        assert!(deletion_bypass_remaining_ms(&layout, "dev").is_some());
        // First consume succeeds and removes the marker; a second one fails.
        assert!(consume_deletion_bypass(&layout, "dev"));
        assert!(deletion_bypass_remaining_ms(&layout, "dev").is_none());
        assert!(!consume_deletion_bypass(&layout, "dev"));
    }

    #[test]
    fn deletion_bypass_expired_is_not_valid() {
        let (_d, layout) = fresh_layout();
        register_device(&layout, "dev", "x", &token_64()).unwrap();
        // An already-expired marker.
        fs::write(layout.device_dir("dev").join("guard-bypass"), "1").unwrap();
        assert!(deletion_bypass_remaining_ms(&layout, "dev").is_none());
        // Consuming it removes the marker and reports invalid.
        assert!(!consume_deletion_bypass(&layout, "dev"));
        assert!(!layout.device_dir("dev").join("guard-bypass").exists());
    }

    #[test]
    fn get_device_missing_returns_none() {
        let (_d, layout) = fresh_layout();
        assert!(get_device(&layout, "nope").is_none());
    }

    #[test]
    fn list_devices_skips_tokens_dir_and_sorts_by_last_seen() {
        let (_d, layout) = fresh_layout();
        // Register two devices; manually backdate one's last_seen so we can
        // assert the sort order (most-recent-first).
        register_device(&layout, "old-dev", "Old", &"1".repeat(64)).unwrap();
        register_device(&layout, "new-dev", "New", &"2".repeat(64)).unwrap();

        // Backdate "old-dev".
        let old_path = layout.device_dir("old-dev").join("device.json");
        let mut info: DeviceInfo =
            serde_json::from_str(&fs::read_to_string(&old_path).unwrap()).unwrap();
        info.last_seen = 0;
        fs::write(&old_path, serde_json::to_string_pretty(&info).unwrap()).unwrap();

        let list = list_devices(&layout).unwrap();
        let ids: Vec<_> = list.iter().map(|d| d.device_id.as_str()).collect();
        assert_eq!(ids, vec!["new-dev", "old-dev"]);
    }

    #[test]
    fn list_devices_returns_empty_when_no_devices_dir() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path()); // no init_directories
        let list = list_devices(&layout).unwrap();
        assert!(list.is_empty());
    }

    #[test]
    fn touch_last_seen_throttle_skips_recent_writes() {
        let (_d, layout) = fresh_layout();
        register_device(&layout, "dev", "x", &token_64()).unwrap();

        let path = layout.device_dir("dev").join("device.json");
        let before: DeviceInfo = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();

        // Right after register, last_seen is `now`. A touch within 30s is throttled
        // and must NOT bump last_seen.
        touch_last_seen(&layout, "dev").unwrap();
        let after: DeviceInfo = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(before.last_seen, after.last_seen);
    }

    #[test]
    fn touch_last_seen_updates_after_throttle_window() {
        let (_d, layout) = fresh_layout();
        register_device(&layout, "dev", "x", &token_64()).unwrap();

        // Backdate last_seen so the throttle gate opens.
        let path = layout.device_dir("dev").join("device.json");
        let mut info: DeviceInfo =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        let original = info.last_seen;
        info.last_seen = 0;
        fs::write(&path, serde_json::to_string_pretty(&info).unwrap()).unwrap();

        touch_last_seen(&layout, "dev").unwrap();
        let updated: DeviceInfo =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert!(updated.last_seen > 0);
        assert!(updated.last_seen >= original);
    }

    #[test]
    fn device_info_serde_roundtrip() {
        let info = DeviceInfo {
            name: "n".into(),
            device_id: "id".into(),
            enrolled_at: 1,
            last_seen: 2,
            vaults: vec!["v1".into(), "v2".into()],
            bearer_token: token_64(),
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: DeviceInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.vaults, info.vaults);
        assert_eq!(back.bearer_token, info.bearer_token);
    }

    #[test]
    fn device_info_vaults_default_empty_when_missing() {
        // `vaults` is `#[serde(default)]` — older device.json without that key
        // must still parse.
        let json = r#"{
            "name": "n",
            "device_id": "id",
            "enrolled_at": 1,
            "last_seen": 2,
            "bearer_token": ""
        }"#;
        let info: DeviceInfo = serde_json::from_str(json).unwrap();
        assert!(info.vaults.is_empty());
    }

    #[test]
    fn registry_auth_is_memory_only_after_startup() {
        let (_d, layout) = fresh_layout();
        let token = token_64();
        register_device(&layout, "dev", "Desktop", &token).unwrap();
        let registry = DeviceRegistry::load(layout.clone()).unwrap();

        // Removing both legacy lookup files after startup cannot make the hot
        // path perform a filesystem read. This simulates an unavailable disk,
        // not a supported external mutation (the server remains sole writer).
        fs::remove_file(layout.token_path(&token)).unwrap();
        fs::remove_file(layout.device_dir("dev").join("device.json")).unwrap();
        let authenticated = registry.authenticate(&token).unwrap();
        assert_eq!(authenticated.device_id, "dev");
        assert_eq!(registry.get("dev").unwrap().name, "Desktop");
    }

    #[test]
    fn revoke_is_durable_then_immediate_and_cancels_sessions() {
        let (_d, layout) = fresh_layout();
        let token = token_64();
        register_device(&layout, "dev", "Desktop", &token).unwrap();
        let registry = DeviceRegistry::load(layout.clone()).unwrap();
        let session = registry.session_token("dev").unwrap();
        assert!(!session.is_cancelled());

        registry.revoke("dev").unwrap();

        assert!(layout.device_dir("dev").join("revoked").exists());
        assert_eq!(
            registry.authenticate(&token).unwrap_err(),
            AuthenticationError::Revoked
        );
        assert!(session.is_cancelled());
        assert!(registry.session_token("dev").is_none());
    }

    #[test]
    fn failed_revoke_does_not_change_memory_or_cancel_sessions() {
        let (_d, layout) = fresh_layout();
        let token = token_64();
        register_device(&layout, "dev", "Desktop", &token).unwrap();
        let registry = DeviceRegistry::load(layout.clone()).unwrap();
        let session = registry.session_token("dev").unwrap();
        fs::create_dir(layout.device_dir("dev").join("revoked")).unwrap();

        assert!(registry.revoke("dev").is_err());
        assert_eq!(registry.authenticate(&token).unwrap().device_id, "dev");
        assert!(!session.is_cancelled());
    }

    #[test]
    fn one_revoke_cancels_every_concurrent_session_handle() {
        let (_d, layout) = fresh_layout();
        let token = token_64();
        register_device(&layout, "dev", "Desktop", &token).unwrap();
        let registry = DeviceRegistry::load(layout).unwrap();
        let sessions: Vec<_> = (0..128)
            .map(|_| registry.session_token("dev").unwrap())
            .collect();

        registry.revoke("dev").unwrap();

        assert!(sessions.iter().all(CancellationToken::is_cancelled));
        for _ in 0..1_000 {
            assert_eq!(
                registry.authenticate(&token).unwrap_err(),
                AuthenticationError::Revoked
            );
        }
    }

    #[test]
    fn restart_reconstructs_registry_parity_from_device_files() {
        let (_d, layout) = fresh_layout();
        let live_token = "a".repeat(64);
        let revoked_token = "b".repeat(64);
        register_device(&layout, "live", "Laptop", &live_token).unwrap();
        register_device(&layout, "gone", "Phone", &revoked_token).unwrap();

        let live_path = layout.device_dir("live").join("device.json");
        let mut live = get_device(&layout, "live").unwrap();
        live.last_seen = 123_456;
        fs::write(&live_path, serde_json::to_vec_pretty(&live).unwrap()).unwrap();
        revoke_device(&layout, "gone").unwrap();

        let first = DeviceRegistry::load(layout.clone()).unwrap();
        let second = DeviceRegistry::load(layout).unwrap();
        for registry in [&first, &second] {
            let restored = registry.get("live").unwrap();
            assert_eq!(restored.last_seen, 123_456);
            assert_eq!(restored.name, "Laptop");

            assert_eq!(
                registry.authenticate(&live_token).unwrap().device_id,
                "live"
            );
            assert!(registry.get("live").unwrap().last_seen >= 123_456);
            assert_eq!(
                registry.authenticate(&revoked_token).unwrap_err(),
                AuthenticationError::Revoked
            );
            assert!(registry.revoked_at("gone").is_some());
        }
    }

    #[test]
    fn last_seen_touches_are_coalesced_until_flush() {
        let (_d, layout) = fresh_layout();
        let token = token_64();
        register_device(&layout, "dev", "Desktop", &token).unwrap();
        let path = layout.device_dir("dev").join("device.json");
        let mut on_disk = get_device(&layout, "dev").unwrap();
        on_disk.last_seen = 1;
        fs::write(&path, serde_json::to_vec_pretty(&on_disk).unwrap()).unwrap();
        let registry = DeviceRegistry::load(layout.clone()).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(2));
        registry.touch_last_seen("dev");
        registry.touch_last_seen("dev");
        registry.touch_last_seen("dev");
        assert_eq!(get_device(&layout, "dev").unwrap().last_seen, 1);
        assert_eq!(registry.flush_last_seen(false).unwrap(), 1);
        let persisted = get_device(&layout, "dev").unwrap().last_seen;
        assert!(persisted > 1);

        std::thread::sleep(std::time::Duration::from_millis(2));
        registry.touch_last_seen("dev");
        assert_eq!(registry.flush_last_seen(false).unwrap(), 0);
        assert_eq!(registry.flush_last_seen(true).unwrap(), 1);
    }

    #[test]
    fn runtime_registration_is_durable_before_auth_visibility() {
        let (_d, layout) = fresh_layout();
        let registry = DeviceRegistry::load(layout.clone()).unwrap();
        let token = token_64();
        registry.register("new", "Tablet", &token).unwrap();

        assert_eq!(registry.authenticate(&token).unwrap().device_id, "new");
        assert_eq!(lookup_token(&layout, &token).as_deref(), Some("new"));
        assert_eq!(get_device(&layout, "new").unwrap().name, "Tablet");
        assert_eq!(DeviceRegistry::load(layout).unwrap().list().len(), 1);
    }

    #[test]
    fn identical_registration_retry_after_restart_is_idempotent() {
        let (_d, layout) = fresh_layout();
        let token = token_64();
        register_device(&layout, "dev", "Desktop", &token).unwrap();
        let registry = DeviceRegistry::load(layout.clone()).unwrap();

        registry.register("dev", "Desktop", &token).unwrap();
        assert_eq!(registry.list().len(), 1);
        assert_eq!(registry.authenticate(&token).unwrap().device_id, "dev");
        assert_eq!(get_device(&layout, "dev").unwrap().name, "Desktop");
    }

    #[test]
    fn strict_startup_rejects_corrupt_or_misplaced_metadata() {
        let (_d, layout) = fresh_layout();
        let dir = layout.device_dir("directory-id");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("device.json"), b"not json").unwrap();
        assert_eq!(
            DeviceRegistry::load(layout.clone()).unwrap_err().kind(),
            ErrorKind::InvalidData
        );

        let info = DeviceInfo {
            name: "x".into(),
            device_id: "different-id".into(),
            enrolled_at: 1,
            last_seen: 1,
            vaults: Vec::new(),
            bearer_token: token_64(),
        };
        fs::write(dir.join("device.json"), serde_json::to_vec(&info).unwrap()).unwrap();
        assert_eq!(
            DeviceRegistry::load(layout).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn startup_ignores_unpublished_device_directory_but_rejects_bad_revoke_marker() {
        let (_d, layout) = fresh_layout();
        fs::create_dir_all(layout.device_dir("orphan")).unwrap();
        assert!(DeviceRegistry::load(layout.clone())
            .unwrap()
            .list()
            .is_empty());

        register_device(&layout, "dev", "Desktop", &token_64()).unwrap();
        fs::write(layout.device_dir("dev").join("revoked"), "not-a-time").unwrap();
        assert_eq!(
            DeviceRegistry::load(layout).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }
}
