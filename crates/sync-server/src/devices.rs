use crate::storage::StorageLayout;
use std::fs;

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

/// Persist a newly-enrolled device + index its bearer token for O(1) lookup.
pub fn register_device(
    layout: &StorageLayout,
    device_id: &str,
    name: &str,
    bearer_token: &str,
) -> Result<(), std::io::Error> {
    let dir = layout.device_dir(device_id);
    fs::create_dir_all(&dir)?;
    fs::create_dir_all(layout.token_path("").parent().unwrap())?;

    let now = now_ms();
    let info = DeviceInfo {
        name: name.to_string(),
        device_id: device_id.to_string(),
        enrolled_at: now,
        last_seen: now,
        vaults: vec![],
        bearer_token: bearer_token.to_string(),
    };

    let json = serde_json::to_string_pretty(&info)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(dir.join("device.json"), json)?;
    fs::write(layout.token_path(bearer_token), device_id)?;

    Ok(())
}

/// Look up the device_id that owns a bearer token.
pub fn lookup_token(layout: &StorageLayout, token: &str) -> Option<String> {
    if token.len() != 64 || !token.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    fs::read_to_string(layout.token_path(token))
        .ok()
        .map(|s| s.trim().to_owned())
}

const TOUCH_THROTTLE_MS: u64 = 30_000;

/// Update last_seen for a device. Throttled to ~once per 30s per device so
/// big pushes don't rewrite device.json hundreds of times.
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
    let json = serde_json::to_string_pretty(&info)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(&path, json)?;
    Ok(())
}

/// Mark a device revoked — subsequent requests with its bearer token are rejected.
pub fn revoke_device(layout: &StorageLayout, device_id: &str) -> Result<(), std::io::Error> {
    let dir = layout.device_dir(device_id);
    fs::write(dir.join("revoked"), "")?;
    Ok(())
}

pub fn is_revoked(layout: &StorageLayout, device_id: &str) -> bool {
    layout.device_dir(device_id).join("revoked").exists()
}

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

    devices.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
    Ok(devices)
}

pub fn get_device(layout: &StorageLayout, device_id: &str) -> Option<DeviceInfo> {
    let path = layout.device_dir(device_id).join("device.json");
    let data = fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
