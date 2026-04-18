use crate::storage::StorageLayout;
use std::fs;

/// Information about an enrolled device.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeviceInfo {
    pub name: String,
    pub fingerprint: String,
    pub enrolled_at: u64,
    pub last_seen: u64,
    pub vaults: Vec<String>,
    /// Bearer token for mobile clients (iOS) that cannot present mTLS certs.
    /// Desktop clients also receive this token and send it on every request.
    #[serde(default)]
    pub bearer_token: Option<String>,
}

/// Register a new device after enrollment.
pub fn register_device(
    layout: &StorageLayout,
    fingerprint: &str,
    name: &str,
    cert_pem: &str,
    bearer_token: Option<&str>,
) -> Result<(), std::io::Error> {
    let dir = layout.device_dir(fingerprint);
    fs::create_dir_all(&dir)?;
    // Ensure token index dir exists even on existing installs.
    fs::create_dir_all(layout.token_path("").parent().unwrap())?;

    let now = now_ms();
    let info = DeviceInfo {
        name: name.to_string(),
        fingerprint: fingerprint.to_string(),
        enrolled_at: now,
        last_seen: now,
        vaults: vec![],
        bearer_token: bearer_token.map(str::to_owned),
    };

    let json = serde_json::to_string_pretty(&info)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(dir.join("device.json"), json)?;
    fs::write(dir.join("client.crt"), cert_pem)?;

    // Write token → fingerprint index for O(1) lookup.
    if let Some(token) = bearer_token {
        fs::write(layout.token_path(token), fingerprint)?;
    }

    Ok(())
}

/// Look up which device owns a bearer token. Returns the fingerprint, or None.
pub fn lookup_token(layout: &StorageLayout, token: &str) -> Option<String> {
    // Basic sanity: tokens are 64 lowercase hex chars.
    if token.len() != 64 || !token.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    fs::read_to_string(layout.token_path(token))
        .ok()
        .map(|s| s.trim().to_owned())
}

/// Update the last_seen timestamp for a device.
#[allow(dead_code)]
pub fn update_last_seen(layout: &StorageLayout, fingerprint: &str) -> Result<(), std::io::Error> {
    let path = layout.device_dir(fingerprint).join("device.json");
    let data = fs::read_to_string(&path)?;
    let mut info: DeviceInfo = serde_json::from_str(&data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    info.last_seen = now_ms();
    let json = serde_json::to_string_pretty(&info)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write(&path, json)?;
    Ok(())
}

/// Revoke a device — writes a marker file.
pub fn revoke_device(layout: &StorageLayout, fingerprint: &str) -> Result<(), std::io::Error> {
    let dir = layout.device_dir(fingerprint);
    fs::write(dir.join("revoked"), "")?;
    Ok(())
}

/// Check if a device is revoked.
pub fn is_revoked(layout: &StorageLayout, fingerprint: &str) -> bool {
    layout.device_dir(fingerprint).join("revoked").exists()
}

/// List all enrolled devices.
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
        // Skip the tokens index directory.
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

/// Get a single device by fingerprint.
pub fn get_device(layout: &StorageLayout, fingerprint: &str) -> Option<DeviceInfo> {
    let path = layout.device_dir(fingerprint).join("device.json");
    let data = fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
