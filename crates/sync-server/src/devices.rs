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
        let before: DeviceInfo =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();

        // Right after register, last_seen is `now`. A touch within 30s is throttled
        // and must NOT bump last_seen.
        touch_last_seen(&layout, "dev").unwrap();
        let after: DeviceInfo =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
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
}
