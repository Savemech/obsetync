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

/// Mark a device revoked — subsequent requests with its bearer token are
/// rejected. The revocation timestamp is written so a background sweep can
/// rotate the device out completely after the TTL.
pub fn revoke_device(layout: &StorageLayout, device_id: &str) -> Result<(), std::io::Error> {
    let dir = layout.device_dir(device_id);
    fs::write(dir.join("revoked"), now_ms().to_string())?;
    Ok(())
}

pub fn is_revoked(layout: &StorageLayout, device_id: &str) -> bool {
    layout.device_dir(device_id).join("revoked").exists()
}

/// Epoch-ms when a device was revoked, or None if it isn't revoked. An old
/// empty `revoked` marker (written before timestamps) reads as `Some(0)`.
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
        let _ = fs::remove_file(layout.token_path(&info.bearer_token));
    }
    let dir = layout.device_dir(device_id);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    Ok(())
}

/// Delete revoked devices whose revocation is older than `ttl_secs`. Old empty
/// markers (pre-timestamp) get their clock started now, so they age out over
/// the next window instead of vanishing at once. Returns the count removed.
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
    fs::write(
        layout.device_dir(device_id).join("guard-bypass"),
        expiry.to_string(),
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
}
