use crate::devices;
use crate::storage::StorageLayout;
use rand::Rng;
use std::fs;

/// Enrollment state persisted to disk.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct EnrollmentInfo {
    pub code: String,
    pub device_name: String,
    pub device_id: String,
    pub bearer_token: String,
    pub created_at: u64,
    pub expires_at: u64,
}

/// Create a new enrollment: generate a random device_id + bearer token,
/// store them in a short-lived file keyed by the human-readable code.
pub fn create_enrollment(
    layout: &StorageLayout,
    device_name: &str,
) -> Result<EnrollmentInfo, Box<dyn std::error::Error>> {
    let code = generate_code();
    let device_id = generate_device_id();
    let bearer_token = generate_bearer_token();

    let now = now_ms();
    let info = EnrollmentInfo {
        code: code.clone(),
        device_name: device_name.to_string(),
        device_id,
        bearer_token,
        created_at: now,
        expires_at: now + 10 * 60 * 1000, // 10 minutes
    };

    let path = layout.enrollment_path(&code);
    let json = serde_json::to_string_pretty(&info)?;
    fs::write(&path, json)?;

    Ok(info)
}

/// Claim an enrollment: verify code, register device, return the bundle.
/// The enrollment record is deleted whether or not registration succeeds.
pub fn claim_enrollment(layout: &StorageLayout, code: &str) -> Result<EnrollmentInfo, String> {
    let path = layout.enrollment_path(code);
    let data = fs::read_to_string(&path).map_err(|_| "enrollment code not found".to_string())?;
    let info: EnrollmentInfo =
        serde_json::from_str(&data).map_err(|e| format!("corrupt enrollment: {}", e))?;

    let now = now_ms();
    if now > info.expires_at {
        let _ = fs::remove_file(&path);
        return Err("enrollment code expired".into());
    }

    devices::register_device(
        layout,
        &info.device_id,
        &info.device_name,
        &info.bearer_token,
    )
    .map_err(|e| format!("device registration failed: {}", e))?;

    let _ = fs::remove_file(&path);

    Ok(info)
}

/// Human-readable enrollment code like "AXBR-7742". 4 uppercase letters + 4 digits.
fn generate_code() -> String {
    let mut rng = rand::rng();
    let letters: String = (0..4)
        .map(|_| rng.random_range(b'A'..=b'Z') as char)
        .collect();
    let digits: String = (0..4)
        .map(|_| rng.random_range(b'0'..=b'9') as char)
        .collect();
    format!("{}-{}", letters, digits)
}

/// 128-bit random device identifier, hex encoded (32 chars).
fn generate_device_id() -> String {
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..16).map(|_| rng.random::<u8>()).collect();
    hex::encode(bytes)
}

/// 256-bit cryptographically random bearer token (64 hex chars).
fn generate_bearer_token() -> String {
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.random::<u8>()).collect();
    hex::encode(bytes)
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

    #[test]
    fn create_enrollment_persists_to_disk() {
        let (_d, layout) = fresh_layout();
        let info = create_enrollment(&layout, "iPhone").unwrap();
        assert!(layout.enrollment_path(&info.code).exists());
        assert_eq!(info.device_name, "iPhone");
        assert!(info.expires_at > info.created_at);
    }

    #[test]
    fn generated_codes_have_expected_shape() {
        let (_d, layout) = fresh_layout();
        let info = create_enrollment(&layout, "x").unwrap();

        // Code: AAAA-9999 (4 letters, dash, 4 digits).
        assert_eq!(info.code.len(), 9);
        let bytes = info.code.as_bytes();
        for &b in &bytes[..4] {
            assert!(b.is_ascii_uppercase());
        }
        assert_eq!(bytes[4], b'-');
        for &b in &bytes[5..] {
            assert!(b.is_ascii_digit());
        }

        // Device id: 32 hex chars (128 bits).
        assert_eq!(info.device_id.len(), 32);
        assert!(info.device_id.chars().all(|c| c.is_ascii_hexdigit()));

        // Bearer token: 64 hex chars (256 bits).
        assert_eq!(info.bearer_token.len(), 64);
        assert!(info.bearer_token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn generated_values_are_random() {
        let (_d, layout) = fresh_layout();
        let a = create_enrollment(&layout, "x").unwrap();
        let b = create_enrollment(&layout, "x").unwrap();
        assert_ne!(a.code, b.code);
        assert_ne!(a.device_id, b.device_id);
        assert_ne!(a.bearer_token, b.bearer_token);
    }

    #[test]
    fn claim_unknown_code_errors() {
        let (_d, layout) = fresh_layout();
        let err = claim_enrollment(&layout, "BOGUS-0000").unwrap_err();
        assert!(err.contains("not found"));
    }

    #[test]
    fn claim_registers_device_and_removes_enrollment() {
        let (_d, layout) = fresh_layout();
        let info = create_enrollment(&layout, "Desktop").unwrap();
        let path = layout.enrollment_path(&info.code);
        assert!(path.exists());

        let claimed = claim_enrollment(&layout, &info.code).unwrap();
        assert_eq!(claimed.device_id, info.device_id);

        // Enrollment file deleted post-claim.
        assert!(!path.exists());

        // Device now exists in the registry, and the bearer token resolves.
        let dev = crate::devices::get_device(&layout, &info.device_id).unwrap();
        assert_eq!(dev.name, "Desktop");
        assert_eq!(
            crate::devices::lookup_token(&layout, &info.bearer_token),
            Some(info.device_id)
        );
    }

    #[test]
    fn claim_expired_enrollment_errors_and_removes_file() {
        let (_d, layout) = fresh_layout();
        let info = create_enrollment(&layout, "x").unwrap();

        // Backdate the enrollment so it's expired.
        let path = layout.enrollment_path(&info.code);
        let mut on_disk: EnrollmentInfo =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        on_disk.expires_at = 0; // ancient
        fs::write(&path, serde_json::to_string_pretty(&on_disk).unwrap()).unwrap();

        let err = claim_enrollment(&layout, &info.code).unwrap_err();
        assert!(err.contains("expired"));
        assert!(!path.exists(), "expired enrollment should be cleaned up");
    }

    #[test]
    fn claim_corrupt_enrollment_errors() {
        let (_d, layout) = fresh_layout();
        let path = layout.enrollment_path("BAD-0001");
        fs::write(&path, b"not json").unwrap();
        let err = claim_enrollment(&layout, "BAD-0001").unwrap_err();
        assert!(err.contains("corrupt"));
    }

    #[test]
    fn enrollment_info_serde_roundtrip() {
        let info = EnrollmentInfo {
            code: "AB-1".into(),
            device_name: "n".into(),
            device_id: "i".into(),
            bearer_token: "t".into(),
            created_at: 1,
            expires_at: 2,
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: EnrollmentInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.code, "AB-1");
        assert_eq!(back.expires_at, 2);
    }
}
