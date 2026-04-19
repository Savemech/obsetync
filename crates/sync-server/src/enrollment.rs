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

    devices::register_device(layout, &info.device_id, &info.device_name, &info.bearer_token)
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
