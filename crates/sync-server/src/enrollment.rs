use crate::ca;
use crate::devices;
use crate::storage::StorageLayout;
use rand::Rng;
use std::fs;

/// Enrollment state persisted to disk.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct EnrollmentInfo {
    pub code: String,
    pub device_name: String,
    pub cert_pem: String,
    pub key_pem: String,
    pub fingerprint: String,
    /// 256-bit random token (64 hex chars) for bearer-token auth.
    /// Desktop clients send this on every request; mobile clients use it
    /// as their sole auth mechanism (no mTLS client cert from iOS JS).
    pub bearer_token: String,
    pub created_at: u64,
    pub expires_at: u64,
}

/// Create a new enrollment: generate cert + bearer token, create enrollment code.
/// Returns the enrollment code.
pub fn create_enrollment(
    layout: &StorageLayout,
    device_name: &str,
) -> Result<EnrollmentInfo, Box<dyn std::error::Error>> {
    let code = generate_code();
    let (cert_pem, key_pem, fingerprint) = ca::generate_client_cert(layout, device_name)?;
    let bearer_token = generate_bearer_token();

    let now = now_ms();
    let info = EnrollmentInfo {
        code: code.clone(),
        device_name: device_name.to_string(),
        cert_pem,
        key_pem,
        fingerprint,
        bearer_token,
        created_at: now,
        expires_at: now + 10 * 60 * 1000, // 10 minutes
    };

    let path = layout.enrollment_path(&code);
    let json = serde_json::to_string_pretty(&info)?;
    fs::write(&path, json)?;

    Ok(info)
}

/// Claim an enrollment: verify code, register device, return cert bundle.
/// Deletes the enrollment after claiming.
pub fn claim_enrollment(
    layout: &StorageLayout,
    code: &str,
) -> Result<EnrollmentInfo, String> {
    let path = layout.enrollment_path(code);
    let data = fs::read_to_string(&path).map_err(|_| "enrollment code not found".to_string())?;
    let info: EnrollmentInfo =
        serde_json::from_str(&data).map_err(|e| format!("corrupt enrollment: {}", e))?;

    // Check expiry.
    let now = now_ms();
    if now > info.expires_at {
        let _ = fs::remove_file(&path);
        return Err("enrollment code expired".into());
    }

    // Register the device (stores cert + bearer token index).
    devices::register_device(
        layout,
        &info.fingerprint,
        &info.device_name,
        &info.cert_pem,
        Some(&info.bearer_token),
    )
    .map_err(|e| format!("device registration failed: {}", e))?;

    // Delete the enrollment.
    let _ = fs::remove_file(&path);

    Ok(info)
}

/// Generate a short enrollment code like "AXBR-7742".
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

/// Generate a 256-bit cryptographically random bearer token (64 hex chars).
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
