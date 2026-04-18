use crate::storage::StorageLayout;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};
use std::fs;

/// Initialize the private CA: generate CA keypair and self-signed cert.
pub fn init_ca(layout: &StorageLayout) -> Result<(), Box<dyn std::error::Error>> {
    let ca_dir = layout.base.join("ca");
    fs::create_dir_all(&ca_dir)?;

    let key_pair = KeyPair::generate()?;

    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "ObsetyNC CA");
    params
        .distinguished_name
        .push(DnType::OrganizationName, "ObsetyNC");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.not_before = rcgen::date_time_ymd(2024, 1, 1);
    params.not_after = rcgen::date_time_ymd(2034, 1, 1);

    let ca_cert = params.self_signed(&key_pair)?;

    fs::write(ca_dir.join("ca.crt"), ca_cert.pem())?;
    fs::write(ca_dir.join("ca.key"), key_pair.serialize_pem())?;
    // Also store the DER for easy reloading.
    fs::write(ca_dir.join("ca.der"), ca_cert.der())?;
    fs::write(ca_dir.join("serial"), "1")?;

    tracing::info!("CA initialized");
    Ok(())
}

/// Generate and sign the server certificate using the CA.
pub fn init_server_cert(layout: &StorageLayout) -> Result<(), Box<dyn std::error::Error>> {
    let server_dir = layout.base.join("server");
    fs::create_dir_all(&server_dir)?;

    let (ca_key, ca_cert) = load_ca(layout)?;

    let server_key = KeyPair::generate()?;
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "ObsetyNC Server");
    params.not_before = rcgen::date_time_ymd(2024, 1, 1);
    params.not_after = rcgen::date_time_ymd(2034, 1, 1);
    params.subject_alt_names = vec![rcgen::SanType::DnsName("localhost".try_into()?)];

    let server_cert = params.signed_by(&server_key, &ca_cert, &ca_key)?;

    fs::write(server_dir.join("server.crt"), server_cert.pem())?;
    fs::write(server_dir.join("server.key"), server_key.serialize_pem())?;

    tracing::info!("server certificate generated");
    Ok(())
}

/// Generate a client certificate for a device.
/// Returns (cert_pem, key_pem, fingerprint_hex).
pub fn generate_client_cert(
    layout: &StorageLayout,
    device_name: &str,
) -> Result<(String, String, String), Box<dyn std::error::Error>> {
    let (ca_key, ca_cert) = load_ca(layout)?;

    let client_key = KeyPair::generate()?;
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, device_name);
    params
        .distinguished_name
        .push(DnType::OrganizationName, "ObsetyNC Device");
    params.not_before = rcgen::date_time_ymd(2024, 1, 1);
    params.not_after = rcgen::date_time_ymd(2034, 1, 1);

    let serial = next_serial(layout)?;
    params.serial_number = Some(rcgen::SerialNumber::from_slice(&serial.to_be_bytes()));

    let client_cert = params.signed_by(&client_key, &ca_cert, &ca_key)?;

    let cert_pem = client_cert.pem();
    let key_pem = client_key.serialize_pem();

    // Fingerprint: SHA-256 of DER cert.
    use sha2::Digest;
    let fingerprint = sha2::Sha256::digest(client_cert.der());
    let fingerprint_hex = hex::encode(fingerprint);

    Ok((cert_pem, key_pem, fingerprint_hex))
}

/// Load the CA cert and key for signing operations.
/// Returns (KeyPair, Certificate) — the Certificate is reconstructed from DER + params.
fn load_ca(
    layout: &StorageLayout,
) -> Result<(KeyPair, rcgen::Certificate), Box<dyn std::error::Error>> {
    let ca_dir = layout.base.join("ca");
    let ca_key_pem = fs::read_to_string(ca_dir.join("ca.key"))?;
    let ca_cert_pem = fs::read_to_string(ca_dir.join("ca.crt"))?;

    let ca_key = KeyPair::from_pem(&ca_key_pem)?;

    // Reconstruct the CA certificate from PEM using from_ca_cert_pem + self_signed.
    let ca_params = CertificateParams::from_ca_cert_pem(&ca_cert_pem)?;
    let ca_cert = ca_params.self_signed(&ca_key)?;

    Ok((ca_key, ca_cert))
}

/// Load CA certificate DER bytes (for TLS trust store).
pub fn load_ca_cert_der(layout: &StorageLayout) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let der_path = layout.base.join("ca/ca.der");
    if der_path.exists() {
        return Ok(fs::read(&der_path)?);
    }
    // Fallback: parse from PEM.
    let pem_str = fs::read_to_string(layout.base.join("ca/ca.crt"))?;
    let parsed = pem::parse(pem_str.as_bytes()).map_err(|e| format!("PEM parse error: {}", e))?;
    Ok(parsed.contents().to_vec())
}

/// Load server cert chain (DER) and private key (DER).
pub fn load_server_cert_and_key(
    layout: &StorageLayout,
) -> Result<(Vec<Vec<u8>>, Vec<u8>), Box<dyn std::error::Error>> {
    let cert_pem = fs::read_to_string(layout.base.join("server/server.crt"))?;
    let key_pem = fs::read_to_string(layout.base.join("server/server.key"))?;

    let cert_parsed =
        pem::parse(cert_pem.as_bytes()).map_err(|e| format!("cert PEM parse: {}", e))?;
    let key_parsed = pem::parse(key_pem.as_bytes()).map_err(|e| format!("key PEM parse: {}", e))?;

    Ok((
        vec![cert_parsed.contents().to_vec()],
        key_parsed.contents().to_vec(),
    ))
}

fn next_serial(layout: &StorageLayout) -> Result<u64, Box<dyn std::error::Error>> {
    let serial_path = layout.base.join("ca/serial");
    let current: u64 = fs::read_to_string(&serial_path)?.trim().parse()?;
    let next = current + 1;
    fs::write(&serial_path, next.to_string())?;
    Ok(current)
}
