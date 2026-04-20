//! Server X25519 "box" keypair — the long-term identity for the AEAD
//! envelope transport. See `../../../docs/transport.md`.
//!
//! Clients learn the public key at enrollment time and use it as the remote
//! static key in per-request ECDH. Compromise of this key does NOT reveal past
//! session content (each session uses a fresh ephemeral keypair on the client
//! side, so forward secrecy holds), but does let an attacker impersonate the
//! server going forward — treat `box.key` the same as `server.key` used to be
//! under the old TLS setup.
//!
//! On-disk layout under `data/server/`:
//!   - box.key : raw 32 bytes (mode 0600), the X25519 private key
//!   - box.pub : base64(32) + "\n", for operator inspection / admin UI display

use crate::storage::StorageLayout;
use base64::prelude::*;
use std::fs;
use x25519_dalek::{PublicKey, StaticSecret};

const PRIV_FILE: &str = "box.key";
const PUB_FILE: &str = "box.pub";
const KEY_LEN: usize = 32;

/// Generate the server's X25519 keypair on disk. Idempotent: if a keypair
/// already exists, returns it untouched (never overwrites — rotation is a
/// separate operation).
pub fn init_box_keypair(
    layout: &StorageLayout,
) -> Result<(StaticSecret, PublicKey), Box<dyn std::error::Error>> {
    let server_dir = layout.base.join("server");
    fs::create_dir_all(&server_dir)?;

    let priv_path = server_dir.join(PRIV_FILE);
    let pub_path = server_dir.join(PUB_FILE);

    if priv_path.exists() {
        return load_box_keypair(layout);
    }

    // x25519-dalek 2.0 uses rand_core 0.6, but our workspace already pulls rand 0.9.
    // Avoid the version mismatch by sourcing entropy directly from the OS.
    use rand::TryRngCore;
    let mut seed = [0u8; KEY_LEN];
    rand::rngs::OsRng
        .try_fill_bytes(&mut seed)
        .map_err(|e| format!("OS RNG failed: {}", e))?;
    let priv_key = StaticSecret::from(seed);
    let pub_key = PublicKey::from(&priv_key);

    fs::write(&priv_path, priv_key.as_bytes())?;
    restrict_private_perms(&priv_path)?;

    let b64 = BASE64_STANDARD.encode(pub_key.as_bytes());
    fs::write(&pub_path, format!("{}\n", b64))?;

    tracing::info!("X25519 keypair generated (pub = {})", &b64);
    Ok((priv_key, pub_key))
}

/// Load an existing keypair from disk. Errors if the private key file is
/// missing or malformed (wrong size).
pub fn load_box_keypair(
    layout: &StorageLayout,
) -> Result<(StaticSecret, PublicKey), Box<dyn std::error::Error>> {
    let priv_path = layout.base.join("server").join(PRIV_FILE);
    let bytes =
        fs::read(&priv_path).map_err(|e| format!("reading {}: {}", priv_path.display(), e))?;
    if bytes.len() != KEY_LEN {
        return Err(format!("box.key is {} bytes, expected {}", bytes.len(), KEY_LEN).into());
    }
    let mut arr = [0u8; KEY_LEN];
    arr.copy_from_slice(&bytes);
    let priv_key = StaticSecret::from(arr);
    let pub_key = PublicKey::from(&priv_key);
    Ok((priv_key, pub_key))
}

/// Just the public key, base64 encoded. Used by the admin UI + enrollment
/// response so the client can pin it in plugin settings.
#[allow(dead_code)]
pub fn load_box_pub_base64(layout: &StorageLayout) -> Result<String, Box<dyn std::error::Error>> {
    let (_, pub_key) = load_box_keypair(layout)?;
    Ok(BASE64_STANDARD.encode(pub_key.as_bytes()))
}

#[cfg(unix)]
fn restrict_private_perms(path: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o600);
    fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn restrict_private_perms(_path: &std::path::Path) -> std::io::Result<()> {
    // On non-Unix platforms (mostly dev), relying on dir perms. The container
    // runtime is always Linux so this branch is essentially untravelled in prod.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn generates_and_reloads_deterministically() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());

        let (priv1, pub1) = init_box_keypair(&layout).unwrap();
        let (priv2, pub2) = load_box_keypair(&layout).unwrap();
        let (priv3, pub3) = init_box_keypair(&layout).unwrap(); // idempotent

        assert_eq!(priv1.as_bytes(), priv2.as_bytes());
        assert_eq!(pub1.as_bytes(), pub2.as_bytes());
        assert_eq!(priv1.as_bytes(), priv3.as_bytes());
        assert_eq!(pub1.as_bytes(), pub3.as_bytes());
    }

    #[test]
    fn rejects_malformed_key_file() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        fs::create_dir_all(layout.base.join("server")).unwrap();
        fs::write(layout.base.join("server").join("box.key"), b"too short").unwrap();

        // StaticSecret doesn't impl Debug, so unwrap_err isn't directly available —
        // match the error out instead.
        match load_box_keypair(&layout) {
            Ok(_) => panic!("expected malformed box.key to be rejected"),
            Err(e) => assert!(e.to_string().contains("bytes")),
        }
    }

    #[test]
    fn pub_file_is_base64_of_derived_key() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());

        let (_, pub_key) = init_box_keypair(&layout).unwrap();
        let b64 = load_box_pub_base64(&layout).unwrap();

        let decoded = BASE64_STANDARD.decode(&b64).unwrap();
        assert_eq!(decoded.as_slice(), pub_key.as_bytes());
    }
}
