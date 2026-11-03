//! Server X25519 "box" keypair — the long-term identity for the AEAD
//! envelope transport. See `../../../docs/transport.md`.
//!
//! Clients learn the public key at enrollment time and use it as the remote
//! static key in per-channel ECDH. Wire v2 combines it with a rotating
//! memory-only server key: compromise of `box.key` alone cannot open captured
//! ordinary traffic after the applicable rotating current/previous slot has
//! been dropped. It can still impersonate the server going forward, and it
//! opens captured single-DH bootstrap responses, so protect it like any other
//! long-term server identity key. See the precise limits in transport.md.
//!
//! On-disk layout under `data/server/`:
//!   - box.key : raw 32 bytes (mode 0600), the X25519 private key
//!   - box.pub : base64(32) + "\n", for operator inspection / admin UI display

use crate::storage::StorageLayout;
use base64::prelude::*;
use std::fs;
use std::io::Write;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

const PRIV_FILE: &str = "box.key";
const PUB_FILE: &str = "box.pub";
const KEY_LEN: usize = 32;

/// Initialize the identity without replacing an existing private key.
/// The derived public copy is also repaired after an interrupted init.
pub fn init_box_keypair(
    layout: &StorageLayout,
) -> Result<(StaticSecret, PublicKey), Box<dyn std::error::Error>> {
    let server_dir = layout.base.join("server");
    fs::create_dir_all(&server_dir)?;

    let priv_path = server_dir.join(PRIV_FILE);
    let pub_path = server_dir.join(PUB_FILE);

    if !priv_path.try_exists()? {
        // x25519-dalek uses rand_core 0.6; source entropy directly from the OS.
        use rand::TryRngCore;
        let mut seed = Zeroizing::new([0u8; KEY_LEN]);
        rand::rngs::OsRng
            .try_fill_bytes(&mut *seed)
            .map_err(|e| format!("OS RNG failed: {}", e))?;
        // tempfile creates Unix files as 0600 before any private bytes are written.
        let mut staged = tempfile::NamedTempFile::new_in(&server_dir)?;
        staged.write_all(&*seed)?;
        staged.as_file().sync_all()?;
        match staged.persist_noclobber(&priv_path) {
            Ok(_) => {}
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }

    // A concurrent initializer may have won; always derive from the published key.
    let (priv_key, pub_key) = load_box_keypair(layout)?;
    fs::File::open(&priv_path)?.sync_all()?;
    let b64 = BASE64_STANDARD.encode(pub_key.as_bytes());
    let mut public = tempfile::NamedTempFile::new_in(&server_dir)?;
    writeln!(public, "{b64}")?;
    public.as_file().sync_all()?;
    public.persist(&pub_path)?;
    #[cfg(unix)]
    for ancestor in server_dir.ancestors() {
        let path = if ancestor.as_os_str().is_empty() {
            std::path::Path::new(".")
        } else {
            ancestor
        };
        fs::File::open(path)?.sync_all()?;
    }

    tracing::info!("X25519 identity initialized (pub = {})", &b64);
    Ok((priv_key, pub_key))
}

/// Load an existing keypair from disk. Errors if the private key file is
/// missing or malformed (wrong size).
pub fn load_box_keypair(
    layout: &StorageLayout,
) -> Result<(StaticSecret, PublicKey), Box<dyn std::error::Error>> {
    let priv_path = layout.base.join("server").join(PRIV_FILE);
    let bytes = Zeroizing::new(
        fs::read(&priv_path).map_err(|e| format!("reading {}: {}", priv_path.display(), e))?,
    );
    if bytes.len() != KEY_LEN {
        return Err(format!("box.key is {} bytes, expected {}", bytes.len(), KEY_LEN).into());
    }
    let mut arr = Zeroizing::new([0u8; KEY_LEN]);
    arr.copy_from_slice(&bytes);
    let priv_key = StaticSecret::from(*arr);
    let pub_key = PublicKey::from(&priv_key);
    Ok((priv_key, pub_key))
}

/// Just the public key, base64 encoded. Used by the admin UI + enrollment
/// response so the client can pin it in plugin settings.
pub fn load_box_pub_base64(layout: &StorageLayout) -> Result<String, Box<dyn std::error::Error>> {
    let (_, pub_key) = load_box_keypair(layout)?;
    Ok(BASE64_STANDARD.encode(pub_key.as_bytes()))
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
        assert!(init_box_keypair(&layout).is_err());
        assert_eq!(
            fs::read(layout.base.join("server/box.key")).unwrap(),
            b"too short"
        );
    }

    #[test]
    fn pub_file_is_base64_of_derived_key() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());

        let (_, pub_key) = init_box_keypair(&layout).unwrap();
        let b64 = load_box_pub_base64(&layout).unwrap();

        let decoded = BASE64_STANDARD.decode(&b64).unwrap();
        assert_eq!(decoded.as_slice(), pub_key.as_bytes());
        assert_eq!(
            fs::read_to_string(layout.base.join("server/box.pub")).unwrap(),
            format!("{b64}\n")
        );
    }

    #[test]
    fn initialization_repairs_the_public_copy_without_rotating_identity() {
        let dir = tempdir().unwrap();
        let layout = StorageLayout::new(dir.path());
        let (original, public) = init_box_keypair(&layout).unwrap();
        let public_path = layout.base.join("server/box.pub");
        fs::remove_file(&public_path).unwrap();
        let (reloaded, _) = init_box_keypair(&layout).unwrap();
        assert_eq!(original.as_bytes(), reloaded.as_bytes());
        assert_eq!(
            fs::read_to_string(&public_path).unwrap(),
            format!("{}\n", BASE64_STANDARD.encode(public.as_bytes()))
        );
    }

    #[test]
    fn concurrent_initializers_publish_one_complete_identity() {
        for _ in 0..8 {
            let dir = tempdir().unwrap();
            let layout = StorageLayout::new(dir.path());
            let barrier = std::sync::Barrier::new(16);
            let identities = std::thread::scope(|scope| {
                let workers: Vec<_> = (0..16)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            init_box_keypair(&layout)
                                .map(|(_, public)| *public.as_bytes())
                                .map_err(|error| error.to_string())
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .map(|worker| worker.join().unwrap().unwrap())
                    .collect::<Vec<_>>()
            });
            let (_, public) = load_box_keypair(&layout).unwrap();
            assert!(identities
                .iter()
                .all(|identity| identity == public.as_bytes()));
            assert_eq!(
                fs::read_to_string(layout.base.join("server/box.pub")).unwrap(),
                format!("{}\n", BASE64_STANDARD.encode(public.as_bytes()))
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(layout.base.join("server/box.key"))
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600
                );
            }
        }
    }
}
