//! Rotating in-memory X25519 key used by HTTP transport v2.
//!
//! The long-term box key authenticates the server; this second DH contribution
//! supplies forward secrecy if `box.key` is compromised later. It is never
//! persisted. A restart deliberately creates a fresh key, and clients recover
//! with the authenticated single-DH bootstrap endpoint.

use crate::secure::KEY_LEN;
use base64::prelude::*;
use rand::TryRngCore;
use sha2::{Digest, Sha256};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::{Zeroize, ZeroizeOnDrop};

pub const FINGERPRINT_LEN: usize = 8;
pub const ROTATION_PERIOD_SECONDS: u64 = 24 * 60 * 60;
/// Total lifetime from generation: 24h current + up to 24h previous.
pub const GRACE_SECONDS: u64 = 2 * ROTATION_PERIOD_SECONDS;
pub const BOOTSTRAP_FINGERPRINT: [u8; FINGERPRINT_LEN] = [0; FINGERPRINT_LEN];

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct EphKeyMaterial {
    pub private: [u8; KEY_LEN],
    pub public: [u8; KEY_LEN],
    pub fingerprint: [u8; FINGERPRINT_LEN],
    pub rotated_at: u64,
    pub valid_until: u64,
}

pub struct EphState {
    pub current: EphKeyMaterial,
    pub previous: Option<EphKeyMaterial>,
}

pub fn fingerprint(public: &[u8; KEY_LEN]) -> [u8; FINGERPRINT_LEN] {
    let digest = Sha256::digest(public);
    let mut result = [0u8; FINGERPRINT_LEN];
    result.copy_from_slice(&digest[..FINGERPRINT_LEN]);
    result
}

pub fn init_eph_keys() -> Result<EphState, Box<dyn std::error::Error>> {
    let current = generate_material(now_seconds())?;
    tracing::info!(
        fingerprint = %hex::encode(current.fingerprint),
        "eph: initialized in-memory transport-v2 key"
    );
    Ok(EphState {
        current,
        previous: None,
    })
}

pub fn current_bundle(state: &Arc<RwLock<EphState>>) -> (String, u64) {
    let state = state.read().expect("eph state poisoned");
    (
        BASE64_STANDARD.encode(state.current.public),
        state.current.valid_until,
    )
}

pub fn spawn_rotation_task(state: Arc<RwLock<EphState>>) {
    tokio::spawn(async move {
        loop {
            let valid_until = state
                .read()
                .expect("eph state poisoned")
                .current
                .valid_until;
            let delay = valid_until.saturating_sub(now_seconds()).max(1);
            tokio::time::sleep(Duration::from_secs(delay)).await;
            let rotation_failed = match rotate_now(&state) {
                Ok(()) => false,
                Err(error) => {
                    tracing::error!(reason = %error, "eph: rotation failed; retrying in 60s");
                    true
                }
            };
            if rotation_failed {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
    });
}

pub fn rotate_now(state: &Arc<RwLock<EphState>>) -> Result<(), Box<dyn std::error::Error>> {
    let next = generate_material(now_seconds())?;
    let fingerprint = next.fingerprint;
    let mut state = state.write().expect("eph state poisoned");
    let old_current = state.current.clone();
    // Assignment drops and zeroizes the old previous slot and the redundant
    // old-current copy. Only the retained one-period grace copy survives.
    *state = EphState {
        current: next,
        previous: Some(old_current),
    };
    tracing::info!(
        fingerprint = %hex::encode(fingerprint),
        "eph: rotated in-memory transport-v2 key"
    );
    Ok(())
}

fn generate_material(rotated_at: u64) -> Result<EphKeyMaterial, Box<dyn std::error::Error>> {
    loop {
        let mut private = [0u8; KEY_LEN];
        rand::rngs::OsRng
            .try_fill_bytes(&mut private)
            .map_err(|error| format!("OS RNG failed: {}", error))?;
        let public = *PublicKey::from(&StaticSecret::from(private)).as_bytes();
        let fingerprint = fingerprint(&public);
        if fingerprint != BOOTSTRAP_FINGERPRINT {
            return Ok(EphKeyMaterial {
                private,
                public,
                fingerprint,
                rotated_at,
                valid_until: rotated_at.saturating_add(ROTATION_PERIOD_SECONDS),
            });
        }
        private.zeroize();
    }
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initializes_and_rotates_with_one_previous_slot() {
        let initial = init_eph_keys().unwrap();
        let old_public = initial.current.public;
        let shared = Arc::new(RwLock::new(initial));

        rotate_now(&shared).unwrap();
        let state = shared.read().unwrap();
        assert_ne!(state.current.public, old_public);
        assert_eq!(state.previous.as_ref().unwrap().public, old_public);
        assert_ne!(state.current.fingerprint, BOOTSTRAP_FINGERPRINT);
    }

    #[test]
    fn fingerprint_is_stable_and_eight_bytes() {
        let public = [7u8; KEY_LEN];
        assert_eq!(fingerprint(&public), fingerprint(&public));
        assert_eq!(fingerprint(&public).len(), FINGERPRINT_LEN);
    }
}
