//! Option-B secure transport: X25519 ECDH + HKDF-SHA256 + AES-256-GCM.
//!
//! Replaces the old self-signed CA + mTLS stack. iOS `requestUrl` can reach
//! the server over plain HTTP; the payload is encrypted end-to-end using the
//! server's long-term X25519 keypair (see [`crate::box_key`]) and a fresh
//! client-ephemeral X25519 keypair for every request.
//!
//! ## Wire format
//!
//! Request body (client → server):
//! ```text
//! [ 1B version = 0x01 ]
//! [ 12B AEAD nonce   ]
//! [ 32B client ephemeral X25519 pubkey ]
//! [ AES-256-GCM ciphertext || 16B tag ]
//! ```
//!
//! Response body (server → client):
//! ```text
//! [ 1B version = 0x01 ]
//! [ 12B AEAD nonce   ]
//! [ AES-256-GCM ciphertext || 16B tag ]
//! ```
//!
//! ## Key schedule
//!
//! ```text
//! shared          = X25519(our_priv, their_pub)
//! request_key     = HKDF-SHA256(salt = req_nonce,  ikm = shared, info = "obsetync/v1/c2s")
//! response_key    = HKDF-SHA256(salt = resp_nonce, ikm = shared, info = "obsetync/v1/s2c")
//! ```
//!
//! ## AAD
//!
//! `"obsetync/v1"` || ASCII-uppercased HTTP method || HTTP path.
//! Binds ciphertext to the exact request line so captured blobs can't be
//! replayed against a different endpoint. The method + path themselves are
//! already on the wire (they're in the HTTP request line), so AAD is only
//! authenticated, not confidential.
//!
//! Bearer token lives in the *encrypted* plaintext as a 64-char ASCII prefix.
//! No `Authorization` header anywhere. Packet captures cannot fingerprint
//! which device is talking, only that an obsetync session is active.

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use hkdf::Hkdf;
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};

pub const WIRE_VERSION: u8 = 0x01;
pub const NONCE_LEN: usize = 12; // AES-GCM-96
pub const PUBKEY_LEN: usize = 32;
pub const KEY_LEN: usize = 32;
pub const TAG_LEN: usize = 16;
pub const BEARER_LEN: usize = 64; // 32 random bytes, hex-encoded ASCII

pub const REQUEST_HEADER_LEN: usize = 1 + NONCE_LEN + PUBKEY_LEN; // 45
pub const RESPONSE_HEADER_LEN: usize = 1 + NONCE_LEN; // 13
pub const MIN_REQUEST_LEN: usize = REQUEST_HEADER_LEN + BEARER_LEN + TAG_LEN;

const INFO_C2S: &[u8] = b"obsetync/v1/c2s";
const INFO_S2C: &[u8] = b"obsetync/v1/s2c";
const AAD_PREFIX: &[u8] = b"obsetync/v1";

#[derive(Debug, thiserror::Error)]
pub enum SecureError {
    #[error("wire body too short: {0} bytes, need at least {1}")]
    TooShort(usize, usize),
    #[error("unsupported wire version {0}")]
    BadVersion(u8),
    #[error("AEAD decryption failed (tampered, wrong key, or replay with wrong AAD)")]
    AeadOpen,
    #[error("AEAD encryption failed: {0}")]
    AeadSeal(String),
    #[error("bad ephemeral public key length")]
    BadPubkey,
    #[error("plaintext too short to contain bearer token")]
    MissingBearer,
    #[error("bearer token in plaintext is not valid UTF-8")]
    BadBearer,
}

/// A request the server has decrypted successfully. The caller validates
/// `bearer_token`, then passes `inner_body` to the actual route handler.
/// `shared_secret` is reused by [`encrypt_response`] so the response
/// reuses the same ECDH result — no second key exchange needed.
pub struct DecryptedRequest {
    pub bearer_token: String,
    pub inner_body: Vec<u8>,
    pub shared_secret: [u8; KEY_LEN],
}

fn build_aad(method: &str, path: &str) -> Vec<u8> {
    let mut aad = Vec::with_capacity(AAD_PREFIX.len() + 1 + method.len() + 1 + path.len());
    aad.extend_from_slice(AAD_PREFIX);
    aad.push(b' ');
    aad.extend_from_slice(method.as_bytes());
    aad.push(b' ');
    aad.extend_from_slice(path.as_bytes());
    aad
}

fn hkdf_key(shared: &[u8], nonce: &[u8], info: &[u8]) -> [u8; KEY_LEN] {
    let hk = Hkdf::<Sha256>::new(Some(nonce), shared);
    let mut out = [0u8; KEY_LEN];
    // hkdf::expand can only fail for output lengths that exceed its per-invocation
    // maximum (8160 bytes for SHA-256). We're asking for 32 bytes; this is infallible.
    hk.expand(info, &mut out)
        .expect("HKDF expand 32 bytes from SHA-256 — unreachable");
    out
}

/// Decrypt an option-B request from a raw HTTP body.
///
/// - `body` is the full request body bytes.
/// - `our_priv` is the server's long-term X25519 private key.
/// - `method` / `path` are the HTTP request line components used in AAD.
pub fn decrypt_request(
    body: &[u8],
    our_priv: &StaticSecret,
    method: &str,
    path: &str,
) -> Result<DecryptedRequest, SecureError> {
    if body.len() < MIN_REQUEST_LEN {
        return Err(SecureError::TooShort(body.len(), MIN_REQUEST_LEN));
    }
    if body[0] != WIRE_VERSION {
        return Err(SecureError::BadVersion(body[0]));
    }

    let nonce_bytes: [u8; NONCE_LEN] = body[1..1 + NONCE_LEN].try_into().unwrap();
    let pubkey_bytes: [u8; PUBKEY_LEN] = body[1 + NONCE_LEN..REQUEST_HEADER_LEN]
        .try_into()
        .map_err(|_| SecureError::BadPubkey)?;
    let ct = &body[REQUEST_HEADER_LEN..];

    let their_pub = PublicKey::from(pubkey_bytes);
    let shared = our_priv.diffie_hellman(&their_pub);
    let shared_bytes: [u8; KEY_LEN] = *shared.as_bytes();

    let key_bytes = hkdf_key(&shared_bytes, &nonce_bytes, INFO_C2S);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    let aad = build_aad(method, path);

    let plaintext = cipher
        .decrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: ct,
                aad: &aad,
            },
        )
        .map_err(|_| SecureError::AeadOpen)?;

    if plaintext.len() < BEARER_LEN {
        return Err(SecureError::MissingBearer);
    }
    let bearer_bytes = &plaintext[..BEARER_LEN];
    let bearer_token =
        std::str::from_utf8(bearer_bytes).map_err(|_| SecureError::BadBearer)?.to_owned();
    let inner_body = plaintext[BEARER_LEN..].to_vec();

    Ok(DecryptedRequest {
        bearer_token,
        inner_body,
        shared_secret: shared_bytes,
    })
}

/// Encrypt a response body for the client that shares `shared_secret` with us.
/// The AAD of the response binds method + path too, so the client can't be
/// tricked into accepting a response minted for a different endpoint.
pub fn encrypt_response(
    body: &[u8],
    shared_secret: &[u8; KEY_LEN],
    method: &str,
    path: &str,
) -> Result<Vec<u8>, SecureError> {
    // Fresh random 12-byte nonce. AES-GCM allows ~2^32 random nonces with a
    // single key before collision probability matters; we derive a *new* key
    // per response via HKDF with the nonce as salt, so collision would require
    // identical nonce AND identical shared_secret AND identical info, which is
    // infeasible.
    let mut nonce_bytes = [0u8; NONCE_LEN];
    use rand::TryRngCore;
    rand::rngs::OsRng
        .try_fill_bytes(&mut nonce_bytes)
        .map_err(|e| SecureError::AeadSeal(format!("OS RNG: {}", e)))?;

    let key_bytes = hkdf_key(shared_secret, &nonce_bytes, INFO_S2C);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    let aad = build_aad(method, path);

    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload { msg: body, aad: &aad },
        )
        .map_err(|e| SecureError::AeadSeal(e.to_string()))?;

    let mut out = Vec::with_capacity(RESPONSE_HEADER_LEN + ct.len());
    out.push(WIRE_VERSION);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// Helper used by tests to build a client-side request envelope. Lives in the
/// server crate so the wire-format tests don't drift from the decrypt path.
#[cfg(test)]
pub fn encrypt_request_for_tests(
    our_priv: &StaticSecret,
    server_pub: &PublicKey,
    bearer_token: &str,
    method: &str,
    path: &str,
    body: &[u8],
) -> Vec<u8> {
    assert_eq!(bearer_token.len(), BEARER_LEN);

    let shared = our_priv.diffie_hellman(server_pub);
    let shared_bytes: [u8; KEY_LEN] = *shared.as_bytes();

    let mut nonce_bytes = [0u8; NONCE_LEN];
    use rand::TryRngCore;
    rand::rngs::OsRng.try_fill_bytes(&mut nonce_bytes).unwrap();

    let key_bytes = hkdf_key(&shared_bytes, &nonce_bytes, INFO_C2S);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    let aad = build_aad(method, path);

    let mut plaintext = Vec::with_capacity(BEARER_LEN + body.len());
    plaintext.extend_from_slice(bearer_token.as_bytes());
    plaintext.extend_from_slice(body);

    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: &plaintext,
                aad: &aad,
            },
        )
        .unwrap();

    let our_pub = PublicKey::from(our_priv);

    let mut out = Vec::with_capacity(REQUEST_HEADER_LEN + ct.len());
    out.push(WIRE_VERSION);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(our_pub.as_bytes());
    out.extend_from_slice(&ct);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::TryRngCore;

    fn make_server_keypair() -> (StaticSecret, PublicKey) {
        let mut seed = [0u8; KEY_LEN];
        rand::rngs::OsRng.try_fill_bytes(&mut seed).unwrap();
        let priv_key = StaticSecret::from(seed);
        let pub_key = PublicKey::from(&priv_key);
        (priv_key, pub_key)
    }

    fn make_client_keypair() -> (StaticSecret, PublicKey) {
        make_server_keypair()
    }

    fn bearer_64() -> String {
        "a".repeat(BEARER_LEN)
    }

    #[test]
    fn round_trip_request_and_response() {
        let (server_priv, server_pub) = make_server_keypair();
        let (client_priv, _) = make_client_keypair();

        let body = b"hello vault";
        let wire = encrypt_request_for_tests(
            &client_priv,
            &server_pub,
            &bearer_64(),
            "PUT",
            "/api/v1/root/svx-main",
            body,
        );

        let decrypted = decrypt_request(&wire, &server_priv, "PUT", "/api/v1/root/svx-main").unwrap();
        assert_eq!(decrypted.bearer_token, bearer_64());
        assert_eq!(decrypted.inner_body, body);

        let response_body = b"server says ok";
        let wire_resp = encrypt_response(
            response_body,
            &decrypted.shared_secret,
            "PUT",
            "/api/v1/root/svx-main",
        )
        .unwrap();

        // Symmetric round-trip of response: derive same shared_secret on client
        // (in real plugin, client has it cached), extract nonce, derive s2c key,
        // decrypt and check.
        let client_shared = client_priv.diffie_hellman(&server_pub);
        assert_eq!(client_shared.as_bytes(), &decrypted.shared_secret);

        let ver = wire_resp[0];
        assert_eq!(ver, WIRE_VERSION);
        let nonce: [u8; NONCE_LEN] = wire_resp[1..1 + NONCE_LEN].try_into().unwrap();
        let ct = &wire_resp[RESPONSE_HEADER_LEN..];
        let key = hkdf_key(client_shared.as_bytes(), &nonce, INFO_S2C);
        let plain = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key))
            .decrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: ct,
                    aad: &build_aad("PUT", "/api/v1/root/svx-main"),
                },
            )
            .unwrap();
        assert_eq!(&plain, response_body);
    }

    #[test]
    fn tampered_ciphertext_is_rejected() {
        let (server_priv, server_pub) = make_server_keypair();
        let (client_priv, _) = make_client_keypair();
        let mut wire =
            encrypt_request_for_tests(&client_priv, &server_pub, &bearer_64(), "GET", "/x", b"hi");

        // Flip a byte deep in the ciphertext.
        let idx = wire.len() - 5;
        wire[idx] ^= 0x01;

        let err = decrypt_request(&wire, &server_priv, "GET", "/x");
        assert!(matches!(err, Err(SecureError::AeadOpen)));
    }

    #[test]
    fn wrong_server_key_is_rejected() {
        let (_, server_pub) = make_server_keypair();
        let (client_priv, _) = make_client_keypair();
        let wire =
            encrypt_request_for_tests(&client_priv, &server_pub, &bearer_64(), "GET", "/x", b"hi");

        let (wrong_priv, _) = make_server_keypair();
        let err = decrypt_request(&wire, &wrong_priv, "GET", "/x");
        assert!(matches!(err, Err(SecureError::AeadOpen)));
    }

    #[test]
    fn aad_mismatch_is_rejected() {
        let (server_priv, server_pub) = make_server_keypair();
        let (client_priv, _) = make_client_keypair();
        let wire = encrypt_request_for_tests(
            &client_priv,
            &server_pub,
            &bearer_64(),
            "PUT",
            "/api/v1/root/vault-a",
            b"evicted",
        );

        // Client encrypted for vault-a; server claims it's vault-b → decrypt fails.
        let err = decrypt_request(&wire, &server_priv, "PUT", "/api/v1/root/vault-b");
        assert!(matches!(err, Err(SecureError::AeadOpen)));
    }

    #[test]
    fn too_short_is_rejected() {
        let (server_priv, _) = make_server_keypair();
        let err = decrypt_request(&[0u8; 10], &server_priv, "GET", "/x");
        assert!(matches!(err, Err(SecureError::TooShort(10, _))));
    }

    #[test]
    fn bad_version_is_rejected() {
        let (server_priv, server_pub) = make_server_keypair();
        let (client_priv, _) = make_client_keypair();
        let mut wire =
            encrypt_request_for_tests(&client_priv, &server_pub, &bearer_64(), "GET", "/x", b"hi");
        wire[0] = 0xFF;

        let err = decrypt_request(&wire, &server_priv, "GET", "/x");
        assert!(matches!(err, Err(SecureError::BadVersion(0xFF))));
    }

    #[test]
    fn independent_ephemerals_produce_independent_keys() {
        let (server_priv, server_pub) = make_server_keypair();
        let (client_a, _) = make_client_keypair();
        let (client_b, _) = make_client_keypair();

        let wire_a = encrypt_request_for_tests(
            &client_a,
            &server_pub,
            &bearer_64(),
            "GET",
            "/a",
            b"msg from a",
        );
        let wire_b = encrypt_request_for_tests(
            &client_b,
            &server_pub,
            &bearer_64(),
            "GET",
            "/b",
            b"msg from b",
        );

        let dec_a = decrypt_request(&wire_a, &server_priv, "GET", "/a").unwrap();
        let dec_b = decrypt_request(&wire_b, &server_priv, "GET", "/b").unwrap();

        assert_ne!(dec_a.shared_secret, dec_b.shared_secret);
        assert_eq!(dec_a.inner_body, b"msg from a");
        assert_eq!(dec_b.inner_body, b"msg from b");
    }
}
