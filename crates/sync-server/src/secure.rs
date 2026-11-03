//! Secure transport: X25519 ECDH + HKDF-SHA256 + AES-256-GCM.
//!
//! Full wire-format + threat model: `../../../docs/transport.md`.
//!
//! iOS `requestUrl` reaches the server over plain HTTP; the payload is
//! encrypted end-to-end using the pinned long-term server key, a rotating
//! memory-only server key, and a client-ephemeral key.
//!
//! ## Wire format
//!
//! Request body (client → server):
//! ```text
//! [ 1B version = 0x02 ]
//! [ 12B AEAD nonce   ]
//! [ 32B client ephemeral X25519 pubkey ]
//! [ 8B rotating-server-key fingerprint ]
//! [ AES-256-GCM ciphertext || 16B tag ]
//! ```
//!
//! Response body (server → client):
//! ```text
//! [ 1B version = 0x02 ]
//! [ 12B AEAD nonce   ]
//! [ AES-256-GCM ciphertext || 16B tag ]
//! ```
//!
//! ## Key schedule
//!
//! ```text
//! ikm             = X25519(S_priv, Ec_pub) || X25519(Es_priv, Ec_pub)
//! request_key     = HKDF-SHA256(salt = req_nonce,  ikm, info = "obsetync/v2/c2s")
//! response_key    = HKDF-SHA256(salt = resp_nonce, ikm, info = "obsetync/v2/s2c")
//! ```
//!
//! ## AAD
//!
//! Request:  `"obsetync/v2 <METHOD> <PATH>"`.
//! Response: `"obsetync/v2 <METHOD> <PATH>" || nonce_req` — the response AAD
//! additionally binds the 12-byte nonce of the request it answers, so an
//! in-session MITM can't substitute the response of one request for another
//! that shares the same method + path (e.g. replaying a stale `GET /root`
//! answer to mask a newer one). AAD bytes never travel on the wire; the
//! method + path are already in the HTTP request line, so AAD is only
//! authenticated, not confidential.
//!
//! Bearer token lives in the *encrypted* plaintext as a 64-char ASCII prefix.
//! No `Authorization` header is sent. Outer HTTP metadata and the client's
//! per-channel public key remain visible; the bearer itself is encrypted.

use aes_gcm::aead::{Aead, AeadInPlace, Payload};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use hkdf::Hkdf;
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::eph_rotation::{EphState, BOOTSTRAP_FINGERPRINT, FINGERPRINT_LEN};

pub const WIRE_VERSION: u8 = 0x02;
pub const NONCE_LEN: usize = 12; // AES-GCM-96
pub const PUBKEY_LEN: usize = 32;
pub const KEY_LEN: usize = 32;
pub const TAG_LEN: usize = 16;
pub const BEARER_LEN: usize = 64; // 32 random bytes, hex-encoded ASCII

pub const REQUEST_HEADER_LEN: usize = 1 + NONCE_LEN + PUBKEY_LEN + FINGERPRINT_LEN; // 53
pub const RESPONSE_HEADER_LEN: usize = 1 + NONCE_LEN; // 13
pub const SEQUENCE_LEN: usize = 8;
/// Bootstrap carries an empty plaintext; authenticated requests add the
/// bearer + sequence prefix after decryption.
pub const MIN_REQUEST_LEN: usize = REQUEST_HEADER_LEN + TAG_LEN;

const INFO_C2S: &[u8] = b"obsetync/v2/c2s";
const INFO_S2C: &[u8] = b"obsetync/v2/s2c";
const INFO_C2S_BOOT: &[u8] = b"obsetync/v2/c2s-boot";
const INFO_S2C_BOOT: &[u8] = b"obsetync/v2/s2c-boot";
const AAD_PREFIX: &[u8] = b"obsetync/v2";
pub const BOOTSTRAP_PATH: &str = "/api/v1/server-eph";

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
    #[error("bearer token is not 64 hexadecimal ASCII characters")]
    BadBearerFormat,
    #[error("plaintext too short to contain sequence number")]
    MissingSequence,
    #[error("server ephemeral fingerprint is unknown")]
    UnknownFingerprint,
    #[error("X25519 peer key produced a non-contributory shared secret")]
    NonContributoryKey,
    #[error("bootstrap fingerprint is not valid for this endpoint")]
    InvalidBootstrapPath,
    #[error("bootstrap request plaintext must be empty")]
    InvalidBootstrapBody,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    Default,
    Bootstrap,
}

/// A request the server has decrypted successfully. The caller validates
/// `bearer_token`, then passes `inner_body` to the actual route handler.
/// `key_material` is reused by [`encrypt_response`] so the response
/// reuses the same ECDH result — no second key exchange needed.
pub struct DecryptedRequest {
    pub bearer_token: String,
    pub sequence: u64,
    pub inner_body: Vec<u8>,
    /// Public half of the client's per-process HTTP channel. It is not a
    /// credential, but it is a stable, authenticated session identifier:
    /// every request carrying it proved possession of the matching private
    /// key by opening the AEAD envelope. Capability reports are bound to this
    /// value so downgrading a plugin cannot inherit a newer process' feature
    /// declaration merely by reusing the same enrolled device identity.
    pub client_session: [u8; PUBKEY_LEN],
    /// Double-DH IKM is 64 bytes; bootstrap IKM is 32 bytes.
    pub key_material: Zeroizing<Vec<u8>>,
    pub mode: TransportMode,
    /// Nonce of the request envelope — [`encrypt_response`] folds it into the
    /// response AAD so the answer is bound to exactly this request.
    pub nonce_req: [u8; NONCE_LEN],
}

impl Drop for DecryptedRequest {
    fn drop(&mut self) {
        self.bearer_token.zeroize();
        self.inner_body.zeroize();
        self.key_material.zeroize();
    }
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

/// Response AAD = request AAD || nonce_req. Binding the request nonce closes
/// in-session response replay across requests with identical method + path.
fn build_response_aad(method: &str, path: &str, nonce_req: &[u8; NONCE_LEN]) -> Vec<u8> {
    let mut aad = build_aad(method, path);
    aad.extend_from_slice(nonce_req);
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

/// Decrypt a request from a raw HTTP body.
///
/// - `body` is the full request body bytes.
/// - `our_priv` is the server's long-term X25519 private key.
/// - `method` / `path` are the HTTP request line components used in AAD.
pub fn decrypt_request(
    body: &[u8],
    our_priv: &StaticSecret,
    eph_state: &EphState,
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
    let pubkey_end = 1 + NONCE_LEN + PUBKEY_LEN;
    let pubkey_bytes: [u8; PUBKEY_LEN] = body[1 + NONCE_LEN..pubkey_end]
        .try_into()
        .map_err(|_| SecureError::BadPubkey)?;
    let request_fingerprint: [u8; FINGERPRINT_LEN] = body[pubkey_end..REQUEST_HEADER_LEN]
        .try_into()
        .expect("fixed fingerprint slice");
    let ct = &body[REQUEST_HEADER_LEN..];

    let their_pub = PublicKey::from(pubkey_bytes);
    let static_shared = our_priv.diffie_hellman(&their_pub);
    if !static_shared.was_contributory() {
        return Err(SecureError::NonContributoryKey);
    }
    let (mode, key_material, request_info) = if request_fingerprint == BOOTSTRAP_FINGERPRINT {
        if path != BOOTSTRAP_PATH {
            return Err(SecureError::InvalidBootstrapPath);
        }
        (
            TransportMode::Bootstrap,
            Zeroizing::new(static_shared.as_bytes().to_vec()),
            INFO_C2S_BOOT,
        )
    } else {
        let slot = if request_fingerprint == eph_state.current.fingerprint {
            &eph_state.current
        } else if let Some(previous) = eph_state
            .previous
            .as_ref()
            .filter(|slot| request_fingerprint == slot.fingerprint)
        {
            previous
        } else {
            return Err(SecureError::UnknownFingerprint);
        };
        let eph_private = StaticSecret::from(slot.private);
        let ephemeral_shared = eph_private.diffie_hellman(&their_pub);
        if !ephemeral_shared.was_contributory() {
            return Err(SecureError::NonContributoryKey);
        }
        let mut ikm = Vec::with_capacity(KEY_LEN * 2);
        ikm.extend_from_slice(static_shared.as_bytes());
        ikm.extend_from_slice(ephemeral_shared.as_bytes());
        (TransportMode::Default, Zeroizing::new(ikm), INFO_C2S)
    };

    let mut key_bytes = hkdf_key(&key_material, &nonce_bytes, request_info);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    key_bytes.zeroize();
    let aad = build_aad(method, path);

    let plaintext = Zeroizing::new(
        cipher
            .decrypt(
                Nonce::from_slice(&nonce_bytes),
                Payload { msg: ct, aad: &aad },
            )
            .map_err(|_| SecureError::AeadOpen)?,
    );

    let (bearer_token, sequence, inner_body) = match mode {
        TransportMode::Bootstrap => {
            if !plaintext.is_empty() {
                return Err(SecureError::InvalidBootstrapBody);
            }
            (String::new(), 0, Vec::new())
        }
        TransportMode::Default => {
            if plaintext.len() < BEARER_LEN {
                return Err(SecureError::MissingBearer);
            }
            if plaintext.len() < BEARER_LEN + SEQUENCE_LEN {
                return Err(SecureError::MissingSequence);
            }
            let bearer_bytes = &plaintext[..BEARER_LEN];
            if !bearer_bytes.iter().all(u8::is_ascii_hexdigit) {
                return Err(SecureError::BadBearerFormat);
            }
            let bearer_token = std::str::from_utf8(bearer_bytes)
                .map_err(|_| SecureError::BadBearer)?
                .to_owned();
            let sequence = u64::from_be_bytes(
                plaintext[BEARER_LEN..BEARER_LEN + SEQUENCE_LEN]
                    .try_into()
                    .expect("fixed sequence slice"),
            );
            (
                bearer_token,
                sequence,
                plaintext[BEARER_LEN + SEQUENCE_LEN..].to_vec(),
            )
        }
    };

    Ok(DecryptedRequest {
        bearer_token,
        sequence,
        inner_body,
        client_session: pubkey_bytes,
        key_material,
        mode,
        nonce_req: nonce_bytes,
    })
}

/// Encrypt a response body using the request's DH key material.
/// The AAD binds method + path + the request's nonce, so the client can't be
/// tricked into accepting a response minted for a different endpoint — or for
/// a different request to the same endpoint within the session.
pub fn encrypt_response(
    status: u16,
    body: &[u8],
    key_material: &[u8],
    mode: TransportMode,
    method: &str,
    path: &str,
    nonce_req: &[u8; NONCE_LEN],
) -> Result<Vec<u8>, SecureError> {
    // The fresh 96-bit nonce also salts the response key; repeating a nonce
    // with the same DH material would repeat both the key and nonce.
    let mut nonce_bytes = [0u8; NONCE_LEN];
    use rand::TryRngCore;
    rand::rngs::OsRng
        .try_fill_bytes(&mut nonce_bytes)
        .map_err(|e| SecureError::AeadSeal(format!("OS RNG: {}", e)))?;

    let response_info = match mode {
        TransportMode::Default => INFO_S2C,
        TransportMode::Bootstrap => INFO_S2C_BOOT,
    };
    let mut key_bytes = hkdf_key(key_material, &nonce_bytes, response_info);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    key_bytes.zeroize();
    let aad = build_response_aad(method, path, nonce_req);

    // Build and encrypt the final wire allocation in place. Keeping separate
    // plaintext, ciphertext and output Vecs would transiently require three
    // response-sized allocations in addition to the handler body, defeating
    // the transport admission bound for large downloads.
    let output_bytes = RESPONSE_HEADER_LEN
        .checked_add(2)
        .and_then(|bytes| bytes.checked_add(body.len()))
        .and_then(|bytes| bytes.checked_add(TAG_LEN))
        .ok_or_else(|| SecureError::AeadSeal("response length overflow".into()))?;
    let mut out = Zeroizing::new(Vec::with_capacity(output_bytes));
    out.push(WIRE_VERSION);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&status.to_be_bytes());
    out.extend_from_slice(body);
    let tag = cipher
        .encrypt_in_place_detached(
            Nonce::from_slice(&nonce_bytes),
            &aad,
            &mut out[RESPONSE_HEADER_LEN..],
        )
        .map_err(|error| SecureError::AeadSeal(error.to_string()))?;
    out.extend_from_slice(&tag);
    debug_assert_eq!(out.len(), output_bytes);
    Ok(std::mem::take(&mut *out))
}

/// Helper used by tests to build a client-side request envelope. Lives in the
/// server crate so the wire-format tests don't drift from the decrypt path.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub fn encrypt_request_for_tests(
    our_priv: &StaticSecret,
    server_pub: &PublicKey,
    server_eph_pub: Option<&PublicKey>,
    bearer_token: &str,
    sequence: u64,
    method: &str,
    path: &str,
    body: &[u8],
) -> Vec<u8> {
    assert_eq!(bearer_token.len(), BEARER_LEN);

    let static_shared = our_priv.diffie_hellman(server_pub);
    let (key_material, fingerprint, request_info) = match server_eph_pub {
        Some(eph_pub) => {
            let eph_shared = our_priv.diffie_hellman(eph_pub);
            let mut ikm = Vec::with_capacity(KEY_LEN * 2);
            ikm.extend_from_slice(static_shared.as_bytes());
            ikm.extend_from_slice(eph_shared.as_bytes());
            (
                ikm,
                crate::eph_rotation::fingerprint(eph_pub.as_bytes()),
                INFO_C2S,
            )
        }
        None => (
            static_shared.as_bytes().to_vec(),
            BOOTSTRAP_FINGERPRINT,
            INFO_C2S_BOOT,
        ),
    };

    let mut nonce_bytes = [0u8; NONCE_LEN];
    use rand::TryRngCore;
    rand::rngs::OsRng.try_fill_bytes(&mut nonce_bytes).unwrap();

    let key_bytes = hkdf_key(&key_material, &nonce_bytes, request_info);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    let aad = build_aad(method, path);

    let plaintext = if server_eph_pub.is_some() {
        let mut authenticated = Vec::with_capacity(BEARER_LEN + SEQUENCE_LEN + body.len());
        authenticated.extend_from_slice(bearer_token.as_bytes());
        authenticated.extend_from_slice(&sequence.to_be_bytes());
        authenticated.extend_from_slice(body);
        authenticated
    } else {
        assert!(
            body.is_empty(),
            "bootstrap test requests have an empty plaintext"
        );
        Vec::new()
    };

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
    out.extend_from_slice(&fingerprint);
    out.extend_from_slice(&ct);
    out
}

/// Open a transport-v2 response in integration tests. Keeping this next to
/// the production KDF/AAD builders prevents full-stack tests from silently
/// validating only the outer HTTP 200 while ignoring the encrypted semantic
/// status.
#[cfg(test)]
pub fn decrypt_response_for_tests(
    wire: &[u8],
    key_material: &[u8],
    mode: TransportMode,
    method: &str,
    path: &str,
    request_nonce: &[u8; NONCE_LEN],
) -> (u16, Vec<u8>) {
    assert!(wire.len() >= RESPONSE_HEADER_LEN + TAG_LEN + 2);
    assert_eq!(wire[0], WIRE_VERSION);
    let nonce: [u8; NONCE_LEN] = wire[1..RESPONSE_HEADER_LEN].try_into().unwrap();
    let info = match mode {
        TransportMode::Default => INFO_S2C,
        TransportMode::Bootstrap => INFO_S2C_BOOT,
    };
    let key = hkdf_key(key_material, &nonce, info);
    let plaintext = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key))
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &wire[RESPONSE_HEADER_LEN..],
                aad: &build_response_aad(method, path, request_nonce),
            },
        )
        .unwrap();
    let status = u16::from_be_bytes(plaintext[..2].try_into().unwrap());
    (status, plaintext[2..].to_vec())
}

#[cfg(test)]
mod http_v2_tests {
    use super::*;
    use crate::eph_rotation::{fingerprint, EphKeyMaterial};
    use rand::TryRngCore;

    fn keypair() -> (StaticSecret, PublicKey) {
        let mut bytes = [0u8; KEY_LEN];
        rand::rngs::OsRng.try_fill_bytes(&mut bytes).unwrap();
        let private = StaticSecret::from(bytes);
        let public = PublicKey::from(&private);
        (private, public)
    }

    fn eph_state(private: &StaticSecret, public: &PublicKey) -> EphState {
        EphState {
            current: EphKeyMaterial {
                private: *private.as_bytes(),
                public: *public.as_bytes(),
                fingerprint: fingerprint(public.as_bytes()),
                rotated_at: 1,
                valid_until: u64::MAX,
            },
            previous: None,
        }
    }

    #[test]
    fn double_dh_round_trip_carries_sequence_and_encrypted_status() {
        let (static_private, static_public) = keypair();
        let (eph_private, eph_public) = keypair();
        let state = eph_state(&eph_private, &eph_public);
        let (client_private, _) = keypair();
        let path = "/api/v1/root/vault";
        let wire = encrypt_request_for_tests(
            &client_private,
            &static_public,
            Some(&eph_public),
            &"a".repeat(BEARER_LEN),
            42,
            "PUT",
            path,
            b"root",
        );
        let decrypted = decrypt_request(&wire, &static_private, &state, "PUT", path).unwrap();
        assert_eq!(decrypted.sequence, 42);
        assert_eq!(decrypted.inner_body, b"root");
        assert_eq!(decrypted.mode, TransportMode::Default);

        let response = encrypt_response(
            409,
            b"conflict",
            &decrypted.key_material,
            decrypted.mode,
            "PUT",
            path,
            &decrypted.nonce_req,
        )
        .unwrap();
        let (status, plain) = decrypt_response_for_tests(
            &response,
            &decrypted.key_material,
            decrypted.mode,
            "PUT",
            path,
            &decrypted.nonce_req,
        );
        assert_eq!(status, 409);
        assert_eq!(plain, b"conflict");
    }

    #[test]
    fn bootstrap_is_single_dh_and_endpoint_scoped() {
        let (static_private, static_public) = keypair();
        let (eph_private, eph_public) = keypair();
        let state = eph_state(&eph_private, &eph_public);
        let (client_private, _) = keypair();
        let wire = encrypt_request_for_tests(
            &client_private,
            &static_public,
            None,
            &"b".repeat(BEARER_LEN),
            7,
            "POST",
            BOOTSTRAP_PATH,
            b"",
        );
        let decrypted =
            decrypt_request(&wire, &static_private, &state, "POST", BOOTSTRAP_PATH).unwrap();
        assert_eq!(decrypted.mode, TransportMode::Bootstrap);
        assert_eq!(decrypted.sequence, 0);
        assert!(decrypted.bearer_token.is_empty());
        assert!(matches!(
            decrypt_request(&wire, &static_private, &state, "POST", "/api/v1/root/x"),
            Err(SecureError::InvalidBootstrapPath) | Err(SecureError::AeadOpen)
        ));
    }

    #[test]
    fn wrong_ephemeral_fingerprint_is_rejected_before_open() {
        let (static_private, static_public) = keypair();
        let (eph_private, eph_public) = keypair();
        let state = eph_state(&eph_private, &eph_public);
        let (client_private, _) = keypair();
        let mut wire = encrypt_request_for_tests(
            &client_private,
            &static_public,
            Some(&eph_public),
            &"c".repeat(BEARER_LEN),
            1,
            "GET",
            "/x",
            b"",
        );
        wire[REQUEST_HEADER_LEN - 1] ^= 1;
        assert!(matches!(
            decrypt_request(&wire, &static_private, &state, "GET", "/x"),
            Err(SecureError::UnknownFingerprint)
        ));
    }

    #[test]
    fn low_order_client_public_key_is_rejected() {
        let (static_private, _) = keypair();
        let (eph_private, eph_public) = keypair();
        let state = eph_state(&eph_private, &eph_public);
        let mut wire = vec![0u8; MIN_REQUEST_LEN];
        wire[0] = WIRE_VERSION;
        wire[1 + NONCE_LEN + PUBKEY_LEN..REQUEST_HEADER_LEN]
            .copy_from_slice(&state.current.fingerprint);
        assert!(matches!(
            decrypt_request(&wire, &static_private, &state, "GET", "/x"),
            Err(SecureError::NonContributoryKey)
        ));
    }
}

// --- WebSocket sealed frames (Ph3, wire v2) ----------------------------------
//
// The notify channel's v1 frames were plaintext JSON — fine while they only
// carried root hashes, unacceptable once presence frames name FILE PATHS and
// Ph4 ops carry note content. v2 seals every frame after the auth handshake
// with per-session directional keys:
//
//   shared   = X25519(server_eph_priv, client_eph_pub)      (fresh per ticket)
//   c2s_key  = HKDF-SHA256(salt = ticket_bytes, ikm = shared, info = "obsetync/ws/v2/c2s")
//   s2c_key  = HKDF-SHA256(salt = ticket_bytes, ikm = shared, info = "obsetync/ws/v2/s2c")
//
// Frame (Binary ws message): [ 12B nonce | AES-256-GCM ciphertext || 16B tag ]
// AAD = "obsetync/ws/v2 <dir> <seq_be8>" where <dir> ∈ {c2s, s2c} and seq is
// each direction's monotonically increasing frame counter. TCP already
// guarantees ordering, so a seq mismatch means tampering/replay → close.

const WS_INFO_C2S: &[u8] = b"obsetync/ws/v2/c2s";
const WS_INFO_S2C: &[u8] = b"obsetync/ws/v2/s2c";
const WS_AAD_PREFIX: &[u8] = b"obsetync/ws/v2";
const WS_DATA_AAD_PREFIX: &[u8] = b"obsetync/ws-data/v1";

/// Directional key pair for one WS session.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct WsSessionKeys {
    pub c2s: [u8; KEY_LEN],
    pub s2c: [u8; KEY_LEN],
}

/// Derive the session keys from the ECDH shared secret and the ticket (salt).
pub fn derive_ws_keys(shared: &[u8; KEY_LEN], ticket_hex: &str) -> WsSessionKeys {
    WsSessionKeys {
        c2s: hkdf_key(shared, ticket_hex.as_bytes(), WS_INFO_C2S),
        s2c: hkdf_key(shared, ticket_hex.as_bytes(), WS_INFO_S2C),
    }
}

fn ws_aad(prefix: &[u8], dir: &str, seq: u64) -> Vec<u8> {
    let mut aad = Vec::with_capacity(prefix.len() + 1 + dir.len() + 1 + 8);
    aad.extend_from_slice(prefix);
    aad.push(b' ');
    aad.extend_from_slice(dir.as_bytes());
    aad.push(b' ');
    aad.extend_from_slice(&seq.to_be_bytes());
    aad
}

/// Seal one frame. `dir` is the SENDER's direction ("c2s" or "s2c").
pub fn ws_seal(
    key: &[u8; KEY_LEN],
    dir: &str,
    seq: u64,
    plaintext: &[u8],
) -> Result<Vec<u8>, SecureError> {
    ws_seal_with_prefix(key, WS_AAD_PREFIX, dir, seq, plaintext)
}

/// Seal a ws-data-v1 frame. It deliberately keeps the ticket-derived keys
/// but uses a generation-specific AAD domain, so a valid realtime frame
/// cannot be replayed onto the data endpoint (or vice versa).
pub fn ws_data_seal(
    key: &[u8; KEY_LEN],
    dir: &str,
    seq: u64,
    plaintext: &[u8],
) -> Result<Vec<u8>, SecureError> {
    ws_seal_with_prefix(key, WS_DATA_AAD_PREFIX, dir, seq, plaintext)
}

fn ws_seal_with_prefix(
    key: &[u8; KEY_LEN],
    prefix: &[u8],
    dir: &str,
    seq: u64,
    plaintext: &[u8],
) -> Result<Vec<u8>, SecureError> {
    let mut nonce = [0u8; NONCE_LEN];
    use rand::RngCore;
    rand::rng().fill_bytes(&mut nonce);

    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let aad = ws_aad(prefix, dir, seq);
    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|e| SecureError::AeadSeal(e.to_string()))?;

    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// Open one frame sealed by the peer whose direction is `dir`.
pub fn ws_open(
    key: &[u8; KEY_LEN],
    dir: &str,
    seq: u64,
    frame: &[u8],
) -> Result<Vec<u8>, SecureError> {
    ws_open_with_prefix(key, WS_AAD_PREFIX, dir, seq, frame)
}

/// Open one ws-data-v1 frame in its protocol-specific AAD domain.
pub fn ws_data_open(
    key: &[u8; KEY_LEN],
    dir: &str,
    seq: u64,
    frame: &[u8],
) -> Result<Vec<u8>, SecureError> {
    ws_open_with_prefix(key, WS_DATA_AAD_PREFIX, dir, seq, frame)
}

fn ws_open_with_prefix(
    key: &[u8; KEY_LEN],
    prefix: &[u8],
    dir: &str,
    seq: u64,
    frame: &[u8],
) -> Result<Vec<u8>, SecureError> {
    if frame.len() < NONCE_LEN + TAG_LEN {
        return Err(SecureError::TooShort(frame.len(), NONCE_LEN + TAG_LEN));
    }
    let (nonce, ct) = frame.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let aad = ws_aad(prefix, dir, seq);
    cipher
        .decrypt(Nonce::from_slice(nonce), Payload { msg: ct, aad: &aad })
        .map_err(|_| SecureError::AeadOpen)
}

#[cfg(test)]
mod ws_frame_tests {
    use super::*;

    fn keys() -> WsSessionKeys {
        derive_ws_keys(&[7u8; KEY_LEN], &"ab".repeat(32))
    }

    #[test]
    fn seal_open_roundtrip_both_directions() {
        let k = keys();
        let frame = ws_seal(&k.c2s, "c2s", 0, b"{\"t\":\"sub\"}").unwrap();
        assert_eq!(
            ws_open(&k.c2s, "c2s", 0, &frame).unwrap(),
            b"{\"t\":\"sub\"}"
        );

        let frame = ws_seal(&k.s2c, "s2c", 5, b"{\"t\":\"root\"}").unwrap();
        assert_eq!(
            ws_open(&k.s2c, "s2c", 5, &frame).unwrap(),
            b"{\"t\":\"root\"}"
        );
    }

    #[test]
    fn wrong_seq_or_direction_fails() {
        let k = keys();
        let frame = ws_seal(&k.c2s, "c2s", 3, b"x").unwrap();
        // Replay at another position dies on AAD.
        assert!(ws_open(&k.c2s, "c2s", 4, &frame).is_err());
        // Cross-direction reflection dies on key AND AAD.
        assert!(ws_open(&k.s2c, "s2c", 3, &frame).is_err());
    }

    #[test]
    fn realtime_and_data_aad_domains_reject_each_other() {
        let k = keys();
        let realtime = ws_seal(&k.c2s, "c2s", 0, b"OBW1").unwrap();
        assert!(ws_data_open(&k.c2s, "c2s", 0, &realtime).is_err());

        let data = ws_data_seal(&k.c2s, "c2s", 0, b"OBW1").unwrap();
        assert!(ws_open(&k.c2s, "c2s", 0, &data).is_err());
        assert_eq!(ws_data_open(&k.c2s, "c2s", 0, &data).unwrap(), b"OBW1");
    }

    #[test]
    fn keys_differ_per_direction_and_ticket() {
        let a = derive_ws_keys(&[7u8; KEY_LEN], &"aa".repeat(32));
        let b = derive_ws_keys(&[7u8; KEY_LEN], &"bb".repeat(32));
        assert_ne!(a.c2s, a.s2c);
        assert_ne!(a.c2s, b.c2s);
    }

    #[test]
    fn tampered_frame_fails() {
        let k = keys();
        let mut frame = ws_seal(&k.c2s, "c2s", 0, b"payload").unwrap();
        let last = frame.len() - 1;
        frame[last] ^= 0x01;
        assert!(ws_open(&k.c2s, "c2s", 0, &frame).is_err());
    }
}
