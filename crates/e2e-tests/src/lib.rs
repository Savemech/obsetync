//! End-to-end test harness against a running obsetync server.
//!
//! Connects over plain HTTP to the e2e docker stack on 127.0.0.1:27282 (sync)
//! and 127.0.0.1:27283 (admin). Reimplements the AEAD wire protocol (X25519
//! ECDH + HKDF-SHA256 + AES-256-GCM) deliberately *separately* from
//! `sync-server::secure` — if both implementations share a bug the test would
//! pass wrongly. Specification: `docs/transport.md`.
//!
//! Layered API:
//!   - [`E2eEnv::from_env`]            connect to the running stack
//!   - [`E2eEnv::wait_for_health`]     block until /health responds
//!   - [`E2eEnv::enroll_device`]       admin: create code + claim → DeviceCreds
//!   - [`E2eEnv::revoke_device`]       admin: mark a device revoked
//!   - [`WireClient`]                  per-device sealed-envelope client
//!   - [`push_vault_snapshot`]         high-level: upload a `(path, bytes)` set
//!   - [`pull_vault_snapshot`]         high-level: download whatever's current
//!   - [`build_root_for_files`]        construct a Merkle root locally without
//!                                     touching the network — used for low-level
//!                                     conflict orchestration tests.

use aes_gcm::{
    aead::{Aead, Payload},
    Aes256Gcm, Key, KeyInit, Nonce,
};
use anyhow::{anyhow, bail, Context, Result};
use base64::prelude::*;
use hkdf::Hkdf;
use rand::TryRngCore;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use sync_core::chunk::{FileEntry, InternalNode, LeafChunk, RootNode};
use sync_core::hash::{hash_bytes, hash_to_hex, hex_to_hash, FileHash};
use x25519_dalek::{PublicKey, StaticSecret};

// --- wire constants (mirror sync_server::secure) -----------------------------

pub const WIRE_VERSION: u8 = 0x01;
pub const NONCE_LEN: usize = 12;
pub const PUBKEY_LEN: usize = 32;
pub const KEY_LEN: usize = 32;
pub const TAG_LEN: usize = 16;
pub const BEARER_LEN: usize = 64;
pub const REQUEST_HEADER_LEN: usize = 1 + NONCE_LEN + PUBKEY_LEN;
pub const RESPONSE_HEADER_LEN: usize = 1 + NONCE_LEN;

const INFO_C2S: &[u8] = b"obsetync/v1/c2s";
const INFO_S2C: &[u8] = b"obsetync/v1/s2c";
const AAD_PREFIX: &[u8] = b"obsetync/v1";

/// Hex string for a 32-byte hash of all zeros — the "fresh client / no parent"
/// sentinel the server recognises when prepended to PUT /root and POST /diff.
pub const ZERO_HASH_HEX: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

// --- env / harness -----------------------------------------------------------

/// Connection details for a running e2e stack.
pub struct E2eEnv {
    pub base_url: String,  // sync API
    pub admin_url: String, // admin API
    pub http: reqwest::Client,
}

impl E2eEnv {
    pub fn from_env() -> Self {
        let base_url = std::env::var("OBSETYNC_E2E_BASE")
            .unwrap_or_else(|_| "http://127.0.0.1:27282".into());
        let admin_url = std::env::var("OBSETYNC_E2E_ADMIN")
            .unwrap_or_else(|_| "http://127.0.0.1:27283".into());
        Self {
            base_url,
            admin_url,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        }
    }

    /// Block until /health responds 200 or timeout elapses.
    pub async fn wait_for_health(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(resp) = self.http.get(format!("{}/health", self.base_url)).send().await {
                if resp.status().is_success() {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                bail!("server did not become healthy within {:?}", timeout);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Create an enrollment via the admin form, then claim the code.
    /// Returns full credentials including the server's pinned X25519 pubkey.
    pub async fn enroll_device(&self, device_name: &str) -> Result<DeviceCreds> {
        // 1. POST /admin/devices/new (form-urlencoded) — issues a code, returns HTML
        //    embedding the code. Parse the code out.
        let form = [("device_name", device_name)];
        let resp = self
            .http
            .post(format!("{}/admin/devices/new", self.admin_url))
            .form(&form)
            .send()
            .await
            .context("POST /admin/devices/new")?;
        let status = resp.status();
        let html = resp.text().await?;
        if !status.is_success() {
            bail!("admin returned {}: {}", status, html);
        }
        let code = parse_enrollment_code(&html)
            .context("could not find enrollment code in admin HTML response")?;

        // 2. GET /admin/enrollment/{code} — claims it. Returns JSON bundle.
        let resp = self
            .http
            .get(format!("{}/admin/enrollment/{}", self.admin_url, code))
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            bail!("claim returned {}: {}", status, body);
        }

        let bundle: EnrollmentBundle =
            serde_json::from_str(&body).context("parsing enrollment bundle JSON")?;
        let pub_bytes = BASE64_STANDARD
            .decode(&bundle.server_box_pub)
            .context("decoding server_box_pub base64")?;
        if pub_bytes.len() != PUBKEY_LEN {
            bail!("server_box_pub is {} bytes, expected 32", pub_bytes.len());
        }
        let mut arr = [0u8; PUBKEY_LEN];
        arr.copy_from_slice(&pub_bytes);
        Ok(DeviceCreds {
            code,
            device_id: bundle.device_id,
            device_name: bundle.device_name,
            bearer: bundle.bearer_token,
            server_box_pub: PublicKey::from(arr),
        })
    }

    /// Mark a device revoked via the admin endpoint.
    pub async fn revoke_device(&self, device_id: &str) -> Result<()> {
        let resp = self
            .http
            .post(format!(
                "{}/admin/devices/{}/revoke",
                self.admin_url, device_id
            ))
            .send()
            .await?;
        // Admin redirects on success; treat any 2xx/3xx as ok.
        let s = resp.status();
        if s.is_success() || s.is_redirection() {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            bail!("revoke failed: {} — {}", s, body);
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeviceCreds {
    pub code: String,
    pub device_id: String,
    pub device_name: String,
    pub bearer: String,
    pub server_box_pub: PublicKey,
}

#[derive(Deserialize)]
struct EnrollmentBundle {
    device_name: String,
    device_id: String,
    bearer_token: String,
    server_box_pub: String,
}

fn parse_enrollment_code(html: &str) -> Option<String> {
    // The admin response renders the code inside `<code class="code">XXXX-9999</code>`.
    // Accept anything matching the generator's shape: 4 letters, dash, 4 digits.
    let bytes = html.as_bytes();
    for i in 0..bytes.len().saturating_sub(9) {
        let win = &bytes[i..i + 9];
        if win[0..4].iter().all(|b| b.is_ascii_uppercase())
            && win[4] == b'-'
            && win[5..9].iter().all(|b| b.is_ascii_digit())
        {
            return Some(std::str::from_utf8(win).ok()?.to_string());
        }
    }
    None
}

// --- WireClient — per-device encrypted-envelope HTTP client ------------------

/// A client bound to one set of device credentials. All requests go through
/// the AEAD envelope using a freshly generated ephemeral X25519 keypair per
/// request — no client state survives between requests besides the bearer
/// token and the pinned server pubkey.
pub struct WireClient {
    base_url: String,
    creds: DeviceCreds,
    http: reqwest::Client,
}

impl WireClient {
    pub fn new(env: &E2eEnv, creds: DeviceCreds) -> Self {
        Self {
            base_url: env.base_url.clone(),
            creds,
            http: env.http.clone(),
        }
    }

    pub fn creds(&self) -> &DeviceCreds {
        &self.creds
    }

    /// Send a sealed envelope. `semantic_method` is what the server sees after
    /// envelope decryption (PUT/GET/POST/etc); the wire method is always POST.
    pub async fn raw(
        &self,
        semantic_method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<RawResponse> {
        // Per-request ephemeral keypair. Forward secrecy: an attacker who later
        // compromises the server's box.key cannot decrypt this exchange.
        let mut seed = [0u8; KEY_LEN];
        rand::rngs::OsRng.try_fill_bytes(&mut seed).unwrap();
        let client_eph = StaticSecret::from(seed);

        let envelope = encrypt_request(
            &client_eph,
            &self.creds.server_box_pub,
            &self.creds.bearer,
            semantic_method,
            path,
            body,
        )?;

        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .http
            .post(&url)
            .header("X-Obsetync-Method", semantic_method)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(envelope)
            .send()
            .await
            .context("HTTP send")?;

        let status = resp.status();
        let body = resp.bytes().await?.to_vec();

        // Errors from the middleware (401/403/400) are returned as plaintext —
        // they never reach the secure-envelope wrapping path. Successful and
        // handler-error responses (4xx/5xx generated by inner handlers) are
        // both encrypted.
        let plaintext = if status.is_success() {
            decrypt_response(
                &client_eph,
                &self.creds.server_box_pub,
                semantic_method,
                path,
                &body,
            )
            .context("decrypt response")?
        } else {
            body
        };

        Ok(RawResponse { status, body: plaintext })
    }

    pub async fn get_root(&self, vault_id: &str) -> Result<RootNode> {
        let r = self
            .raw("GET", &format!("/api/v1/root/{}", vault_id), &[])
            .await?;
        if !r.status.is_success() {
            bail!("get_root: {} — {}", r.status, String::from_utf8_lossy(&r.body));
        }
        Ok(RootNode::deserialize(&r.body)?)
    }

    /// PUT /api/v1/root/{vault}. Body layout: 64-byte ASCII parent_hex prefix,
    /// then the FlatBuffers RootNode bytes. Server compares parent against
    /// current; mismatch triggers a three-way merge.
    pub async fn put_root(
        &self,
        vault_id: &str,
        root: &RootNode,
        parent_hex: &str,
    ) -> Result<PutRootResponse> {
        if parent_hex.len() != 64 {
            bail!("parent_hex must be 64 chars, got {}", parent_hex.len());
        }
        let mut body = Vec::with_capacity(64 + 256);
        body.extend_from_slice(parent_hex.as_bytes());
        body.extend_from_slice(&root.serialize());

        let r = self
            .raw("PUT", &format!("/api/v1/root/{}", vault_id), &body)
            .await?;
        if !r.status.is_success() {
            bail!("put_root: {} — {}", r.status, String::from_utf8_lossy(&r.body));
        }
        let resp: PutRootResponse =
            serde_json::from_slice(&r.body).context("parsing put_root JSON")?;
        Ok(resp)
    }

    pub async fn post_diff(
        &self,
        vault_id: &str,
        device_root_hex: &str,
    ) -> Result<DiffResponse> {
        if device_root_hex.len() != 64 {
            bail!("device_root_hex must be 64 chars");
        }
        let r = self
            .raw(
                "POST",
                &format!("/api/v1/diff/{}", vault_id),
                device_root_hex.as_bytes(),
            )
            .await?;
        // 304 was promoted to 200 by the middleware before encryption (so the
        // AEAD envelope reaches the wire) — but the body stays "[]".
        if !r.status.is_success() {
            bail!("post_diff: {} — {}", r.status, String::from_utf8_lossy(&r.body));
        }
        let text = std::str::from_utf8(&r.body)?;
        if text.trim() == "[]" {
            return Ok(DiffResponse::InSync);
        }
        let deltas: Vec<WireDelta> =
            serde_json::from_slice(&r.body).context("parsing diff response JSON")?;
        Ok(DiffResponse::Deltas(deltas))
    }

    pub async fn put_chunk(&self, hash: &FileHash, bytes: &[u8]) -> Result<()> {
        let path = format!("/api/v1/chunk/{}", hash_to_hex(hash));
        let r = self.raw("PUT", &path, bytes).await?;
        if !r.status.is_success() {
            bail!("put_chunk: {} — {}", r.status, String::from_utf8_lossy(&r.body));
        }
        Ok(())
    }

    pub async fn get_chunk(&self, hash: &FileHash) -> Result<Vec<u8>> {
        let path = format!("/api/v1/chunk/{}", hash_to_hex(hash));
        let r = self.raw("GET", &path, &[]).await?;
        if !r.status.is_success() {
            bail!("get_chunk: {} — {}", r.status, String::from_utf8_lossy(&r.body));
        }
        Ok(r.body)
    }

    pub async fn put_content(&self, hash: &FileHash, bytes: &[u8]) -> Result<()> {
        let path = format!("/api/v1/content/{}", hash_to_hex(hash));
        let r = self.raw("PUT", &path, bytes).await?;
        if !r.status.is_success() {
            bail!("put_content: {} — {}", r.status, String::from_utf8_lossy(&r.body));
        }
        Ok(())
    }

    pub async fn get_content(&self, hash: &FileHash) -> Result<Vec<u8>> {
        let path = format!("/api/v1/content/{}", hash_to_hex(hash));
        let r = self.raw("GET", &path, &[]).await?;
        if !r.status.is_success() {
            bail!("get_content: {} — {}", r.status, String::from_utf8_lossy(&r.body));
        }
        Ok(r.body)
    }

    pub async fn chunks_check(&self, hashes: &[FileHash]) -> Result<Vec<String>> {
        let payload: Vec<String> = hashes.iter().map(hash_to_hex).collect();
        let body = serde_json::to_vec(&payload)?;
        let r = self
            .raw("POST", "/api/v1/chunks/check", &body)
            .await?;
        if !r.status.is_success() {
            bail!("chunks_check: {}", r.status);
        }
        #[derive(Deserialize)]
        struct CheckResp {
            needed: Vec<String>,
        }
        let resp: CheckResp = serde_json::from_slice(&r.body)?;
        Ok(resp.needed)
    }
}

pub struct RawResponse {
    pub status: StatusCode,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PutRootResponse {
    #[serde(default)]
    pub accepted: bool,
    #[serde(default)]
    pub merged: bool,
    pub root_hash: String,
    #[serde(default)]
    pub conflicts: Vec<FileConflictWire>,
    #[serde(default)]
    pub auto_resolved: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FileConflictWire {
    pub path: String,
    pub base_hash: String,
    pub side_a_hash: String,
    pub side_b_hash: String,
}

#[derive(Debug, Clone)]
pub enum DiffResponse {
    InSync,
    Deltas(Vec<WireDelta>),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum WireDelta {
    Added {
        path: String,
        hash: String,
        size: u64,
    },
    Modified {
        path: String,
        hash: String,
        size: u64,
    },
    Deleted {
        path: String,
    },
    Renamed {
        path: String,
        old_path: String,
        hash: String,
    },
}

// --- AEAD wire format (independent reimplementation) ------------------------

fn build_aad(method: &str, path: &str) -> Vec<u8> {
    let mut aad = Vec::with_capacity(AAD_PREFIX.len() + 2 + method.len() + path.len());
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
    hk.expand(info, &mut out).expect("HKDF 32 bytes");
    out
}

fn encrypt_request(
    eph_priv: &StaticSecret,
    server_pub: &PublicKey,
    bearer: &str,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    if bearer.len() != BEARER_LEN {
        bail!("bearer must be {} chars, got {}", BEARER_LEN, bearer.len());
    }
    let shared = eph_priv.diffie_hellman(server_pub);
    let shared_bytes: [u8; KEY_LEN] = *shared.as_bytes();

    let mut nonce = [0u8; NONCE_LEN];
    rand::rngs::OsRng.try_fill_bytes(&mut nonce).unwrap();

    let key = hkdf_key(&shared_bytes, &nonce, INFO_C2S);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));
    let aad = build_aad(method, path);

    let mut plaintext = Vec::with_capacity(BEARER_LEN + body.len());
    plaintext.extend_from_slice(bearer.as_bytes());
    plaintext.extend_from_slice(body);

    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: &aad,
            },
        )
        .map_err(|e| anyhow!("AEAD seal: {}", e))?;

    let our_pub = PublicKey::from(eph_priv);
    let mut out = Vec::with_capacity(REQUEST_HEADER_LEN + ct.len());
    out.push(WIRE_VERSION);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(our_pub.as_bytes());
    out.extend_from_slice(&ct);
    Ok(out)
}

fn decrypt_response(
    eph_priv: &StaticSecret,
    server_pub: &PublicKey,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    if body.len() < RESPONSE_HEADER_LEN + TAG_LEN {
        bail!("response too short: {} bytes", body.len());
    }
    if body[0] != WIRE_VERSION {
        bail!("bad response wire version: {}", body[0]);
    }
    let nonce: [u8; NONCE_LEN] = body[1..1 + NONCE_LEN].try_into().unwrap();
    let ct = &body[RESPONSE_HEADER_LEN..];

    let shared = eph_priv.diffie_hellman(server_pub);
    let key = hkdf_key(shared.as_bytes(), &nonce, INFO_S2C);
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));
    let aad = build_aad(method, path);

    cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload { msg: ct, aad: &aad },
        )
        .map_err(|_| anyhow!("response AEAD open failed"))
}

// --- High-level vault helpers ------------------------------------------------

/// Build a Merkle root locally for a given set of files. Mirrors what
/// `sync_core::tree::build_tree` would produce, but stays synchronous and
/// returns the index chunks the caller will need to upload alongside the root.
///
/// Assumes <1000 entries per top-level prefix (single leaf per directory).
/// Tests don't push thousand-file vaults — sync-core unit tests already cover
/// the multi-leaf-chunk + InternalNode path.
pub fn build_root_for_files(
    vault_id: &str,
    device_id: &str,
    files: &[(String, Vec<u8>)],
) -> (RootNode, Vec<(FileHash, Vec<u8>)>) {
    let entries: Vec<FileEntry> = files
        .iter()
        .map(|(path, content)| {
            FileEntry::new(
                path.clone(),
                hash_bytes(content),
                0,
                content.len() as u64,
            )
        })
        .collect();

    let mut by_prefix: BTreeMap<String, Vec<FileEntry>> = BTreeMap::new();
    for e in entries {
        let prefix = match e.path.find('/') {
            Some(i) => e.path[..=i].to_string(),
            None => String::new(),
        };
        by_prefix.entry(prefix).or_default().push(e);
    }

    let mut chunks = Vec::new();
    let mut root_children = Vec::new();
    let mut total_files = 0u64;

    for (prefix, mut group) in by_prefix {
        group.sort();
        total_files += group.len() as u64;
        let leaf = LeafChunk::new(group);
        let bytes = leaf.serialize();
        let hash = hash_bytes(&bytes);
        chunks.push((hash, bytes));
        root_children.push((prefix, hash));
    }

    let root = RootNode {
        vault_id: vault_id.to_string(),
        created_ms: 0,
        version: 1,
        children: root_children,
        total_files,
        parent_hash: None,
        device_id: device_id.to_string(),
    };

    (root, chunks)
}

/// High-level: push a snapshot of `files` as the new root for `vault_id`.
///   - `parent_hex` should be ZERO_HASH_HEX for first-time push, else the
///     hex of the device's last-seen root.
/// Uploads content blobs, index chunks, then the root itself.
/// Returns the parsed PutRootResponse so callers can assert merge/conflict info.
pub async fn push_vault_snapshot(
    client: &WireClient,
    vault_id: &str,
    files: &[(String, Vec<u8>)],
    parent_hex: &str,
) -> Result<(RootNode, PutRootResponse)> {
    let (mut root, chunks) =
        build_root_for_files(vault_id, &client.creds.device_name, files);
    if parent_hex != ZERO_HASH_HEX {
        root.parent_hash = Some(hex_to_hash(parent_hex)?);
    }

    // Upload content blobs (ignore "already exists" — the server is
    // content-addressed and doesn't error on idempotent writes).
    for (_path, content) in files {
        let hash = hash_bytes(content);
        let _ = client.put_content(&hash, content).await;
    }

    // Upload index chunks (LeafChunk bytes etc).
    for (hash, bytes) in &chunks {
        client.put_chunk(hash, bytes).await?;
    }

    let resp = client.put_root(vault_id, &root, parent_hex).await?;
    Ok((root, resp))
}

/// High-level: pull every file currently visible at `vault_id` as
/// (path, bytes). Walks the root → child chunks → entries → content blobs.
pub async fn pull_vault_snapshot(
    client: &WireClient,
    vault_id: &str,
) -> Result<Vec<(String, Vec<u8>)>> {
    let root = client.get_root(vault_id).await?;
    let mut out = Vec::new();
    for (_prefix, child_hash) in &root.children {
        let bytes = client.get_chunk(child_hash).await?;
        let entries = if let Ok(leaf) = LeafChunk::deserialize(&bytes) {
            leaf.entries
        } else if let Ok(node) = InternalNode::deserialize(&bytes) {
            // One level of internal-node recursion is enough for our test
            // vaults (always <1000 entries per prefix) — sync-core tests cover
            // deeper nesting.
            let mut all = Vec::new();
            for (_, sub_hash) in node.children {
                let sub = client.get_chunk(&sub_hash).await?;
                let leaf = LeafChunk::deserialize(&sub)?;
                all.extend(leaf.entries);
            }
            all
        } else {
            bail!("could not deserialize child chunk as Leaf or Internal");
        };
        for e in entries {
            let content = client.get_content(&e.hash).await?;
            out.push((e.path, content));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// Generate a unique vault id for a test so tests can run sequentially in the
/// same container without colliding on vault state.
pub fn unique_vault_id(prefix: &str) -> String {
    let mut bytes = [0u8; 8];
    rand::rngs::OsRng.try_fill_bytes(&mut bytes).unwrap();
    format!("{}-{}", prefix, hex::encode(bytes))
}

/// Convenience: bring up a fresh harness, wait for /health, return env.
/// Most test files start with this.
pub async fn harness() -> E2eEnv {
    let env = E2eEnv::from_env();
    env.wait_for_health(Duration::from_secs(30))
        .await
        .expect("/health did not respond — is `just e2e-up` running?");
    env
}
