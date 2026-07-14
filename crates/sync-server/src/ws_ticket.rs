//! Short-lived, single-use tickets that authenticate WebSocket connections.
//!
//! The notify channel cannot sit behind `secure_envelope` (the middleware
//! buffers whole responses, which is fundamentally incompatible with a
//! stream), and putting the long-lived bearer token in a ws:// URL would
//! leak it into logs and proxies. Instead a device calls the SEALED
//! `POST /api/v1/ws-ticket` route, receives a random 10-minute ticket, and
//! spends it on the WebSocket handshake — same create/claim/burn shape as
//! device enrollment codes (enrollment.rs), which this deliberately mirrors.

use crate::storage::StorageLayout;
use rand::Rng;
use std::fs;
use std::path::PathBuf;

const TICKET_TTL_MS: u64 = 10 * 60 * 1000;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WsTicket {
    pub ticket: String,
    pub device_id: String,
    pub created_at: u64,
    pub expires_at: u64,
    /// v2 sealed-frame session keys (hex), derived at mint time from
    /// X25519(server_eph, client_eph) — present only when the client sent
    /// its ephemeral pubkey. Empty = ticket is only valid for a legacy v1
    /// (plaintext, root-frames-only) session. The file lives ≤ 10 minutes
    /// inside the data dir that already holds the server's long-term key.
    #[serde(default)]
    pub c2s_key_hex: String,
    #[serde(default)]
    pub s2c_key_hex: String,
}

/// Result of minting: the stored ticket + the server's ephemeral pubkey the
/// client needs to derive the same session keys (v2 only).
pub struct MintOutcome {
    pub ticket: WsTicket,
    pub server_eph_pub_b64: Option<String>,
}

fn tickets_dir(layout: &StorageLayout) -> PathBuf {
    layout.base.join("server").join("ws-tickets")
}

fn ticket_path(layout: &StorageLayout, ticket: &str) -> Option<PathBuf> {
    // Tickets are 64 lowercase hex chars; anything else never touches disk
    // (defends the path from traversal via user-supplied strings).
    if ticket.len() != 64 || !ticket.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(tickets_dir(layout).join(ticket.to_lowercase()))
}

/// Mint a ticket bound to an authenticated device. When the client supplies
/// its ephemeral X25519 pubkey, a fresh server ephemeral is generated and
/// the v2 sealed-frame session keys are derived and stored with the ticket.
pub fn mint(
    layout: &StorageLayout,
    device_id: &str,
    client_eph_pub: Option<[u8; 32]>,
) -> Result<MintOutcome, std::io::Error> {
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.random::<u8>()).collect();
    let ticket = hex::encode(bytes);

    let (c2s_hex, s2c_hex, server_eph_pub_b64) = match client_eph_pub {
        Some(client_pub_bytes) => {
            use rand::TryRngCore;
            let mut seed = [0u8; 32];
            rand::rngs::OsRng
                .try_fill_bytes(&mut seed)
                .map_err(|e| std::io::Error::other(format!("OS RNG failed: {}", e)))?;
            let server_eph = x25519_dalek::StaticSecret::from(seed);
            let server_pub = x25519_dalek::PublicKey::from(&server_eph);
            let client_pub = x25519_dalek::PublicKey::from(client_pub_bytes);
            let shared = server_eph.diffie_hellman(&client_pub);
            let keys = crate::secure::derive_ws_keys(shared.as_bytes(), &ticket);
            use base64::prelude::*;
            (
                hex::encode(keys.c2s),
                hex::encode(keys.s2c),
                Some(BASE64_STANDARD.encode(server_pub.as_bytes())),
            )
        }
        None => (String::new(), String::new(), None),
    };

    let now = now_ms();
    let info = WsTicket {
        ticket: ticket.clone(),
        device_id: device_id.to_string(),
        created_at: now,
        expires_at: now + TICKET_TTL_MS,
        c2s_key_hex: c2s_hex,
        s2c_key_hex: s2c_hex,
    };

    let dir = tickets_dir(layout);
    fs::create_dir_all(&dir)?;
    let path = ticket_path(layout, &ticket).expect("hex ticket is always a valid path");
    fs::write(&path, serde_json::to_string(&info)?)?;

    // Opportunistic sweep so abandoned tickets don't accumulate forever.
    sweep_expired(layout);

    Ok(MintOutcome {
        ticket: info,
        server_eph_pub_b64,
    })
}

/// Claim a ticket: burn-on-use, expiry-checked. Returns the full ticket
/// (device + session keys), or None for unknown/expired/reused tickets.
pub fn claim(layout: &StorageLayout, ticket: &str) -> Option<WsTicket> {
    let path = ticket_path(layout, ticket)?;
    let data = fs::read_to_string(&path).ok()?;
    // Single use: remove before validating so a parallel claim loses.
    let _ = fs::remove_file(&path);
    let info: WsTicket = serde_json::from_str(&data).ok()?;
    if now_ms() > info.expires_at {
        return None;
    }
    Some(info)
}

fn sweep_expired(layout: &StorageLayout) {
    let now = now_ms();
    let Ok(entries) = fs::read_dir(tickets_dir(layout)) else {
        return;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let Ok(data) = fs::read_to_string(entry.path()) else {
            continue;
        };
        if let Ok(info) = serde_json::from_str::<WsTicket>(&data) {
            if now > info.expires_at {
                let _ = fs::remove_file(entry.path());
            }
        } else {
            let _ = fs::remove_file(entry.path());
        }
    }
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
    fn mint_then_claim_returns_device_and_burns() {
        let (_d, layout) = fresh_layout();
        let m = mint(&layout, "device-abc", None).unwrap();
        assert_eq!(m.ticket.ticket.len(), 64);
        assert!(m.server_eph_pub_b64.is_none());
        let claimed = claim(&layout, &m.ticket.ticket).unwrap();
        assert_eq!(claimed.device_id, "device-abc");
        assert!(claimed.c2s_key_hex.is_empty(), "v1 mint carries no keys");
        // Burned: second claim fails.
        assert!(claim(&layout, &m.ticket.ticket).is_none());
    }

    #[test]
    fn v2_mint_derives_matching_session_keys() {
        let (_d, layout) = fresh_layout();
        // Client side of the exchange.
        use rand::TryRngCore;
        let mut seed = [0u8; 32];
        rand::rngs::OsRng.try_fill_bytes(&mut seed).unwrap();
        let client_secret = x25519_dalek::StaticSecret::from(seed);
        let client_pub = x25519_dalek::PublicKey::from(&client_secret);

        let m = mint(&layout, "device-v2", Some(*client_pub.as_bytes())).unwrap();
        let server_pub_b64 = m.server_eph_pub_b64.expect("v2 mint returns server pub");

        // Client derives with ITS secret + server's pub — must match stored keys.
        use base64::prelude::*;
        let server_pub_bytes: [u8; 32] = BASE64_STANDARD
            .decode(server_pub_b64)
            .unwrap()
            .try_into()
            .unwrap();
        let shared = client_secret.diffie_hellman(&x25519_dalek::PublicKey::from(server_pub_bytes));
        let keys = crate::secure::derive_ws_keys(shared.as_bytes(), &m.ticket.ticket);
        assert_eq!(hex::encode(keys.c2s), m.ticket.c2s_key_hex);
        assert_eq!(hex::encode(keys.s2c), m.ticket.s2c_key_hex);
    }

    #[test]
    fn expired_ticket_is_rejected() {
        let (_d, layout) = fresh_layout();
        let t = mint(&layout, "device-abc", None).unwrap().ticket;
        let path = ticket_path(&layout, &t.ticket).unwrap();
        let mut on_disk: WsTicket =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        on_disk.expires_at = 0;
        fs::write(&path, serde_json::to_string(&on_disk).unwrap()).unwrap();
        assert!(claim(&layout, &t.ticket).is_none());
    }

    #[test]
    fn garbage_tickets_never_touch_disk() {
        let (_d, layout) = fresh_layout();
        assert!(claim(&layout, "../../../etc/passwd").is_none());
        assert!(claim(&layout, "short").is_none());
        assert!(claim(&layout, &"zz".repeat(32)).is_none()); // non-hex
    }

    #[test]
    fn sweep_removes_expired_only() {
        let (_d, layout) = fresh_layout();
        let live = mint(&layout, "live", None).unwrap().ticket;
        let dead = mint(&layout, "dead", None).unwrap().ticket;
        let dead_path = ticket_path(&layout, &dead.ticket).unwrap();
        let mut on_disk: WsTicket =
            serde_json::from_str(&fs::read_to_string(&dead_path).unwrap()).unwrap();
        on_disk.expires_at = 0;
        fs::write(&dead_path, serde_json::to_string(&on_disk).unwrap()).unwrap();

        sweep_expired(&layout);
        assert!(!dead_path.exists());
        assert!(ticket_path(&layout, &live.ticket).unwrap().exists());
    }
}
