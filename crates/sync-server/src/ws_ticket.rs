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

/// Mint a ticket bound to an authenticated device.
pub fn mint(layout: &StorageLayout, device_id: &str) -> Result<WsTicket, std::io::Error> {
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.random::<u8>()).collect();
    let ticket = hex::encode(bytes);

    let now = now_ms();
    let info = WsTicket {
        ticket: ticket.clone(),
        device_id: device_id.to_string(),
        created_at: now,
        expires_at: now + TICKET_TTL_MS,
    };

    let dir = tickets_dir(layout);
    fs::create_dir_all(&dir)?;
    let path = ticket_path(layout, &ticket).expect("hex ticket is always a valid path");
    fs::write(&path, serde_json::to_string(&info)?)?;

    // Opportunistic sweep so abandoned tickets don't accumulate forever.
    sweep_expired(layout);

    Ok(info)
}

/// Claim a ticket: burn-on-use, expiry-checked. Returns the device_id the
/// ticket was minted for, or None for unknown/expired/reused tickets.
pub fn claim(layout: &StorageLayout, ticket: &str) -> Option<String> {
    let path = ticket_path(layout, ticket)?;
    let data = fs::read_to_string(&path).ok()?;
    // Single use: remove before validating so a parallel claim loses.
    let _ = fs::remove_file(&path);
    let info: WsTicket = serde_json::from_str(&data).ok()?;
    if now_ms() > info.expires_at {
        return None;
    }
    Some(info.device_id)
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
        let t = mint(&layout, "device-abc").unwrap();
        assert_eq!(t.ticket.len(), 64);
        assert_eq!(claim(&layout, &t.ticket), Some("device-abc".into()));
        // Burned: second claim fails.
        assert_eq!(claim(&layout, &t.ticket), None);
    }

    #[test]
    fn expired_ticket_is_rejected() {
        let (_d, layout) = fresh_layout();
        let t = mint(&layout, "device-abc").unwrap();
        let path = ticket_path(&layout, &t.ticket).unwrap();
        let mut on_disk: WsTicket =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        on_disk.expires_at = 0;
        fs::write(&path, serde_json::to_string(&on_disk).unwrap()).unwrap();
        assert_eq!(claim(&layout, &t.ticket), None);
    }

    #[test]
    fn garbage_tickets_never_touch_disk() {
        let (_d, layout) = fresh_layout();
        assert_eq!(claim(&layout, "../../../etc/passwd"), None);
        assert_eq!(claim(&layout, "short"), None);
        assert_eq!(claim(&layout, &"zz".repeat(32)), None); // non-hex
    }

    #[test]
    fn sweep_removes_expired_only() {
        let (_d, layout) = fresh_layout();
        let live = mint(&layout, "live").unwrap();
        let dead = mint(&layout, "dead").unwrap();
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
