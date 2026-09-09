//! Application root identities, separate from transport anti-replay counters.
use crate::error::ServerError;
use crate::root_head::RootReceipt;
use base64::Engine;
use serde::Deserialize;

pub const MAX_ROOT_BYTES: usize = 512 * 1024;
pub const MAX_COMMIT_REQUEST_BYTES: usize = 704 * 1024;
pub const MAX_QUERY_BYTES: usize = 4 * 1024;
pub const MAX_CANCEL_REQUEST_BYTES: usize = 4 * 1024;
pub const MAX_SEQUENCE: u64 = 9_007_199_254_740_991;
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug)]
pub struct RootCommit {
    pub server_incarnation: String,
    pub sequence: u64,
    pub mutation_id: String,
    pub request_hash: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitRequest {
    protocol_version: u32,
    server_incarnation: String,
    sequence: u64,
    mutation_id: String,
    parent_root: String,
    root: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelRequest {
    protocol_version: u32,
    server_incarnation: String,
    sequence: u64,
    mutation_id: String,
    request_hash: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeQuery {
    protocol_version: u32,
    #[serde(default, deserialize_with = "present")]
    pub sequence: Option<u64>,
    #[serde(default, deserialize_with = "present")]
    pub mutation_id: Option<String>,
    #[serde(default, deserialize_with = "present")]
    pub request_hash: Option<String>,
}

// Omission means stream discovery; explicit null is not a missing identity.
fn present<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(decoder).map(Some)
}

fn invalid() -> ServerError {
    ServerError::BadRequest("invalid root-outcome-v1 request".into())
}

fn hex_field(value: &str, bytes: usize) -> bool {
    value.len() == bytes * 2
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_sequence(value: u64) -> bool {
    (1..=MAX_SEQUENCE).contains(&value)
}

pub fn decode_commit(
    bytes: &[u8],
    vault: &str,
    device: &str,
) -> Result<(RootCommit, Vec<u8>), ServerError> {
    if bytes.len() > MAX_COMMIT_REQUEST_BYTES {
        return Err(ServerError::PayloadTooLarge(
            "root commit request exceeds limit".into(),
        ));
    }
    let request: CommitRequest = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if request.protocol_version != PROTOCOL_VERSION
        || !valid_sequence(request.sequence)
        || !hex_field(&request.server_incarnation, 32)
        || !hex_field(&request.mutation_id, 16)
        || (!request.parent_root.is_empty() && !hex_field(&request.parent_root, 32))
        || vault.is_empty()
        || vault.len() > 128
        || device.is_empty()
        || device.len() > 128
    {
        return Err(invalid());
    }
    if request.root.len() > MAX_ROOT_BYTES.div_ceil(3) * 4 {
        return Err(ServerError::PayloadTooLarge(
            "root bytes exceed limit".into(),
        ));
    }
    let root = base64::engine::general_purpose::STANDARD
        .decode(&request.root)
        .map_err(|_| invalid())?;
    if root.len() > MAX_ROOT_BYTES {
        return Err(ServerError::PayloadTooLarge(
            "root bytes exceed limit".into(),
        ));
    }
    if root.is_empty() {
        return Err(invalid());
    }
    // Fixed sequence encoding and length-delimited fields avoid concatenation
    // ambiguity. Hash exact decoded root bytes, not just its semantic hash.
    let mut hash = blake3::Hasher::new();
    hash.update(b"obsetync.root-commit.v1\0");
    hash.update(&request.sequence.to_le_bytes());
    for field in [
        vault.as_bytes(),
        device.as_bytes(),
        request.server_incarnation.as_bytes(),
        request.mutation_id.as_bytes(),
        request.parent_root.as_bytes(),
        root.as_slice(),
    ] {
        hash.update(&(field.len() as u64).to_le_bytes());
        hash.update(field);
    }
    let commit = RootCommit {
        server_incarnation: request.server_incarnation,
        sequence: request.sequence,
        mutation_id: request.mutation_id,
        request_hash: hash.finalize().to_hex().to_string(),
    };
    let mut legacy_body = Vec::with_capacity(64 + root.len());
    legacy_body.extend_from_slice(format!("{:<64}", request.parent_root).as_bytes());
    legacy_body.extend_from_slice(&root);
    Ok((commit, legacy_body))
}

pub fn decode_query(bytes: &[u8]) -> Result<OutcomeQuery, ServerError> {
    if bytes.len() > MAX_QUERY_BYTES {
        return Err(ServerError::PayloadTooLarge(
            "root outcome query exceeds limit".into(),
        ));
    }
    let query: OutcomeQuery = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if query.protocol_version != PROTOCOL_VERSION {
        return Err(invalid());
    }
    match (&query.sequence, &query.mutation_id, &query.request_hash) {
        (None, None, None) => {}
        (Some(sequence), Some(mutation), Some(hash))
            if valid_sequence(*sequence) && hex_field(mutation, 16) && hex_field(hash, 32) => {}
        _ => return Err(invalid()),
    }
    Ok(query)
}

/// Cancellation names the original commit digest. It does not rebind that
/// identity to a new incarnation or authorize publication of different bytes.
pub fn decode_cancel(bytes: &[u8], vault: &str, device: &str) -> Result<RootCommit, ServerError> {
    if bytes.len() > MAX_CANCEL_REQUEST_BYTES {
        return Err(ServerError::PayloadTooLarge(
            "root cancellation request exceeds limit".into(),
        ));
    }
    let request: CancelRequest = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if request.protocol_version != PROTOCOL_VERSION
        || !valid_sequence(request.sequence)
        || !hex_field(&request.server_incarnation, 32)
        || !hex_field(&request.mutation_id, 16)
        || !hex_field(&request.request_hash, 32)
        || vault.is_empty()
        || vault.len() > 128
        || device.is_empty()
        || device.len() > 128
    {
        return Err(invalid());
    }
    Ok(RootCommit {
        server_incarnation: request.server_incarnation,
        sequence: request.sequence,
        mutation_id: request.mutation_id,
        request_hash: request.request_hash,
    })
}

pub fn terminal_outcome(incarnation: &str, receipt: &RootReceipt) -> serde_json::Value {
    serde_json::json!({ "protocol_version": PROTOCOL_VERSION, "server_incarnation": incarnation,
        "status": if receipt.is_cancelled() {"cancelled"} else {"accepted"},
        "sequence": receipt.sequence, "mutation_id": receipt.mutation_id,
        "request_hash": receipt.request_hash, "result": receipt.result })
}

/// Called with the vault write lock held, before tree validation or guard
/// consumption. A higher current root is irrelevant to an exact old receipt.
pub fn replay_or_admit(
    commit: &RootCommit,
    receipt: Option<&RootReceipt>,
    incarnation: &str,
) -> Result<Option<serde_json::Value>, ServerError> {
    let last = receipt.map_or(0, |receipt| receipt.sequence);
    if let Some(receipt) = receipt {
        if commit.sequence == last {
            return if commit.mutation_id == receipt.mutation_id
                && commit.request_hash == receipt.request_hash
            {
                Ok(Some(terminal_outcome(incarnation, receipt)))
            } else {
                Err(ServerError::Conflict(
                    "root mutation identity does not match stored request".into(),
                ))
            };
        }
    }
    if commit.sequence <= last {
        return Err(ServerError::Conflict(
            "root mutation outcome expired; reconciliation required".into(),
        ));
    }
    if commit.sequence != last + 1 {
        return Err(ServerError::Conflict(
            "root mutation sequence gap; reconciliation required".into(),
        ));
    }
    if commit.server_incarnation != incarnation {
        return Err(ServerError::Conflict(
            "root mutation incarnation changed; outcome unknown, reconciliation required".into(),
        ));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> serde_json::Value {
        serde_json::json!({ "protocol_version": 1, "server_incarnation": "a".repeat(64),
            "sequence": 1, "mutation_id": "b".repeat(32), "parent_root": "",
            "root": base64::engine::general_purpose::STANDARD.encode(b"exact-root-bytes") })
    }

    fn decode(value: &serde_json::Value, vault: &str, device: &str) -> RootCommit {
        decode_commit(&serde_json::to_vec(value).unwrap(), vault, device)
            .unwrap()
            .0
    }

    #[test]
    fn request_digest_cross_language_golden_vectors() {
        for (sequence, expected) in [
            (
                1,
                "e39b5ae8022adf0611a7136d172bfa1d088f2c873e47ac815adae4769b039c31",
            ),
            (
                MAX_SEQUENCE,
                "bf7c1de11c00a9f3a9319d5f2605ca41af857ce7c6f49722dbc453b49707553e",
            ),
        ] {
            let mut value = request();
            value["sequence"] = serde_json::json!(sequence);
            assert_eq!(decode(&value, "vault", "device").request_hash, expected);
        }
    }

    #[test]
    fn cancellation_decoder_is_strict_bounded_and_preserves_the_original_digest() {
        let valid = serde_json::json!({"protocol_version":1,"server_incarnation":"a".repeat(64),
            "sequence":1,"mutation_id":"b".repeat(32),"request_hash":"c".repeat(64)});
        let bytes = serde_json::to_vec(&valid).unwrap();
        let parsed = decode_cancel(&bytes, "vault", "device").unwrap();
        assert_eq!(parsed.request_hash, "c".repeat(64));
        let mut padded = bytes.clone();
        padded.resize(MAX_CANCEL_REQUEST_BYTES, b' ');
        decode_cancel(&padded, "vault", "device").unwrap();
        padded.push(b' ');
        assert!(matches!(
            decode_cancel(&padded, "vault", "device"),
            Err(ServerError::PayloadTooLarge(_))
        ));
        for (field, value) in [
            ("protocol_version", serde_json::json!(2)),
            ("sequence", serde_json::json!(0)),
            ("sequence", serde_json::json!(MAX_SEQUENCE + 1)),
            ("sequence", serde_json::json!(1.0)),
            ("request_hash", serde_json::json!("C".repeat(64))),
            ("request_hash", serde_json::Value::Null),
            ("mutation_id", serde_json::json!("d".repeat(31))),
            ("server_incarnation", serde_json::json!("a".repeat(63))),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            assert!(
                decode_cancel(&serde_json::to_vec(&invalid).unwrap(), "vault", "device").is_err()
            );
        }
        for field in [
            "protocol_version",
            "sequence",
            "mutation_id",
            "request_hash",
            "server_incarnation",
        ] {
            let mut missing = valid.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                decode_cancel(&serde_json::to_vec(&missing).unwrap(), "vault", "device").is_err()
            );
        }
        let duplicate = serde_json::to_string(&valid)
            .unwrap()
            .replacen('{', "{\"sequence\":1,", 1);
        assert!(decode_cancel(duplicate.as_bytes(), "vault", "device").is_err());
        let mut unknown = valid;
        unknown["root"] = serde_json::json!("ignored");
        assert!(decode_cancel(&serde_json::to_vec(&unknown).unwrap(), "vault", "device").is_err());
        for (vault, device) in [
            ("", "device"),
            ("vault", ""),
            (&"v".repeat(129), "device"),
            ("vault", &"d".repeat(129)),
        ] {
            assert!(decode_cancel(&bytes, vault, device).is_err());
        }
    }

    #[test]
    fn cancelled_terminal_replay_wins_before_epoch_and_can_never_become_accepted() {
        let commit = decode(&request(), "vault", "device");
        let receipt = RootReceipt {
            sequence: commit.sequence,
            mutation_id: commit.mutation_id.clone(),
            request_hash: commit.request_hash.clone(),
            result: serde_json::json!({"cancelled":true}),
        };
        let terminal = replay_or_admit(&commit, Some(&receipt), &"f".repeat(64))
            .unwrap()
            .unwrap();
        assert_eq!(terminal["status"], "cancelled");
        assert_eq!(terminal["result"], serde_json::json!({"cancelled":true}));
        let mut mismatch = commit;
        mismatch.request_hash = "e".repeat(64);
        assert!(matches!(
            replay_or_admit(&mismatch, Some(&receipt), &mismatch.server_incarnation),
            Err(ServerError::Conflict(_))
        ));
    }

    #[test]
    fn binding_separates_every_application_field_and_not_json_key_order() {
        let request = request();
        let original = decode(&request, "vault", "device");
        assert_ne!(
            decode(&request, "other-vault", "device").request_hash,
            original.request_hash
        );
        assert_ne!(
            decode(&request, "vault", "other-device").request_hash,
            original.request_hash
        );
        for (field, value) in [
            ("server_incarnation", serde_json::json!("c".repeat(64))),
            ("sequence", serde_json::json!(2)),
            ("mutation_id", serde_json::json!("d".repeat(32))),
            ("parent_root", serde_json::json!("e".repeat(64))),
            (
                "root",
                serde_json::json!(
                    base64::engine::general_purpose::STANDARD.encode(b"other-root-bytes")
                ),
            ),
        ] {
            let mut changed = request.clone();
            changed[field] = value;
            assert_ne!(
                decode(&changed, "vault", "device").request_hash,
                original.request_hash,
                "unbound {field}"
            );
        }
        let reordered = format!("{{\"root\":{},\"parent_root\":{},\"mutation_id\":{},\"sequence\":{},\"server_incarnation\":{},\"protocol_version\":1}}",
            request["root"], request["parent_root"], request["mutation_id"], request["sequence"], request["server_incarnation"]);
        assert_eq!(
            decode_commit(reordered.as_bytes(), "vault", "device")
                .unwrap()
                .0
                .request_hash,
            original.request_hash
        );
        assert_ne!(
            decode(&request, "ab", "c").request_hash,
            decode(&request, "a", "bc").request_hash
        );
    }

    #[test]
    fn duplicate_fields_and_out_of_range_numbers_are_not_canonical_requests() {
        let bytes = serde_json::to_string(&request()).unwrap();
        let duplicated = bytes.replacen('{', "{\"sequence\":1,", 1);
        assert!(decode_commit(duplicated.as_bytes(), "vault", "device").is_err());
        for sequence in [0, MAX_SEQUENCE + 1, u64::MAX] {
            let mut invalid = request();
            invalid["sequence"] = serde_json::json!(sequence);
            assert!(
                decode_commit(&serde_json::to_vec(&invalid).unwrap(), "vault", "device").is_err()
            );
        }
        let mut maximum = request();
        maximum["sequence"] = serde_json::json!(MAX_SEQUENCE);
        assert_eq!(decode(&maximum, "vault", "device").sequence, MAX_SEQUENCE);
        assert!(decode_query(br#"{"protocol_version":1,"protocol_version":1}"#).is_err());
        for bytes in [
            br#"{"protocol_version":1,"sequence":null}"#.as_slice(),
            br#"{"protocol_version":1,"sequence":null,"mutation_id":null,"request_hash":null}"#
                .as_slice(),
            br#"{"protocol_version":1,"mutation_id":null}"#.as_slice(),
            br#"{"protocol_version":1,"request_hash":null}"#.as_slice(),
        ] {
            assert!(decode_query(bytes).is_err());
        }
    }

    #[test]
    fn receipt_replay_precedes_incarnation_fence_and_sequence_exhaustion_never_wraps() {
        let request = request();
        let commit = decode(&request, "vault", "device");
        let receipt = RootReceipt {
            sequence: commit.sequence,
            mutation_id: commit.mutation_id.clone(),
            request_hash: commit.request_hash.clone(),
            result: serde_json::json!({ "accepted": true, "root_hash": "f".repeat(64) }),
        };
        let new_incarnation = "c".repeat(64);
        let replay = replay_or_admit(&commit, Some(&receipt), &new_incarnation)
            .unwrap()
            .unwrap();
        assert_eq!(replay["result"], receipt.result);
        assert_eq!(replay["server_incarnation"], new_incarnation);
        assert!(replay_or_admit(&commit, None, &new_incarnation).is_err());
        assert!(replay_or_admit(&commit, None, &commit.server_incarnation)
            .unwrap()
            .is_none());
        let mut exhausted = receipt;
        exhausted.sequence = MAX_SEQUENCE;
        assert!(replay_or_admit(&commit, Some(&exhausted), &commit.server_incarnation).is_err());
    }
}
