//! Bounded authoritative root/receipt codec. A receipt is a historical outcome,
//! not proof that its root is still current or that referenced objects exist.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{self, Write};
use sync_core::hash::{hash_to_hex, hex_to_hash, FileHash};

pub const MAX_ROOT_HEAD_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_ROOT_RECEIPT_BYTES: usize = 64 * 1024;
pub const MAX_ROOT_RECEIPT_DEVICES: usize = 128;
pub const MAX_ROOT_SEQUENCE: u64 = 9_007_199_254_740_991;
const MAGIC: &[u8] = b"OBSETYNC_ROOT_HEAD_V1\n";
const DOMAIN: &[u8] = b"obsetync:root-head:v1\0";
const PREFIX_BYTES: usize = MAGIC.len() + 64 + 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootReceipt {
    pub sequence: u64,
    pub mutation_id: String,
    pub request_hash: String,
    pub result: serde_json::Value,
}

impl RootReceipt {
    pub fn is_cancelled(&self) -> bool {
        self.result.as_object().is_some_and(|object| {
            object.len() == 1 && object.get("cancelled") == Some(&serde_json::Value::Bool(true))
        })
    }

    pub fn validate(&self) -> io::Result<()> {
        if self.sequence == 0 || self.sequence > MAX_ROOT_SEQUENCE {
            return Err(invalid(
                "root receipt sequence is not a positive safe integer",
            ));
        }
        if !lower_hex(&self.mutation_id, 32) || !lower_hex(&self.request_hash, 64) {
            return Err(invalid("invalid root receipt identity"));
        }
        validate_result(&self.result)?;
        // Bound the complete receipt, including identities and conflict paths,
        // before allocating the enclosing head's serialization buffer.
        bounded_json(self, MAX_ROOT_RECEIPT_BYTES)?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RootHead {
    pub root_hash: Option<FileHash>,
    pub receipts: BTreeMap<String, RootReceipt>,
}

#[derive(Serialize)]
struct PayloadRef<'a> {
    schema: u32,
    vault_id: &'a str,
    root_hash: Option<String>,
    receipts: &'a BTreeMap<String, RootReceipt>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    schema: u32,
    vault_id: String,
    root_hash: Option<String>,
    receipts: BTreeMap<String, RootReceipt>,
}

pub fn validate_device_id(device: &str) -> io::Result<()> {
    if device.is_empty()
        || device.len() > 128
        || device == "."
        || device == ".."
        || !device
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(invalid("invalid root receipt device identity"));
    }
    Ok(())
}

fn validate_head(head: &RootHead) -> io::Result<()> {
    if head.receipts.len() > MAX_ROOT_RECEIPT_DEVICES {
        return Err(invalid("root receipt device capacity exceeded"));
    }
    for (device, receipt) in &head.receipts {
        validate_device_id(device)?;
        receipt.validate()?;
    }
    if head.root_hash.is_none()
        && (head.receipts.is_empty()
            || head
                .receipts
                .values()
                .any(|receipt| !receipt.is_cancelled()))
    {
        return Err(invalid(
            "null root requires retained cancellation-only history",
        ));
    }
    Ok(())
}

/// Receipt-free heads remain legacy-readable; once any receipt exists, writers
/// must preserve it and cannot flatten the authoritative head back to 64 hex.
pub fn encode(vault_id: &str, head: &RootHead) -> io::Result<Vec<u8>> {
    validate_head(head)?;
    if head.receipts.is_empty() {
        return Ok(hash_to_hex(
            &head
                .root_hash
                .ok_or_else(|| invalid("missing legacy root"))?,
        )
        .into_bytes());
    }
    let payload = payload_bytes(vault_id, head)?;
    Ok(frame_payload(&payload))
}

pub fn decode(vault_id: &str, bytes: &[u8]) -> io::Result<RootHead> {
    if bytes.len() > MAX_ROOT_HEAD_BYTES {
        return Err(invalid("root head byte capacity exceeded"));
    }
    // Old writers used exactly 64 hex; retain their reader's surrounding
    // whitespace tolerance, but never interpret an unknown envelope as empty.
    if let Ok(text) = std::str::from_utf8(bytes) {
        let trimmed = text.trim();
        if trimmed.len() == 64 && trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Ok(RootHead {
                root_hash: Some(
                    hex_to_hash(trimmed).map_err(|_| invalid("invalid legacy root hash"))?,
                ),
                receipts: BTreeMap::new(),
            });
        }
    }
    if bytes.len() < PREFIX_BYTES || !bytes.starts_with(MAGIC) {
        return Err(invalid("unknown or truncated root head format"));
    }
    let checksum = &bytes[MAGIC.len()..MAGIC.len() + 64];
    if bytes[MAGIC.len() + 64] != b'\n'
        || !checksum
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
    {
        return Err(invalid("invalid root head checksum encoding"));
    }
    let payload = &bytes[PREFIX_BYTES..];
    if checksum != checksum_hex(payload).as_bytes() {
        return Err(invalid("root head checksum mismatch"));
    }
    let parsed: Payload =
        serde_json::from_slice(payload).map_err(|_| invalid("invalid root head payload"))?;
    if parsed.schema != 1
        || parsed.vault_id != vault_id
        || parsed
            .root_hash
            .as_ref()
            .is_some_and(|hash| !lower_hex(hash, 64))
    {
        return Err(invalid("unsupported root head schema or scope"));
    }
    let head = RootHead {
        root_hash: parsed
            .root_hash
            .as_ref()
            .map(|hash| hex_to_hash(hash).map_err(|_| invalid("invalid root hash")))
            .transpose()?,
        receipts: parsed.receipts,
    };
    validate_head(&head)?;
    // Writers emit a single canonical form. Besides binding the exact selected
    // bytes, this rejects ambiguous duplicate map/result keys after parsing.
    if payload_bytes(vault_id, &head)? != payload {
        return Err(invalid("noncanonical or ambiguous root head payload"));
    }
    Ok(head)
}

fn payload_bytes(vault_id: &str, head: &RootHead) -> io::Result<Vec<u8>> {
    bounded_json(
        &PayloadRef {
            schema: 1,
            vault_id,
            root_hash: head.root_hash.as_ref().map(hash_to_hex),
            receipts: &head.receipts,
        },
        MAX_ROOT_HEAD_BYTES - PREFIX_BYTES,
    )
}

fn checksum_hex(payload: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    hasher.update(payload);
    hasher.finalize().to_hex().to_string()
}

fn frame_payload(payload: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(PREFIX_BYTES + payload.len());
    result.extend_from_slice(MAGIC);
    result.extend_from_slice(checksum_hex(payload).as_bytes());
    result.push(b'\n');
    result.extend_from_slice(payload);
    result
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn validate_result(value: &serde_json::Value) -> io::Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("root outcome must be an object"))?;
    if object.len() == 1 && object.get("cancelled") == Some(&serde_json::Value::Bool(true)) {
        return Ok(());
    }
    if !object
        .get("root_hash")
        .and_then(|v| v.as_str())
        .is_some_and(|v| lower_hex(v, 64))
    {
        return Err(invalid("invalid root outcome hash"));
    }
    if object.get("accepted") == Some(&serde_json::Value::Bool(true)) && object.len() == 2 {
        return Ok(());
    }
    if object.get("merged") != Some(&serde_json::Value::Bool(true)) || object.len() != 5 {
        return Err(invalid("invalid root outcome discriminator or fields"));
    }
    for field in ["auto_resolved", "text_merged"] {
        if object
            .get(field)
            .and_then(|v| v.as_u64())
            .is_none_or(|v| v > MAX_ROOT_SEQUENCE)
        {
            return Err(invalid("invalid root merge count"));
        }
    }
    let conflicts = object
        .get("conflicts")
        .and_then(|v| v.as_array())
        .ok_or_else(|| invalid("invalid root merge conflicts"))?;
    for conflict in conflicts {
        let row = conflict
            .as_object()
            .ok_or_else(|| invalid("invalid root conflict row"))?;
        if row.len() != 4
            || row
                .get("path")
                .and_then(|v| v.as_str())
                .is_none_or(|p| p.is_empty())
        {
            return Err(invalid("invalid root conflict fields"));
        }
        for field in ["base_hash", "side_a_hash", "side_b_hash"] {
            if !row
                .get(field)
                .and_then(|v| v.as_str())
                .is_some_and(|v| lower_hex(v, 64))
            {
                return Err(invalid("invalid root conflict hash"));
            }
        }
    }
    Ok(())
}

fn bounded_json(value: &impl Serialize, limit: usize) -> io::Result<Vec<u8>> {
    struct LimitedWriter {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl Write for LimitedWriter {
        fn write(&mut self, input: &[u8]) -> io::Result<usize> {
            if input.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(invalid("root metadata byte capacity exceeded"));
            }
            self.bytes.extend_from_slice(input);
            Ok(input.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = LimitedWriter {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| invalid("root metadata encoding or capacity failure"))?;
    Ok(writer.bytes)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(super) fn receipt(sequence: u64) -> RootReceipt {
        RootReceipt {
            sequence,
            mutation_id: "a".repeat(32),
            request_hash: "b".repeat(64),
            result: json!({"accepted":true,"root_hash":"c".repeat(64)}),
        }
    }

    fn head() -> RootHead {
        RootHead {
            root_hash: Some([0x44; 32]),
            receipts: BTreeMap::from([("device".into(), receipt(1))]),
        }
    }

    #[test]
    fn cancelled_receipts_allow_explicit_null_root_but_never_accepted_empty_authority() {
        let mut cancelled = receipt(1);
        cancelled.result = json!({"cancelled":true});
        cancelled.validate().unwrap();
        assert!(cancelled.is_cancelled());
        let head = RootHead {
            root_hash: None,
            receipts: BTreeMap::from([("device".into(), cancelled)]),
        };
        let bytes = encode("vault", &head).unwrap();
        assert_eq!(decode("vault", &bytes).unwrap(), head);
        assert!(String::from_utf8(bytes.clone())
            .unwrap()
            .contains("\"root_hash\":null"));
        assert!(decode("other", &bytes).is_err());
        let mut empty = head.clone();
        empty.receipts.clear();
        assert!(encode("vault", &empty).is_err());
        let mut invalid_head = head.clone();
        invalid_head
            .receipts
            .insert("accepted-device".into(), receipt(1));
        assert!(encode("vault", &invalid_head).is_err());
        let payload = payload_bytes("vault", &invalid_head).unwrap();
        assert!(decode("vault", &frame_payload(&payload)).is_err());
        let missing_root = String::from_utf8(payload_bytes("vault", &head).unwrap())
            .unwrap()
            .replace("\"root_hash\":null,", "");
        assert!(decode("vault", &frame_payload(missing_root.as_bytes())).is_err());
        for result in [
            json!({"cancelled":false}),
            json!({"cancelled":null}),
            json!({"cancelled":true,"root_hash":"a".repeat(64)}),
            json!({"cancelled":true,"accepted":true}),
            json!({"cancelled":true,"unknown":0}),
        ] {
            let mut invalid = receipt(1);
            invalid.result = result;
            assert!(invalid.validate().is_err());
            assert!(!invalid.is_cancelled());
        }
    }

    #[test]
    fn canonical_roundtrip_and_legacy_read() {
        let h = head();
        let bytes = encode("vault", &h).unwrap();
        assert_eq!(decode("vault", &bytes).unwrap(), h);
        assert!(decode("other-vault", &bytes).is_err());
        let legacy = RootHead {
            root_hash: Some([1; 32]),
            receipts: BTreeMap::new(),
        };
        assert_eq!(encode("vault", &legacy).unwrap().len(), 64);
        assert_eq!(
            decode("vault", &encode("vault", &legacy).unwrap()).unwrap(),
            legacy
        );
        assert_eq!(
            decode(
                "vault",
                format!(" {}\n", hash_to_hex(&legacy.root_hash.unwrap())).as_bytes()
            )
            .unwrap(),
            legacy
        );
    }

    #[test]
    fn corruption_truncation_unknown_schema_and_duplicate_keys_fail_closed() {
        let h = head();
        let bytes = encode("vault", &h).unwrap();
        for end in 0..bytes.len() {
            assert!(
                decode("vault", &bytes[..end]).is_err(),
                "truncation at {end}"
            );
        }
        let mut corrupt = bytes.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(decode("vault", &corrupt).is_err());
        let payload = String::from_utf8(payload_bytes("vault", &h).unwrap()).unwrap();
        for changed in [
            payload.replace("\"schema\":1", "\"schema\":2"),
            payload.replace("\"schema\":1", "\"schema\":1,\"unknown\":0"),
            payload.replace("\"accepted\":true", "\"accepted\":false,\"accepted\":true"),
            payload.replace("\"sequence\":1", "\"sequence\":0,\"sequence\":1"),
            payload.replace(
                "\"receipts\":{",
                &format!(
                    "\"receipts\":{{\"device\":{},",
                    serde_json::to_string(&receipt(2)).unwrap()
                ),
            ),
        ] {
            assert!(decode("vault", &frame_payload(changed.as_bytes())).is_err());
        }
        assert!(decode("vault", &vec![b' '; MAX_ROOT_HEAD_BYTES + 1]).is_err());
    }

    #[test]
    fn receipt_identity_outcome_and_count_limits_are_strict() {
        for seq in [0, MAX_ROOT_SEQUENCE + 1, u64::MAX] {
            assert!(receipt(seq).validate().is_err());
        }
        receipt(MAX_ROOT_SEQUENCE).validate().unwrap();
        let mut h = head();
        for id in ["", "..", "../x", "x/y", "x\\y", "x\0y", &"x".repeat(129)] {
            assert!(validate_device_id(id).is_err());
        }
        for result in [
            json!(null),
            json!({"accepted":false,"root_hash":"c".repeat(64)}),
            json!({"accepted":true,"merged":true,"root_hash":"c".repeat(64)}),
            json!({"accepted":true,"root_hash":"C".repeat(64)}),
            json!({"merged":true,"root_hash":"c".repeat(64),"conflicts":[],"auto_resolved":-1,"text_merged":0}),
        ] {
            let mut r = receipt(1);
            r.result = result;
            assert!(r.validate().is_err());
        }
        for i in 0..MAX_ROOT_RECEIPT_DEVICES {
            h.receipts.insert(format!("device-{i}"), receipt(1));
        }
        assert!(encode("vault", &h).is_err());
    }

    #[test]
    fn byte_caps_cover_conflicts_and_aggregate_not_just_receipt_count() {
        let mut r = receipt(1);
        r.result = json!({"merged":true,"root_hash":"c".repeat(64),"conflicts":[{
            "path":"x".repeat(60*1024),"base_hash":"a".repeat(64),"side_a_hash":"b".repeat(64),"side_b_hash":"c".repeat(64)
        }],"auto_resolved":0,"text_merged":MAX_ROOT_SEQUENCE});
        r.validate().unwrap();
        let h = RootHead {
            root_hash: Some([1; 32]),
            receipts: (0..40)
                .map(|i| (format!("device-{i}"), r.clone()))
                .collect(),
        };
        assert!(encode("vault", &h).is_err());
        r.result["conflicts"][0]["path"] = json!("x".repeat(MAX_ROOT_RECEIPT_BYTES));
        assert!(r.validate().is_err());
    }
}
