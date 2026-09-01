//! Strict binary codecs for the bounded bulk HTTP data plane.
//!
//! The sealed transport authenticates and encrypts these bytes.  This module
//! deliberately remains transport-agnostic and performs every count/length
//! check before reserving storage derived from peer input.

use sync_core::hash::FileHash;

pub const CHECK_MAGIC: &[u8; 4] = b"OBC1";
pub const CHECK_ACK_MAGIC: &[u8; 4] = b"OBA1";
pub const PACK_MAGIC: &[u8; 4] = b"OBP1";
pub const PACK_ACK_MAGIC: &[u8; 4] = b"OBK1";
pub const GET_MAGIC: &[u8; 4] = b"OBG1";
pub const DOWNLOAD_MAGIC: &[u8; 4] = b"OBD1";

pub const CHECK_HEADER_BYTES: usize = 9;
pub const PACK_HEADER_BYTES: usize = 10;
pub const RECORD_HEADER_BYTES: usize = 42;
pub const GET_HEADER_BYTES: usize = 17;
pub const DOWNLOAD_HEADER_BYTES: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjectKind {
    Content = 0,
    ContentChunk = 1,
    IndexChunk = 2,
    Manifest = 3,
}

impl TryFrom<u8> for ObjectKind {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Content),
            1 => Ok(Self::ContentChunk),
            2 => Ok(Self::IndexChunk),
            3 => Ok(Self::Manifest),
            _ => Err(CodecError::new("unknown object kind")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckRequest {
    pub kind: ObjectKind,
    pub hashes: Vec<FileHash>,
}

#[derive(Debug, Clone, Copy)]
pub struct UploadRecord<'a> {
    pub kind: ObjectKind,
    pub flags: u8,
    pub hash: FileHash,
    pub plain_len: u32,
    pub bytes: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct UploadPack<'a> {
    pub records: Vec<UploadRecord<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetRequest {
    pub kind: ObjectKind,
    pub cursor: u32,
    pub max_response_bytes: u32,
    pub hashes: Vec<FileHash>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UploadStatus {
    Stored = 0,
    AlreadyPresent = 1,
    BadHash = 2,
    RejectedLimit = 3,
    RetryableStorageError = 4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError {
    message: &'static str,
}

impl CodecError {
    const fn new(message: &'static str) -> Self {
        Self { message }
    }
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for CodecError {}

pub fn bitmap_bytes(count: usize) -> Result<usize, CodecError> {
    count
        .checked_add(7)
        .map(|value| value / 8)
        .ok_or_else(|| CodecError::new("bitmap length overflow"))
}

#[cfg(test)]
pub fn bitmap_get(bitmap: &[u8], index: usize) -> bool {
    bitmap
        .get(index / 8)
        .is_some_and(|byte| byte & (1 << (index % 8)) != 0)
}

pub fn bitmap_set(bitmap: &mut [u8], index: usize) {
    if let Some(byte) = bitmap.get_mut(index / 8) {
        *byte |= 1 << (index % 8);
    }
}

pub fn decode_check_request(body: &[u8], max_objects: usize) -> Result<CheckRequest, CodecError> {
    require_magic(body, CHECK_MAGIC)?;
    if body.len() < CHECK_HEADER_BYTES {
        return Err(CodecError::new("truncated check header"));
    }
    let kind = ObjectKind::try_from(body[4])?;
    let count = read_u32(body, 5)? as usize;
    if count > max_objects {
        return Err(CodecError::new("check object count exceeds limit"));
    }
    let hash_bytes = count
        .checked_mul(32)
        .ok_or_else(|| CodecError::new("check hash length overflow"))?;
    let expected = CHECK_HEADER_BYTES
        .checked_add(hash_bytes)
        .ok_or_else(|| CodecError::new("check length overflow"))?;
    if body.len() != expected {
        return Err(CodecError::new("check body length mismatch"));
    }

    let mut hashes = Vec::with_capacity(count);
    for raw in body[CHECK_HEADER_BYTES..].chunks_exact(32) {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(raw);
        hashes.push(hash);
    }
    Ok(CheckRequest { kind, hashes })
}

#[cfg(test)]
pub fn encode_check_request(kind: ObjectKind, hashes: &[FileHash]) -> Result<Vec<u8>, CodecError> {
    let count =
        u32::try_from(hashes.len()).map_err(|_| CodecError::new("check request count overflow"))?;
    let capacity = CHECK_HEADER_BYTES
        .checked_add(
            hashes
                .len()
                .checked_mul(32)
                .ok_or_else(|| CodecError::new("check request length overflow"))?,
        )
        .ok_or_else(|| CodecError::new("check request length overflow"))?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(CHECK_MAGIC);
    output.push(kind as u8);
    output.extend_from_slice(&count.to_le_bytes());
    for hash in hashes {
        output.extend_from_slice(hash);
    }
    Ok(output)
}

pub fn encode_check_response(needed: &[bool]) -> Result<Vec<u8>, CodecError> {
    let count = u32::try_from(needed.len())
        .map_err(|_| CodecError::new("check response count overflow"))?;
    let bitmap_len = bitmap_bytes(needed.len())?;
    let mut output = Vec::with_capacity(8 + bitmap_len);
    output.extend_from_slice(CHECK_ACK_MAGIC);
    output.extend_from_slice(&count.to_le_bytes());
    output.resize(8 + bitmap_len, 0);
    for (index, is_needed) in needed.iter().copied().enumerate() {
        if is_needed {
            bitmap_set(&mut output[8..], index);
        }
    }
    Ok(output)
}

pub fn decode_upload_pack<'a>(
    body: &'a [u8],
    max_objects: usize,
    max_request_bytes: usize,
) -> Result<UploadPack<'a>, CodecError> {
    if body.len() > max_request_bytes {
        return Err(CodecError::new("upload pack exceeds byte limit"));
    }
    require_magic(body, PACK_MAGIC)?;
    if body.len() < PACK_HEADER_BYTES {
        return Err(CodecError::new("truncated upload pack header"));
    }
    let flags = read_u16(body, 4)?;
    if flags != 0 {
        return Err(CodecError::new("unsupported upload pack flags"));
    }
    let count = read_u32(body, 6)? as usize;
    if count > max_objects {
        return Err(CodecError::new("upload object count exceeds limit"));
    }

    // Prove that the fixed record headers fit before reserving count-derived
    // storage.  Variable payload lengths are then checked against the already
    // bounded request slice; no payload is copied or allocated by the codec.
    let minimum = PACK_HEADER_BYTES
        .checked_add(
            count
                .checked_mul(RECORD_HEADER_BYTES)
                .ok_or_else(|| CodecError::new("upload record header overflow"))?,
        )
        .ok_or_else(|| CodecError::new("upload minimum length overflow"))?;
    if body.len() < minimum {
        return Err(CodecError::new("truncated upload record headers"));
    }

    let mut records = Vec::with_capacity(count);
    let mut cursor = PACK_HEADER_BYTES;
    for _ in 0..count {
        let header_end = cursor
            .checked_add(RECORD_HEADER_BYTES)
            .ok_or_else(|| CodecError::new("upload record cursor overflow"))?;
        if header_end > body.len() {
            return Err(CodecError::new("truncated upload record header"));
        }
        let kind = ObjectKind::try_from(body[cursor])?;
        let record_flags = body[cursor + 1];
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&body[cursor + 2..cursor + 34]);
        let plain_len = read_u32(body, cursor + 34)?;
        let stored_len = read_u32(body, cursor + 38)? as usize;
        let data_end = header_end
            .checked_add(stored_len)
            .ok_or_else(|| CodecError::new("upload stored length overflow"))?;
        if data_end > body.len() {
            return Err(CodecError::new("truncated upload record bytes"));
        }
        records.push(UploadRecord {
            kind,
            flags: record_flags,
            hash,
            plain_len,
            bytes: &body[header_end..data_end],
        });
        cursor = data_end;
    }
    if cursor != body.len() {
        return Err(CodecError::new("trailing bytes after upload pack"));
    }
    Ok(UploadPack { records })
}

pub fn encoded_pack_len(records: &[UploadRecord<'_>]) -> Result<usize, CodecError> {
    let mut total = PACK_HEADER_BYTES;
    for record in records {
        total = total
            .checked_add(RECORD_HEADER_BYTES)
            .and_then(|value| value.checked_add(record.bytes.len()))
            .ok_or_else(|| CodecError::new("encoded upload pack length overflow"))?;
    }
    Ok(total)
}

pub fn encode_upload_pack(records: &[UploadRecord<'_>]) -> Result<Vec<u8>, CodecError> {
    let count =
        u32::try_from(records.len()).map_err(|_| CodecError::new("upload pack count overflow"))?;
    let mut output = Vec::with_capacity(encoded_pack_len(records)?);
    output.extend_from_slice(PACK_MAGIC);
    output.extend_from_slice(&0u16.to_le_bytes());
    output.extend_from_slice(&count.to_le_bytes());
    for record in records {
        let stored_len = u32::try_from(record.bytes.len())
            .map_err(|_| CodecError::new("upload record length overflow"))?;
        output.push(record.kind as u8);
        output.push(record.flags);
        output.extend_from_slice(&record.hash);
        output.extend_from_slice(&record.plain_len.to_le_bytes());
        output.extend_from_slice(&stored_len.to_le_bytes());
        output.extend_from_slice(record.bytes);
    }
    Ok(output)
}

pub fn encode_upload_ack(statuses: &[UploadStatus]) -> Result<Vec<u8>, CodecError> {
    let count =
        u32::try_from(statuses.len()).map_err(|_| CodecError::new("upload ack count overflow"))?;
    let mut output = Vec::with_capacity(8 + statuses.len());
    output.extend_from_slice(PACK_ACK_MAGIC);
    output.extend_from_slice(&count.to_le_bytes());
    output.extend(statuses.iter().map(|status| *status as u8));
    Ok(output)
}

pub fn decode_get_request(body: &[u8], max_objects: usize) -> Result<GetRequest, CodecError> {
    require_magic(body, GET_MAGIC)?;
    if body.len() < GET_HEADER_BYTES {
        return Err(CodecError::new("truncated bulk get header"));
    }
    let kind = ObjectKind::try_from(body[4])?;
    let count = read_u32(body, 5)? as usize;
    if count > max_objects {
        return Err(CodecError::new("bulk get object count exceeds limit"));
    }
    let cursor = read_u32(body, 9)?;
    if cursor as usize > count {
        return Err(CodecError::new("bulk get cursor exceeds count"));
    }
    let max_response_bytes = read_u32(body, 13)?;
    let expected = GET_HEADER_BYTES
        .checked_add(
            count
                .checked_mul(32)
                .ok_or_else(|| CodecError::new("bulk get hash length overflow"))?,
        )
        .ok_or_else(|| CodecError::new("bulk get length overflow"))?;
    if body.len() != expected {
        return Err(CodecError::new("bulk get body length mismatch"));
    }

    let mut hashes = Vec::with_capacity(count);
    for raw in body[GET_HEADER_BYTES..].chunks_exact(32) {
        let mut hash = [0u8; 32];
        hash.copy_from_slice(raw);
        hashes.push(hash);
    }
    Ok(GetRequest {
        kind,
        cursor,
        max_response_bytes,
        hashes,
    })
}

#[cfg(test)]
pub fn encode_get_request(
    kind: ObjectKind,
    hashes: &[FileHash],
    cursor: u32,
    max_response_bytes: u32,
) -> Result<Vec<u8>, CodecError> {
    if cursor as usize > hashes.len() {
        return Err(CodecError::new("bulk get cursor exceeds count"));
    }
    let count = u32::try_from(hashes.len())
        .map_err(|_| CodecError::new("bulk get request count overflow"))?;
    let capacity = GET_HEADER_BYTES
        .checked_add(
            hashes
                .len()
                .checked_mul(32)
                .ok_or_else(|| CodecError::new("bulk get request length overflow"))?,
        )
        .ok_or_else(|| CodecError::new("bulk get request length overflow"))?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(GET_MAGIC);
    output.push(kind as u8);
    output.extend_from_slice(&count.to_le_bytes());
    output.extend_from_slice(&cursor.to_le_bytes());
    output.extend_from_slice(&max_response_bytes.to_le_bytes());
    for hash in hashes {
        output.extend_from_slice(hash);
    }
    Ok(output)
}

/// `remaining` is indexed against the original request, not the returned
/// pack.  `next_cursor == request_count` denotes completion.
pub fn encode_download_response(
    request_count: usize,
    next_cursor: usize,
    remaining: &[u8],
    records: &[UploadRecord<'_>],
) -> Result<Vec<u8>, CodecError> {
    if next_cursor > request_count {
        return Err(CodecError::new("download cursor exceeds request count"));
    }
    if remaining.len() != bitmap_bytes(request_count)? {
        return Err(CodecError::new("download remaining bitmap length mismatch"));
    }
    let count = u32::try_from(request_count)
        .map_err(|_| CodecError::new("download request count overflow"))?;
    let cursor =
        u32::try_from(next_cursor).map_err(|_| CodecError::new("download cursor overflow"))?;
    let pack = encode_upload_pack(records)?;
    let capacity = DOWNLOAD_HEADER_BYTES
        .checked_add(remaining.len())
        .and_then(|value| value.checked_add(pack.len()))
        .ok_or_else(|| CodecError::new("download response length overflow"))?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(DOWNLOAD_MAGIC);
    output.extend_from_slice(&count.to_le_bytes());
    output.extend_from_slice(&cursor.to_le_bytes());
    output.extend_from_slice(remaining);
    output.extend_from_slice(&pack);
    Ok(output)
}

fn require_magic(body: &[u8], expected: &[u8; 4]) -> Result<(), CodecError> {
    if body.get(..4) == Some(expected.as_slice()) {
        Ok(())
    } else {
        Err(CodecError::new("invalid bulk message magic"))
    }
}

fn read_u16(body: &[u8], offset: usize) -> Result<u16, CodecError> {
    let bytes = body
        .get(offset..offset + 2)
        .ok_or_else(|| CodecError::new("truncated u16"))?;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_u32(body: &[u8], offset: usize) -> Result<u32, CodecError> {
    let bytes = body
        .get(offset..offset + 4)
        .ok_or_else(|| CodecError::new("truncated u32"))?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sync_core::hash::hash_bytes;

    fn check_request(kind: ObjectKind, hashes: &[FileHash]) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(CHECK_MAGIC);
        output.push(kind as u8);
        output.extend_from_slice(&(hashes.len() as u32).to_le_bytes());
        for hash in hashes {
            output.extend_from_slice(hash);
        }
        output
    }

    fn get_request(
        kind: ObjectKind,
        hashes: &[FileHash],
        cursor: u32,
        max_response_bytes: u32,
    ) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(GET_MAGIC);
        output.push(kind as u8);
        output.extend_from_slice(&(hashes.len() as u32).to_le_bytes());
        output.extend_from_slice(&cursor.to_le_bytes());
        output.extend_from_slice(&max_response_bytes.to_le_bytes());
        for hash in hashes {
            output.extend_from_slice(hash);
        }
        output
    }

    #[test]
    fn check_round_trip_preserves_raw_hash_order() {
        let hashes = [hash_bytes(b"first"), hash_bytes(b"second")];
        let decoded =
            decode_check_request(&check_request(ObjectKind::ContentChunk, &hashes), 256).unwrap();
        assert_eq!(decoded.kind, ObjectKind::ContentChunk);
        assert_eq!(decoded.hashes, hashes);
    }

    #[test]
    fn check_rejects_wrong_magic_unknown_kind_count_and_lengths() {
        let hash = hash_bytes(b"x");
        let valid = check_request(ObjectKind::Content, &[hash]);
        let mut wrong_magic = valid.clone();
        wrong_magic[0] = b'X';
        assert!(decode_check_request(&wrong_magic, 256).is_err());
        let mut wrong_kind = valid.clone();
        wrong_kind[4] = 99;
        assert!(decode_check_request(&wrong_kind, 256).is_err());
        assert!(decode_check_request(&valid, 0).is_err());
        assert!(decode_check_request(&valid[..valid.len() - 1], 256).is_err());
        let mut trailing = valid;
        trailing.push(0);
        assert!(decode_check_request(&trailing, 256).is_err());
    }

    #[test]
    fn check_ack_bitmap_is_lsb_first() {
        let response =
            encode_check_response(&[true, false, false, true, false, false, false, true, true])
                .unwrap();
        assert_eq!(&response[..4], CHECK_ACK_MAGIC);
        assert_eq!(u32::from_le_bytes(response[4..8].try_into().unwrap()), 9);
        assert_eq!(&response[8..], &[0b1000_1001, 0b0000_0001]);
    }

    #[test]
    fn upload_pack_round_trip_is_zero_copy_and_ordered() {
        let first = b"hello";
        let second = b"world!";
        let records = [
            UploadRecord {
                kind: ObjectKind::Content,
                flags: 0,
                hash: hash_bytes(first),
                plain_len: first.len() as u32,
                bytes: first,
            },
            UploadRecord {
                kind: ObjectKind::IndexChunk,
                flags: 0,
                hash: hash_bytes(second),
                plain_len: second.len() as u32,
                bytes: second,
            },
        ];
        let encoded = encode_upload_pack(&records).unwrap();
        let decoded = decode_upload_pack(&encoded, 256, 8 * 1024 * 1024).unwrap();
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].bytes, first);
        assert_eq!(decoded.records[1].kind, ObjectKind::IndexChunk);
        assert_eq!(decoded.records[1].bytes, second);
    }

    #[test]
    fn upload_rejects_truncation_trailing_bytes_flags_and_hard_limits() {
        let bytes = b"payload";
        let record = UploadRecord {
            kind: ObjectKind::Content,
            flags: 0,
            hash: hash_bytes(bytes),
            plain_len: bytes.len() as u32,
            bytes,
        };
        let valid = encode_upload_pack(&[record]).unwrap();
        assert!(decode_upload_pack(&valid[..valid.len() - 1], 256, valid.len()).is_err());
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(decode_upload_pack(&trailing, 256, trailing.len()).is_err());
        let mut pack_flags = valid.clone();
        pack_flags[4] = 1;
        assert!(decode_upload_pack(&pack_flags, 256, pack_flags.len()).is_err());
        assert!(decode_upload_pack(&valid, 0, valid.len()).is_err());
        assert!(decode_upload_pack(&valid, 256, valid.len() - 1).is_err());
    }

    #[test]
    fn upload_rejects_declared_stored_length_before_slicing() {
        let bytes = b"x";
        let record = UploadRecord {
            kind: ObjectKind::Content,
            flags: 0,
            hash: hash_bytes(bytes),
            plain_len: 1,
            bytes,
        };
        let mut encoded = encode_upload_pack(&[record]).unwrap();
        encoded[48..52].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_upload_pack(&encoded, 256, 8 * 1024 * 1024).is_err());
    }

    #[test]
    fn upload_ack_preserves_mixed_statuses() {
        let encoded = encode_upload_ack(&[
            UploadStatus::Stored,
            UploadStatus::AlreadyPresent,
            UploadStatus::BadHash,
            UploadStatus::RejectedLimit,
            UploadStatus::RetryableStorageError,
        ])
        .unwrap();
        assert_eq!(&encoded[..4], PACK_ACK_MAGIC);
        assert_eq!(&encoded[8..], &[0, 1, 2, 3, 4]);
    }

    #[test]
    fn get_request_round_trip_and_bounds() {
        let hashes = [hash_bytes(b"a"), hash_bytes(b"b")];
        let encoded = get_request(ObjectKind::Manifest, &hashes, 1, 2 * 1024 * 1024);
        let decoded = decode_get_request(&encoded, 256).unwrap();
        assert_eq!(decoded.kind, ObjectKind::Manifest);
        assert_eq!(decoded.cursor, 1);
        assert_eq!(decoded.max_response_bytes, 2 * 1024 * 1024);
        assert_eq!(decoded.hashes, hashes);

        let invalid_cursor = get_request(ObjectKind::Content, &hashes, 3, 1024);
        assert!(decode_get_request(&invalid_cursor, 256).is_err());
        assert!(decode_get_request(&encoded[..encoded.len() - 1], 256).is_err());
    }

    #[test]
    fn download_response_contains_request_bitmap_and_nested_pack() {
        let bytes = b"download";
        let record = UploadRecord {
            kind: ObjectKind::Content,
            flags: 0,
            hash: hash_bytes(bytes),
            plain_len: bytes.len() as u32,
            bytes,
        };
        let mut remaining = vec![0; bitmap_bytes(3).unwrap()];
        bitmap_set(&mut remaining, 2);
        let encoded = encode_download_response(3, 2, &remaining, &[record]).unwrap();
        assert_eq!(&encoded[..4], DOWNLOAD_MAGIC);
        assert_eq!(u32::from_le_bytes(encoded[4..8].try_into().unwrap()), 3);
        assert_eq!(u32::from_le_bytes(encoded[8..12].try_into().unwrap()), 2);
        assert!(bitmap_get(&encoded[12..13], 2));
        let decoded_pack = decode_upload_pack(&encoded[13..], 256, encoded.len()).unwrap();
        assert_eq!(decoded_pack.records[0].bytes, bytes);
    }
}
