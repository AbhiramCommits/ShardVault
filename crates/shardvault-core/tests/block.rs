use shardvault_core::block::{self, Block, BlockError};

#[test]
fn crc32c_known_vector() {
    assert_eq!(block::crc32c(b"123456789"), 0xE306_9283);
}

#[test]
fn crc32c_empty_input() {
    assert_eq!(block::crc32c(b""), 0);
}

#[test]
fn encode_decode_roundtrip() {
    let payload: Vec<u8> = (0..1000usize)
        .map(|i| ((i * 31 + 7) & 0xff) as u8)
        .collect();
    let lsn = 0x1122_3344_5566_7788;
    let flags = 0x5A;
    let encoded = block::encode(lsn, &payload, flags).unwrap();
    assert_eq!(encoded.len(), block::BLOCK_SIZE);
    let decoded = block::decode(&encoded).unwrap();
    assert_eq!(
        decoded,
        Block {
            lsn,
            flags,
            payload,
        }
    );
}

#[test]
fn max_payload_roundtrip() {
    let payload: Vec<u8> = (0..block::PAYLOAD_MAX).map(|i| (i & 0xff) as u8).collect();
    let encoded = block::encode(1, &payload, 0xFF).unwrap();
    let decoded = block::decode(&encoded).unwrap();
    assert_eq!(decoded.payload, payload);
}

#[test]
fn empty_payload_roundtrip() {
    let encoded = block::encode(7, &[], 0).unwrap();
    let decoded = block::decode(&encoded).unwrap();
    assert_eq!(
        decoded,
        Block {
            lsn: 7,
            flags: 0,
            payload: Vec::new(),
        }
    );
}

#[test]
fn payload_corruption_returns_crc_error() {
    let payload = vec![0xAB; 512];
    let mut encoded = block::encode(42, &payload, 0).unwrap();
    encoded[block::HEADER_SIZE + 128] ^= 0x01;
    assert_eq!(block::decode(&encoded), Err(BlockError::Crc));
}

#[test]
fn header_corruption_returns_crc_error() {
    let payload = vec![0xCD; 64];
    let mut encoded = block::encode(42, &payload, 0).unwrap();
    encoded[3] ^= 0x80;
    assert_eq!(block::decode(&encoded), Err(BlockError::Crc));
}

#[test]
fn encode_rejects_oversized_payload() {
    let payload = vec![0u8; block::PAYLOAD_MAX + 1];
    assert_eq!(block::encode(1, &payload, 0), Err(BlockError::Len));
}

#[test]
fn decode_rejects_oversized_len_field() {
    let mut raw = [0u8; block::BLOCK_SIZE];
    let bad_len = (block::PAYLOAD_MAX + 1) as u32;
    raw[8..12].copy_from_slice(&bad_len.to_le_bytes());
    assert_eq!(block::decode(&raw), Err(BlockError::Len));
}
