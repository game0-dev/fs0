use fs0_core::{
    DEFAULT_ZSTD_LEVEL, Fs0Error, HashId, MAX_FRAME_BODY_LEN, blake3_hash, decode_frame,
    encode_frame, zstd_compress, zstd_decompress,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestPayload {
    object_id: u64,
    client_id: u64,
    chunk_id: HashId,
    name: String,
}

#[test]
fn u64_ids_postcard_roundtrip() {
    let payload = TestPayload {
        object_id: 7,
        client_id: 9,
        chunk_id: HashId([42; 32]),
        name: "payload".to_owned(),
    };

    let encoded = postcard::to_allocvec(&payload).unwrap();
    let decoded: TestPayload = postcard::from_bytes(&encoded).unwrap();

    assert_eq!(decoded, payload);
}

#[test]
fn blake3_hash_returns_hash_id() {
    let hash_id = blake3_hash(b"hello fs0");
    let expected = *blake3::hash(b"hello fs0").as_bytes();

    assert_eq!(hash_id, HashId(expected));
}

#[test]
fn zstd_roundtrip() {
    let raw = b"orderbook:".repeat(4096);
    let compressed = zstd_compress(&raw, DEFAULT_ZSTD_LEVEL).unwrap();
    let decoded = zstd_decompress(&compressed, raw.len()).unwrap();

    assert_eq!(decoded, raw);
}

#[test]
fn frame_encode_decode_postcard_payload() {
    let payload = TestPayload {
        object_id: 11,
        client_id: 13,
        chunk_id: blake3_hash(b"/fs0/test"),
        name: "frame".to_owned(),
    };

    let encoded = encode_frame(&payload).unwrap();
    let decoded: TestPayload = decode_frame(&encoded).unwrap();

    assert_eq!(decoded, payload);
}

#[test]
fn frame_rejects_truncated_body() {
    let payload = TestPayload {
        object_id: 11,
        client_id: 13,
        chunk_id: blake3_hash(b"/fs0/test"),
        name: "frame".to_owned(),
    };

    let mut encoded = encode_frame(&payload).unwrap();
    encoded.pop();

    let err = decode_frame::<TestPayload>(&encoded).unwrap_err();
    assert_eq!(err, Fs0Error::InvalidFrame);
}

#[test]
fn frame_rejects_declared_body_over_limit() {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&((MAX_FRAME_BODY_LEN as u32) + 1).to_le_bytes());

    let err = decode_frame::<TestPayload>(&encoded).unwrap_err();
    assert_eq!(err, Fs0Error::FrameTooLarge);
}
