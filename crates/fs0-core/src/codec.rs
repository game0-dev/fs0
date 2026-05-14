use crate::{Fs0Error, Result};
use serde::{Serialize, de::DeserializeOwned};

pub const FRAME_LEN_BYTES: usize = 4;
pub const MAX_FRAME_BODY_LEN: usize = 1024 * 1024;

pub fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let body = postcard::to_allocvec(value)?;
    encode_frame_body(&body)
}

pub fn encode_frame_body(body: &[u8]) -> Result<Vec<u8>> {
    if body.len() > MAX_FRAME_BODY_LEN {
        return Err(Fs0Error::FrameTooLarge {
            actual: body.len(),
            max: MAX_FRAME_BODY_LEN,
        });
    }

    let mut out = Vec::with_capacity(FRAME_LEN_BYTES + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

pub fn decode_frame<T: DeserializeOwned>(frame: &[u8]) -> Result<T> {
    let body = decode_frame_body(frame)?;
    Ok(postcard::from_bytes(body)?)
}

pub fn decode_frame_body(frame: &[u8]) -> Result<&[u8]> {
    if frame.len() < FRAME_LEN_BYTES {
        return Err(Fs0Error::InvalidFrame(format!(
            "frame too short: {} bytes",
            frame.len()
        )));
    }

    let body_len = u32::from_le_bytes(
        frame[..FRAME_LEN_BYTES]
            .try_into()
            .expect("slice length is fixed"),
    ) as usize;

    if body_len > MAX_FRAME_BODY_LEN {
        return Err(Fs0Error::FrameTooLarge {
            actual: body_len,
            max: MAX_FRAME_BODY_LEN,
        });
    }

    let expected_len = FRAME_LEN_BYTES
        .checked_add(body_len)
        .ok_or_else(|| Fs0Error::InvalidFrame("frame length overflow".to_owned()))?;

    if frame.len() != expected_len {
        return Err(Fs0Error::InvalidFrame(format!(
            "frame length mismatch: header={}, actual={}",
            expected_len,
            frame.len()
        )));
    }

    Ok(&frame[FRAME_LEN_BYTES..])
}
