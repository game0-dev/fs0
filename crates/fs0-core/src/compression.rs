use crate::{Fs0Error, Result};

pub fn zstd_compress(input: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::bulk::compress(input, level).map_err(|err| Fs0Error::Zstd(err.to_string()))
}

pub fn zstd_decompress(input: &[u8], max_output_size: usize) -> Result<Vec<u8>> {
    zstd::bulk::decompress(input, max_output_size).map_err(|err| Fs0Error::Zstd(err.to_string()))
}
