use crate::Fs0Error;

pub fn zstd_compress(input: &[u8], level: i32) -> std::result::Result<Vec<u8>, Fs0Error> {
    zstd::bulk::compress(input, level).map_err(|err| Fs0Error::Zstd {
        message: err.to_string(),
    })
}

pub fn zstd_decompress(
    input: &[u8],
    max_output_size: usize,
) -> std::result::Result<Vec<u8>, Fs0Error> {
    zstd::bulk::decompress(input, max_output_size).map_err(|err| Fs0Error::Zstd {
        message: err.to_string(),
    })
}
