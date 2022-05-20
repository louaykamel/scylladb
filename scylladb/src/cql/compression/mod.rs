//! This crates implements the uncompressed, LZ4, and snappy compression methods for Cassandra.

use super::frame::header::COMPRESSION;
use std::convert::TryInto;

/// This compression thread provides the buffer compression/decompression methods for uncompressed/Lz4/snappy.
pub trait Compression: Sync {
    /// The compression type string, `lz4` or `snappy` or None.
    fn option(&self) -> Option<&'static str>;
    /// Decompress buffer only if compression flag is set
    fn decompress(&self, compressed: Vec<u8>) -> anyhow::Result<Vec<u8>>;
    /// Compression the buffer according to the compression type (Lz4 for snappy).
    fn compress(&self, uncompressed: Vec<u8>) -> anyhow::Result<Vec<u8>>;
}

/// LZ4 compression type.
pub const LZ4: Lz4 = Lz4;
/// LZ4 unit structure which implements compression trait.
pub struct Lz4;

impl Compression for Lz4 {
    fn option(&self) -> Option<&'static str> {
        Some("lz4")
    }
    fn decompress(&self, mut buffer: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        // check if buffer is compressed
        anyhow::ensure!(buffer.len() >= 9, "Buffer is too small!");
        Ok(if buffer[1] & COMPRESSION == COMPRESSION {
            let compressed_body_length = i32::from_be_bytes(buffer[5..9].try_into()?) as usize;
            // Decompress the body by lz4
            match lz4::block::decompress(&buffer[9..(9 + compressed_body_length)], None) {
                Ok(decompressed_buffer) => {
                    // reduce the frame to be a header only without length
                    buffer.truncate(5);
                    // make the body length to be the decompressed body length
                    buffer.extend(&i32::to_be_bytes(decompressed_buffer.len() as i32));
                    // Extend the decompressed body
                    buffer.extend(&decompressed_buffer);
                    buffer
                }
                Err(_) => {
                    // return only the header as this is mostly a result of header only
                    buffer.truncate(9);
                    buffer
                }
            }
        } else {
            // return the buffer as it is
            buffer
        })
    }
    fn compress(&self, mut buffer: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(buffer.len() >= 9, "Buffer is too small!");
        // Compress the body
        let compressed_buffer: Vec<u8> = lz4::block::compress(&buffer[9..], None, true)?;
        // Truncate the buffer to be header without length
        buffer.truncate(5);
        // make the body length to be the compressed body length
        buffer.extend(&i32::to_be_bytes(compressed_buffer.len() as i32));
        // Extend the compressed body
        buffer.extend(&compressed_buffer);
        Ok(buffer)
    }
}

/// SNAPPY compression type.
pub const SNAPPY: Snappy = Snappy;
/// Snappy unit structure which implements compression trait.
pub struct Snappy;
impl Compression for Snappy {
    fn option(&self) -> Option<&'static str> {
        Some("snappy")
    }
    fn decompress(&self, mut buffer: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(buffer.len() >= 9, "Buffer is too small!");
        Ok(if buffer[1] & COMPRESSION == COMPRESSION {
            let compressed_body_length = i32::from_be_bytes(buffer[5..9].try_into()?) as usize;
            // Decompress the body by snappy
            let decompressed_buffer: Vec<u8> =
                snap::raw::Decoder::new().decompress_vec(&buffer[9..(9 + compressed_body_length)])?;
            // reduce the frame to be a header only without length
            buffer.truncate(5);
            // make the body length to be the decompressed body length
            buffer.extend(&i32::to_be_bytes(decompressed_buffer.len() as i32));
            // Extend the decompressed body
            buffer.extend(&decompressed_buffer);
            buffer
        } else {
            buffer
        })
    }
    fn compress(&self, mut buffer: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(buffer.len() >= 9, "Buffer is too small!");
        // Compress the body
        let compressed_buffer: Vec<u8> = snap::raw::Encoder::new().compress_vec(&buffer[9..])?;
        // Truncate the buffer to be header only without length
        buffer.truncate(5);
        // Update the body length to be the compressed body length
        buffer.extend(&i32::to_be_bytes(compressed_buffer.len() as i32));
        // Extend the compressed body
        buffer.extend(&compressed_buffer);
        Ok(buffer)
    }
}

/// Uncompresed type.
pub const UNCOMPRESSED: Uncompressed = Uncompressed;
/// Uncompressed unit structure which implements compression trait.
pub struct Uncompressed;
impl Compression for Uncompressed {
    fn option(&self) -> Option<&'static str> {
        None
    }
    fn decompress(&self, buffer: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(buffer.len() >= 9, "Buffer is too small!");
        Ok(buffer)
    }
    fn compress(&self, mut buffer: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(buffer.len() >= 9, "Buffer is too small!");
        // no need to compress, only adjust the body length
        let body_length = i32::to_be_bytes((buffer.len() as i32) - 9);
        buffer[5..9].copy_from_slice(&body_length);
        Ok(buffer)
    }
}
/// `MY_COMPRESSION` is used to enable user defines a global compression structure.
pub static mut MY_COMPRESSION: MyCompression = MyCompression(&UNCOMPRESSED);
/// `MY_COMPRESSION_FLAG` is used to indicate whether the compression is applied to the buffer.
pub static mut MY_COMPRESSION_FLAG: u8 = 0;
#[derive(Copy, Clone)]
/// `MyCompression` structure provides a higher-level wrapper to let the user use a compresion method, i.e.,
/// ````LZ4`, `SNAPPY`, or `UNCOMPRESSED`.
pub struct MyCompression(pub &'static dyn Compression);

impl MyCompression {
    /// Set the global compression syte as `LZ4`.
    #[allow(unused)]
    pub fn set_lz4() {
        unsafe {
            MY_COMPRESSION = MyCompression(&LZ4);
            MY_COMPRESSION_FLAG = 1;
        }
    }
    /// Set the global compression syte as `SNAPPY`.
    #[allow(unused)]
    pub fn set_snappy() {
        unsafe {
            MY_COMPRESSION = MyCompression(&SNAPPY);
            MY_COMPRESSION_FLAG = 1;
        }
    }
    /// Set the global compression syte as `UNCOMPRESSED`.
    #[allow(unused)]
    pub fn set_uncompressed() {
        unsafe {
            MY_COMPRESSION = MyCompression(&UNCOMPRESSED);
            MY_COMPRESSION_FLAG = 0;
        }
    }
    /// Get the global structure, `MY_COMPRESSION`.
    pub fn get() -> impl Compression {
        unsafe { MY_COMPRESSION }
    }
    /// Get the global structure, `MY_COMPRESSION_FLAG`.
    pub fn flag() -> u8 {
        unsafe { MY_COMPRESSION_FLAG }
    }
    /// Get the `Option` of compression method type, i.e., `lz4`, `snappy`, or None.
    pub fn option() -> Option<&'static str> {
        unsafe { MY_COMPRESSION }.option()
    }
}

impl Compression for MyCompression {
    fn option(&self) -> Option<&'static str> {
        self.0.option()
    }
    fn decompress(&self, buffer: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        // get the inner compression and then decompress
        self.0.decompress(buffer)
    }
    fn compress(&self, buffer: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        // get the inner compression and then compress
        self.0.compress(buffer)
    }
}
