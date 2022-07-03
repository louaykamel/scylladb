//! This module implements the frame encoder.

use chrono::{
    Datelike,
    NaiveDate,
    NaiveDateTime,
    NaiveTime,
    Timelike,
};
use std::{
    collections::HashMap,
    io::Cursor,
    net::{
        IpAddr,
        Ipv4Addr,
        Ipv6Addr,
    },
};

/// The 16-byte body length.
pub const BE_16_BYTES_LEN: [u8; 4] = [0, 0, 0, 16];
/// The 8-byte body length.
pub const BE_8_BYTES_LEN: [u8; 4] = [0, 0, 0, 8];
/// The 4-byte body length.
pub const BE_4_BYTES_LEN: [u8; 4] = [0, 0, 0, 4];
/// The 2-byte body length.
pub const BE_2_BYTES_LEN: [u8; 4] = [0, 0, 0, 2];
/// The 1-byte body length.
pub const BE_1_BYTES_LEN: [u8; 4] = [0, 0, 0, 1];
/// The 0-byte body length.
pub const BE_0_BYTES_LEN: [u8; 4] = [0, 0, 0, 0];
/// The NULL body length.
pub const BE_NULL_BYTES_LEN: [u8; 4] = [255, 255, 255, 255]; // -1 length
/// The UNSET body length.
pub const BE_UNSET_BYTES_LEN: [u8; 4] = [255, 255, 255, 254]; // -2 length
/// The NULL value used to indicate the body length.
#[allow(unused)]
pub const NULL_VALUE: Null = Null;
/// The unset value used to indicate the body length.
pub const UNSET_VALUE: Unset = Unset;
/// The Null unit stucture.
pub struct Null;
/// The Unset unit stucture.
pub struct Unset;

/// An encode chain. Allows sequential encodes stored back-to-back in a buffer.
pub struct ColumnEncodeChain {
    buffer: Vec<u8>,
}

impl ColumnEncodeChain {
    /// Chain a new column
    pub fn chain<T: ColumnEncoder>(mut self, other: &T) -> Self {
        other.encode(&mut self.buffer);
        self
    }

    /// Complete the chain and return the buffer
    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }
}

/// The frame column encoder.
pub trait ColumnEncoder {
    /// Encode the column without its length
    fn encode_column(&self, buffer: &mut Vec<u8>);
    /// Encoder the column buffer, which starts with be length
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_0_BYTES_LEN);
        let p = buffer.len();
        self.encode_column(buffer);
        let byte_size = buffer.len() - p;
        buffer[p - 4..p].copy_from_slice(&i32::to_be_bytes(byte_size as i32));
    }
    /// Encode this value to a new buffer
    fn encode_new(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf);
        buf
    }

    /// Encode this value to a new buffer with a given capacity
    fn encode_with_capacity(&self, capacity: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity(capacity);
        self.encode(&mut buf);
        buf
    }

    /// Start an encoding chain
    fn chain_encode<T: ColumnEncoder>(&self, other: &T) -> ColumnEncodeChain
    where
        Self: Sized,
    {
        let buffer = self.encode_new();
        ColumnEncodeChain { buffer }.chain(other)
    }
}

impl<T: ColumnEncoder + ?Sized> ColumnEncoder for &T {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        T::encode_column(*self, buffer)
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        T::encode(*self, buffer)
    }
}

impl<T: ColumnEncoder + ?Sized> ColumnEncoder for Box<T> {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        T::encode_column(&*self, buffer)
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        T::encode(&*self, buffer)
    }
}

impl<T> ColumnEncoder for Option<T>
where
    T: ColumnEncoder,
{
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        match self {
            Some(value) => value.encode(buffer),
            None => ColumnEncoder::encode(&UNSET_VALUE, buffer),
        }
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        self.encode_column(buffer)
    }
}

impl ColumnEncoder for i64 {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i64::to_be_bytes(*self));
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_8_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for u64 {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&u64::to_be_bytes(*self));
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_8_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for f64 {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&f64::to_be_bytes(*self));
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_8_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for i32 {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i32::to_be_bytes(*self));
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_4_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for u32 {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&u32::to_be_bytes(*self));
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_4_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for f32 {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&f32::to_be_bytes(*self));
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_4_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for i16 {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i16::to_be_bytes(*self));
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_2_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for u16 {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&u16::to_be_bytes(*self));
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_2_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for i8 {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i8::to_be_bytes(*self));
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_1_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for u8 {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.push(*self);
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_1_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for bool {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.push(*self as u8);
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_1_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for String {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(self.bytes());
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        self.encode_column(buffer);
    }
}
impl ColumnEncoder for str {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(self.bytes());
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        self.encode_column(buffer);
    }
}
impl ColumnEncoder for &[u8] {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(*self);
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for IpAddr {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        match *self {
            IpAddr::V4(ip) => {
                buffer.extend(&ip.octets());
            }
            IpAddr::V6(ip) => {
                buffer.extend(&ip.octets());
            }
        }
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        match *self {
            IpAddr::V4(ip) => {
                buffer.extend(&BE_4_BYTES_LEN);
                buffer.extend(&ip.octets());
            }
            IpAddr::V6(ip) => {
                buffer.extend(&BE_16_BYTES_LEN);
                buffer.extend(&ip.octets());
            }
        }
    }
}

impl ColumnEncoder for Ipv4Addr {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&self.octets());
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_4_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for Ipv6Addr {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&self.octets());
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_16_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for Cursor<Vec<u8>> {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(self.get_ref());
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        let inner = self.get_ref();
        buffer.extend(&i32::to_be_bytes(inner.len() as i32));
        buffer.extend(inner);
    }
}

impl<E> ColumnEncoder for Vec<E>
where
    E: ColumnEncoder,
{
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        for e in self {
            e.encode(buffer);
        }
    }
}

impl<K, V, S: ::std::hash::BuildHasher> ColumnEncoder for HashMap<K, V, S>
where
    K: ColumnEncoder,
    V: ColumnEncoder,
{
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&i32::to_be_bytes(self.len() as i32));
        for (k, v) in self {
            k.encode(buffer);
            v.encode(buffer);
        }
    }
}

impl ColumnEncoder for Unset {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_UNSET_BYTES_LEN);
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_UNSET_BYTES_LEN);
    }
}

impl ColumnEncoder for Null {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_NULL_BYTES_LEN);
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_NULL_BYTES_LEN);
    }
}

impl ColumnEncoder for NaiveDate {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        let days = self.num_days_from_ce() as u32 - 719_163 + (1u32 << 31);
        buffer.extend(&u32::to_be_bytes(days))
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_4_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for NaiveTime {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        let nanos: u64 = self.hour() as u64 * 3_600_000_000_000
            + self.minute() as u64 * 60_000_000_000
            + self.second() as u64 * 1_000_000_000
            + self.nanosecond() as u64;
        nanos.encode_column(buffer);
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_8_BYTES_LEN);
        self.encode_column(buffer);
    }
}

impl ColumnEncoder for NaiveDateTime {
    fn encode_column(&self, buffer: &mut Vec<u8>) {
        let cql_timestamp = self.timestamp_millis() as u64;
        cql_timestamp.encode_column(buffer);
    }
    fn encode(&self, buffer: &mut Vec<u8>) {
        buffer.extend(&BE_8_BYTES_LEN);
        self.encode_column(buffer)
    }
}

/// An encode chain. Allows sequential encodes stored back-to-back in a buffer.
#[derive(Default, Debug)]
pub struct TokenEncodeChain {
    len: usize,
    buffer: Option<Vec<u8>>,
}

impl<T: ColumnEncoder + ?Sized> From<&T> for TokenEncodeChain {
    fn from(t: &T) -> Self {
        TokenEncodeChain {
            len: 1,
            buffer: Some(t.encode_new()[2..].into()),
        }
    }
}

impl TokenEncodeChain {
    /// Chain a new value
    pub fn chain<T: TokenEncoder + ?Sized>(mut self, other: &T) -> Self
    where
        Self: Sized,
    {
        self.append(other);
        self
    }

    /// Chain a new value
    pub fn append<T: TokenEncoder + ?Sized>(&mut self, other: &T) {
        let other = other.encode_token();
        if other.len > 0 {
            if let Some(other_buffer) = other.buffer {
                match self.buffer.as_mut() {
                    Some(buffer) => {
                        buffer.push(0);
                        buffer.extend_from_slice(&other_buffer[..]);
                        self.len += 1;
                    }
                    None => {
                        self.buffer = Some(other_buffer);
                        self.len = 1;
                    }
                }
            }
        }
    }

    /// Complete the chain and return the token
    pub fn finish(self) -> i64 {
        // TODO: Do we need a trailing byte?
        // self.buffer.push(0);
        match self.len {
            0 => rand::random(),
            1 => crate::cql::murmur3_cassandra_x64_128(&self.buffer.unwrap()[2..], 0).0,
            _ => crate::cql::murmur3_cassandra_x64_128(&self.buffer.unwrap(), 0).0,
        }
    }
}

/// Encoding functionality for tokens
pub trait TokenEncoder {
    /// Start an encode chain
    fn chain<E: TokenEncoder>(&self, other: &E) -> TokenEncodeChain
    where
        Self: Sized,
    {
        self.encode_token().chain(other)
    }

    /// Start an encode chain
    fn dyn_chain(&self, other: &dyn TokenEncoder) -> TokenEncodeChain {
        let mut chain = self.encode_token();
        chain.append(other);
        chain
    }
    /// Create a token encoding chain for this value
    fn encode_token(&self) -> TokenEncodeChain {
        TokenEncodeChain::default()
    }
    /// Encode a single token
    fn token(&self) -> i64 {
        self.encode_token().finish()
    }
}

macro_rules! impl_token_encoder {
    ($($t:ty),*) => {
        $(
            impl TokenEncoder for $t {
                fn encode_token(&self) -> TokenEncodeChain {
                    self.into()
                }
            }
        )*
    };
    (@tuple ($($t:tt),*)) => {
        impl<$($t: TokenEncoder),*> TokenEncoder for ($($t,)*) {
            fn encode_token(&self) -> TokenEncodeChain {
                #[allow(non_snake_case)]
                let ($($t,)*) = self;
                let mut token_chain = TokenEncodeChain::default();
                $(
                    token_chain.append($t);
                )*
                token_chain
            }
        }
    };
}

impl_token_encoder!(
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    bool,
    String,
    str,
    Cursor<Vec<u8>>,
    Unset,
    Null
);

impl_token_encoder!(@tuple (T));
impl_token_encoder!(@tuple (T,TT));
impl_token_encoder!(@tuple (T, TT, TTT));
impl_token_encoder!(@tuple (T, TT, TTT, TTTT));

impl TokenEncoder for () {
    fn encode_token(&self) -> TokenEncodeChain {
        TokenEncodeChain::default()
    }
}

impl<T: TokenEncoder + ?Sized> TokenEncoder for &T {
    fn encode_token(&self) -> TokenEncodeChain {
        T::encode_token(*self)
    }
}

impl<T: ColumnEncoder> TokenEncoder for Option<T> {
    fn encode_token(&self) -> TokenEncodeChain {
        self.into()
    }
}

impl<T: ColumnEncoder> TokenEncoder for Vec<T> {
    fn encode_token(&self) -> TokenEncodeChain {
        self.into()
    }
}

impl<K: ColumnEncoder, V: ColumnEncoder> TokenEncoder for HashMap<K, V> {
    fn encode_token(&self) -> TokenEncodeChain {
        self.into()
    }
}

impl<T: TokenEncoder> TokenEncoder for [T] {
    fn encode_token(&self) -> TokenEncodeChain {
        let mut token_chain = TokenEncodeChain::default();
        for v in self.iter() {
            token_chain.append(v);
        }
        token_chain
    }
}
